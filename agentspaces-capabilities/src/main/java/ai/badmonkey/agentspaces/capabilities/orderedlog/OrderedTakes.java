/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.space.replicated.TakeClaim;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import java.util.Objects;
import java.util.Optional;

/**
 * The {@code ORDERED} strategy (spec §7.4) as a coordinator over the
 * {@link RaftLog}: signed take claims are submitted as log commands, the log's
 * commit order arbitrates identically on every member, and the first valid
 * committed claim per entry generation wins. Takes are exactly-once at the price
 * of quorum liveness: a partitioned minority cannot take at all (spec P1), which
 * is precisely the trade the strategy sells.
 *
 * <p>Use this coordinator as the take API for ORDERED spaces; {@code Space.take}
 * stays reserved for LEASE_RACE and AUCTION. Reads, writes, and completes go
 * through the space as usual.
 */
public final class OrderedTakes {

    private record ClaimCommand(EntryId entryId, TakeClaim claim,
                                byte[] holderKey, byte[] signature,
                                // v0.1.13: the holder agent's certificate when its
                                // own key signed the claim; omitted otherwise.
                                @com.fasterxml.jackson.annotation.JsonInclude(
                                        com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                                ai.badmonkey.agentspaces.api.security.AgentCertificate holderCertificate) {
    }

    private final RaftLog raft;
    private final ReplicatedSpace space;
    private final PeerIdentity identity;
    private final AgentId issuer;
    /** The holder's own identity when it signs as itself (v0.1.13); null signs with the peer key. */
    private final ai.badmonkey.agentspaces.api.spi.AgentIdentity holderIdentity;
    private final HybridLogicalClock hlc;
    private final CborCodec codec;
    private final InstantSource clock;

    /**
     * Creates the coordinator. Register the returned instance's
     * {@link #onCommand(byte[], long)} as (part of) the Raft applied listener on
     * every member, including members that never take.
     *
     * @param raft      this member's Raft log
     * @param space     the ORDERED space
     * @param identity  the local peer identity
     * @param agentName the local agent name takes are attributed to
     * @param codec     the CBOR codec
     * @param clock     the time source
     */
    public OrderedTakes(RaftLog raft, ReplicatedSpace space, PeerIdentity identity,
                        String agentName, CborCodec codec, InstantSource clock) {
        this.raft = Objects.requireNonNull(raft, "raft");
        this.space = Objects.requireNonNull(space, "space");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.issuer = identity.agent(Objects.requireNonNull(agentName, "agentName"));
        this.holderIdentity = null;
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.hlc = new HybridLogicalClock(clock, issuer.encoded());
    }

    /**
     * A coordinator whose claims are signed by the holder agent's own identity
     * (SPEC §11a.4, v0.1.13): a subordinate holder signs each claim command with
     * its key and attaches the certificate covering the claim's stamp, and the
     * claim it adopts completes as that agent. The space's writer must be the
     * same identity, so its completions are the holder's.
     *
     * @param raft     the ordered log
     * @param space    the coordinated space
     * @param identity this peer's identity
     * @param holder   the holder agent's identity
     * @param codec    the CBOR codec
     * @param clock    the clock
     */
    public OrderedTakes(RaftLog raft, ReplicatedSpace space, PeerIdentity identity,
                        ai.badmonkey.agentspaces.api.spi.AgentIdentity holder, CborCodec codec,
                        InstantSource clock) {
        this.raft = Objects.requireNonNull(raft, "raft");
        this.space = Objects.requireNonNull(space, "space");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.holderIdentity = Objects.requireNonNull(holder, "holder");
        if (!holder.id().peer().equals(identity.peerId())) {
            throw new IllegalArgumentException("holder " + holder.id().encoded()
                    + " is not an agent of " + identity.peerId().display());
        }
        this.issuer = holder.id();
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.hlc = new HybridLogicalClock(clock, issuer.encoded());
    }

    /**
     * Applies one committed log command. This must be a pure function of
     * committed state so that every member reaches the identical decision no
     * matter when it applies the entry (apply lag, a follower catching up in a
     * burst after an election, clock skew). It therefore reads no wall clock:
     * the signature is verified, and the claim is accepted iff its epoch is
     * exactly one past the epoch currently committed for the entry. Because a
     * claim's epoch is {@code observedGeneration + 1}, this accepts the first
     * claim of each generation and rejects every duplicate or stale one from
     * the committed state alone.
     *
     * <p>An earlier version judged the prior claim's expiry against
     * {@code clock.instant()} here; two members applying the same entry at
     * different real times could then disagree on whether a lease had lapsed
     * and grant the same entry to two agents. Lease-expiry-driven re-take is
     * now decided entirely on the submission side (see {@link #take}, which
     * only submits a fresh-generation claim once it observes the prior claim as
     * expired), exactly as LEASE_RACE decides it; the applier only enforces
     * per-generation uniqueness, which is what makes ORDERED takes exactly-once.
     *
     * @param command  the committed command bytes
     * @param logIndex the command's log index
     */
    /**
     * Builds a log member and its take coordinator together, wiring the log's
     * apply callback to the coordinator, so a consumer never writes the
     * {@code AtomicReference} cycle by hand. Register {@link #raft()} on the
     * peer's {@code CapabilityRuntime} so the peer tick drives elections and the
     * leader lease is advertised.
     *
     * @param pipes     the group's capability pipes
     * @param codec     the codec
     * @param identity  this peer's identity
     * @param agentName the agent the coordinator takes as
     * @param members   every log member's PeerId, the same list on every member
     * @param clock     the time source
     * @param seed      a per-member election-timer seed
     * @param space     the space takes are coordinated over
     * @return the coordinator, its {@link #raft()} constructed and wired
     */
    public static OrderedTakes over(CapabilityPipes pipes, CborCodec codec, PeerIdentity identity,
                                    String agentName, List<PeerId> members, InstantSource clock,
                                    long seed, ReplicatedSpace space) {
        java.util.concurrent.atomic.AtomicReference<OrderedTakes> ref =
                new java.util.concurrent.atomic.AtomicReference<>();
        RaftLog raft = new RaftLog(pipes, codec, identity.peerId(), members, clock, seed,
                (command, index) -> {
                    OrderedTakes ordered = ref.get();
                    if (ordered != null) {
                        ordered.onCommand(command, index);
                    }
                });
        OrderedTakes ordered = new OrderedTakes(raft, space, identity, agentName, codec, clock);
        ref.set(ordered);
        return ordered;
    }

    /** The log this coordinator submits to. */
    /**
     * As {@link #over(CapabilityPipes, CborCodec, PeerIdentity, String, List, InstantSource, long,
     * ReplicatedSpace)}, with claims signed by the holder agent's own identity (v0.1.13).
     */
    public static OrderedTakes over(CapabilityPipes pipes, CborCodec codec, PeerIdentity identity,
                                    ai.badmonkey.agentspaces.api.spi.AgentIdentity holder,
                                    List<PeerId> members, InstantSource clock, long seed,
                                    ReplicatedSpace space) {
        java.util.concurrent.atomic.AtomicReference<OrderedTakes> ref =
                new java.util.concurrent.atomic.AtomicReference<>();
        RaftLog raft = new RaftLog(pipes, codec, identity.peerId(), members, clock, seed,
                (command, index) -> {
                    OrderedTakes ordered = ref.get();
                    if (ordered != null) {
                        ordered.onCommand(command, index);
                    }
                });
        OrderedTakes ordered = new OrderedTakes(raft, space, identity, holder, codec, clock);
        ref.set(ordered);
        return ordered;
    }

    public RaftLog raft() {
        return raft;
    }

    /** The space takes are coordinated over. */
    public ReplicatedSpace space() {
        return space;
    }

    /** The agent every claim this coordinator submits names as holder. */
    public AgentId holder() {
        return issuer;
    }

    /** Whether claims are signed by the holder agent's own identity rather than the peer key. */
    public boolean signsAsHolder() {
        return holderIdentity != null;
    }

    public void onCommand(byte[] command, long logIndex) {
        ClaimCommand parsed;
        try {
            parsed = codec.fromBytes(command, ClaimCommand.class);
        } catch (RuntimeException e) {
            return;
        }
        if (parsed == null || parsed.entryId() == null || parsed.claim() == null
                || parsed.holderKey() == null || parsed.signature() == null
                || parsed.holderKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !PeerId.fromPublicKey(parsed.holderKey())
                        .equals(parsed.claim().holder().peer())
                // ASF-002: the signed claim is bound to its entry, so a claim
                // signed for one entry cannot be committed against another.
                || !parsed.claim().entryId().equals(parsed.entryId())) {
            return;
        }
        long committedEpoch = space.currentClaim(parsed.entryId())
                .map(TakeClaim::epoch).orElse(0L);
        if (parsed.claim().epoch() != committedEpoch + 1) {
            return; // duplicate, stale, or conflicting generation: the log already decided
        }
        // Install the claim exactly as the holder signed it, attestation and all
        // (QA4 A4-5). Re-stamping it with the log index used to buy arbitration
        // priority at the cost of the holder's signature, and a claim signed by
        // the wrong key cannot authenticate the holder's completion anywhere
        // else. Commit order is carried by the epoch — the check above admits
        // exactly one claim per epoch — and the space treats a log-decided
        // epoch as authoritative against gossip, so nothing is lost.
        //
        // The signature is verified by the space's own claim verifier (the
        // two-key rule for an agent-signed claim, SPEC §4.2), so the log and
        // gossip judge a claim identically; an inauthentic command is dropped.
        try {
            space.applyAuthorizedClaim(parsed.entryId(), parsed.claim(),
                    parsed.holderKey(), parsed.signature(), parsed.holderCertificate());
        } catch (IllegalArgumentException inauthentic) {
            // dropped: not authentic, or bound to another entry
        }
    }

    /**
     * Takes one matching entry with exactly-once semantics: submit a signed claim
     * through the log, wait for commitment, and adopt the entry when this agent's
     * claim was the first committed for the entry's current generation.
     *
     * @param template  the template to match
     * @param takeLease the TAKE lease
     * @param timeout   how long to keep trying (spans leader elections)
     * @param <T>       the entry type
     * @return the taken entry, or empty when out-arbitrated or timed out
     */
    public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease,
                                            Duration timeout) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(takeLease, "takeLease");
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            long now = clock.instant().toEpochMilli();
            List<T> candidates = space.readAll(template, 16);
            for (T candidate : candidates) {
                Optional<EntryId> entryId = space.entryIdOf(candidate);
                if (entryId.isEmpty()) {
                    continue;
                }
                Optional<TakeClaim> current = space.currentClaim(entryId.get());
                if (current.isPresent() && !current.get().expired(now)) {
                    continue;
                }
                long epoch = current.map(c -> c.epoch() + 1).orElse(1L);
                TakeClaim claim = new TakeClaim(entryId.get(), space.id(), epoch,
                        hlc.now(), issuer, 0.0,
                        now + takeLease.duration().toMillis());
                byte[] claimBytes = codec.toBytes(claim);
                byte[] command = codec.toBytes(signedCommand(entryId.get(), claim, claimBytes));
                raft.submit(command);
                // Wait for the log to decide this generation, but only for a
                // bounded attempt window: a command lost to a leader election is
                // resubmitted on the next scan rather than waited on forever.
                long attemptDeadline = Math.min(deadline,
                        System.nanoTime() + Duration.ofSeconds(3).toNanos());
                while (System.nanoTime() < attemptDeadline) {
                    Optional<TakeClaim> decided = space.currentClaim(entryId.get());
                    if (decided.isPresent() && decided.get().epoch() >= epoch) {
                        if (decided.get().holder().equals(issuer)
                                && decided.get().epoch() == epoch) {
                            Optional<TakenEntry<T>> adopted = holderIdentity == null
                                    ? space.adoptClaim(template, entryId.get())
                                    : space.adoptClaim(template, entryId.get(), holderIdentity);
                            if (adopted.isPresent()) {
                                return adopted;
                            }
                        }
                        break; // someone else won this generation; try another entry
                    }
                    sleep(Duration.ofMillis(20));
                }
                break; // re-scan candidates (and resubmit when still undecided)
            }
            if (System.nanoTime() >= deadline) {
                return Optional.empty();
            }
            sleep(Duration.ofMillis(20));
        }
    }

    /** The claim command, signed by the holder: its own key and certificate, or the peer key. */
    private ClaimCommand signedCommand(EntryId entryId, TakeClaim claim, byte[] claimBytes) {
        if (holderIdentity == null || !holderIdentity.isSubordinate()) {
            byte[] signature = holderIdentity == null ? identity.sign(claimBytes)
                    : holderIdentity.sign(claimBytes);
            return new ClaimCommand(entryId, claim, identity.rawPublicKey(), signature, null);
        }
        java.time.Instant signingTime = java.time.Instant.ofEpochMilli(claim.stamp().physical());
        ai.badmonkey.agentspaces.api.security.AgentCertificate certificate =
                holderIdentity.certificateCovering(signingTime).orElseThrow(() ->
                        new IllegalStateException("holder " + issuer.encoded()
                                + " holds no certificate covering " + signingTime));
        return new ClaimCommand(entryId, claim, identity.rawPublicKey(),
                holderIdentity.sign(claimBytes), certificate);
    }

    private static void sleep(Duration duration) {
        if (duration.isZero()) {
            return;
        }
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
