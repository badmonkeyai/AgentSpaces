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
package ai.badmonkey.agentspaces.peering.membership;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Leased membership for one group (spec §5.2): the membership view is fed by
 * leased PeerAdvertisements arriving over gossip and by direct liveness contact,
 * and a member that neither gossips nor answers within its TTL is dropped without
 * ceremony (spec P2). A SWIM-style probe cycle runs on {@link #tick}: ping a
 * random member, and on timeout ask {@code indirectPeers} others to probe the
 * target before dropping it.
 *
 * <p>Deterministic by construction: time comes from the injected clock, randomness
 * from the injected {@link Random}, and probing advances only on {@code tick}.
 */
public final class GroupMembership implements PeerSampler {

    /**
     * Membership tuning.
     *
     * @param memberTtl     how long a silent member stays in the view
     * @param pingTimeout   how long to wait for an ACK before indirect probing
     * @param indirectPeers how many peers to ask in the indirect round
     */
    public record Config(Duration memberTtl, Duration pingTimeout, int indirectPeers, Duration probeInterval) {

        /** The SWIM protocol period: one probe per member per interval (memberlist's default, 1 s). */
        public static final Duration DEFAULT_PROBE_INTERVAL = Duration.ofSeconds(1);

        public Config {
            if (probeInterval == null) {
                probeInterval = DEFAULT_PROBE_INTERVAL;
            }
        }

        /** The three-argument form, probing once per second. */
        public Config(Duration memberTtl, Duration pingTimeout, int indirectPeers) {
            this(memberTtl, pingTimeout, indirectPeers, DEFAULT_PROBE_INTERVAL);
        }

        /** Returns defaults: TTL 30s, ping timeout 2s, 2 indirect peers, one probe per second. */
        public static Config defaults() {
            return new Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2, DEFAULT_PROBE_INTERVAL);
        }
    }

    /** One member's view state. */
    public static final class Member {
        private final PeerId id;
        private volatile List<PeerAdvertisement.Endpoint> endpoints;
        private volatile java.util.Set<PeerAdvertisement.PeerRole> roles = java.util.Set.of();
        private volatile long lastHeardMillis;
        private volatile boolean suspect;

        private Member(PeerId id, List<PeerAdvertisement.Endpoint> endpoints, long lastHeardMillis) {
            this.id = id;
            this.endpoints = endpoints;
            this.lastHeardMillis = lastHeardMillis;
        }

        /** Returns the topology roles the member advertises (spec §5.4). */
        public java.util.Set<PeerAdvertisement.PeerRole> roles() {
            return roles;
        }

        /** Returns the member's PeerID. */
        public PeerId id() {
            return id;
        }

        /** Returns the member's advertised endpoints, priority-ordered. */
        public List<PeerAdvertisement.Endpoint> endpoints() {
            return endpoints;
        }

        /** Returns whether the member is currently suspected. */
        public boolean suspect() {
            return suspect;
        }
    }

    /** Callback used by the probe cycle to send PING and PING_REQ frames. */
    public interface Prober {

        /**
         * Sends a PING to a member.
         *
         * @param target the member to probe
         * @param nonce  the correlation nonce
         */
        void ping(PeerId target, long nonce);

        /**
         * Asks a relay to probe a target on this peer's behalf.
         *
         * @param relay  the peer asked to probe
         * @param target the member to probe
         * @param nonce  the correlation nonce
         */
        void pingReq(PeerId relay, PeerId target, long nonce);
    }

    private record Pending(PeerId target, long deadlineMillis, boolean indirectStage,
                           java.util.Set<PeerId> relays) {
    }

    private final PeerId self;
    private volatile java.util.function.Predicate<PeerId> refused = peer -> false;
    private final InstantSource clock;
    private final Random random;
    private final Config config;
    private final Map<PeerId, Member> members = new ConcurrentHashMap<>();
    /** Peers removed by {@link #evict} rather than by silence, until the owner consumes the mark. */
    private final java.util.Set<PeerId> ejected = ConcurrentHashMap.newKeySet();

    /**
     * Whether a peer left this view by an authoritative {@link #evict} rather than by lapsing,
     * consuming the mark: the owner retries lost members' endpoints for a while, and an ejected
     * peer is not lost but refused, so it is not courted (v0.1.17).
     *
     * @param peer the peer that left the view
     * @return true once, if it was ejected
     */
    public boolean consumeEjected(PeerId peer) {
        return ejected.remove(peer);
    }
    private final Map<Long, Pending> pendingProbes = new ConcurrentHashMap<>();
    /** Probe nonces are unguessable (ASF-011): a sequential counter let any
     * member cancel or satisfy probes it never saw by guessing the next value. */
    private final java.security.SecureRandom nonceRandom = new java.security.SecureRandom();

    /**
     * Creates the membership view.
     *
     * @param self   the local peer (never a member of its own view)
     * @param clock  the time source
     * @param random the randomness source (seed it in tests)
     * @param config membership tuning
     */
    public GroupMembership(PeerId self, InstantSource clock, Random random, Config config) {
        this.self = Objects.requireNonNull(self, "self");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Sets the peers this view must never admit, consulted on every
     * advertisement: the group's revocations (ASF-047). Late-bound because the
     * view is built before the group's revocation registry.
     *
     * @param refused the peers to refuse
     */
    public void refuse(java.util.function.Predicate<PeerId> refused) {
        this.refused = Objects.requireNonNull(refused, "refused");
    }

    /**
     * Feeds a verified PeerAdvertisement into the view, refreshing endpoints and
     * liveness.
     *
     * @param ad the advertisement (already signature-verified by the caller)
     */
    public void onPeerAdvertisement(PeerAdvertisement ad) {
        Objects.requireNonNull(ad, "ad");
        if (ad.issuer().equals(self) || refused.test(ad.issuer())) {
            return; // ourselves, or a peer the group has revoked (ASF-047)
        }
        long now = nowMillis();
        members.compute(ad.issuer(), (id, existing) -> {
            if (existing == null) {
                activeSinceProbe = true; // a newcomer: probe on the base interval again
                quietCycles = 0;
            }
            Member member = existing == null
                    ? new Member(id, ad.endpoints(), now) : existing;
            member.endpoints = ad.endpoints();
            member.roles = ad.roles();
            member.lastHeardMillis = now;
            member.suspect = false;
            return member;
        });
    }

    /**
     * Removes a peer from the view immediately (a revocation's enforcement, or
     * any other authoritative eject). Idempotent.
     *
     * @param peer the peer to evict
     */
    public void evict(PeerId peer) {
        ejected.add(Objects.requireNonNull(peer, "peer"));
        members.remove(peer);
    }

    /**
     * Records direct contact from a peer (any verified frame counts as liveness).
     *
     * @param peer the peer heard from
     */
    /** This group's failure-detection parameters. */
    public Config config() {
        return config;
    }

    public void recordHeard(PeerId peer) {
        Member member = members.get(peer);
        if (member != null) {
            member.lastHeardMillis = nowMillis();
            member.suspect = false;
        }
    }

    /**
     * Handles an ACK correlated to an outstanding probe. The liveness credit
     * always goes to the probe's own recorded target — never to any identity
     * the wire supplies — and the ACK counts only when its authenticated
     * sender is that target (a direct probe) or a relay this node actually
     * asked (an indirect probe). A sprayed or forged ACK neither cancels the
     * probe nor marks anyone alive (ASF-011).
     *
     * @param nonce the echoed nonce
     * @param from  the frame's authenticated sender
     */
    public void onAck(long nonce, PeerId from) {
        Pending pending = pendingProbes.get(nonce);
        if (pending == null) {
            return;
        }
        boolean fromTarget = from.equals(pending.target());
        boolean fromAskedRelay = pending.relays().contains(from);
        if (!fromTarget && !fromAskedRelay) {
            return; // not who we asked: the probe stands
        }
        pendingProbes.remove(nonce);
        recordHeard(pending.target());
    }

    /**
     * Runs one membership round: expire silent members, escalate or drop timed-out
     * probes, and probe one random member.
     *
     * @param prober the frame sender
     */
    private long lastProbeMillis = Long.MIN_VALUE;
    /** How far the probe interval has stretched on a quiet fleet: the interval is the base times two to this. */
    private volatile int quietCycles;
    /** Whether anything moved (gossip activity, a member admitted) since the last probe. */
    private volatile boolean activeSinceProbe = true;
    /** The probe interval stretches to the base times two to this at most (v0.1.17). */
    public static final int MAX_QUIET_STRETCH = 2;
    /** The members still to probe in this cycle, in a shuffled order; refilled when exhausted. */
    private final java.util.ArrayDeque<PeerId> probeCycle = new java.util.ArrayDeque<>();

    private PeerId nextProbeTarget() {
        while (true) {
            PeerId next = probeCycle.pollFirst();
            if (next == null) {
                List<PeerId> ids = new ArrayList<>();
                for (PeerId id : members.keySet()) {
                    if (!id.equals(self)) {
                        ids.add(id);
                    }
                }
                if (ids.isEmpty()) {
                    return null;
                }
                Collections.shuffle(ids, random);
                probeCycle.addAll(ids);
                continue;
            }
            if (members.containsKey(next)) {
                return next;
            }
        }
    }

    public synchronized void tick(Prober prober) {
        tick(prober, false);
    }

    /**
     * One membership tick. While the fleet is quiet (no gossip activity reported by the caller
     * since the last tick, no member admitted) the probe interval doubles at each probe up to
     * {@link #MAX_QUIET_STRETCH} doublings, and snaps back to the base on the first active
     * tick, so an idle fleet probes at a quarter of the rate and a busy one as before. Probe
     * timeouts are judged on every tick regardless.
     *
     * @param prober the prober
     * @param quiet  whether nothing moved since the last tick
     */
    public synchronized void tick(Prober prober, boolean quiet) {
        Objects.requireNonNull(prober, "prober");
        long now = nowMillis();
        if (!quiet) {
            activeSinceProbe = true;
            quietCycles = 0;
        }

        // Expire members whose lease of our attention ran out.
        members.values().removeIf(m -> now - m.lastHeardMillis > config.memberTtl().toMillis());

        // Resolve timed-out probes: escalate direct to indirect, drop after indirect.
        for (Map.Entry<Long, Pending> e : List.copyOf(pendingProbes.entrySet())) {
            Pending pending = e.getValue();
            if (now < pending.deadlineMillis()) {
                continue;
            }
            pendingProbes.remove(e.getKey());
            Member target = members.get(pending.target());
            if (target == null) {
                continue;
            }
            if (!pending.indirectStage()) {
                target.suspect = true;
                activeSinceProbe = true; // doubt about a member: probe on the base interval again
                quietCycles = 0;
                List<PeerId> relays = randomMembersExcluding(config.indirectPeers(), pending.target());
                if (relays.isEmpty()) {
                    members.remove(pending.target());
                    continue;
                }
                long nonce = nextNonce();
                pendingProbes.put(nonce, new Pending(pending.target(),
                        now + config.pingTimeout().toMillis(), true,
                        java.util.Set.copyOf(relays)));
                for (PeerId relay : relays) {
                    prober.pingReq(relay, pending.target(), nonce);
                }
            } else {
                members.remove(pending.target());
            }
        }

        // Probe one random member once per protocol period (SWIM): probing on every 250 ms tick
        // was four fifths of an idle peer's frames, and detection speed comes from the suspicion
        // timeout, not the probe rate. Timeouts above are still judged on every tick.
        long interval = config.probeInterval().toMillis() * (1L << quietCycles);
        if (lastProbeMillis != Long.MIN_VALUE && now - lastProbeMillis < interval) {
            return;
        }
        lastProbeMillis = now;
        if (!activeSinceProbe) {
            quietCycles = Math.min(quietCycles + 1, MAX_QUIET_STRETCH);
        }
        activeSinceProbe = false;
        // Round-robin over a shuffled order (SWIM's protocol period), so every member is probed
        // once per cycle and detection time is bounded, rather than a random draw each period.
        PeerId target = nextProbeTarget();
        if (target != null) {
            long nonce = nextNonce();
            pendingProbes.put(nonce, new Pending(target,
                    now + config.pingTimeout().toMillis(), false, java.util.Set.of()));
            prober.ping(target, nonce);
        }
    }

    /**
     * Looks up a member.
     *
     * @param peer the member id
     * @return the member, when present in the view
     */
    public Optional<Member> member(PeerId peer) {
        return Optional.ofNullable(members.get(peer));
    }

    /** Returns a snapshot of all current members. */
    public List<Member> allMembers() {
        return List.copyOf(members.values());
    }

    /**
     * Returns the members currently advertising a topology role.
     *
     * @param role the role
     * @return members offering it
     */
    public List<PeerId> withRole(PeerAdvertisement.PeerRole role) {
        List<PeerId> matching = new ArrayList<>();
        for (Member member : members.values()) {
            if (member.roles.contains(role)) {
                matching.add(member.id);
            }
        }
        return matching;
    }

    @Override
    public List<PeerId> randomMembers(int n) {
        return randomMembersExcluding(n, null);
    }

    private List<PeerId> randomMembersExcluding(int n, PeerId excluded) {
        List<PeerId> ids = new ArrayList<>();
        for (Member m : members.values()) {
            if (!m.id.equals(excluded)) {
                ids.add(m.id);
            }
        }
        Collections.shuffle(ids, random);
        return ids.size() <= n ? ids : ids.subList(0, n);
    }

    private synchronized long nextNonce() {
        return nonceRandom.nextLong();
    }

    private long nowMillis() {
        return clock.instant().toEpochMilli();
    }
}
