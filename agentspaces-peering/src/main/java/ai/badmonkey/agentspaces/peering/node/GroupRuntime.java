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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry;

import java.util.Optional;
import ai.badmonkey.agentspaces.peering.wire.Envelope;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * One joined group on a peer: its membership view, its gossip bus, and the
 * registration point upper layers (discovery, replicated spaces, capabilities)
 * use to receive frames and publish rumors. Obtained from
 * {@link PeerNode#joinGroup}.
 */
public final class GroupRuntime {

    private final GroupId groupId;
    private final GroupAdvertisement advertisement;
    private final GroupMembership membership;
    private final GossipBus gossip;
    private final RevocationRegistry revocations;
    private final ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry credentialRevocations;
    private final ai.badmonkey.agentspaces.api.security.RevocationView revocationView;
    private final PeerNode node;
    /** The self-certifying founding document, when the group was joined with one. */
    private final SignedGroupAdvertisement founding;
    private final Map<Envelope.Kind, BiConsumer<PeerId, byte[]>> kindHandlers =
            new ConcurrentHashMap<>();
    /**
     * Periodic work registered by upper layers, driven once per peer tick.
     * Copy-on-write so the tick iterates without locking and keeps registration
     * order; registration and removal are rare.
     */
    private final java.util.List<Runnable> tickWork =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Node-clock millis of the last anti-entropy round; null before the first. */
    private volatile Long lastAntiEntropyMillis;

    GroupRuntime(GroupId groupId, GroupAdvertisement advertisement,
                 GroupMembership membership, GossipBus gossip,
                 RevocationRegistry revocations, PeerNode node) {
        this(groupId, advertisement, membership, gossip, revocations,
                new ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry(advertisement,
                        ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry.Validator.defaults(),
                        ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec(),
                        java.time.InstantSource.system()),
                node, null);
    }

    GroupRuntime(GroupId groupId, GroupAdvertisement advertisement,
                 GroupMembership membership, GossipBus gossip,
                 RevocationRegistry revocations,
                 ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry credentialRevocations,
                 PeerNode node, SignedGroupAdvertisement founding) {
        this.groupId = groupId;
        this.advertisement = advertisement;
        this.membership = membership;
        this.gossip = gossip;
        this.revocations = revocations;
        this.credentialRevocations = java.util.Objects.requireNonNull(credentialRevocations,
                "credentialRevocations");
        this.revocationView = new ai.badmonkey.agentspaces.api.security.RevocationView() {
            @Override
            public boolean revoked(PeerId peer) {
                return revocations.revoked(peer);
            }

            @Override
            public boolean refuses(ai.badmonkey.agentspaces.common.id.AgentId agent, byte[] agentKey,
                                   java.time.Instant signingTime) {
                return revocations.revoked(agent.peer())
                        || credentialRevocations.refuses(agent, agentKey, signingTime);
            }
        };
        this.node = node;
        this.founding = founding;
    }

    /**
     * Returns the signed, self-certifying founding advertisement this group
     * was joined with (spec §4.4, §5.1), or empty for a locally configured
     * literal-id group. Only a present founding document is ever served to a
     * newcomer asking by GroupID ({@code GROUP_AD_WANT}).
     */
    public Optional<SignedGroupAdvertisement> founding() {
        return Optional.ofNullable(founding);
    }

    /**
     * Whether an anti-entropy round is due (spec §5.3): the first round is
     * always due; afterwards one is due once the group's gossip period has
     * elapsed on the node's clock since the previous round. Marks the round
     * as taken when it returns {@code true}.
     *
     * @param nowMillis the node clock, in epoch millis
     * @return whether the caller should run anti-entropy now
     */
    boolean claimAntiEntropyRound(long nowMillis) {
        Long last = lastAntiEntropyMillis;
        if (last != null && nowMillis - last < advertisement.gossip().period().toMillis()) {
            return false;
        }
        lastAntiEntropyMillis = nowMillis;
        return true;
    }

    /** Returns the group id. */
    public GroupId id() {
        return groupId;
    }

    /** Returns the group's founding advertisement. */
    public GroupAdvertisement advertisement() {
        return advertisement;
    }

    /** Returns the group's membership view. */
    public GroupMembership membership() {
        return membership;
    }

    /** Returns the group's gossip bus. */
    public GossipBus gossip() {
        return gossip;
    }

    /** Returns the group's peer sampler (spec §5.2), backed by live membership. */
    public PeerSampler sampler() {
        return membership;
    }

    /** This group's accepted revocations. */
    public RevocationRegistry revocations() {
        return revocations;
    }

    /** This group's accepted revocations of agents, agent keys, leaves, and join credentials (v0.1.13). */
    public ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry credentialRevocations() {
        return credentialRevocations;
    }

    /**
     * Both registries as every enforcement point asks about them (SPEC §5.6,
     * v0.1.13): revoked peers, and agents and agent keys under the freeze rule.
     *
     * @return the view
     */
    public ai.badmonkey.agentspaces.api.security.RevocationView revocationView() {
        return revocationView;
    }

    /**
     * Revokes an agent, every key it holds (SPEC §6.1, v0.1.13). Honoured when
     * this node is the founder or the agent's own peer.
     *
     * @param agent  the agent
     * @param reason one of {@code CredentialRevocation.REASONS}
     * @return the revocation when this node had the authority to issue it
     */
    public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revokeAgent(
            ai.badmonkey.agentspaces.common.id.AgentId agent, String reason) {
        return revoke(ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.agent(agent),
                reason, null);
    }

    /**
     * Revokes one key of an agent (SPEC §6.1, v0.1.13): signatures by other
     * keys of the agent, such as a renewed one, still verify.
     *
     * @param agent  the agent
     * @param key    the agent's raw Ed25519 public key
     * @param reason one of {@code CredentialRevocation.REASONS}
     * @return the revocation when this node had the authority to issue it
     */
    public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revokeAgentKey(
            ai.badmonkey.agentspaces.common.id.AgentId agent, byte[] key, String reason) {
        return revoke(ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.agentKey(agent,
                ai.badmonkey.agentspaces.common.crypto.Digests.sha256(key)), reason, null);
    }

    /**
     * Revokes a join credential (SPEC §6.1, v0.1.13): it admits nobody from now
     * on, and a member admitted under it is evicted. Honoured from the founder.
     *
     * @param credential the credential string {@code JoinCredentials.issue} returned
     * @param reason     one of {@code CredentialRevocation.REASONS}
     * @return the revocation when this node had the authority to issue it
     */
    public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revokeJoinCredential(
            String credential, String reason) {
        return revoke(ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.joinCredential(
                JoinCredentials.hash(credential)), reason, null);
    }

    /**
     * Issues a revocation of any certified target (SPEC §6.1, v0.1.13), signed
     * by this node and honoured fleet-wide only when the group's validator
     * ranks this node's authority for that target.
     *
     * @param target        what is revoked
     * @param reason        one of {@code CredentialRevocation.REASONS}
     * @param effectiveFrom from when signatures stop verifying under a
     *                      non-compromise reason (null: now); no later than now
     * @return the revocation when this node had the authority to issue it
     */
    public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revoke(
            ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target target, String reason,
            java.time.Instant effectiveFrom) {
        return node.issueCredentialRevocation(this, target, reason, effectiveFrom);
    }

    /**
     * Issues a revocation of a peer's identity (remediation plan §9): signed by
     * this node and honored fleet-wide only when this node is the group's
     * trust root (the registry's validator refuses anything else, locally and
     * on every receiver). The revocation gossips immediately and converges to
     * late joiners through anti-entropy.
     *
     * @param revoked the peer whose identity is withdrawn
     * @param reason  operator-facing reason
     * @return the advertisement when this node had the authority to issue it
     */
    public Optional<RevocationAdvertisement> revoke(PeerId revoked, String reason) {
        return node.issueRevocation(this, revoked, reason, null);
    }

    /**
     * Rotates a peer's identity: revokes the old PeerID and names its
     * successor, the simplest rotation shape (see
     * {@link RevocationAdvertisement}). The successor joins and is authorized
     * like any new peer; richer succession protocols can replace this without
     * touching enforcement.
     *
     * @param rotated   the identity being retired
     * @param successor the replacement identity
     * @param reason    operator-facing reason
     * @return the advertisement when this node had the authority to issue it
     */
    public Optional<RevocationAdvertisement> rotate(PeerId rotated, PeerId successor,
                                                    String reason) {
        return node.issueRevocation(this, rotated, reason, successor);
    }

    /**
     * Reports one witnessed protocol violation by a group peer (WS5): an upper
     * layer (a replicated space rejecting a forged claim, a capability
     * rejecting an out-of-bounds value) saw hostile content on an
     * authenticated frame. Enough reports quarantine the peer locally.
     *
     * @param peer   the misbehaving peer
     * @param reason what was witnessed
     */
    public void reportMisbehavior(PeerId peer, String reason) {
        node.strike(Objects.requireNonNull(peer, "peer"), reason);
    }

    /**
     * Registers a handler for a frame kind the peering core does not consume
     * itself ({@code QUERY}, {@code QUERY_HIT}, {@code PIPE_DATA}).
     *
     * @param kind    the frame kind
     * @param handler receives (sender, body bytes)
     */
    public void onKind(Envelope.Kind kind, BiConsumer<PeerId, byte[]> handler) {
        kindHandlers.put(Objects.requireNonNull(kind), Objects.requireNonNull(handler));
    }

    /**
     * Registers periodic work to be driven by the peer's tick, and returns the
     * handle that deregisters it (QA3 A3-1). This is the clock seam for layers
     * above peering: a replicated space already joins the same clock by handing
     * the gossip bus a {@code ReconcilableState}, and a capability runtime joins
     * it here, so anything built on a joined group advances for the same reason
     * membership and anti-entropy do, with no scheduler of its own.
     *
     * <p>Work runs on the peer's single tick thread, in registration order,
     * after membership probing and anti-entropy for this group. It must return
     * promptly and must not block on the network. Work that throws is caught
     * and logged by the node: one misbehaving registrant can neither stop the
     * others nor kill the tick.
     *
     * @param work the work to run once per tick
     * @return a handle that removes the work when closed; closing twice is safe
     */
    public AutoCloseable onTick(Runnable work) {
        Objects.requireNonNull(work, "work");
        tickWork.add(work);
        return () -> tickWork.remove(work);
    }

    /**
     * Runs every registered periodic task once. Called by {@link PeerNode#tick()};
     * a task that throws is reported and skipped so the rest still run.
     *
     * @param onError invoked with any task failure, for the node to log
     */
    void runTickWork(java.util.function.Consumer<RuntimeException> onError) {
        for (Runnable work : tickWork) {
            try {
                work.run();
            } catch (RuntimeException e) {
                onError.accept(e);
            }
        }
    }

    /** Returns how many periodic tasks are registered; for tests and diagnostics. */
    public int tickWorkCount() {
        return tickWork.size();
    }

    /**
     * The cadence {@link #onTick} work runs at, when the node knows it; empty
     * while the host drives {@link PeerNode#tick()} by hand (QA3 A3-4).
     *
     * @return the peer's tick period, or empty when hand-driven
     */
    public Optional<java.time.Duration> tickPeriod() {
        return node.tickPeriod();
    }

    /**
     * Sends a kind-tagged body to one peer in this group.
     *
     * @param to   the destination peer
     * @param kind the frame kind
     * @param body the body object; CBOR-serialized
     */
    public void send(PeerId to, Envelope.Kind kind, Object body) {
        node.send(groupId, to, kind, body);
    }

    BiConsumer<PeerId, byte[]> handlerFor(Envelope.Kind kind) {
        return kindHandlers.get(kind);
    }
}
