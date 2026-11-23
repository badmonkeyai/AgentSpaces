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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * One joined group on a peer: its membership view, its gossip bus, and the
 * registration point upper layers (discovery, replicated spaces, capabilities)
 * use to receive frames and publish rumors. Obtained from
 * {@link PeerNode#joinGroup}.
 */
public final class GroupRuntime implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(GroupRuntime.class.getName());

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
     * Periodic work registered by upper layers through the deprecated {@link #onTick}, each run by an adapter on the
     * node's loop (srad/agentspaces-event-loop.md EL-L-05). Copy-on-write; registration and removal are rare.
     */
    private final java.util.List<Runnable> tickWork =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Every registration this runtime holds on the node's loop, closed when the group is left. */
    private final java.util.List<ai.badmonkey.agentspaces.api.spi.DueWork.Registration> registrations =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Node-clock millis of the last anti-entropy round; null before the first. */
    private volatile Long lastAntiEntropyMillis;
    /** Told when the group is left (ISSUE-PeerRobustness-2, R2-2.5); copy-on-write, run in registration order. */
    private final java.util.List<Runnable> closeListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile boolean closed;

    GroupRuntime(GroupId groupId, GroupAdvertisement advertisement,
                 GroupMembership membership, GossipBus gossip,
                 RevocationRegistry revocations,
                 ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry credentialRevocations,
                 PeerNode node, SignedGroupAdvertisement founding) {
        java.util.Objects.requireNonNull(node, "node"); // every runtime belongs to a node (DC-03)
        this.groupId = groupId;
        this.advertisement = advertisement;
        this.membership = membership;
        this.gossip = gossip;
        gossip.antiEntropyBase(advertisement.gossip().period().toMillis()); // RE-F-03: the founding's period, settable
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
    /** Consecutive anti-entropy rounds that found nothing to do; the period doubles per round, capped. */
    private volatile int quietRounds;
    /** The longest the anti-entropy period stretches to on an idle group, as a multiple of the base period. */
    static final int MAX_BACKOFF_MULTIPLIER = 32;

    boolean claimAntiEntropyRound(long nowMillis) {
        return claimAntiEntropyRound(nowMillis, false);
    }

    /**
     * Whether the periodic anti-entropy round has backed off because the group is quiet. While
     * it has not, the round runs at the base period and reconciles whatever moves; a probe that
     * carries a differing digest starts a targeted round only while it has (v0.1.17), since
     * under load digests differ on every probe for the moment gossip takes to deliver.
     */
    public boolean antiEntropyStretched() {
        return quietRounds > 0;
    }

    /**
     * Whether an anti-entropy round is due. The base period is the group's gossip period; while
     * rounds find nothing to reconcile and no rumor moves, the period doubles per round up to
     * {@link #MAX_BACKOFF_MULTIPLIER} times (Demers et al.: anti-entropy is the rare backstop),
     * and any activity resets it to the base.
     *
     * @param nowMillis the clock
     * @param active    whether anything moved on the gossip bus since the last round
     */
    boolean claimAntiEntropyRound(long nowMillis, boolean active) {
        if (active) {
            quietRounds = 0;
        }
        long period = antiEntropyPeriodMillis();
        Long last = lastAntiEntropyMillis;
        if (last != null && nowMillis - last < period) {
            return false;
        }
        lastAntiEntropyMillis = nowMillis;
        if (!active) {
            quietRounds = Math.min(quietRounds + 1, 5);
        }
        return true;
    }

    /** The period the next anti-entropy round waits, the base stretched by the quiet rounds so far. */
    private long antiEntropyPeriodMillis() {
        // RE-F-03: the base period and the stretch are runtime paces on the bus; the founding sets the first base.
        long base = gossip.antiEntropyPeriod().toMillis();
        return base * Math.min(Math.min(MAX_BACKOFF_MULTIPLIER, gossip.antiEntropyStretch()),
                1L << Math.min(quietRounds, 5));
    }

    /**
     * When the next anti-entropy round is due, on the node's clock (srad/agentspaces-event-loop.md EL-L-01, EL-L-07):
     * a stretched period after the last round, at once before the first. The first activity after a quiet stretch
     * asks the round again, which brings the period back to the base (EL-L-13).
     *
     * @param nowMillis the clock now
     * @return the time in the clock's milliseconds
     */
    long nextAntiEntropyMillis(long nowMillis) {
        Long last = lastAntiEntropyMillis;
        return last == null ? nowMillis : last + antiEntropyPeriodMillis();
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

    /**
     * Whether the spaces of this group log, once each, the first renewal that asks for less than its lease has left
     * (RE-H-01, {@code agentspaces.space.renew-shortening-log}); a node setting, on by default.
     */
    public boolean renewShorteningLog() {
        return node.renewShorteningLog();
    }

    /** Returns the group's gossip bus. */
    public GossipBus gossip() {
        return gossip;
    }

    /**
     * This member's replication status for the group (srad/replication-by-entry.md RE-Q-MNT-02, RE-F-03): the bus's
     * status with the membership probe's paces beside the bus's own. {@code toMap()} gives the shape a feed's
     * {@code /healthz} and the console's panel render. For a status page: each space walks what it holds to answer.
     *
     * @return the status
     */
    public GossipBus.ReplicationStatus replicationStatus() {
        GossipBus.ReplicationStatus bus = gossip.replicationStatus();
        java.util.List<ai.badmonkey.agentspaces.peering.gossip.Pace.Snapshot> paces =
                new java.util.ArrayList<>(bus.paces());
        paces.addAll(membership.paces());
        return new GossipBus.ReplicationStatus(bus.partnersByForm(), bus.formChanges(), bus.streams(),
                java.util.List.copyOf(paces), bus.rounds(), bus.audit(), bus.pacing());
    }

    /** Returns the group's peer sampler (spec §5.2), backed by live membership. */
    public PeerSampler sampler() {
        return membership;
    }

    /**
     * Whether a member advertises {@link PeerNode#RECORDLESS_HINT}, so a space may send it a
     * renew, cancel, or complete without the record (srad/agentspaces-pdt.md PD-L-03).
     *
     * @param peer the member
     * @return true when the member accepts the recordless form
     */
    public boolean advertisesRecordless(PeerId peer) {
        return node.advertisesRecordless(peer);
    }

    /**
     * Whether every member of this group's current view advertises
     * {@link PeerNode#RECORDLESS_HINT} (PD-L-03). An empty view answers true: a
     * state published then reaches nobody who could object.
     *
     * @return true when a recordless state may go to the group's stream
     */
    public boolean viewAdvertisesRecordless() {
        for (GroupMembership.Member member : membership.allMembers()) {
            if (!advertisesRecordless(member.id())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether this node and every member of this group's current view advertise {@link PeerNode#HORIZON_HINT}
     * (SPEC §7.3, v0.1.35; srad/replication-by-entry.md RE-H-04): the precondition under which a space may collect a
     * withdrawn entry by its own lease. A member whose advertisement has not arrived counts as unhinted, so a newcomer
     * turns the answer false the moment it enters the view. An empty view answers this node's own setting: a lone
     * node's tombstones are judged by its own conforming writers alone.
     *
     * <p>The answer is read live on every call; the space applies its own hysteresis on top (RE-Q-REL-09).
     *
     * @return true when the per-entry horizon's precondition holds for this view now
     */
    public boolean viewAdvertisesHorizon() {
        if (!node.horizonPerEntry()) {
            return false;
        }
        // idle-loop O5: every space judges this on every tick; the walk runs only when the view or a hint moved.
        // The versions are read before the walk, so a change during it leaves a stale key and the next call walks.
        long view = membership.viewVersion();
        long hints = node.horizonHintsVersion();
        HorizonJudgement cached = horizonJudgement;
        if (cached != null && cached.view == view && cached.hints == hints) {
            return cached.result;
        }
        boolean result = true;
        for (GroupMembership.Member member : membership.allMembers()) {
            if (!node.advertisesHorizon(member.id())) {
                result = false;
                break;
            }
        }
        horizonJudgement = new HorizonJudgement(view, hints, result);
        return result;
    }

    /** The last judgement of {@link #viewAdvertisesHorizon()}, by the view's and the hints' versions it read. */
    private record HorizonJudgement(long view, long hints, boolean result) { }

    private volatile HorizonJudgement horizonJudgement;

    /**
     * Asks every participant of this group to run again on the node loop's next run (srad/agentspaces-event-loop.md
     * EL-L-15, DW-2): a component that moved a deadline earlier and cannot name its participant calls this. A
     * participant that can be named is asked through its own {@link ai.badmonkey.agentspaces.api.spi.DueWork.Registration#wake()}.
     */
    public void wake() {
        node.wake(id());
    }

    /**
     * A counter that moves whenever this group's view or any member's horizon hint moves (idle-loop O5, O8): a sum of
     * two counters that only grow, so it moves whenever either does. Only its equality between reads means anything.
     *
     * @return the view's epoch
     */
    public long viewEpoch() {
        return membership.viewVersion() + node.horizonHintsVersion();
    }

    /**
     * A counter that moves whenever a member's AppCard level moves (srad/agentspaces-event-loop.md EL-T-07): a
     * registrant that judges the AppCard gate re-judges only when it or {@link #viewEpoch()} moved.
     *
     * @return the version; only its equality between reads means anything
     */
    public long appCardVersion() {
        return node.appCardVersion();
    }

    /**
     * Whether a member's current advertisement carries {@link PeerNode#HORIZON_HINT} (RE-H-04).
     *
     * @param peer the member
     * @return true when it does
     */
    public boolean advertisesHorizon(PeerId peer) {
        return node.advertisesHorizon(peer);
    }

    /**
     * For how long a writer's promise behind {@link PeerNode#HORIZON_HINT} can be read here (RE-H-04):
     * {@link Long#MAX_VALUE} for this node when it advertises the hint; for a member of this group's current view
     * whose current card carries it, the time since it entered the view, on the node's clock; and -1 otherwise. A
     * writer outside the view, on the far side of a partition or gone since, answers -1: what this node knew of its
     * card says nothing about what it renewed while out of sight. A space trusts a writer only once this exceeds its
     * longest lease plus the grace, so that whatever the writer renewed out of sight has lapsed and passed its own
     * lapse horizon first.
     *
     * @param peer the node that wrote the entry
     * @return milliseconds the writer has been in view and hinted, {@link Long#MAX_VALUE} for this node, or -1
     */
    public long hintedWriterInViewMillis(PeerId peer) {
        if (peer == null) {
            return -1;
        }
        if (peer.equals(node.identity().peerId())) {
            return node.horizonPerEntry() ? Long.MAX_VALUE : -1;
        }
        return node.advertisesHorizon(peer) ? membership.inViewForMillis(peer) : -1;
    }

    /**
     * Whether this node and every member of this group's current view are at an AppCard level (SPEC §6.1a.2;
     * AppCard origin and formats, 2026-11-09): the gate under which an issuer writes a card's origin, formats, an
     * {@code idp} host, and a spill reference. A member whose advertisement has not arrived counts as level 1.
     *
     * @param level the level the gated fields need
     * @return true when the whole view reads them
     */
    public boolean viewAtAppCardLevel(int level) {
        if (node.appCardLevel() < level) {
            return false;
        }
        for (GroupMembership.Member member : membership.allMembers()) {
            if (node.appCardLevel(member.id()) < level) {
                return false;
            }
        }
        return true;
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
     * Revokes a delegation (an invite key) by its hash, refusing every
     * credential issued under it at once and evicting the members admitted
     * under it. Only the founder's revocation verifies.
     *
     * @param delegationOrCredential the delegation string, or any credential issued under it
     * @param reason                 the reason
     * @return the revocation, or empty when this peer may not issue it or the string is not delegated
     */
    public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revokeDelegation(
            String delegationOrCredential, String reason) {
        byte[] hash = JoinCredentials.delegationHash(delegationOrCredential);
        if (hash == null) {
            return Optional.empty();
        }
        return revoke(ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.joinCredential(hash),
                reason, null);
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
        Objects.requireNonNull(kind);
        Objects.requireNonNull(handler);
        if (closed) {
            return; // a left group dispatches nothing
        }
        kindHandlers.put(kind, handler);
    }

    /**
     * Registers due work on the node's loop (srad/agentspaces-event-loop.md §3.2, EL-L-05): the work runs when the
     * deadline it last returned falls due, when its registration or this group is woken, on the loop's backstop, and
     * on every {@link PeerNode#tick()}. It runs on the node's loop, serially with every other participant of the node
     * and after the protocol participants of every group (NL-2, NL-8); it must not block (DW-3). A participant that
     * throws is logged with its name, counted and re-armed at the backstop (NL-4).
     *
     * @param name the participant's stable name, as {@link PeerNode#loopStats()} reports it (DW-4)
     * @param work the work
     * @return the registration: {@code wake()} asks the work again, {@code close()} removes it
     */
    public ai.badmonkey.agentspaces.api.spi.DueWork.Registration onDue(String name,
                                                                     ai.badmonkey.agentspaces.api.spi.DueWork work) {
        return onDue(name, work, NodeLoop.Timing.FIXED);
    }

    /**
     * Registers due work with a timing for the loop's stretch (srad/agentspaces-event-loop.md §3.10, EL-O-04 to
     * EL-O-07): {@link NodeLoop.Timing#local} for a period no peer judges, which may stretch under sustained overrun up
     * to its cap; {@link NodeLoop.Timing#wireVisible} for a cadence a peer judges (a lease's renewal, an
     * advertisement's re-issue), whose refresh action the loop runs with the stretched period before the stretch
     * applies, so that no peer reads a lapse. {@link #onDue(String, ai.badmonkey.agentspaces.api.spi.DueWork)}
     * registers {@link NodeLoop.Timing#FIXED}, which never stretches.
     *
     * @param name   the participant's stable name (DW-4)
     * @param work   the work
     * @param timing how its period may stretch
     * @return the registration
     */
    public ai.badmonkey.agentspaces.api.spi.DueWork.Registration onDue(String name,
                                                                     ai.badmonkey.agentspaces.api.spi.DueWork work,
                                                                     NodeLoop.Timing timing) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(work, "work");
        Objects.requireNonNull(timing, "timing");
        if (closed) {
            return NO_REGISTRATION; // a left group runs nothing
        }
        ai.badmonkey.agentspaces.api.spi.DueWork.Registration registration = node.registerDue(groupId, name, work,
                timing);
        registrations.add(registration);
        return new ai.badmonkey.agentspaces.api.spi.DueWork.Registration() {
            @Override
            public void wake() {
                registration.wake();
            }

            @Override
            public void close() {
                registration.close();
                registrations.remove(registration);
            }
        };
    }

    /**
     * Registers due work that keeps its deadlines on its own wall clock (srad/agentspaces-event-loop.md EL-T-09,
     * §3.6): the work is given its clock's now and returns its next due on that clock, which this method converts to
     * the loop's monotonic base against both clocks read together, after the run (the fix of §10.8), and caps at an
     * hour ahead. This is the one converter for registrants whose leases and expiries are wall instants. A wall step
     * (TS-6) asks every participant of the node again, these among them, so each converts its deadline again on the
     * stepped clock (EL-C-12).
     *
     * @param name  the participant's stable name (DW-4)
     * @param clock the registrant's wall clock
     * @param work  the work
     * @return the registration
     */
    public ai.badmonkey.agentspaces.api.spi.DueWork.Registration onDueWall(String name, java.time.InstantSource clock,
                                                                         ai.badmonkey.agentspaces.api.spi.DueWork.Wall work) {
        return onDueWall(name, clock, work, NodeLoop.Timing.FIXED);
    }

    /**
     * {@link #onDueWall(String, java.time.InstantSource, ai.badmonkey.agentspaces.api.spi.DueWork.Wall)} with a timing
     * for the loop's stretch and re-initialisation (EL-O-05 to EL-O-07, WD-4): a registrant whose cadence a peer judges
     * (a card's re-issue, a lease's renewal) registers {@link NodeLoop.Timing#wireVisible} with its refresh action.
     *
     * @param name   the participant's stable name (DW-4)
     * @param clock  the registrant's wall clock
     * @param work   the work
     * @param timing how its period may stretch
     * @return the registration
     */
    public ai.badmonkey.agentspaces.api.spi.DueWork.Registration onDueWall(String name, java.time.InstantSource clock,
                                                                         ai.badmonkey.agentspaces.api.spi.DueWork.Wall work,
                                                                         NodeLoop.Timing timing) {
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(work, "work");
        if (closed) {
            return NO_REGISTRATION;
        }
        ai.badmonkey.agentspaces.common.time.TimeSource time = node.time();
        return onDue(name, now -> {
            long next = work.run(clock.millis());
            return next == ai.badmonkey.agentspaces.api.spi.DueWork.NEVER
                    ? ai.badmonkey.agentspaces.api.spi.DueWork.NEVER
                    : PeerNode.due(time.monoNanos(), clock.millis(), next);
        }, timing);
    }


    /**
     * A wall deadline on {@code clock} converted to the loop's monotonic base, both clocks read now together, for a
     * registrant that reports through {@link #onDue} and keeps some deadlines on a wall clock.
     *
     * @param clock      the registrant's wall clock
     * @param wallMillis the wall deadline, or NEVER
     * @return the deadline on the loop's monotonic base
     */
    public long dueAt(java.time.InstantSource clock, long wallMillis) {
        if (wallMillis == ai.badmonkey.agentspaces.api.spi.DueWork.NEVER) {
            return ai.badmonkey.agentspaces.api.spi.DueWork.NEVER;
        }
        ai.badmonkey.agentspaces.common.time.TimeSource time = time();
        return PeerNode.due(time.monoNanos(), clock.millis(), wallMillis);
    }

    /**
     * Whether the calling thread is inside a run that {@link PeerNode#tick()} started, which calls every participant
     * whether due or not: a registrant keeps its legacy one-step behaviour there (owner answer 4, 2026-11-22).
     *
     * @return whether the current run is a direct one
     */
    public boolean directRun() {
        return node.directRun();
    }

    /**
     * Whether the node's loop is running on its own ({@code startTicking}), as against driven by hand.
     *
     * @return whether the loop runs
     */
    public boolean loopRunning() {
        return node.tickPeriod().isPresent();
    }

    /**
     * The node's monotonic now, the base due work reports on.
     *
     * @return the monotonic now in nanoseconds
     */
    public long monoNanos() {
        return time().monoNanos();
    }

    /**
     * The node's time source.
     *
     * @return the time source; the system's for a runtime with no node
     */
    public ai.badmonkey.agentspaces.common.time.TimeSource time() {
        return node.time();
    }

    /**
     * The clock a layer built on this group defaults to when it is given none (srad/agentspaces-event-loop.md
     * EL-C-06): the node's time source, so a space, a content swarm or a store judges its leases on the same time as
     * the node; for a node built on a plain test clock, the system's, as before phase 3, since such a clock moves only
     * by a test's hand.
     *
     * @return the clock
     */
    public ai.badmonkey.agentspaces.common.time.TimeSource defaultClock() {
        ai.badmonkey.agentspaces.common.time.TimeSource t = time();
        return ai.badmonkey.agentspaces.common.time.TimeSources.followsWall(t)
                ? ai.badmonkey.agentspaces.common.time.TimeSource.system() : t;
    }

    /**
     * How many registrations this group holds on the node's loop: {@link #onDue}, {@link #onDueWall} and
     * {@link #onTick} work; for tests and diagnostics.
     *
     * @return the count
     */
    public int registrationCount() {
        return registrations.size();
    }

    private static final ai.badmonkey.agentspaces.api.spi.DueWork.Registration NO_REGISTRATION =
            new ai.badmonkey.agentspaces.api.spi.DueWork.Registration() {
                @Override
                public void wake() {
                }

                @Override
                public void close() {
                }
            };

    /**
     * Registers periodic work to be driven by the peer's tick, and returns the handle that deregisters it (QA3 A3-1).
     *
     * <p>Deprecated since the node's loop (srad/agentspaces-event-loop.md decision 7, EL-L-05): register
     * {@link #onDue} work that reports its own next due time instead. The adapter behind this method keeps the old
     * behaviour: the work runs every {@link #tickPeriod()} while the node's loop runs ({@code tick-millis}), and on
     * every {@link PeerNode#tick()} a host drives by hand; it runs on the node's loop after the protocol participants,
     * must return promptly and must not block on the network. Work that throws is caught and logged; one misbehaving
     * registrant can neither stop the others nor kill the loop. The first registration from each class logs the
     * deprecation once.
     *
     * @param work the work to run once per tick
     * @return a handle that removes the work when closed; closing twice is safe
     * @deprecated register {@link #onDue} work that returns its next due time
     */
    @Deprecated(since = "0.7.0")
    public AutoCloseable onTick(Runnable work) {
        Objects.requireNonNull(work, "work");
        if (closed) {
            return () -> { }; // a left group ticks nothing
        }
        tickWork.add(work);
        ai.badmonkey.agentspaces.api.spi.DueWork.Registration registration = node.registerTickAdapter(groupId, work);
        registrations.add(registration);
        return () -> {
            tickWork.remove(work);
            registration.close();
            registrations.remove(registration);
        };
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
     * @deprecated due work keeps its own deadlines in time (srad/agentspaces-event-loop.md decision 6); use
     *     {@link #loopRunning()} to tell a running loop from a hand-driven node
     */
    @Deprecated(since = "0.7.0")
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

    /**
     * How many relays an addressed frame to a member crosses (LB-R-10): 0 for a
     * member this node holds a link to or can dial, the length of the pinned or
     * computable relay path otherwise, and -1 when no path is known.
     *
     * @param to the member
     * @return the hop count
     */
    public int hopsTo(PeerId to) {
        return node.hopsTo(groupId, Objects.requireNonNull(to, "to"));
    }

    /**
     * The RELAY-role members this node holds links to in this group: the reach
     * hint its advertisement carries when it is dial-only (LB-R-01), and what a
     * holding copies as {@code via} (LB-H-04).
     *
     * @return the relays, sorted by id; empty when none
     */
    public List<PeerId> viaRelays() {
        return node.viaRelays(groupId);
    }

    BiConsumer<PeerId, byte[]> handlerFor(Envelope.Kind kind) {
        return kindHandlers.get(kind);
    }

    /**
     * Registers a listener told when this group is left, by
     * {@link PeerNode#leaveGroup} or by {@link #close()} (ISSUE-PeerRobustness-2,
     * R2-2.5): a layer above peering (a replicated space, a capability runtime)
     * tears down what it built on the group. Listeners run on the leaving
     * thread, in registration order, after the node stopped sending in the
     * group; one that throws is logged and the rest still run. A listener
     * registered after the group was left runs at once.
     *
     * @param listener the listener
     * @return a handle that removes the listener when closed; closing twice is safe
     */
    public AutoCloseable onClose(Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        if (closed) {
            listener.run();
            return () -> { };
        }
        closeListeners.add(listener);
        return () -> closeListeners.remove(listener);
    }

    /** Whether the node has left this group; a left runtime sends nothing and runs no tick work. */
    public boolean closed() {
        return closed;
    }

    /**
     * Leaves the group: the same as {@link PeerNode#leaveGroup} for this group's
     * id. Everything the node holds for the group goes; every other group is
     * untouched; the {@link #onClose} listeners are told.
     */
    @Override
    public void close() {
        node.leaveGroup(groupId);
    }

    /**
     * The teardown {@link PeerNode#leaveGroup} runs once the group is out of the
     * node's tables: periodic work and kind handlers are dropped so nothing of
     * the group's runs on a later tick or frame, and the listeners are told.
     * Idempotent.
     */
    void release() {
        if (closed) {
            return;
        }
        closed = true;
        tickWork.clear();
        for (ai.badmonkey.agentspaces.api.spi.DueWork.Registration registration : registrations) {
            registration.close();
        }
        registrations.clear();
        kindHandlers.clear();
        for (Runnable listener : closeListeners) {
            try {
                listener.run();
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "a close listener of group " + groupId.value() + " failed", e);
            }
        }
        closeListeners.clear();
    }
}
