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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RE-H-04 and RE-Q-REL-09 of srad/replication-by-entry.md, probe P41 of reviews/RBE-REGRESSION-MATRIX.md: the
 * per-entry horizon is read from the space's membership view. It engages only after the view has been fully hinted
 * for three rounds, comes into force one maximum lease plus five minutes later, and disengages at once when a member
 * without {@code wire.horizon=1} joins. Real nodes on a {@link SimNetwork} with one fake clock, ticked as a fleet is.
 */
class HorizonModeTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final java.util.Set<String> closed = new java.util.HashSet<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zHorizonMode", PeerIdentity.generate().peerId(), GroupId.of("zHorizonMode"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "horizon",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
        PeerId id() {
            return identity.peerId();
        }
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.space().close();
            p.node().close();
        });
    }

    private Peer newPeer(String address, long seed, boolean hinted, String... seedAddresses) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).horizon(hinted).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock).settleWindow(Duration.ZERO).build();
        Peer peer = new Peer(address, identity, node, runtime, space);
        peers.add(peer);
        return peer;
    }

    /** One fleet tick: every live node ticks (its spaces judge the mode there), then the clock moves on. */
    private void tickAll(Duration step) {
        for (Peer p : peers) {
            if (!closed.contains(p.name())) {
                p.node().tick();
            }
        }
        clock.advance(step);
    }

    private boolean runUntil(int maxTicks, Duration step, BooleanSupplier until) {
        for (int i = 0; i < maxTicks; i++) {
            if (until.getAsBoolean()) {
                return true;
            }
            tickAll(step);
        }
        return until.getAsBoolean();
    }

    /** Closes a peer as a departing member would, without a goodbye; the others drop it when it stops answering. */
    private void leave(Peer p) {
        closed.add(p.name());
        p.space().close();
        p.node().close();
    }

    /** Whether a replica holds a state of the entry, a tombstone included: what it would offer a partner. */
    private static boolean holds(Peer p, EntryId id) {
        byte[] delta = p.space().deltaFor(new byte[0]);
        if (delta.length == 0) {
            return false;
        }
        SpaceWire.SyncDelta sync = ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec()
                .fromWire(delta, SpaceWire.SyncDelta.class);
        return sync.states() != null && sync.states().stream()
                .anyMatch(dto -> id.equals(dto.record() != null ? dto.record().entryId() : dto.entryId()));
    }

    private static boolean sees(Peer viewer, Peer member) {
        return viewer.runtime().membership().member(member.id()).isPresent();
    }

    @Test
    void anUnhintedMemberDisengagesAtOnceAndTheModeReengagesOnlyAfterThreeRoundsAndTheSettle() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        assertThat(a.node().wireHints()).containsEntry(PeerNode.HORIZON_HINT, PeerNode.HORIZON_LEVEL);
        assertThat(runUntil(200, Duration.ofMillis(250), () -> sees(a, b) && sees(b, a)
                && a.runtime().advertisesHorizon(b.id()) && b.runtime().advertisesHorizon(a.id()))).isTrue();

        // Engage: three fully hinted rounds of one gossip period each, judged on the ticks, then the settle.
        assertThat(runUntil(200, Duration.ofMillis(250),
                () -> a.space().horizonMode() == ReplicatedSpace.HorizonMode.SETTLING)).isTrue();
        assertThat(a.space().horizonEngagements()).isEqualTo(1);
        // A lease of 30 minutes raises a's maximum lease for good, so the legacy horizon and the per-entry one are far
        // apart from here on, whether or not that entry is still live.
        a.space().write(new TaskEntry("long", 0), Lease.of(Duration.ofMinutes(30)));
        long settleBy = clock.instant().toEpochMilli() + a.space().maxLeaseMillis() + SpaceStateCrdt.LAPSED_GRACE_MILLIS;
        assertThat(runUntil(4_000, Duration.ofSeconds(1),
                () -> a.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY)).isTrue();
        assertThat(clock.instant().toEpochMilli()).as("in force no sooner than one maximum lease and the grace")
                .isGreaterThanOrEqualTo(settleBy - 1_000);
        assertThat(a.space().horizonSettlements()).isEqualTo(1);
        assertThat(runUntil(50, Duration.ofSeconds(1),
                () -> b.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY)).isTrue();

        // In force: a withdrawal is collected ten minutes past its own lease (the drift floor of SPEC §7.3), long
        // before 2 x 30 minutes.
        EntryHandle early = a.space().write(new TaskEntry("early", 1), Lease.of(Duration.ofMinutes(1)));
        EntryId earlyId = early.entryId();
        early.cancel();
        long earlyL = clock.instant().toEpochMilli() + 60_000;
        assertThat(runUntil(1_000, Duration.ofSeconds(1), () -> !holds(a, earlyId)))
                .isTrue();
        assertThat(clock.instant().toEpochMilli()).isLessThan(earlyL + 2 * a.space().maxLeaseMillis());
        assertThat(clock.instant().toEpochMilli())
                .isGreaterThanOrEqualTo(earlyL + SpaceStateCrdt.WITHDRAWN_DRIFT_FLOOR_MILLIS);

        // A withdrawal still held when an unhinted member joins keeps 2 x maxLease from that moment.
        EntryHandle late = a.space().write(new TaskEntry("late", 2), Lease.of(Duration.ofMinutes(1)));
        EntryId lateId = late.entryId();
        late.cancel();
        Peer c = newPeer("c", 3, false, "a");
        assertThat(c.node().wireHints()).doesNotContainKey(PeerNode.HORIZON_HINT);
        boolean[] judgedAtJoin = new boolean[1];
        assertThat(runUntil(200, Duration.ofMillis(250), () -> {
            if (!sees(a, c)) {
                return false;
            }
            // The first moment c is in a's view, a judges the legacy rule: safety first, no rounds to wait.
            a.space().sweepNow();
            judgedAtJoin[0] = a.space().horizonMode() == ReplicatedSpace.HorizonMode.LEGACY;
            return true;
        })).isTrue();
        assertThat(judgedAtJoin[0]).as("disengaged at the first judgement with c in view").isTrue();
        assertThat(a.space().horizonDisengagements()).isEqualTo(1);
        assertThat(a.runtime().viewAdvertisesHorizon()).isFalse();
        clock.advance(Duration.ofMinutes(12)); // past the late entry's per-entry L + 10 min, far short of L + 60 min
        a.space().sweepNow();
        assertThat(holds(a, lateId)).as("kept under the legacy rule").isTrue();
        assertThat(a.space().horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.LEGACY);

        // c leaves. The view is fully hinted again, but the mode waits three rounds, then the settle again.
        leave(c);
        assertThat(runUntil(400, Duration.ofMillis(250), () -> !sees(a, c) && !sees(b, c))).isTrue();
        a.space().sweepNow();
        assertThat(a.space().horizonMode()).as("not straight back").isIn(ReplicatedSpace.HorizonMode.LEGACY,
                ReplicatedSpace.HorizonMode.WAITING);
        assertThat(a.space().horizonEngagements()).isEqualTo(1);
        assertThat(runUntil(40, Duration.ofMillis(250),
                () -> a.space().horizonMode() == ReplicatedSpace.HorizonMode.SETTLING)).isTrue();
        assertThat(a.space().horizonEngagements()).isEqualTo(2);
        a.space().sweepNow();
        assertThat(holds(a, lateId)).as("still the legacy rule while it settles").isTrue();
    }

    @Test
    void aMemberThatFlapsResetsTheStreakAndTheRoundsAreSettable() throws Exception {
        Peer a = newPeer("a", 11, true);
        Peer b = newPeer("b", 12, true, "a");
        assertThat(runUntil(200, Duration.ofMillis(250), () -> sees(a, b) && a.runtime().advertisesHorizon(b.id())))
                .isTrue();
        a.space().horizonEngageRounds(8);
        assertThat(a.space().horizonEngageRounds()).isEqualTo(8);
        assertThatThrownBy(() -> a.space().horizonEngageRounds(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> a.space().horizonEngageRounds(65)).isInstanceOf(IllegalArgumentException.class);
        // Five rounds in, an unhinted member arrives: the streak starts again from nothing once it has gone.
        for (int i = 0; i < 20; i++) {
            tickAll(Duration.ofMillis(250));
        }
        assertThat(a.space().horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.WAITING);
        Peer c = newPeer("c", 13, false, "a");
        assertThat(runUntil(200, Duration.ofMillis(250), () -> sees(a, c))).isTrue();
        a.space().sweepNow();
        assertThat(a.space().horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.LEGACY);
        assertThat(a.space().horizonDisengagements()).as("it never engaged, so nothing disengaged").isZero();
        leave(c);
        assertThat(runUntil(400, Duration.ofMillis(250), () -> !sees(a, c))).isTrue();
        long since = clock.instant().toEpochMilli();
        assertThat(runUntil(200, Duration.ofMillis(250),
                () -> a.space().horizonMode() == ReplicatedSpace.HorizonMode.SETTLING)).isTrue();
        assertThat(clock.instant().toEpochMilli() - since).as("eight fresh rounds of one second")
                .isGreaterThanOrEqualTo(8_000);
    }

    /**
     * The writer's half of the precondition (RE-H-04, the adversarial review of phase 5): the mode says the view is
     * hinted, but a withdrawal is collected by the per-entry rule only when its writer is this node or a hinted member
     * that has been in the view longer than the maximum lease plus the grace. A newcomer's withdrawals keep the legacy
     * horizon until then, since what it renewed before it came into sight is not known here; a writer out of view
     * keeps it throughout (AdversaryHorizonViewScopeTest).
     */
    @Test
    void aHintedNewcomersWithdrawalKeepsTheLegacyHorizonUntilItHasBeenInViewALeaseAndTheGrace() throws Exception {
        Peer a = newPeer("a", 31, true);
        Peer b = newPeer("b", 32, true, "a");
        assertThat(runUntil(200, Duration.ofMillis(250), () -> sees(a, b) && a.runtime().advertisesHorizon(b.id())))
                .isTrue();
        // A 30-minute lease puts the legacy horizon an hour past a withdrawal's L, and the writer's wait at 35 minutes.
        a.space().write(new TaskEntry("long", 0), Lease.of(Duration.ofMinutes(30)));
        assertThat(runUntil(4_000, Duration.ofSeconds(1),
                () -> a.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY)).isTrue();

        Peer c = newPeer("c", 33, true, "a");
        assertThat(runUntil(200, Duration.ofMillis(250), () -> sees(a, c) && a.runtime().advertisesHorizon(c.id())))
                .isTrue();
        long joined = clock.instant().toEpochMilli();
        a.space().sweepNow();
        assertThat(a.space().horizonMode()).as("a hinted newcomer does not disengage the mode")
                .isEqualTo(ReplicatedSpace.HorizonMode.PER_ENTRY);
        EntryHandle written = c.space().write(new TaskEntry("newcomer", 1), Lease.of(Duration.ofMinutes(1)));
        EntryId id = written.entryId();
        assertThat(runUntil(400, Duration.ofMillis(250), () -> a.space().recordOf(id).isPresent())).isTrue();
        long lease = a.space().recordOf(id).orElseThrow().lease().expiresAtMillis();
        written.cancel();
        assertThat(runUntil(400, Duration.ofMillis(250),
                () -> a.space().recordOf(id).isEmpty() && holds(a, id))).as("a holds c's withdrawal").isTrue();
        assertThat(a.space().ruleFor(a.space().signedState(id).orElseThrow().toState()))
                .as("c has been in view for less than 35 minutes").isEqualTo(SpaceStateCrdt.Horizon.LEGACY);

        // Past the per-entry horizon, L + 10 min, and c still in view for less than its wait: a keeps the withdrawal.
        assertThat(runUntil(4_000, Duration.ofSeconds(1),
                () -> clock.instant().toEpochMilli() >= lease + SpaceStateCrdt.WITHDRAWN_DRIFT_FLOOR_MILLIS + 60_000))
                .isTrue();
        a.space().sweepNow();
        assertThat(holds(a, id)).as("kept under the legacy rule for a newcomer's withdrawal").isTrue();

        // Once c has been in view a maximum lease and the grace, its withdrawal is collected by its own L, long before
        // the legacy L + 60 min.
        long settle = a.space().maxLeaseMillis() + SpaceStateCrdt.LAPSED_GRACE_MILLIS;
        assertThat(runUntil(4_000, Duration.ofSeconds(1), () -> !holds(a, id))).isTrue();
        long collectedAt = clock.instant().toEpochMilli();
        assertThat(collectedAt).as("not before c had been in view for its wait")
                .isGreaterThanOrEqualTo(joined + settle - 1_000);
        assertThat(collectedAt).as("by the per-entry rule, not the legacy one")
                .isLessThan(lease + 2 * a.space().maxLeaseMillis());
        assertThat(a.space().horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.PER_ENTRY);
    }

    @Test
    void aLegacyNodeNeverEngagesAndAdvertisesNothing() throws Exception {
        Peer a = newPeer("a", 21, false);
        Peer b = newPeer("b", 22, true, "a");
        assertThat(runUntil(200, Duration.ofMillis(250), () -> sees(a, b) && sees(b, a))).isTrue();
        assertThat(a.node().wireHints()).doesNotContainKey(PeerNode.HORIZON_HINT);
        assertThat(runUntil(100, Duration.ofSeconds(1), () -> b.runtime().membership().allMembers().size() == 1))
                .isTrue();
        for (int i = 0; i < 20; i++) {
            tickAll(Duration.ofSeconds(1));
        }
        assertThat(a.space().horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.LEGACY);
        assertThat(b.space().horizonMode()).as("b's view holds a, which lacks the hint")
                .isEqualTo(ReplicatedSpace.HorizonMode.LEGACY);
        assertThat(a.space().horizonEngagements() + b.space().horizonEngagements()).isZero();
    }
}
