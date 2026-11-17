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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.reduce.Reductions;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Probe P38 of reviews/RBE-REGRESSION-MATRIX.md, the agent layer's part (srad/replication-by-entry.md RE-H-04,
 * RE-Q-REL-04), for two shapes:
 * <ul>
 *   <li>an old {@code @SpaceReduce} accumulator, which the reducer cancels once its successor is written
 *   ({@code ReduceBinding.retire}); each step is the reducer's own {@code complete(element, accumulator, lease,
 *   tags)};</li>
 *   <li>a remote action's input withdrawn on timeout ({@code RemoteAction.OnTimeout.WITHDRAW}).</li>
 * </ul>
 * Each is collected ten minutes past its own lease (the drift floor of SPEC §7.3) under the per-entry horizon, and a
 * lagging replica's re-offer is refused at the door. A remote action's input that a worker took and completed is a
 * completion instead, and keeps {@code 2 x maxLease}: the third case shows it is not collected at the per-entry
 * horizon.
 */
class HorizonLaggingCopyTest {

    /** Inside the five-minute frame age every receiver allows (PeerNode.MAX_FRAME_AGE_MILLIS). */
    private static final long LAG_MILLIS = Duration.ofMinutes(4).toMillis();

    /**
     * The lag the drifted clock reaches while it is cut off: past the five-minute lapse grace and within MAX_DRIFT (ten
     * minutes, SPEC §11). Since the drift floor of SPEC §7.3 the actor collects a withdrawal ten minutes past its
     * lease, and a replica still four minutes behind has dropped its own lapsed copy a minute before that, so nothing
     * is left to re-offer. One that has drifted to eight minutes behind still holds the copy within its grace, and
     * offers it once the healed link has carried the actor's stamps to its clock.
     */
    private static final long DRIFTED_LAG_MILLIS = Duration.ofMinutes(8).toMillis();

    private final TestClock clock = TestClock.create();
    /** The lagging clock's lag now, and the lag it drifts to; it grows by half of each step, so it never runs back. */
    private volatile long lag = LAG_MILLIS;
    private volatile long lagTarget = LAG_MILLIS;
    private final InstantSource lagging = () -> clock.instant().minusMillis(lag);
    /**
     * The bridge's clock, four minutes behind the actor's throughout. A replica whose clock trails the actor's by more
     * than the five-minute frame age is heard by the actor only while its frames carry newer stamps than its own clock
     * reads: stamps its hybrid logical clock took from members it hears. Cut off together with the bridge, the lagging
     * replica's stamps follow the bridge's, within five minutes of the actor's, so when the partition heals the actor
     * hears it again and it offers the copy it still holds. Alone, its stamps would fall eight minutes behind and the
     * actor would drop its frames as too old, so the copy would never reach the door.
     */
    private final InstantSource bridging = () -> clock.instant().minusMillis(LAG_MILLIS);
    private final SimNetwork network = new SimNetwork();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zHorizonAgent",
            PeerIdentity.generate().peerId(), GroupId.of("zHorizonAgent"), Instant.EPOCH, Duration.ofDays(1),
            "horizon", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    private record Member(PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /**
     * A member of the probe's group. The lagging replica reconciles by the text digest, which still lists a present
     * entry whose lease lapsed; between two members on {@code wire.ae=2} such an entry is out of the audited set and
     * is never offered. A clock lagging far enough to hold it live is heard only through a bridge (the bridge's clock
     * below).
     */
    private Member member(String address, InstantSource memberClock, boolean audit, String... seeds) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(memberClock).randomSeed(address.hashCode()).audit(audit)
                .build();
        nodes.add(node);
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        // A long member TTL keeps the partitioned member in view, as a slow or cut-off member stays in a real one.
        GroupRuntime runtime = node.joinGroup(groupAd,
                new GroupMembership.Config(Duration.ofHours(1), Duration.ofSeconds(2), 2), endpoints);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(memberClock).settleWindow(Duration.ZERO).build();
        return new Member(identity, node, runtime, space);
    }

    private boolean runUntil(int ticks, Duration step, BooleanSupplier until) {
        for (int i = 0; i < ticks && !until.getAsBoolean(); i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(step);
            lag = Math.min(lagTarget, lag + step.toMillis() / 2);
        }
        return until.getAsBoolean();
    }

    /** The actor and the lagging replica, both under the per-entry horizon. */
    private Member[] fleet() throws Exception {
        Member a = member("a", clock, true);
        Member b = member("b", bridging, true, "a");
        Member c = member("c", lagging, false, "b");
        assertThat(runUntil(4_000, Duration.ofSeconds(1),
                () -> a.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && c.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && b.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && a.runtime().membership().allMembers().size() == 2
                        && c.runtime().membership().allMembers().size() == 2))
                .as("both replicas under the per-entry horizon").isTrue();
        return new Member[]{a, c};
    }

    /**
     * The probe itself: the entry reaches the lagging replica, a partition cuts it off, the actor withdraws the entry
     * as the module does, and collects the tombstone ten minutes past its own lease (the drift floor of SPEC §7.3),
     * long before twice the maximum lease. When the partition heals the lagging replica offers its present copy; the
     * door refuses and acknowledges it, and the actor never holds the entry again.
     */
    private void probe(Member a, Member c, EntryHandle handle, Runnable withdraw) {
        EntryId id = handle.entryId();
        assertThat(runUntil(200, Duration.ofMillis(250), () -> c.space().recordOf(id).isPresent())).isTrue();
        long lease = a.space().recordOf(id).orElseThrow().lease().expiresAtMillis();
        // The actor alone on one side; the bridge and the lagging replica together on the other, so neither hears
        // the withdrawal and the lagging replica's hybrid logical clock keeps following the bridge's.
        network.partition("a", "c");
        network.partition("a", "b");
        lagTarget = DRIFTED_LAG_MILLIS; // the cut-off clock slows, as a drifting clock does, never stepping back
        withdraw.run();
        assertThat(a.space().recordOf(id)).isEmpty();
        assertThat(runUntil(1_000, Duration.ofSeconds(1), () -> a.space().signedState(id).isEmpty()))
                .as("collected under the per-entry horizon").isTrue();
        assertThat(clock.millis()).isGreaterThanOrEqualTo(lease + SpaceStateCrdt.WITHDRAWN_DRIFT_FLOOR_MILLIS)
                .isLessThan(lease + 2 * a.space().maxLeaseMillis());
        assertThat(c.space().signedState(id)).as("still held, present, by the lagging replica").isPresent();
        long refusedBefore = a.space().collectedReoffersAcknowledged();
        long healedAt = clock.millis();
        network.heal();
        assertThat(runUntil(400, Duration.ofMillis(250), () -> {
            assertThat(a.space().recordOf(id, true)).as("never back on the replica that withdrew it").isEmpty();
            return a.space().collectedReoffersAcknowledged() > refusedBefore;
        })).as("the lagging copy reached the door and was refused there").isTrue();
        System.out.println("P38 agent: the lagging copy was refused at the door " + (clock.millis() - healedAt)
                + " ms after the heal, on the fake clock");
    }

    /** An entry with a long lease, so the space's maximum lease, and with it the legacy horizon, is far out. */
    public record Marker(String name) {
    }

    private static void longLease(Member a) {
        a.space().write(new Marker("long"), Lease.of(Duration.ofHours(1)));
    }

    /** A reduction's element and its accumulator, as an application declares them. */
    public record Reading(String key, long value) {
    }

    public record Total(String key, long sum) {
    }

    /** A remote action's input. */
    public record Ask(String question) {
    }

    private static Map<String, String> reduceTags(long steps) {
        return Map.of(Reductions.REDUCE_TAG, "totals", Reductions.KEY_TAG, "k", Reductions.STEPS_TAG,
                Long.toString(steps));
    }

    @Test
    void anOldAccumulatorReofferedByALaggingReplicaIsRefusedAtTheDoor() throws Exception {
        Member[] fleet = fleet();
        Member a = fleet[0];
        Member c = fleet[1];
        longLease(a);
        a.space().write(new Reading("k", 1), Lease.of(Duration.ofMinutes(10)));
        TakenEntry<Reading> first = a.space().take(Template.of(Reading.class), Lease.of(Duration.ofMinutes(1)),
                Duration.ZERO).orElseThrow();
        EntryHandle old = a.space().complete(first, new Total("k", 1), Lease.of(Duration.ofMinutes(2)), reduceTags(1));
        probe(a, c, old, () -> {
            a.space().write(new Reading("k", 2), Lease.of(Duration.ofMinutes(10)));
            TakenEntry<Reading> next = a.space().take(Template.of(Reading.class), Lease.of(Duration.ofMinutes(1)),
                    Duration.ZERO).orElseThrow();
            a.space().complete(next, new Total("k", 3), Lease.of(Duration.ofMinutes(30)), reduceTags(2));
            old.cancel(); // ReduceBinding.retire: the reducer wrote the predecessor, so it cancels it
        });
        assertThat(a.space().readAll(Template.of(Total.class), 10)).as("readers see the latest accumulator alone")
                .containsExactly(new Total("k", 3));
    }

    @Test
    void aWithdrawnRemoteActionInputReofferedByALaggingReplicaIsRefusedAtTheDoor() throws Exception {
        Member[] fleet = fleet();
        Member a = fleet[0];
        Member c = fleet[1];
        longLease(a);
        EntryHandle input = a.space().write(new Ask("late?"), Lease.of(Duration.ofMinutes(2)),
                Map.of("aspace.action", "answer"));
        probe(a, c, input, input::cancel); // RemoteAction: OnTimeout.WITHDRAW, nobody took it
        assertThat(a.space().readAll(Template.of(Ask.class), 10)).as("no worker can take it again").isEmpty();
    }

    @Test
    void aCompletedRemoteActionInputKeepsTwiceTheMaximumLease() throws Exception {
        Member[] fleet = fleet();
        Member a = fleet[0];
        longLease(a);
        EntryHandle input = a.space().write(new Ask("done?"), Lease.of(Duration.ofMinutes(2)),
                Map.of("aspace.action", "answer"));
        EntryId id = input.entryId();
        long lease = a.space().recordOf(id).orElseThrow().lease().expiresAtMillis();
        TakenEntry<Ask> taken = a.space().take(Template.of(Ask.class), Lease.of(Duration.ofMinutes(1)), Duration.ZERO)
                .orElseThrow();
        a.space().complete(taken);
        runUntil(900, Duration.ofSeconds(1), () -> false); // fifteen minutes: well past L + 5 min
        assertThat(clock.millis()).isGreaterThan(lease + 2 * SpaceStateCrdt.LAPSED_GRACE_MILLIS);
        assertThat(a.space().signedState(id)).as("a completion keeps 2 x maxLease under both rules").isPresent();
        assertThat(a.space().horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.PER_ENTRY);
    }
}
