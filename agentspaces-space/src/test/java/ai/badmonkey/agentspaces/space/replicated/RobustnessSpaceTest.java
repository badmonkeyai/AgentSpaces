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
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §4, the space level: the same sequence challenges driven through writes, takes
 * and completions. Replayed deltas fire no event twice and resurrect nothing; held deltas
 * released in any order converge on one state with each event once; a one-way loss during a
 * take race still yields at most one winner; a late joiner after a completion holds the final
 * state; a backlog older than the staleness window is dropped and anti-entropy restores it.
 */
class RobustnessSpaceTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zRobustSpace");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRobustSpace", PeerIdentity.generate().peerId(), groupId,
            Instant.EPOCH, Duration.ofDays(1), "robust",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, Duration settleWindow, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed)
                .channelAuth(PeerNode.ChannelAuth.SESSION).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock).settleWindow(settleWindow).build();
        nodes.add(node);
        return new Peer(node, runtime, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private static List<SpaceEvent.Kind> eventsAt(Peer peer) {
        List<SpaceEvent.Kind> seen = new CopyOnWriteArrayList<>();
        peer.space().notify(Template.of(TaskEntry.class), e -> seen.add(e.kind()), MINUTES_30);
        return seen;
    }

    private static long count(List<SpaceEvent.Kind> events, SpaceEvent.Kind kind) {
        return events.stream().filter(kind::equals).count();
    }

    @Test
    @Timeout(60)
    void replayedDeltasFireNoEventTwiceAndResurrectNothing() throws Exception {
        Peer a = newPeer("a", 1, Duration.ZERO);
        Peer b = newPeer("b", 2, Duration.ZERO, "a");
        tickAll(6);
        List<byte[]> aToB = new CopyOnWriteArrayList<>();
        network.tap("a", "b", aToB::add);
        List<SpaceEvent.Kind> atB = eventsAt(b);
        for (int i = 0; i < 3; i++) {
            a.space().write(new TaskEntry("t" + i, i), MINUTES_30);
        }
        tickAll(2);
        assertThat(count(atB, SpaceEvent.Kind.WRITTEN)).isEqualTo(3);
        List<byte[]> captured = List.copyOf(aToB);

        for (byte[] frame : captured) {
            network.inject("a", "b", frame);
        }
        List<byte[]> reversed = new ArrayList<>(captured);
        java.util.Collections.reverse(reversed);
        for (byte[] frame : reversed) {
            network.inject("a", "b", frame);
            network.inject("a", "b", frame);
        }
        assertThat(count(atB, SpaceEvent.Kind.WRITTEN)).as("no WRITTEN fires twice").isEqualTo(3);
        assertThat(b.space().readAll(Template.of(TaskEntry.class), 10)).hasSize(3);

        // B takes one; the old write deltas replayed afterwards do not bring it back.
        Optional<TakenEntry<TaskEntry>> taken = b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();
        tickAll(2);
        for (byte[] frame : captured) {
            network.inject("a", "b", frame);
        }
        assertThat(b.space().readAll(Template.of(TaskEntry.class), 10)).as("a taken entry stays taken").hasSize(2);
    }

    @Test
    @Timeout(60)
    void deltasHeldAndReleasedInAnyOrderConvergeWithEachEventOnce() throws Exception {
        Peer a = newPeer("a", 1, Duration.ZERO);
        Peer b = newPeer("b", 2, Duration.ZERO, "a");
        Peer c = newPeer("c", 3, Duration.ZERO, "a");
        tickAll(6);
        for (SimNetwork.Order order : SimNetwork.Order.values()) {
            List<SpaceEvent.Kind> atC = eventsAt(c);
            network.hold("a", "c");
            network.hold("b", "c");
            a.space().write(new TaskEntry(order + "-job", 1), MINUTES_30);
            tickAll(1);
            Optional<TakenEntry<TaskEntry>> taken = b.space().take(
                    Template.of(TaskEntry.class).where("topic", ai.badmonkey.agentspaces.api.space.Matchers.eq(order + "-job")),
                    MINUTES_10, Duration.ZERO);
            assertThat(taken).isPresent();
            tickAll(1);
            b.space().complete(taken.get(), new FindingEntry(order + "-job", "done"), MINUTES_30);
            tickAll(2);
            assertThat(atC).as("held: C saw nothing").isEmpty();
            network.release("a", "c", order, 3);
            network.release("b", "c", order, 3);
            tickAll(4);
            // A write that lands after its own completion is history, not news: no WRITTEN then.
            assertThat(count(atC, SpaceEvent.Kind.WRITTEN)).as(order + ": the write fired at most once").isLessThanOrEqualTo(1);
            assertThat(count(atC, SpaceEvent.Kind.COMPLETED)).as(order + ": the completion landed once").isEqualTo(1);
            assertThat(c.space().readAll(Template.of(TaskEntry.class)
                    .where("topic", ai.badmonkey.agentspaces.api.space.Matchers.eq(order + "-job")), 10))
                    .as(order + ": a completed task is not open at C").isEmpty();
            assertThat(c.space().readAll(Template.of(FindingEntry.class), 10))
                    .as(order + ": the result is at C").anyMatch(f -> f.topic().equals(order + "-job"));
        }
    }

    @Test
    @Timeout(60)
    void aOneWayLossDuringATakeRaceStillYieldsAtMostOneWinner() throws Exception {
        Peer a = newPeer("a", 1, Duration.ofSeconds(1));
        Peer b = newPeer("b", 2, Duration.ofSeconds(1), "a");
        Peer c = newPeer("c", 3, Duration.ofSeconds(1), "a");
        tickAll(6);
        c.space().write(new TaskEntry("contested", 1), MINUTES_30);
        tickAll(2);
        network.oneWay("a", "b"); // B does not hear A's claim; A hears B's
        Thread takerA = new Thread(() -> a.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofSeconds(2))
                .ifPresent(t -> complete(a, t)));
        Thread takerB = new Thread(() -> b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofSeconds(2))
                .ifPresent(t -> complete(b, t)));
        takerA.start();
        takerB.start();
        for (int i = 0; i < 6; i++) {
            Thread.sleep(50);
            tickAll(1);
        }
        takerA.join(5_000);
        takerB.join(5_000);
        network.twoWay("a", "b");
        tickAll(6);
        assertThat(completions).as("at most one completion wins; a lost direction never makes two").hasSizeLessThanOrEqualTo(1);
        assertThat(c.space().readAll(Template.of(FindingEntry.class), 10)).hasSizeLessThanOrEqualTo(1);
    }

    private final List<String> completions = new CopyOnWriteArrayList<>();

    private void complete(Peer peer, TakenEntry<TaskEntry> taken) {
        try {
            peer.space().complete(taken, new FindingEntry("contested", "by " + peer.node().peerId().display()), MINUTES_30);
            completions.add(peer.node().peerId().display());
        } catch (LeaseExpiredException lost) {
            // the race went the other way
        }
    }

    @Test
    @Timeout(60)
    void aLateJoinerAfterWritesTakesAndACompletionHoldsTheFinalState() throws Exception {
        Peer a = newPeer("a", 1, Duration.ZERO);
        Peer b = newPeer("b", 2, Duration.ZERO, "a");
        tickAll(6);
        for (int i = 0; i < 3; i++) {
            a.space().write(new TaskEntry("t" + i, i), MINUTES_30);
        }
        tickAll(2);
        Optional<TakenEntry<TaskEntry>> taken = b.space().take(
                Template.of(TaskEntry.class).where("topic", ai.badmonkey.agentspaces.api.space.Matchers.eq("t1")), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();
        tickAll(2);
        b.space().complete(taken.get(), new FindingEntry("t1", "done early"), MINUTES_30);
        tickAll(2);

        Peer c = newPeer("c", 3, Duration.ZERO, "b");
        List<SpaceEvent.Kind> atC = eventsAt(c);
        tickAll(6);
        assertThat(c.space().readAll(Template.of(TaskEntry.class), 10)).extracting(TaskEntry::topic)
                .as("the two open tasks, not the completed one").containsExactlyInAnyOrder("t0", "t2");
        assertThat(c.space().readAll(Template.of(FindingEntry.class), 10)).extracting(FindingEntry::topic).contains("t1");
        assertThat(count(atC, SpaceEvent.Kind.WRITTEN)).as("history fires WRITTEN for what a reader could see").isEqualTo(2);
        assertThat(c.space().take(Template.of(TaskEntry.class).where("topic", ai.badmonkey.agentspaces.api.space.Matchers.eq("t1")),
                MINUTES_10, Duration.ZERO)).as("a completed task is not takeable by a late joiner").isEmpty();
    }

    @Test
    @Timeout(60)
    void aStaleBacklogOfDeltasIsDroppedAndAntiEntropyRestoresTheState() throws Exception {
        Peer a = newPeer("a", 1, Duration.ZERO);
        Peer b = newPeer("b", 2, Duration.ZERO, "a");
        tickAll(6);
        List<SpaceEvent.Kind> atB = eventsAt(b);
        network.hold("a", "b");
        for (int i = 0; i < 3; i++) {
            a.space().write(new TaskEntry("late-" + i, i), MINUTES_30);
            tickAll(1);
        }
        clock.advance(Duration.ofMinutes(6));
        long droppedBefore = b.node().framesDropped();
        network.release("a", "b", SimNetwork.Order.FIFO, 0);
        assertThat(atB).as("stale deltas are dropped before any side effect").isEmpty();
        assertThat(b.node().framesDropped()).as("stale is not malformed").isEqualTo(droppedBefore);
        tickAll(45); // the lost-member retry rejoins the pair; the admission round pulls the space
        assertThat(b.space().readAll(Template.of(TaskEntry.class), 10)).extracting(TaskEntry::topic)
                .containsExactlyInAnyOrder("late-0", "late-1", "late-2");
        assertThat(count(atB, SpaceEvent.Kind.WRITTEN)).as("each entry fires once when it finally lands").isEqualTo(3);
    }
}
