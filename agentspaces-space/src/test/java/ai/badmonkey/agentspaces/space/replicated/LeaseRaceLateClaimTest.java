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
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The settle guarantee of SPEC §7.4 under LEASE_RACE (2026-11-15): one take wins an entry only while every claim
 * reaches every rival claimant within the settle window. Here a claim is held on the wire past the window
 * ({@link SimNetwork#hold}), the way a gossip repair delivered claims 0.3 to 2.4 s late in Party Bus, so two takers
 * each judge themselves the winner. The claim with the lower stamp ({@code c}'s, made first) wins the lattice; the
 * later claimant ({@code b}) never heard of it and completes first. What each holder does next is what the guarantee
 * promises beyond the window: the lattice winner that hears of the displaced holder's completion before it completes
 * stands down and writes no result, and when neither hears of the other in time, the shared state still keeps one
 * completion and the duplicate is counted where the winning claim is held.
 *
 * <p>The order of everything is scripted; the claims' stamps are ordered on the test clock.
 */
class LeaseRaceLateClaimTest {

    /** The late claimant's window: long, so the scripted steps inside it (a few milliseconds) always fit. */
    private static final Duration LONG_SETTLE = Duration.ofSeconds(2);
    /** The rival's window: short, so its take is judged while the long window is still open. */
    private static final Duration SHORT_SETTLE = Duration.ofMillis(200);
    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zLateLeaseRace", PeerIdentity.generate().peerId(), GroupId.of("zLateLeaseRace"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "late-lease-race",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace newPeer(String address, long seed, Duration settle, String... seedAddresses)
            throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        var runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        nodes.add(node);
        return ReplicatedSpace.builder(runtime, "tasks-late-race", identity, "taker")
                .clock(clock)
                .settleWindow(settle)
                .build();
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private void hold(String from, String... to) {
        for (String t : to) {
            network.hold(from, t);
        }
    }

    private void release(String from, String... to) {
        for (String t : to) {
            network.release(from, t, SimNetwork.Order.FIFO, 0);
        }
    }

    /** Every replica ends with the entry completed: the shared state keeps one completion whatever happened. */
    private static void assertCompletedEverywhere(List<ReplicatedSpace> replicas) {
        for (ReplicatedSpace replica : replicas) {
            assertThat(replica.read(Template.of(TaskEntry.class))).as("the entry is completed everywhere").isEmpty();
            assertThat(replica.stats().completed()).as("one completed entry in the shared state").isEqualTo(1);
        }
    }

    @Test
    void aRivalsCompletionArrivingInTheWinnersWindowMakesItsTakeStandDown() throws Exception {
        ReplicatedSpace writer = newPeer("a", 1, SHORT_SETTLE);
        ReplicatedSpace rival = newPeer("b", 2, SHORT_SETTLE, "a");
        ReplicatedSpace late = newPeer("c", 3, LONG_SETTLE, "a");
        tickAll(4);
        writer.write(new TaskEntry("contested", 1), MINUTES_30);
        tickAll(1);
        // c's claim is the earlier one, so it wins the lattice, but nobody hears it until the end.
        hold("c", "a", "b");

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> winner = CompletableFuture.supplyAsync(
                () -> late.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO));
        long waitFrom = System.nanoTime();
        while (network.heldCount("c", "a") + network.heldCount("c", "b") == 0 && !winner.isDone()) {
            assertThat(System.nanoTime() - waitFrom).as("c's claim was sent")
                    .isLessThan(TimeUnit.SECONDS.toNanos(30));
            Thread.sleep(1);
        }
        assertThat(winner).as("c is in its settle window").isNotDone();
        long sinceClaim = System.nanoTime();

        clock.advance(Duration.ofMillis(10)); // b's claim is stamped after c's
        Optional<TakenEntry<TaskEntry>> displaced =
                rival.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(displaced).as("b never heard of c's claim, so it judged itself the winner").isPresent();
        rival.complete(displaced.get(), new FindingEntry("contested", "done by b"), MINUTES_30);
        System.out.printf("b completed %d ms after c claimed (c's window %d ms)%n",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sinceClaim), LONG_SETTLE.toMillis());

        assertThat(winner.get(30, TimeUnit.SECONDS))
                .as("c's claim won the lattice, but the work is done: its take stands down").isEmpty();
        assertThat(late.outbidAcknowledged()).isEqualTo(1);

        release("c", "a", "b");
        tickAll(3);
        List<ReplicatedSpace> all = List.of(writer, rival, late);
        assertCompletedEverywhere(all);
        for (ReplicatedSpace replica : all) {
            assertThat(replica.readAll(Template.of(FindingEntry.class), 10))
                    .as("one result, b's").containsExactly(new FindingEntry("contested", "done by b"));
            assertThat(replica.duplicateCompletions()).as("nothing was done twice").isZero();
        }
    }

    @Test
    void aRivalsCompletionArrivingAfterTheWinnerWonStopsItsResult() throws Exception {
        ReplicatedSpace writer = newPeer("a", 1, SHORT_SETTLE);
        ReplicatedSpace rival = newPeer("b", 2, SHORT_SETTLE, "a");
        ReplicatedSpace late = newPeer("c", 3, SHORT_SETTLE, "a");
        tickAll(4);
        writer.write(new TaskEntry("contested", 1), MINUTES_30);
        tickAll(1);
        hold("c", "a", "b");
        hold("a", "c");
        hold("b", "c");

        Optional<TakenEntry<TaskEntry>> winning = late.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(winning).as("c judged its take alone").isPresent();
        clock.advance(Duration.ofMillis(10));
        Optional<TakenEntry<TaskEntry>> displaced =
                rival.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(displaced).as("two holders each judged themselves the winner").isPresent();
        rival.complete(displaced.get(), new FindingEntry("contested", "done by b"), MINUTES_30);

        // b's claim and completion reach c while c is still working; c's own result must not be written.
        release("a", "c");
        release("b", "c");
        assertThatThrownBy(() -> late.complete(winning.get(), new FindingEntry("contested", "done by c"), MINUTES_30))
                .isInstanceOf(LeaseExpiredException.class)
                .hasMessageContaining("completed first");
        assertThat(late.outbidAcknowledged()).isEqualTo(1);

        release("c", "a", "b");
        tickAll(3);
        List<ReplicatedSpace> all = List.of(writer, rival, late);
        assertCompletedEverywhere(all);
        for (ReplicatedSpace replica : all) {
            assertThat(replica.readAll(Template.of(FindingEntry.class), 10))
                    .as("one result, b's").containsExactly(new FindingEntry("contested", "done by b"));
            assertThat(replica.duplicateCompletions()).isZero();
        }
    }

    @Test
    void aBetterClaimArrivingBeforeTheCompletionMakesTheDisplacedHolderYield() throws Exception {
        ReplicatedSpace writer = newPeer("a", 1, SHORT_SETTLE);
        ReplicatedSpace rival = newPeer("b", 2, SHORT_SETTLE, "a");
        ReplicatedSpace late = newPeer("c", 3, SHORT_SETTLE, "a");
        tickAll(4);
        writer.write(new TaskEntry("contested", 1), MINUTES_30);
        tickAll(1);
        hold("c", "a", "b");
        hold("a", "c");
        hold("b", "c");

        Optional<TakenEntry<TaskEntry>> winning = late.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(winning).isPresent();
        clock.advance(Duration.ofMillis(10));
        Optional<TakenEntry<TaskEntry>> displaced =
                rival.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(displaced).isPresent();

        // c's earlier claim reaches b before b completes: b learned of an earlier winner, so it must not complete.
        release("c", "a", "b");
        assertThatThrownBy(() -> rival.complete(displaced.get(), new FindingEntry("contested", "done by b"),
                MINUTES_30)).isInstanceOf(LeaseExpiredException.class);
        late.complete(winning.get(), new FindingEntry("contested", "done by c"), MINUTES_30);

        release("a", "c");
        release("b", "c");
        tickAll(3);
        List<ReplicatedSpace> all = List.of(writer, rival, late);
        assertCompletedEverywhere(all);
        for (ReplicatedSpace replica : all) {
            assertThat(replica.readAll(Template.of(FindingEntry.class), 10))
                    .as("one result, c's").containsExactly(new FindingEntry("contested", "done by c"));
            assertThat(replica.duplicateCompletions()).isZero();
            assertThat(replica.outbidAcknowledged()).isZero();
        }
    }

    @Test
    void twoHoldersThatHearOfEachOtherOnlyAfterCompletingLeaveOneCompletionAndACountedDuplicate() throws Exception {
        ReplicatedSpace writer = newPeer("a", 1, SHORT_SETTLE);
        ReplicatedSpace rival = newPeer("b", 2, SHORT_SETTLE, "a");
        ReplicatedSpace late = newPeer("c", 3, SHORT_SETTLE, "a");
        tickAll(4);
        writer.write(new TaskEntry("contested", 1), MINUTES_30);
        tickAll(1);
        hold("c", "a", "b");
        hold("a", "c");
        hold("b", "c");

        Optional<TakenEntry<TaskEntry>> winning = late.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(winning).isPresent();
        late.complete(winning.get(), new FindingEntry("contested", "done by c"), MINUTES_30);
        clock.advance(Duration.ofMillis(10));
        Optional<TakenEntry<TaskEntry>> displaced =
                rival.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(displaced).isPresent();
        rival.complete(displaced.get(), new FindingEntry("contested", "done by b"), MINUTES_30);

        // Beyond the window SPEC §7.4 promises no single winner: both did the work. The duplicate is detectable.
        release("c", "a", "b");
        release("a", "c");
        release("b", "c");
        tickAll(3);
        List<ReplicatedSpace> all = List.of(writer, rival, late);
        assertCompletedEverywhere(all);
        assertThat(late.duplicateCompletions()).as("counted where the winning claim is held").isEqualTo(1);
        assertThat(rival.duplicateCompletions()).as("the displaced holder cannot tell an acknowledgement from work")
                .isZero();
        assertThat(writer.duplicateCompletions()).as("a replica that held no claim judges nothing").isZero();
        for (ReplicatedSpace replica : all) {
            assertThat(replica.readAll(Template.of(FindingEntry.class), 10))
                    .as("both results stand: the duplicate SPEC §7.4 allows beyond the window")
                    .containsExactlyInAnyOrder(new FindingEntry("contested", "done by b"),
                            new FindingEntry("contested", "done by c"));
        }
        tickAll(3);
        assertThat(late.duplicateCompletions()).as("once per entry, however often it is offered again").isEqualTo(1);
    }

    @Test
    void aRaceDecidedInsideTheWindowLeavesOneWinnerAndNothingToAcknowledge() throws Exception {
        ReplicatedSpace writer = newPeer("a", 1, SHORT_SETTLE);
        ReplicatedSpace first = newPeer("b", 2, SHORT_SETTLE, "a");
        ReplicatedSpace second = newPeer("c", 3, SHORT_SETTLE, "a");
        tickAll(4);
        writer.write(new TaskEntry("plain", 1), MINUTES_30);
        tickAll(1);

        Optional<TakenEntry<TaskEntry>> taken = first.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();
        assertThat(second.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .as("the claim arrived in time: the entry is held").isEmpty();
        first.complete(taken.get(), new FindingEntry("plain", "done by b"), MINUTES_30);
        tickAll(3);
        List<ReplicatedSpace> all = List.of(writer, first, second);
        assertCompletedEverywhere(all);
        for (ReplicatedSpace replica : all) {
            assertThat(replica.readAll(Template.of(FindingEntry.class), 10))
                    .containsExactly(new FindingEntry("plain", "done by b"));
            assertThat(replica.outbidAcknowledged()).isZero();
            assertThat(replica.duplicateCompletions()).isZero();
        }
    }
}
