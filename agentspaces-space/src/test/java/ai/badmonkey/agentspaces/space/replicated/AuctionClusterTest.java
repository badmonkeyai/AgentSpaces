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
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The AUCTION strategy (spec §7.4): the space as a decentralized least-cost
 * allocator. Deterministic: SimNetwork, TestClock, zero settle windows.
 */
class AuctionClusterTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zAuction");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zAuction", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "auction",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.AUCTION,
            GroupAdvertisement.GossipParameters.defaults());
    private final GroupAdvertisement raceGroupAd = new GroupAdvertisement(
            "aspace://zAuctionRace", PeerIdentity.generate().peerId(), GroupId.of("zAuctionRace"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "auction-race",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, ToDoubleFunction<Object> bid,
                         String... seedAddresses) throws IOException {
        return newPeer(address, seed, bid, ConflictStrategyType.AUCTION, seedAddresses);
    }

    private Peer newPeer(String address, long seed, ToDoubleFunction<Object> bid, ConflictStrategyType strategy,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(strategy == ConflictStrategyType.AUCTION ? groupAd : raceGroupAd,
                GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace
                .builder(runtime, "tasks-auction", identity, "bidder")
                .clock(clock)
                .settleWindow(Duration.ZERO)
                .strategy(strategy)
                .bidFunction(bid)
                .build();
        nodes.add(node);
        return new Peer(node, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    @Test
    void aBetterBidOutbidsWithinTheSettleWindow() throws Exception {
        Peer coordinator = newPeer("a", 1, task -> 100.0);
        Peer expensive = newPeer("b", 2, task -> 10.0, "a");
        Peer cheap = newPeer("c", 3, task -> 2.0, "a");
        tickAll(4);
        coordinator.space().write(new TaskEntry("contested", 1), MINUTES_30);

        // The expensive bidder claims first and believes it holds the entry.
        Optional<TakenEntry<TaskEntry>> costly =
                expensive.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(costly).isPresent();

        // The cheap bidder counter-claims inside the settle window and wins.
        Optional<TakenEntry<TaskEntry>> bargain =
                cheap.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(bargain).isPresent();

        assertThatThrownBy(() -> expensive.space().complete(costly.get()))
                .isInstanceOf(LeaseExpiredException.class);
        cheap.space().complete(bargain.get());
        assertThat(coordinator.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    /** Spec §7.4 AUCTION: once the incumbent's settle window has closed, a later cheaper bid is refused. */
    @Test
    void aLateCheaperBidIsRefusedOnceTheSettleWindowCloses() throws Exception {
        Peer coordinator = newPeer("a", 1, task -> 100.0);
        Peer expensive = newPeer("b", 2, task -> 10.0, "a");
        Peer cheap = newPeer("c", 3, task -> 2.0, "a");
        tickAll(4);
        coordinator.space().write(new TaskEntry("settled", 1), MINUTES_30);

        Optional<TakenEntry<TaskEntry>> costly =
                expensive.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(costly).isPresent();

        // The (zero-length) settle window closes as soon as the clock moves.
        clock.advance(Duration.ofMillis(1));
        Optional<TakenEntry<TaskEntry>> bargain =
                cheap.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(bargain).as("no counter-claim after the settle window").isEmpty();

        expensive.space().complete(costly.get());
        assertThat(coordinator.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(cheap.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void partitionedBidsResolveToTheLowestCostOnHeal() throws Exception {
        Peer coordinator = newPeer("a", 1, task -> 100.0);
        Peer expensive = newPeer("b", 2, task -> 9.0, "a");
        Peer cheap = newPeer("c", 3, task -> 3.0, "a");
        tickAll(4);
        coordinator.space().write(new TaskEntry("contested", 1), MINUTES_30);
        assertThat(cheap.space().read(Template.of(TaskEntry.class))).isPresent();

        // Full isolation: both bid knowing nothing of each other.
        network.partition("b", "c");
        network.partition("a", "c");
        Optional<TakenEntry<TaskEntry>> byExpensive =
                expensive.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        Optional<TakenEntry<TaskEntry>> byCheap =
                cheap.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(byExpensive).isPresent();
        assertThat(byCheap).isPresent();

        network.heal();
        tickAll(6);

        // The lattice picks the lower bid deterministically: cheap completes,
        // expensive discovers it lost.
        cheap.space().complete(byCheap.get());
        assertThatThrownBy(() -> expensive.space().complete(byExpensive.get()))
                .isInstanceOf(LeaseExpiredException.class);
    }

    @Test
    void workAllocatesByCostAcrossManyTasks() throws Exception {
        // The cheap worker bids low on even topics, high on odd; the other worker
        // mirrors it. Each should win exactly its cheap half.
        ToDoubleFunction<Object> evenCheap = task ->
                Integer.parseInt(((TaskEntry) task).topic().substring(5)) % 2 == 0 ? 1.0 : 50.0;
        ToDoubleFunction<Object> oddCheap = task ->
                Integer.parseInt(((TaskEntry) task).topic().substring(5)) % 2 == 1 ? 1.0 : 50.0;
        Peer coordinator = newPeer("a", 1, task -> 1000.0);
        Peer evens = newPeer("b", 2, evenCheap, "a");
        Peer odds = newPeer("c", 3, oddCheap, "a");
        tickAll(4);
        for (int i = 0; i < 10; i++) {
            coordinator.space().write(new TaskEntry("task-" + i, 1), MINUTES_30);
        }

        // Both workers scan repeatedly; counter-claims settle to the cheapest.
        List<String> wonByEvens = new ArrayList<>();
        List<String> wonByOdds = new ArrayList<>();
        for (int round = 0; round < 12; round++) {
            evens.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)
                    .ifPresent(taken -> {
                        wonByEvens.add(taken.entry().topic());
                        evens.space().complete(taken);
                    });
            odds.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)
                    .ifPresent(taken -> {
                        wonByOdds.add(taken.entry().topic());
                        odds.space().complete(taken);
                    });
        }

        assertThat(wonByEvens).allMatch(topic ->
                Integer.parseInt(topic.substring(5)) % 2 == 0);
        assertThat(wonByOdds).allMatch(topic ->
                Integer.parseInt(topic.substring(5)) % 2 == 1);
        assertThat(wonByEvens.size() + wonByOdds.size()).isEqualTo(10);
    }

    // ---- P3 (reviews/RBE-REGRESSION-MATRIX.md §3): contending claims merged in one batch

    /**
     * P3, AUCTION: two bidders take every task while neither hears the other, so every task carries two contending
     * claims. Two replicas that hear nothing live receive the claims only as anti-entropy batches, one bidder's whole
     * set in one {@code PULL_RESP} and then the other's, in either order, so each claim of the second batch contends
     * with the one held; the coordinator and the bidders hear them live. Every replica ends with the lattice's winner,
     * the cheaper bid, on every task, holds one digest, and only the winner can complete.
     */
    @Test
    void contendingAuctionClaimsMergedInOneBatchPickTheWinnerLiveGossipPicks() throws Exception {
        contendInBatches(ConflictStrategyType.AUCTION);
    }

    /** P3, LEASE_RACE: as the AUCTION case, with the lowest stamp the winner. */
    @Test
    void contendingLeaseRaceClaimsMergedInOneBatchPickTheWinnerLiveGossipPicks() throws Exception {
        contendInBatches(ConflictStrategyType.LEASE_RACE);
    }

    private static int topicNumber(Object task) {
        return Integer.parseInt(((TaskEntry) task).topic().substring(5));
    }

    private void contendInBatches(ConflictStrategyType strategy) throws Exception {
        int tasks = 8;
        ToDoubleFunction<Object> evenCheap = task -> topicNumber(task) % 2 == 0 ? 1.0 : 50.0;
        ToDoubleFunction<Object> oddCheap = task -> topicNumber(task) % 2 == 1 ? 1.0 : 50.0;
        Peer coordinator = newPeer("a", 1, task -> 1000.0, strategy);
        Peer evens = newPeer("b", 2, evenCheap, strategy, "a");
        Peer odds = newPeer("c", 3, oddCheap, strategy, "a");
        Peer batchedOneWay = newPeer("x", 4, task -> 1000.0, strategy); // no seeds: hears nothing live
        Peer batchedOtherWay = newPeer("y", 5, task -> 1000.0, strategy);
        tickAll(4);
        List<EntryId> ids = new ArrayList<>();
        for (int i = 0; i < tasks; i++) {
            ids.add(coordinator.space().write(new TaskEntry("task-" + i, 1), MINUTES_30).entryId());
        }
        tickAll(3);
        assertThat(evens.space().readAll(Template.of(TaskEntry.class), 100)).hasSize(tasks);
        assertThat(odds.space().readAll(Template.of(TaskEntry.class), 100)).hasSize(tasks);

        // Neither bidder hears the other while both take every task.
        List<String> live = List.of("a", "b", "c");
        for (String from : live) {
            for (String to : live) {
                if (!from.equals(to)) {
                    network.hold(from, to);
                }
            }
        }
        Map<EntryId, TakenEntry<TaskEntry>> byEvens = new HashMap<>();
        Map<EntryId, TakenEntry<TaskEntry>> byOdds = new HashMap<>();
        for (int i = 0; i < tasks; i++) {
            evens.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)
                    .ifPresent(taken -> byEvens.put(taken.entryId(), taken));
            odds.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)
                    .ifPresent(taken -> byOdds.put(taken.entryId(), taken));
        }
        assertThat(byEvens).hasSize(tasks);
        assertThat(byOdds).hasSize(tasks);
        Map<EntryId, AgentId> winners = new HashMap<>();
        for (EntryId id : ids) {
            TakeClaim mine = evens.space().currentClaim(id).orElseThrow();
            TakeClaim theirs = odds.space().currentClaim(id).orElseThrow();
            assertThat(mine.holder()).isNotEqualTo(theirs.holder());
            winners.put(id, TakeClaim.merge(mine, theirs).holder());
        }
        if (strategy == ConflictStrategyType.AUCTION) {
            AgentId evensHolder = evens.space().writer().orElseThrow();
            AgentId oddsHolder = odds.space().writer().orElseThrow();
            for (EntryId id : ids) {
                int topic = topicNumber(byEvens.get(id).entry());
                assertThat(winners.get(id)).as("the cheaper bid wins task-%d", topic)
                        .isEqualTo(topic % 2 == 0 ? evensHolder : oddsHolder);
            }
        }

        // The batched replicas: the entries first, then each bidder's claims as one PULL_RESP of their own.
        CborCodec codec = CborCodec.defaultCodec();
        for (Peer batched : List.of(batchedOneWay, batchedOtherWay)) {
            assertThat(batched.space().applySyncDelta(coordinator.space().deltaFor(batched.space().digest())))
                    .isTrue();
            assertThat(batched.space().knownEntries()).isEqualTo(tasks);
        }
        for (List<Peer> order : List.of(List.of(evens, odds), List.of(odds, evens))) {
            Peer batched = order.get(0) == evens ? batchedOneWay : batchedOtherWay;
            for (Peer bidder : order) {
                byte[] delta = bidder.space().deltaFor(batched.space().digest());
                SpaceWire.SyncDelta sync = codec.fromWire(delta, SpaceWire.SyncDelta.class);
                assertThat(sync.states()).as("the entries are held already").isEmpty();
                assertThat(sync.claims()).as("one PULL_RESP carries the bidder's every claim").hasSize(tasks);
                batched.space().applySyncDelta(delta);
            }
        }

        // The live replicas hear the held claims now.
        for (String from : live) {
            for (String to : live) {
                if (!from.equals(to)) {
                    network.release(from, to, SimNetwork.Order.FIFO, 7);
                }
            }
        }
        tickAll(6);

        for (Peer peer : List.of(coordinator, evens, odds, batchedOneWay, batchedOtherWay)) {
            for (EntryId id : ids) {
                assertThat(peer.space().currentClaim(id).orElseThrow().holder())
                        .as("the winner of %s at %s", id, peer.node()).isEqualTo(winners.get(id));
            }
        }
        assertThat(batchedOtherWay.space().digest()).as("the order of the batches changes nothing")
                .isEqualTo(batchedOneWay.space().digest());
        assertThat(batchedOneWay.space().digest()).as("batched and live end on one digest")
                .isEqualTo(coordinator.space().digest());
        assertThat(batchedOneWay.space().streamHash()).isEqualTo(coordinator.space().streamHash());

        for (EntryId id : ids) {
            boolean evensWon = winners.get(id).equals(evens.space().writer().orElseThrow());
            TakenEntry<TaskEntry> won = evensWon ? byEvens.get(id) : byOdds.get(id);
            TakenEntry<TaskEntry> lost = evensWon ? byOdds.get(id) : byEvens.get(id);
            Peer winner = evensWon ? evens : odds;
            Peer loser = evensWon ? odds : evens;
            assertThatThrownBy(() -> loser.space().complete(lost)).isInstanceOf(LeaseExpiredException.class);
            winner.space().complete(won);
        }
        tickAll(4);
        assertThat(coordinator.space().read(Template.of(TaskEntry.class))).isEmpty();
    }
}
