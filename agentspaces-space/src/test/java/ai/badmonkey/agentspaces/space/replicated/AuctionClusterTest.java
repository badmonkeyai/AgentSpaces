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
import java.util.List;
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

    private record Peer(PeerNode node, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, ToDoubleFunction<Object> bid,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace
                .builder(runtime, "tasks-auction", identity, "bidder")
                .clock(clock)
                .settleWindow(Duration.ZERO)
                .strategy(ConflictStrategyType.AUCTION)
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
}
