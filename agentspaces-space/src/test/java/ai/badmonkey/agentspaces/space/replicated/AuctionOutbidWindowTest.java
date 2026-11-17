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
 * When an AUCTION counter-claim may still be made (spec §7.4): in the first half of
 * the incumbent's settle window only. The incumbent decides it won when its window
 * ends; a counter-claim made after the middle has less than half a window to reach
 * it, and one that arrives after the decision makes two holders complete the same
 * entry, each at its own replica (Party Bus, 2026-11-15). The settle window here is
 * real (200 ms), and the claim's age is set on the test clock.
 */
class AuctionOutbidWindowTest {

    private static final Duration SETTLE = Duration.ofMillis(200);
    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zOutbidWindow", PeerIdentity.generate().peerId(), GroupId.of("zOutbidWindow"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "outbid-window",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.AUCTION,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace newPeer(String address, long seed, ToDoubleFunction<Object> bid,
                                    String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        var runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        nodes.add(node);
        return ReplicatedSpace.builder(runtime, "tasks-outbid", identity, "bidder")
                .clock(clock)
                .settleWindow(SETTLE)
                .strategy(ConflictStrategyType.AUCTION)
                .bidFunction(bid)
                .build();
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void aBetterBidCounterClaimsInTheFirstHalfOfTheSettleWindow() throws Exception {
        ReplicatedSpace coordinator = newPeer("a", 1, task -> 100.0);
        ReplicatedSpace expensive = newPeer("b", 2, task -> 10.0, "a");
        ReplicatedSpace cheap = newPeer("c", 3, task -> 2.0, "a");
        tickAll(4);
        coordinator.write(new TaskEntry("contested", 1), MINUTES_30);

        Optional<TakenEntry<TaskEntry>> costly =
                expensive.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(costly).isPresent();

        // Half a window after the incumbent's stamp: still time for the counter-claim to arrive.
        clock.advance(SETTLE.dividedBy(2));
        Optional<TakenEntry<TaskEntry>> bargain =
                cheap.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(bargain).as("the cheaper bid counter-claims in the first half").isPresent();

        assertThatThrownBy(() -> expensive.complete(costly.get()))
                .isInstanceOf(LeaseExpiredException.class);
        cheap.complete(bargain.get());
        assertThat(coordinator.read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void aBetterBidStaysOutInTheSecondHalfOfTheSettleWindow() throws Exception {
        ReplicatedSpace coordinator = newPeer("a", 1, task -> 100.0);
        ReplicatedSpace expensive = newPeer("b", 2, task -> 10.0, "a");
        ReplicatedSpace cheap = newPeer("c", 3, task -> 2.0, "a");
        tickAll(4);
        coordinator.write(new TaskEntry("late", 1), MINUTES_30);

        Optional<TakenEntry<TaskEntry>> costly =
                expensive.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(costly).isPresent();

        // Past the middle, inside the window: a counter-claim now could land after the
        // incumbent decided, so the cheaper bidder leaves the entry to it.
        clock.advance(SETTLE.dividedBy(2).plusMillis(1));
        Optional<TakenEntry<TaskEntry>> bargain =
                cheap.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(bargain).as("no counter-claim in the second half").isEmpty();

        expensive.complete(costly.get());
        assertThat(coordinator.read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(cheap.read(Template.of(TaskEntry.class))).isEmpty();
    }
}
