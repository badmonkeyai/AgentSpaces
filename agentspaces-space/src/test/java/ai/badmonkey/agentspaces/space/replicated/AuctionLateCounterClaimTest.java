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
import java.util.function.ToDoubleFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An AUCTION counter-claim that reaches the incumbent only after the incumbent judged its take (example 18,
 * 2026-11-15: a junior adjuster outbid a senior one at the edge of its settle window, both priced the claim, and the
 * flow saw two results for one claim). Travel time has no bound, so the half-window rule of
 * {@link AuctionOutbidWindowTest} makes this rare and cannot rule it out. The incumbent completes, its completion
 * reaches the counter-claimant, and is dropped there, since the better bid holds the entry and a completion
 * authenticates against the claim held (SPEC §11a.4). The counter-claimant must still not do the work a second time:
 * its take stands down and acknowledges the incumbent's completion under its own claim, which every replica
 * authenticates, so the replicas converge on one completion and one result.
 *
 * <p>The counter-claim is held on the wire ({@link SimNetwork#hold}) so the incumbent never sees it before it
 * completes; the order of everything else is scripted. The claims' ages are set on the test clock.
 */
class AuctionLateCounterClaimTest {

    /**
     * Long enough that the scripted steps between the counter-claim and the incumbent's completion (a few
     * milliseconds measured) always fit inside the counter-claimant's window: a miss means something is clearly
     * broken, not slow.
     */
    private static final Duration SETTLE = Duration.ofSeconds(2);
    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zLateCounterClaim", PeerIdentity.generate().peerId(), GroupId.of("zLateCounterClaim"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "late-counter-claim",
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
        return ReplicatedSpace.builder(runtime, "tasks-late", identity, "bidder")
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

    /** Holds every frame the counter-claimant sends, so the incumbent judges and completes without its claim. */
    private void muteCounterClaimant() {
        network.hold("c", "a");
        network.hold("c", "b");
    }

    private void unmuteCounterClaimant() {
        network.release("c", "a", SimNetwork.Order.FIFO, 0);
        network.release("c", "b", SimNetwork.Order.FIFO, 0);
    }

    @Test
    void aCompletionArrivingInTheCounterClaimantsWindowMakesItsTakeStandDown() throws Exception {
        ReplicatedSpace coordinator = newPeer("a", 1, task -> 100.0);
        ReplicatedSpace incumbent = newPeer("b", 2, task -> 10.0, "a");
        ReplicatedSpace cheap = newPeer("c", 3, task -> 2.0, "a");
        tickAll(4);
        coordinator.write(new TaskEntry("contested", 1), MINUTES_30);
        muteCounterClaimant();

        Optional<TakenEntry<TaskEntry>> costly =
                incumbent.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(costly).as("the incumbent judged its take and won").isPresent();

        // Inside the first half of the incumbent's window by its stamp, so the cheaper bid counter-claims.
        clock.advance(SETTLE.dividedBy(2));
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> bargain = CompletableFuture.supplyAsync(
                () -> cheap.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO));
        long waitFrom = System.nanoTime();
        while (network.heldCount("c", "a") + network.heldCount("c", "b") == 0 && !bargain.isDone()) {
            assertThat(System.nanoTime() - waitFrom).as("the counter-claim was sent")
                    .isLessThan(TimeUnit.SECONDS.toNanos(30));
            Thread.sleep(1);
        }
        assertThat(bargain).as("the counter-claimant is in its settle window").isNotDone();
        long sinceClaim = System.nanoTime();

        // The incumbent never heard of the counter-claim: it completes, and its completion reaches c in c's window.
        incumbent.complete(costly.get(), new FindingEntry("contested", "done by b"), MINUTES_30);
        System.out.printf("incumbent completed %d ms after the counter-claim was sent (window %d ms)%n",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sinceClaim), SETTLE.toMillis());

        assertThat(bargain.get(30, TimeUnit.SECONDS))
                .as("the counter-claimant stands down: the work is done").isEmpty();
        assertThat(cheap.outbidAcknowledged()).isEqualTo(1);

        unmuteCounterClaimant();
        tickAll(3);
        for (ReplicatedSpace replica : List.of(coordinator, incumbent, cheap)) {
            assertThat(replica.read(Template.of(TaskEntry.class))).as("the entry is completed everywhere").isEmpty();
            assertThat(replica.readAll(Template.of(FindingEntry.class), 10))
                    .as("one result, the incumbent's").containsExactly(new FindingEntry("contested", "done by b"));
        }
    }

    @Test
    void aCompletionArrivingAfterTheCounterClaimantWonStopsItsResult() throws Exception {
        ReplicatedSpace coordinator = newPeer("a", 1, task -> 100.0);
        ReplicatedSpace incumbent = newPeer("b", 2, task -> 10.0, "a");
        ReplicatedSpace cheap = newPeer("c", 3, task -> 2.0, "a");
        tickAll(4);
        coordinator.write(new TaskEntry("contested", 1), MINUTES_30);
        muteCounterClaimant();

        Optional<TakenEntry<TaskEntry>> costly =
                incumbent.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(costly).isPresent();

        // Nothing from the incumbent reaches c either, so c's counter-claim wins its own window too.
        network.hold("a", "c");
        network.hold("b", "c");
        clock.advance(SETTLE.dividedBy(2));
        Optional<TakenEntry<TaskEntry>> bargain =
                cheap.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(bargain).as("two holders each judged themselves the winner").isPresent();

        incumbent.complete(costly.get(), new FindingEntry("contested", "done by b"), MINUTES_30);
        // The incumbent's completion reaches c while c is still working; c's own result must not be written.
        network.release("a", "c", SimNetwork.Order.FIFO, 0);
        network.release("b", "c", SimNetwork.Order.FIFO, 0);
        assertThatThrownBy(() -> cheap.complete(bargain.get(), new FindingEntry("contested", "done by c"), MINUTES_30))
                .isInstanceOf(LeaseExpiredException.class)
                .hasMessageContaining("completed first");
        assertThat(cheap.outbidAcknowledged()).isEqualTo(1);

        unmuteCounterClaimant();
        tickAll(3);
        for (ReplicatedSpace replica : List.of(coordinator, incumbent, cheap)) {
            assertThat(replica.read(Template.of(TaskEntry.class))).as("the entry is completed everywhere").isEmpty();
            assertThat(replica.readAll(Template.of(FindingEntry.class), 10))
                    .as("one result, the incumbent's").containsExactly(new FindingEntry("contested", "done by b"));
        }
    }

    @Test
    void anotherPeersCompletionUnderAClaimNobodyOutbidIsNoEvidence() throws Exception {
        ReplicatedSpace coordinator = newPeer("a", 1, task -> 100.0);
        ReplicatedSpace incumbent = newPeer("b", 2, task -> 2.0, "a");
        ReplicatedSpace dearer = newPeer("c", 3, task -> 10.0, "a");
        tickAll(4);
        coordinator.write(new TaskEntry("plain", 1), MINUTES_30);

        // The cheaper bid holds the entry; a dearer one may not counter-claim, so c never holds it, and b's
        // completion merges everywhere as usual. No take stands down and nothing is noted.
        Optional<TakenEntry<TaskEntry>> taken =
                incumbent.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();
        assertThat(dearer.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)).isEmpty();
        incumbent.complete(taken.get(), new FindingEntry("plain", "done by b"), MINUTES_30);
        tickAll(3);
        for (ReplicatedSpace replica : List.of(coordinator, incumbent, dearer)) {
            assertThat(replica.read(Template.of(TaskEntry.class))).isEmpty();
            assertThat(replica.outbidAcknowledged()).isZero();
        }
    }
}
