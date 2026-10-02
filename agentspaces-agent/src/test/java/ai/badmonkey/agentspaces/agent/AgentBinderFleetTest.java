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

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.BidFunction;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The binder's crash and auction semantics over a two-peer fleet on replicated
 * spaces (spec §10.3): a crashed worker on one peer hands its task to a worker
 * on another through lease lapse, and {@code @BidFunction} composes into the
 * AUCTION strategy so the cheaper peer is selected. The fleet runs on the
 * in-JVM loopback transport with the system clock: worker loops and settle
 * windows are real time, while gossip delivery is synchronous, so a bidder
 * always sees a task before the incumbent's settle window closes and the
 * auction outcome is decided by the bids alone.
 */
class AgentBinderFleetTest {

    /** Never completes: every attempt throws, so its take lease always lapses. */
    @AgentSpec(name = "crasher")
    public static class AlwaysCrashes {
        final AtomicInteger attempts = new AtomicInteger();

        @SpaceTake(space = "work", lease = "PT0.3S", pollTimeout = "PT0.1S")
        public FindingEntry research(TaskEntry task) {
            attempts.incrementAndGet();
            throw new IllegalStateException("simulated crash");
        }
    }

    /** Always completes. */
    @AgentSpec(name = "reliable")
    public static class Reliable {
        @SpaceTake(space = "work", lease = "PT10M", pollTimeout = "PT0.1S")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "reliable: " + task.topic());
        }
    }

    /** Bids low: the cheap worker the auction should select. */
    @AgentSpec(name = "cheap")
    public static class Cheap {
        @BidFunction(space = "work")
        public double bid(TaskEntry task) {
            return 1;
        }

        @SpaceTake(space = "work", pollTimeout = "PT0.1S")
        public FindingEntry work(TaskEntry task) {
            return new FindingEntry(task.topic(), "cheap");
        }
    }

    /** Bids high: should lose every auction it contests. */
    @AgentSpec(name = "pricey")
    public static class Pricey {
        @BidFunction(space = "work")
        public double bid(TaskEntry task) {
            return 100;
        }

        @SpaceTake(space = "work", pollTimeout = "PT0.1S")
        public FindingEntry work(TaskEntry task) {
            return new FindingEntry(task.topic(), "pricey");
        }
    }

    private record Peer(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                        ReplicatedSpace work, AgentBinder binder) {
    }

    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Peer peer : peers) {
            peer.binder().close();
            peer.work().close();
            peer.node().close();
        }
    }

    /** SPEC §10.3: a crash on one peer lets the lease lapse and another peer's worker finishes the task. */
    @Test
    @Timeout(90)
    void aCrashedWorkersTaskReappearsForAWorkerOnAnotherPeer() throws Exception {
        Peer crasherPeer = startPeer("crasher", null, ConflictStrategyType.LEASE_RACE, "crash-handoff-v1");
        Peer reliablePeer = startPeer("reliable", "crasher", ConflictStrategyType.LEASE_RACE,
                "crash-handoff-v1");
        AlwaysCrashes crasher = new AlwaysCrashes();
        crasherPeer.binder().bind(crasher);

        crasherPeer.work().write(new TaskEntry("handoff", 1), Lease.of(Duration.ofMinutes(10)));
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (crasher.attempts.get() < 1 && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertThat(crasher.attempts.get()).isGreaterThanOrEqualTo(1);
        // The worker "dies" after its crash: its binder stops, so nothing on
        // that peer renews or re-takes, and the TAKE lease simply lapses. A
        // worker that kept re-taking and re-crashing would instead be a poison
        // loop contending every race, which is not the crash-recovery idiom.
        crasherPeer.binder().close();

        reliablePeer.binder().bind(new Reliable());

        assertThat(reliablePeer.work().read(Template.of(FindingEntry.class), Duration.ofSeconds(40)))
                .hasValueSatisfying(f -> assertThat(f.summary()).isEqualTo("reliable: handoff"));
        assertThat(crasherPeer.work().read(Template.of(FindingEntry.class), Duration.ofSeconds(20)))
                .hasValueSatisfying(f -> assertThat(f.summary()).isEqualTo("reliable: handoff"));
        assertThat(reliablePeer.work().read(Template.of(TaskEntry.class), Duration.ofSeconds(5)))
                .isEmpty();
    }

    /** SPEC §10.3: @BidFunction composes into AUCTION; lower = cheaper = selected across peers. */
    @Test
    @Timeout(120)
    void theCheaperBidWinsTheAuctionAcrossPeers() throws Exception {
        Peer priceyPeer = startPeer("pricey", null, ConflictStrategyType.AUCTION, "auction-v1");
        Peer cheapPeer = startPeer("cheap", "pricey", ConflictStrategyType.AUCTION, "auction-v1");
        priceyPeer.binder().bind(new Pricey());
        cheapPeer.binder().bind(new Cheap());
        awaitMutualMembership(priceyPeer, cheapPeer); // claims must gossip both ways

        // Each task is written on the pricey peer's replica, so pricey claims
        // first and the cheaper bid must counter-claim inside the settle window
        // to win. Tasks are contested one at a time: both workers are
        // single-threaded take loops, so a task that appears while the cheap
        // worker is still settling an earlier one has only one bidder, and the
        // auction correctly awards it to that bidder (SPEC §7.4 allocates
        // among the current bidders, not among every peer that could bid).
        for (int i = 0; i < 3; i++) {
            String topic = "auction-" + i;
            priceyPeer.work().write(new TaskEntry(topic, i), Lease.of(Duration.ofMinutes(10)));
            assertThat(cheapPeer.work().read(
                    Template.of(FindingEntry.class).where("topic", eq(topic)), Duration.ofSeconds(30)))
                    .hasValueSatisfying(f -> assertThat(f.summary()).isEqualTo("cheap"));
        }
        assertThat(priceyPeer.work().readAll(Template.of(FindingEntry.class), 10))
                .extracting(FindingEntry::summary).containsOnly("cheap");
    }

    // ------------------------------------------------------------------ fixture

    private static GroupAdvertisement group(ConflictStrategyType strategy, String founding) {
        GroupId groupId = GroupId.fromFounding(founding.getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), founding,
                GroupAdvertisement.MembershipPolicy.OPEN, strategy,
                GroupAdvertisement.GossipParameters.defaults());
    }

    private void awaitMutualMembership(Peer a, Peer b) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline
                && (a.runtime().membership().member(b.identity().peerId()).isEmpty()
                || b.runtime().membership().member(a.identity().peerId()).isEmpty())) {
            Thread.sleep(50);
        }
        assertThat(a.runtime().membership().member(b.identity().peerId())).isPresent();
        assertThat(b.runtime().membership().member(a.identity().peerId())).isPresent();
    }

    private Peer startPeer(String address, String seedAddress, ConflictStrategyType strategy,
                           String founding) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).randomSeed(peers.size() + 1).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = seedAddress == null ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("mem", seedAddress, 0));
        GroupRuntime runtime = node.joinGroup(group(strategy, founding),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        ReplicatedSpace work = ReplicatedSpace.builder(runtime, "work", identity, "host")
                .strategy(strategy)
                .settleWindow(strategy == ConflictStrategyType.AUCTION
                        ? Duration.ofSeconds(1) : Duration.ofMillis(150))
                .build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), null, InstantSource.system());
        binder.space("work", work);
        node.startTicking(Duration.ofMillis(250));
        Peer peer = new Peer(node, identity, runtime, work, binder);
        peers.add(peer);
        return peer;
    }
}
