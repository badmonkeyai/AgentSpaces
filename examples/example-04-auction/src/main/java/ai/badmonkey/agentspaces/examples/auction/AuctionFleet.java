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
package ai.badmonkey.agentspaces.examples.auction;

import ai.badmonkey.agentspaces.agent.AgentBinder;
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
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Example 04: the AUCTION strategy end to end. Two model-worker agents price the
 * same tasks differently through {@code @BidFunction} cost models: the mini
 * worker is cheap on routine work and expensive on hard work, the premium worker
 * mirrors it, and the space allocates each task to the lowest bidder. Cost-aware
 * routing is a property of the coordination layer; no dispatcher exists.
 *
 * <p>Run: {@code mvn -q -pl examples/example-04-auction exec:java}
 */
public final class AuctionFleet {

    /** A model task with a difficulty from 1 (routine) to 10 (hard). */
    public record ModelTask(String prompt, int difficulty) {
    }

    /** A completed model call, stamped with the worker that won it. */
    public record ModelResult(String prompt, int difficulty, String worker, double cost) {
    }

    /** The small, cheap model: great value on routine tasks, poor on hard ones. */
    @AgentSpec(name = "mini-model", description = "Cheap small-model worker")
    public static class MiniModelWorker {
        /** Prices a task: linear in difficulty, cheap at the low end. */
        @BidFunction(space = "model-tasks")
        public double bid(ModelTask task) {
            return 1.0 + task.difficulty() * 2.0;   // 3 .. 21
        }

        /** Executes a task this worker won. */
        @SpaceTake(space = "model-tasks", pollTimeout = "PT0.3S")
        public ModelResult run(ModelTask task) {
            return new ModelResult(task.prompt(), task.difficulty(), "mini-model", bid(task));
        }
    }

    /** The premium model: flat pricing that only pays off on hard tasks. */
    @AgentSpec(name = "premium-model", description = "Premium large-model worker")
    public static class PremiumModelWorker {
        /** Prices a task: flat rate regardless of difficulty. */
        @BidFunction(space = "model-tasks")
        public double bid(ModelTask task) {
            return 12.0;
        }

        /** Executes a task this worker won. */
        @SpaceTake(space = "model-tasks", pollTimeout = "PT0.3S")
        public ModelResult run(ModelTask task) {
            return new ModelResult(task.prompt(), task.difficulty(), "premium-model", bid(task));
        }
    }

    /** One assembled peer of this example's fleet. */
    public record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace tasks,
                       AgentBinder binder) {

        /** Shuts the peer down. */
        public void close() {
            binder.close();
            tasks.close();
            node.close();
        }
    }

    private static final String HOST = "127.0.0.1";

    private AuctionFleet() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "auction-demo-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "auction-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.AUCTION,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer with an AUCTION task space.
     *
     * @param agentName the local agent name
     * @param port      the TCP port
     * @param seedPort  an existing member's port, or 0 for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startPeer(String agentName, int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), HOST + ":" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", HOST + ":" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        ReplicatedSpace tasks = ReplicatedSpace.builder(runtime, "model-tasks", identity, agentName)
                .settleWindow(Duration.ofMillis(200))
                .strategy(ConflictStrategyType.AUCTION)
                // No placeholder bid here: the bound worker's @BidFunction is the one
                // and only cost function this space will accept (QA4 A4-8).
                .build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), null,
                java.time.InstantSource.system());
        binder.space("model-tasks", tasks);
        node.startTicking(Duration.ofMillis(250));
        return new Peer(node, runtime, tasks, binder);
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("auction: cost-aware allocation with no dispatcher\n");
        Peer coordinator = startPeer("coordinator", 7471, 0);
        Peer mini = startPeer("mini", 7472, 7471);
        Peer premium = startPeer("premium", 7473, 7471);
        try {
            mini.binder().bind(new MiniModelWorker());
            premium.binder().bind(new PremiumModelWorker());
            Thread.sleep(1500);

            for (int difficulty = 1; difficulty <= 8; difficulty++) {
                coordinator.tasks().write(new ModelTask("task-" + difficulty, difficulty),
                        Lease.of(Duration.ofMinutes(10)));
            }
            System.out.println("8 tasks published, difficulty 1..8 "
                    + "(mini bids 1+2d, premium bids flat 12)\n");

            List<ModelResult> results = List.of();
            long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (results.size() < 8 && System.nanoTime() < deadline) {
                results = coordinator.tasks().readAll(Template.of(ModelResult.class), 20);
                Thread.sleep(300);
            }
            results.stream()
                    .sorted(java.util.Comparator.comparingInt(ModelResult::difficulty))
                    .forEach(r -> System.out.printf("  difficulty %d -> %-13s (bid %.1f)%n",
                            r.difficulty(), r.worker(), r.cost()));
            System.out.println("\nthe space allocated by cost; agents only priced their work.");
        } finally {
            premium.close();
            mini.close();
            coordinator.close();
        }
    }
}
