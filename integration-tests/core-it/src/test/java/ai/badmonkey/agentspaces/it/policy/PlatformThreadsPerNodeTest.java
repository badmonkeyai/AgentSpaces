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
package ai.badmonkey.agentspaces.it.policy;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.time.SystemTime;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Ports;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code core.threads.platform-per-node} (srad/agentspaces-event-loop.md EL-X-03, EL-Q-PRF-05, §7.3): ten nodes in one
 * JVM over TCP, each with the per-node pieces that once owned platform threads (the tick and the flusher in
 * {@code PeerNode}, a local space's sweeper, the capability card refresh) and the ones that run on the loop now (a
 * replicated space, a binder's take bindings, the coalescer). The platform threads alive with one node are compared with
 * those alive with ten: the five per-process sites of the thread policy's allowlist may be there (the timer, the
 * watchdog, the loop pool within its bound, the two CPU pools), with the JDK's virtual-thread carriers; anything else
 * that came with nodes two to ten is per node.
 * {@link ThreadPolicyTest} rules on the code; this reads the running JVM. No sleep: the test waits on the views.
 */
class PlatformThreadsPerNodeTest {

    private static final int NODES = 10;
    private static final CborCodec CODEC = CborCodec.defaultCodec();
    /** The allowlist's per-process thread names (thread-policy.txt). */
    private static final String CARRIER = "jdk.internal.misc.CarrierThread";
    private static final String CARRIERS = "jdk-carrier:";
    private static final List<String> PER_PROCESS = List.of("agentspaces-timer", "agentspaces-watchdog",
            "agentspaces-loop-", "space-verify-", "agentspaces-blake3", CARRIERS);

    /** A take binding, as an agent holds. */
    public record Order(String id) {
    }

    /** One take binding: its lease renewer is a task on the node's time source. */
    public static class Worker {
        /** Ships an order. */
        @SpaceTake(lease = "10m")
        public void ship(Order o) {
        }
    }

    private record Node(PeerNode node, GroupRuntime runtime, LocalSpace local, CapabilityRuntime caps,
                        AgentBinder binder) implements AutoCloseable {
        @Override
        public void close() {
            binder.close();
            caps.close();
            local.close();
            node.close();
        }
    }

    @Test
    void tenNodesInOneJvmOwnNoPlatformThreadOfTheirOwn() throws Exception {
        String founding = "platform-threads-" + Long.toHexString(System.nanoTime());
        List<Node> nodes = new ArrayList<>();
        try {
            int seed = Ports.free();
            nodes.add(start(founding, seed, 0));
            await(nodes, 0);
            Map<Long, String> one = platformThreads();
            for (int i = 1; i < NODES; i++) {
                nodes.add(start(founding, Ports.free(), seed));
            }
            await(nodes, NODES - 1);
            Map<Long, String> ten = platformThreads();

            Map<String, Long> process = new TreeMap<>(); // per-process site -> threads with ten nodes
            List<String> came = new ArrayList<>(); // non-process threads that came with nodes two to ten
            ten.forEach((id, name) -> {
                String site = site(name);
                if (site != null) {
                    process.merge(site, 1L, Long::sum);
                } else if (!one.containsKey(id)) {
                    came.add(name);
                }
            });
            double perNode = came.size() / (double) (NODES - 1);
            int poolMax = TimeSource.system() instanceof SystemTime st ? st.loopPool().stats().max() : 0;
            System.out.println("PlatformThreadsPerNodeTest: platform threads with one node " + one.size() + ", with "
                    + NODES + " " + ten.size() + "; per-process sites " + process + " (pool max " + poolMax
                    + "); came with nodes 2 to " + NODES + ": " + came);
            Measure.record("core.threads.platform-per-node", Unit.COUNT, Direction.LOWER, perNode,
                    Rec.with().kind(Kind.COUNT).params(Params.of("nodes", NODES, "transport", "tcp")));

            assertThat(came).as("platform threads that came with nodes 2 to %d (EL-X-03)", NODES).isEmpty();
            assertThat(process.getOrDefault("agentspaces-timer", 0L)).as("one timer thread per process").isEqualTo(1);
            assertThat(process.getOrDefault("agentspaces-watchdog", 0L)).as("one watchdog thread per process")
                    .isEqualTo(1);
            assertThat(process.getOrDefault("agentspaces-loop-", 0L)).as("the loop pool within its bound")
                    .isBetween(1L, (long) poolMax);
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                nodes.get(i).close();
            }
        }
    }

    private static String site(String name) {
        return PER_PROCESS.stream().filter(name::startsWith).findFirst().orElse(null);
    }

    /**
     * Live platform threads by id (getAllStackTraces leaves virtual threads out). The virtual-thread scheduler's
     * carriers are platform threads too; they are the JDK's, per process and bounded by its parallelism, and grow with
     * any virtual work, so they are named as their own site rather than by their {@code ForkJoinPool-N-worker} name.
     */
    private static Map<Long, String> platformThreads() {
        return Thread.getAllStackTraces().keySet().stream().filter(t -> !t.isVirtual())
                .collect(Collectors.toMap(Thread::threadId,
                        t -> CARRIER.equals(t.getClass().getName()) ? CARRIERS + t.getName() : t.getName(),
                        (a, b) -> a, TreeMap::new));
    }

    private static void await(List<Node> nodes, int others) {
        Measure.await("core.harness.wait", Duration.ofSeconds(60), () -> nodes.stream()
                        .allMatch(n -> n.runtime().membership().allMembers().size() >= others),
                Wait.with().poll(Duration.ofMillis(25)).params(Params.of("what", "converge", "members", NODES)));
    }

    private static Node start(String founding, int port, int seed) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).coalesce(Duration.ofMillis(20)).build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        GroupId groupId = GroupId.fromFounding(founding.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement ad = new GroupAdvertisement("aspace://" + groupId.value(), identity.peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(365), founding, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        List<PeerAdvertisement.Endpoint> seeds = seed == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seed, 0));
        GroupRuntime runtime = node.joinGroup(ad, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace work = ReplicatedSpace.builder(runtime, "work", identity, "host").build();
        LocalSpace local = LocalSpace.builder("scratch", identity.agent("scratch"))
                .sweepEvery(Duration.ofSeconds(1)).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(CODEC, InstantSource.system()), CODEC,
                identity.peerId());
        CapabilityRuntime caps = new CapabilityRuntime(runtime, discovery, identity).autoRefresh(Duration.ofMinutes(5));
        AgentBinder binder = new AgentBinder(identity, runtime.id(), null, InstantSource.system());
        binder.space("work", work);
        binder.bind(new Worker());
        node.startTicking(Duration.ofMillis(250));
        return new Node(node, runtime, local, caps, binder);
    }
}
