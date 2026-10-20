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
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
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
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The snippets in the root README's "How to use AI with AgentSpaces" section,
 * verbatim, run over the in-JVM {@code mem} transport (FEATURES.md §15 item
 * 18: every documentation snippet is cut from checked-in code that runs under
 * the gate). A desk peer writes orders and binds no agent; worker peers take
 * them. {@link Fulfiller} is the README's opening class.
 */
class ReadmeExamplesTest {

    /** An order somebody wants fulfilled. */
    public record Order(String orderId, String item) {
    }

    /** The worker's output. */
    public record Shipment(String orderId, String item, String by) {
    }

    /** The README's opening worker: takes orders under a lease, ships them. */
    @AgentSpec(name = "fulfiller", description = "Ships orders")
    public static class Fulfiller {

        @SpaceTake(lease = "10m")
        public Shipment ship(Order order) {
            return new Shipment(order.orderId(), order.item(), "fulfiller");
        }
    }

    /** A worker that dies on every order: the exception lets its take lease lapse. */
    @AgentSpec(name = "crasher", description = "Dies mid-order")
    public static class Crasher {
        final AtomicInteger attempts = new AtomicInteger();

        @SpaceTake(lease = "300ms", pollTimeout = "100ms")
        public Shipment ship(Order order) {
            attempts.incrementAndGet();
            throw new IllegalStateException("simulated crash");
        }
    }

    /** One assembled peer: node, group, the shared space, and a binder over it. */
    private record Peer(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                        ReplicatedSpace space, AgentBinder binder) {
    }

    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Peer peer : peers) {
            peer.binder().close();
            peer.space().close();
            peer.node().close();
        }
    }

    /** README: another peer writes what the worker takes; the result replicates back to the writer. */
    @Test
    @Timeout(60)
    void anotherPeerWritesWhatTheWorkerTakes() throws Exception {
        Peer desk = startPeer("desk", null);            // writes orders, binds no agent
        Peer worker = startPeer("worker", "desk");      // seeds from the desk
        worker.binder().bind(new Fulfiller());

        desk.space().write(new Order("ord-1", "kite"), Lease.of(Duration.ofMinutes(10)));

        assertThat(desk.space().read(Template.of(Shipment.class), Duration.ofSeconds(20)))
                .hasValueSatisfying(s -> assertThat(s.by()).isEqualTo("fulfiller"));
        assertThat(desk.space().readAll(Template.of(Order.class), 10)).isEmpty();
    }

    /** README: a crashed worker's take reappears for a worker on another peer. */
    @Test
    @Timeout(90)
    void aCrashedWorkersOrderReappearsForAWorkerOnAnotherPeer() throws Exception {
        Peer desk = startPeer("desk", null);
        Peer flaky = startPeer("flaky", "desk");
        Peer steady = startPeer("steady", "desk");
        Crasher crasher = new Crasher();
        flaky.binder().bind(crasher);

        desk.space().write(new Order("ord-2", "compass"), Lease.of(Duration.ofMinutes(10)));
        await(() -> crasher.attempts.get() >= 1);       // flaky took the order and died
        flaky.binder().close();                         // the process is gone; nothing renews its lease

        steady.binder().bind(new Fulfiller());          // the lease lapses; the order reappears here
        assertThat(desk.space().read(Template.of(Shipment.class), Duration.ofSeconds(40)))
                .hasValueSatisfying(s -> assertThat(s.orderId()).isEqualTo("ord-2"));
        assertThat(desk.space().readAll(Template.of(Order.class), 10)).isEmpty();
    }

    // ------------------------------------------------------------------ fixture

    private static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding("readme-examples-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "readme-examples",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /** The same assembly as example 11's {@code startPeer}, on the mem transport instead of TCP. */
    private Peer startPeer(String address, String seedAddress) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).randomSeed(peers.size() + 1).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = seedAddress == null ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("mem", seedAddress, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "work", identity, "host")
                .settleWindow(Duration.ofMillis(150))
                .build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), null, InstantSource.system());
        binder.space("work", space);
        node.startTicking(Duration.ofMillis(250));
        Peer peer = new Peer(node, identity, runtime, space, binder);
        peers.add(peer);
        return peer;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
