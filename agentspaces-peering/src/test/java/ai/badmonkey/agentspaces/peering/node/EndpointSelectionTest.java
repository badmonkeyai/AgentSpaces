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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Endpoint selection (spec §5.5): a dialer tries a member's advertised
 * endpoints in ascending priority, not in list order, skips schemes it does
 * not speak, and stops at the first that connects.
 */
class EndpointSelectionTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zEndpoints");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zEndpoints",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "endpoints", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    /** A transport whose every dial fails, counting the attempts. */
    static final class FlakyTransport implements Transport {
        final AtomicInteger dials = new AtomicInteger();

        @Override
        public String scheme() {
            return "flaky";
        }

        @Override
        public TransportConnection dial(String address) throws IOException {
            dials.incrementAndGet();
            throw new IOException("flaky transport never connects: " + address);
        }

        @Override
        public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) {
            return () -> {
            };
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(long seed) {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).build();
        nodes.add(node);
        return node;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /**
     * Wires A (advertising mem and flaky at the given priorities, listened in
     * mem-then-flaky order) and B (mem, plus the flaky dialer when given),
     * converges them, then breaks B's cached link to A so B's next frame must
     * dial A afresh from its advertised endpoints.
     */
    private record Pair(PeerNode a, PeerNode b, GroupRuntime ra, GroupRuntime rb) {
    }

    private Pair wire(int memPriority, int flakyPriority, FlakyTransport flakyAtB) throws Exception {
        PeerNode a = node(1);
        a.listen(network.register("a"), "a", memPriority);
        a.listen(new FlakyTransport(), "a-flaky", flakyPriority);
        PeerNode b = node(2);
        b.listen(network.register("b"), "b");
        if (flakyAtB != null) {
            b.transport(flakyAtB);
        }
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(3);
        assertThat(rb.membership().member(a.peerId())).hasValueSatisfying(m ->
                assertThat(m.endpoints()).extracting(PeerAdvertisement.Endpoint::transport)
                        .as("list order is mem then flaky, whatever the priorities")
                        .containsExactly("mem", "flaky"));
        // A brief partition makes B's cached connection to A fail and be dropped.
        network.partition("a", "b");
        b.tick();
        network.heal();
        assertThat(b.channelModes()).doesNotContainKey(a.peerId());
        return new Pair(a, b, ra, rb);
    }

    /** Spec §5.5: ascending priority wins over list order; the failing priority-0 scheme is tried first, and the frame still arrives over the next. */
    @Test
    void endpointsAreDialledInAscendingPriorityNotListOrder() throws Exception {
        FlakyTransport flaky = new FlakyTransport();
        Pair pair = wire(1, 0, flaky);
        List<String> atA = new CopyOnWriteArrayList<>();
        pair.ra().onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                atA.add(new String(body, StandardCharsets.UTF_8)));
        flaky.dials.set(0);

        pair.rb().send(pair.a().peerId(), Envelope.Kind.PIPE_DATA,
                "over the fallback".getBytes(StandardCharsets.UTF_8));

        assertThat(flaky.dials).as("flaky at priority 0 was tried first").hasValue(1);
        assertThat(atA).containsExactly("over the fallback");
    }

    /** Spec §5.5: when the lowest-priority endpoint connects, higher-priority (later) ones are never dialled. */
    @Test
    void aLaterEndpointIsNeverDialledWhenAnEarlierOneConnects() throws Exception {
        FlakyTransport flaky = new FlakyTransport();
        Pair pair = wire(0, 1, flaky);
        List<String> atA = new CopyOnWriteArrayList<>();
        pair.ra().onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                atA.add(new String(body, StandardCharsets.UTF_8)));
        flaky.dials.set(0);

        pair.rb().send(pair.a().peerId(), Envelope.Kind.PIPE_DATA,
                "first choice".getBytes(StandardCharsets.UTF_8));
        pair.rb().send(pair.a().peerId(), Envelope.Kind.PIPE_DATA,
                "cached now".getBytes(StandardCharsets.UTF_8));

        assertThat(flaky.dials).as("mem succeeded first; flaky never dialled").hasValue(0);
        assertThat(atA).containsExactly("first choice", "cached now");
    }

    /** Spec §5.5: an endpoint whose scheme the dialer does not speak is skipped, whatever its priority. */
    @Test
    void unknownSchemesAreSkipped() throws Exception {
        Pair pair = wire(1, 0, null);
        List<String> atA = new CopyOnWriteArrayList<>();
        pair.ra().onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                atA.add(new String(body, StandardCharsets.UTF_8)));

        pair.rb().send(pair.a().peerId(), Envelope.Kind.PIPE_DATA,
                "skipped the stranger scheme".getBytes(StandardCharsets.UTF_8));

        assertThat(atA).containsExactly("skipped the stranger scheme");
    }
}
