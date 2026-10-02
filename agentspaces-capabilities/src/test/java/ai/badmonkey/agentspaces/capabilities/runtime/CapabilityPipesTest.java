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
package ai.badmonkey.agentspaces.capabilities.runtime;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The direct-pipe binding (SPEC §8, §9 {@code PIPE_DATA}): one multiplexer per
 * group runtime routes frames to the handler registered per capability type, so
 * several capability protocols share the wire without stepping on each other,
 * and frames nobody registered for, or that do not parse, are dropped quietly.
 */
class CapabilityPipesTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zPipes");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zPipes", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "pipes",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, GroupRuntime runtime, CapabilityPipes pipes) {
    }

    /** Mirror of the multiplexer's private frame record, for hand-built frames. */
    private record PipeFrame(String capability, byte[] payload) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Wired newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        nodes.add(node);
        return new Wired(node, runtime, new CapabilityPipes(runtime, codec));
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
    }

    private static String text(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }

    /** SPEC §8 direct binding: two capabilities multiplexed on one pipe each receive only their own frames, attributed to the authenticated sender. */
    @Test
    void framesRouteToTheHandlerOfTheirCapability() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(4);

        List<String> xSaw = new CopyOnWriteArrayList<>();
        List<String> ySaw = new CopyOnWriteArrayList<>();
        List<PeerId> senders = new CopyOnWriteArrayList<>();
        b.pipes().onCapability("aspace:cap/x", (from, payload) -> {
            senders.add(from);
            xSaw.add(text(payload));
        });
        b.pipes().onCapability("aspace:cap/y", (from, payload) -> {
            senders.add(from);
            ySaw.add(text(payload));
        });

        a.pipes().send(b.node().peerId(), "aspace:cap/x", "x-1".getBytes(StandardCharsets.UTF_8));
        a.pipes().send(b.node().peerId(), "aspace:cap/y", "y-1".getBytes(StandardCharsets.UTF_8));
        a.pipes().send(b.node().peerId(), "aspace:cap/x", "x-2".getBytes(StandardCharsets.UTF_8));

        assertThat(xSaw).containsExactly("x-1", "x-2");
        assertThat(ySaw).containsExactly("y-1");
        assertThat(senders).containsOnly(a.node().peerId()).hasSize(3);
    }

    /** SPEC §8 / P6: a frame for a capability nobody registered here is dropped; the other handlers are untouched. */
    @Test
    void framesForUnregisteredCapabilitiesAreDropped() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(4);

        List<String> xSaw = new CopyOnWriteArrayList<>();
        b.pipes().onCapability("aspace:cap/x", (from, payload) -> xSaw.add(text(payload)));

        a.pipes().send(b.node().peerId(), "aspace:cap/nobody-home",
                "lost".getBytes(StandardCharsets.UTF_8));
        a.pipes().send(b.node().peerId(), "aspace:cap/x", "kept".getBytes(StandardCharsets.UTF_8));

        assertThat(xSaw).containsExactly("kept");
    }

    /** SPEC §9: PIPE_DATA bodies that are not a well-formed pipe frame, or name no capability, are ignored without disturbing the pipe. */
    @Test
    void malformedAndCapabilityLessFramesAreIgnored() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(4);

        List<String> xSaw = new CopyOnWriteArrayList<>();
        b.pipes().onCapability("aspace:cap/x", (from, payload) -> xSaw.add(text(payload)));

        // Not a frame at all: a bare CBOR byte string on the PIPE_DATA kind.
        a.runtime().send(b.node().peerId(), Envelope.Kind.PIPE_DATA, new byte[]{1, 2, 3});
        // A frame shape with no capability named.
        a.runtime().send(b.node().peerId(), Envelope.Kind.PIPE_DATA,
                new PipeFrame(null, "orphan".getBytes(StandardCharsets.UTF_8)));
        // The pipe still works afterwards.
        a.pipes().send(b.node().peerId(), "aspace:cap/x", "alive".getBytes(StandardCharsets.UTF_8));

        assertThat(xSaw).containsExactly("alive");
    }

    /** SPEC §8 direct binding is bidirectional: a handler can answer the sender over the same multiplexer. */
    @Test
    void handlersCanReplyOverTheSamePipe() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(4);

        List<String> aSaw = new CopyOnWriteArrayList<>();
        a.pipes().onCapability("aspace:cap/echo", (from, payload) -> aSaw.add(text(payload)));
        b.pipes().onCapability("aspace:cap/echo", (from, payload) ->
                b.pipes().send(from, "aspace:cap/echo",
                        ("echo:" + text(payload)).getBytes(StandardCharsets.UTF_8)));

        a.pipes().send(b.node().peerId(), "aspace:cap/echo", "hi".getBytes(StandardCharsets.UTF_8));

        assertThat(aSaw).containsExactly("echo:hi");
    }
}
