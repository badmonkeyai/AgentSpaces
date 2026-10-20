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

package ai.badmonkey.agentspaces.transport.webrtc;

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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * Scenario S3's fleet on the fake engine and a test clock (SRAD srad/agentspaces-js.md JS-W-11):
 * a RELAY-role seed, Alice dial-only on the fabric with a {@code webrtc} dialer, Bob dial-only on
 * the fabric with a {@code webrtc} acceptor, so the two can reach each other only through the
 * seed until the offload opens a channel. Ticks are hand-driven and advance the clock a second
 * each, so the offload's minute window and its backoff run on simulated time while the engine's
 * events and the dial timeout run on real time.
 */
final class RelayedFleet implements AutoCloseable {

    /** The head of a {@code PIPE_DATA} body, as {@code CapabilityPipes} encodes it. */
    record PipeFrame(String capability, byte[] payload) {
    }

    static final String APP = "aspace:cap/demo";

    final CborCodec codec = CborCodec.defaultCodec();
    final TestClock clock = TestClock.create();
    final SimNetwork network = new SimNetwork();
    final FakeEngine engine = new FakeEngine();
    final GroupId groupId = GroupId.of("zRelayOffload");
    final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zRelayOffload",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "relay-offload", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    final PeerIdentity seedId = PeerIdentity.generate();
    final PeerIdentity aliceId = PeerIdentity.generate();
    final PeerIdentity bobId = PeerIdentity.generate();
    final PeerNode seed;
    final PeerNode alice;
    final PeerNode bob;
    final WebRtcTransport aliceRtc;
    final WebRtcTransport bobRtc;
    final List<PeerNode> nodes = new ArrayList<>();
    GroupRuntime atSeed;
    GroupRuntime atAlice;
    GroupRuntime atBob;
    final List<String> atBobSaw = new CopyOnWriteArrayList<>();
    final List<String> atAliceSaw = new CopyOnWriteArrayList<>();

    RelayedFleet(WebRtcOptions options, PeerNode.RelayOffload offload) throws Exception {
        seed = PeerNode.builder(seedId).clock(clock).randomSeed(1)
                .roles(Set.of(PeerAdvertisement.PeerRole.RELAY)).build();
        alice = PeerNode.builder(aliceId).clock(clock).randomSeed(2).relayOffload(offload).build();
        bob = PeerNode.builder(bobId).clock(clock).randomSeed(3).relayOffload(offload).build();
        nodes.addAll(List.of(seed, alice, bob));
        aliceRtc = new WebRtcTransport(aliceId.peerId(), options, engine);
        bobRtc = new WebRtcTransport(bobId.peerId(), options, engine);
        seed.listen(network.register("seed"), "seed");
        alice.transport(network.register("alice"));   // dial-only on the fabric
        alice.transport(aliceRtc);                      // and a webrtc dialer
        bob.transport(network.register("bob"));       // dial-only on the fabric too
        bob.listen(bobRtc, WebRtcTransport.SIG, 0);     // the webrtc acceptor is Bob's only endpoint
    }

    /** Joins the three through the seed and ticks until each side holds the other's advertisement. */
    void converge() throws Exception {
        List<PeerAdvertisement.Endpoint> seeds = List.of(new PeerAdvertisement.Endpoint("mem", "seed", 0));
        atSeed = seed.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        atAlice = alice.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        atBob = bob.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        route(atAlice, aliceRtc, (from, payload) -> {
            if (from.equals(bobId.peerId())) {
                atAliceSaw.add(new String(payload, StandardCharsets.UTF_8));
            }
        });
        route(atBob, bobRtc, (from, payload) -> {
            if (from.equals(aliceId.peerId())) {
                atBobSaw.add(new String(payload, StandardCharsets.UTF_8));
            }
        });
        await("membership to converge through the seed", () ->
                atAlice.membership().member(bobId.peerId()).map(m -> m.endpoints().equals(List.of(
                        new PeerAdvertisement.Endpoint("webrtc", "sig", 0)))).orElse(false)
                        && atBob.membership().member(aliceId.peerId()).isPresent()
                        && atSeed.membership().member(bobId.peerId()).isPresent()
                        && atSeed.membership().member(aliceId.peerId()).isPresent());
    }

    /** One PIPE_DATA handler per node: rtc-signal frames go to the capability, demo frames to {@code onApp}. */
    private void route(GroupRuntime runtime, WebRtcTransport binding, BiConsumer<PeerId, byte[]> onApp) {
        binding.signalling().members(peer -> runtime.membership().member(peer).isPresent());
        runtime.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> {
            PipeFrame frame;
            try {
                frame = codec.fromBytes(body, PipeFrame.class);
            } catch (RuntimeException e) {
                return;
            }
            if (frame == null || frame.capability() == null) {
                return;
            }
            if (RtcSignal.CAPABILITY.equals(frame.capability())) {
                binding.signalling().onSignal(from, frame.payload());
            } else if (APP.equals(frame.capability())) {
                onApp.accept(from, frame.payload());
            }
        });
        binding.signalling().attach(new RtcSignalCapability.SignalPipe() {
            @Override
            public void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler) {
                // handled by the shared PIPE_DATA route above
            }

            @Override
            public void send(PeerId to, String capabilityType, byte[] payload) {
                runtime.send(to, Envelope.Kind.PIPE_DATA, new PipeFrame(capabilityType, payload));
            }
        });
    }

    void sendApp(GroupRuntime from, PeerId to, String prefix, int count) {
        for (int i = 0; i < count; i++) {
            from.send(to, Envelope.Kind.PIPE_DATA, new PipeFrame(APP, (prefix + i).getBytes(StandardCharsets.UTF_8)));
        }
    }

    /** One protocol round on every node, the clock a second further. */
    void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** Ticks (a simulated second each, 20 ms of real time apart) until the condition holds, for at most 20 s. */
    void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            tickAll(1);
            Thread.sleep(20);
        }
    }

    long relayedTo(PeerId target) {
        PeerNode.RelayStats stats = seed.relayStats(groupId).get(target);
        return stats == null ? 0 : stats.framesForwarded();
    }

    @Override
    public void close() {
        // The bindings first: a binding closed after its node would send its sessions' END
        // signals through a node that has no connection left.
        aliceRtc.close();
        bobRtc.close();
        nodes.forEach(PeerNode::close);
    }
}
