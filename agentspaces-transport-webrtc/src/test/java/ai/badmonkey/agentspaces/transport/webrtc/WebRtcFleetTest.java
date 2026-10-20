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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario S3 of SRAD srad/agentspaces-js.md in the JVM (JS-N-02, JS-W-03, JS-W-08, JS-W-09,
 * JS-W-11, JS-Q-PRF-03): Alice and Bob share a RELAY-role seed and cannot reach each other on the
 * fabric; Bob accepts {@code webrtc} and advertises {@code Endpoint("webrtc", "sig", 0)}. Alice's
 * frames cross the seed until more than the threshold did in a minute; her node then dials Bob's
 * endpoint from its tick, the signalling capability on each runtime relays the signals through
 * the seed, the channel opens over the real engine on host candidates, and from then on the
 * relay carries none of their frames. Skips where the engine does not load.
 */
@Timeout(120)
class WebRtcFleetTest {

    /** The head of a {@code PIPE_DATA} body, as {@code CapabilityPipes} encodes it. */
    private record PipeFrame(String capability, byte[] payload) {
    }

    private static final String APP = "aspace:cap/demo";
    private final CborCodec codec = CborCodec.defaultCodec();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<WebRtcTransport> bindings = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zWebRtcFleet");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zWebRtcFleet",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "webrtc-fleet", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        // The bindings first: a binding closed after its node would send its sessions' END
        // signals through a node that has no connection left, which dials.
        bindings.forEach(WebRtcTransport::close);
        nodes.forEach(PeerNode::close);
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(50);
        }
    }

    private static List<PeerAdvertisement.Endpoint> seed(String address) {
        return List.of(new PeerAdvertisement.Endpoint("mem", address, 0));
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
            } else if (APP.equals(frame.capability()) && onApp != null) {
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

    @Test
    void twoMembersBehindARelaySeedOpenADirectChannelAndTheirFramesLeaveTheRelay() throws Exception {
        // Opt-in (the BrowserFleetTest pattern, -Dwebrtc.fleet=true): this drives the full node path
        // — the offload dial from the tick, its signals relayed by a RELAY-role seed, the real
        // engine, and node shutdown — which is slower and heavier than the direct-engine open
        // that LibDataChannelEngineTest proves on every run. The direct open, the chunking, the
        // attestation, JS-W-08 (WebRtcEndpointTest in agentspaces-peering), and the offload on the
        // fake engine (RelayOffloadTest, WebRtcFallbackTest) run unconditionally.
        Assumptions.assumeTrue(Boolean.getBoolean("webrtc.fleet"), "set -Dwebrtc.fleet=true to run");
        Assumptions.assumeTrue(WebRtcTransport.engineUnavailable().isEmpty(),
                WebRtcTransport.engineUnavailable().orElse(""));
        PeerIdentity seedId = PeerIdentity.generate();
        PeerIdentity aliceId = PeerIdentity.generate();
        PeerIdentity bobId = PeerIdentity.generate();
        PeerNode seed = PeerNode.builder(seedId).roles(Set.of(PeerAdvertisement.PeerRole.RELAY)).build();
        PeerNode alice = PeerNode.builder(aliceId).build();
        PeerNode bob = PeerNode.builder(bobId).build();
        nodes.addAll(List.of(seed, alice, bob));
        WebRtcTransport aliceRtc = new WebRtcTransport(aliceId.peerId());
        WebRtcTransport bobRtc = new WebRtcTransport(bobId.peerId());
        bindings.addAll(List.of(aliceRtc, bobRtc));

        seed.listen(network.register("seed"), "seed");
        alice.transport(network.register("alice"));   // dial-only on the fabric
        alice.transport(aliceRtc);                      // and a webrtc dialer
        bob.listen(network.register("bob"), "bob", 1);  // a fabric listener Alice cannot reach ...
        bob.listen(bobRtc, WebRtcTransport.SIG, 0);     // ... and the webrtc acceptor, preferred (JS-W-08)
        network.partition("alice", "bob");
        assertThat(bob.advertisedEndpoints()).containsExactly(new PeerAdvertisement.Endpoint("mem", "bob", 1),
                new PeerAdvertisement.Endpoint("webrtc", "sig", 0));

        GroupRuntime atSeed = seed.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime atAlice = alice.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("seed"));
        GroupRuntime atBob = bob.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("seed"));
        List<String> atBobSaw = new CopyOnWriteArrayList<>();
        route(atAlice, aliceRtc, null);
        route(atBob, bobRtc, (from, payload) -> {
            if (from.equals(aliceId.peerId())) {
                atBobSaw.add(new String(payload, StandardCharsets.UTF_8));
            }
        });
        for (PeerNode node : nodes) {
            node.startTicking(Duration.ofMillis(200));
        }
        await("membership to converge through the seed", () ->
                atAlice.membership().member(bobId.peerId()).map(m -> m.endpoints().size() == 2).orElse(false)
                        && atBob.membership().member(aliceId.peerId()).isPresent()
                        && atSeed.membership().member(bobId.peerId()).isPresent());

        // Alice's frames to Bob cross the seed (a send never dials a signalled endpoint); past the
        // threshold of thirty in a minute her node dials Bob's sig endpoint from its tick, the
        // signals relay through the seed, and the channel opens (JS-W-11).
        for (int i = 0; i < 31; i++) {
            atAlice.send(bobId.peerId(), Envelope.Kind.PIPE_DATA, new PipeFrame(APP,
                    ("relayed " + i).getBytes(StandardCharsets.UTF_8)));
        }
        await("the relayed frames to reach Bob", () -> atBobSaw.size() >= 31);
        await("the channel to open", () -> aliceRtc.counters().sessionsOpened() == 1
                && bobRtc.counters().sessionsOpened() == 1
                && "webrtc".equals(alice.connectionSchemes().get(bobId.peerId())));
        assertThat(aliceRtc.counters().sessionsOffered()).isEqualTo(1);
        assertThat(bobRtc.counters().sessionsAnswered()).isEqualTo(1);
        assertThat(alice.relayOffloadStats().get(groupId)).isEqualTo(new PeerNode.OffloadStats(1, 1, 0));
        assertThat(bob.channelModes()).as("Bob attested Alice from the channel's fingerprint (decision 5)")
                .containsKey(aliceId.peerId());
        assertThat(alice.channelModes()).containsKey(bobId.peerId());
        long relayedBefore = seed.traffic().receivedByKind().getOrDefault(Envelope.Kind.RELAY_FRAME, 0L);
        assertThat(relayedBefore).as("the signals went through the relay").isPositive();

        // Now Alice's frames to Bob ride the open, attested channel, not the relay (JS-Q-PRF-03).
        for (int i = 0; i < 5; i++) {
            atAlice.send(bobId.peerId(), Envelope.Kind.PIPE_DATA, new PipeFrame(APP,
                    ("direct " + i).getBytes(StandardCharsets.UTF_8)));
        }
        await("five frames over the channel", () -> atBobSaw.size() >= 36);
        assertThat(aliceRtc.counters().chunksSent()).isGreaterThanOrEqualTo(5);
        assertThat(bobRtc.counters().chunksReceived()).isGreaterThanOrEqualTo(5);
        assertThat(seed.traffic().receivedByKind().getOrDefault(Envelope.Kind.RELAY_FRAME, 0L))
                .as("once open, the relay carries none of their frames").isEqualTo(relayedBefore);
    }
}
