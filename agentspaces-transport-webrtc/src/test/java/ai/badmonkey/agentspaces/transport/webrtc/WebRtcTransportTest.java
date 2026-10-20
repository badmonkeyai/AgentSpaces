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

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The binding over the fake engine and the fake fabric: SRAD srad/agentspaces-js.md JS-W-01,
 * JS-W-03, JS-W-04, JS-W-05, JS-W-06, JS-W-07, JS-W-09, JS-W-10, JS-W-13, JS-W-14, JS-D-05,
 * JS-F-05, JS-N-02, JS-Q-SEC-03, JS-Q-SEC-04.
 */
@Timeout(60)
class WebRtcTransportTest {

    private final FakeFabric fabric = new FakeFabric();
    private final FakeEngine engine = new FakeEngine();
    private final List<WebRtcTransport> transports = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();

    record Side(PeerId id, WebRtcTransport transport, LinkedBlockingQueue<TransportConnection> accepted) {
    }

    @AfterEach
    void tearDown() {
        transports.forEach(WebRtcTransport::close);
        fabric.close();
    }

    private Side side(WebRtcOptions options, boolean accept) throws IOException {
        PeerId id = PeerIdentity.generate().peerId();
        WebRtcTransport transport = new WebRtcTransport(id, options, engine);
        transports.add(transport);
        transport.signalling().attach(fabric.pipe(id));
        LinkedBlockingQueue<TransportConnection> accepted = new LinkedBlockingQueue<>();
        if (accept) {
            transport.listen(WebRtcTransport.SIG, accepted::add);
        }
        return new Side(id, transport, accepted);
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(10);
        }
    }

    @Test
    void theBindingDeclaresItsSchemeCapabilitiesAndAddress() throws Exception {
        Side a = side(WebRtcOptions.defaults(), false);
        assertThat(a.transport().scheme()).isEqualTo("webrtc");
        assertThat(a.transport().capabilities()).containsExactlyInAnyOrder(Transport.Capability.ENCRYPTED,
                Transport.Capability.ATTESTING, Transport.Capability.RELIABLE, Transport.Capability.ORDERED);
        assertThat(a.transport().engineName()).isEqualTo("fake");
        assertThatThrownBy(() -> a.transport().dial("sig")).isInstanceOf(IOException.class)
                .hasMessageContaining("needs the target member");
        assertThatThrownBy(() -> a.transport().listen("127.0.0.1:7500", c -> { }))
                .isInstanceOf(IOException.class).hasMessageContaining("'sig'");
        assertThatThrownBy(() -> a.transport().dial("sig", a.id())).hasMessageContaining("itself");
    }

    @Test
    void aDialSignalsAnOfferAndOpensAnAttestedConnectionThatCarriesChunkedFrames() throws Exception {
        Side alice = side(WebRtcOptions.defaults(), false);
        Side bob = side(WebRtcOptions.defaults(), true);
        TransportConnection toBob = alice.transport().dial(WebRtcTransport.SIG, bob.id());
        TransportConnection fromAlice = bob.accepted().poll(5, TimeUnit.SECONDS);
        assertThat(fromAlice).isNotNull();
        assertThat(toBob.attestedPeer()).contains(bob.id());
        assertThat(fromAlice.attestedPeer()).contains(alice.id());
        assertThat(toBob.remoteAddress()).isEqualTo("sig/" + bob.id().value())
                .doesNotContain("192.0.2"); // JS-W-13: no candidate address leaves the signals
        assertThat(fabric.count("OFFER")).isEqualTo(1);
        assertThat(fabric.count("ANSWER")).isEqualTo(1);
        assertThat(fabric.count("CAND")).as("one candidate and the end of the trickle, each way").isEqualTo(4);
        assertThat(fabric.sent).allSatisfy(s -> assertThat(s.bytes()).isLessThanOrEqualTo(64 * 1024));

        LinkedBlockingQueue<byte[]> atBob = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<byte[]> atAlice = new LinkedBlockingQueue<>();
        fromAlice.onReceive(atBob::add);
        toBob.onReceive(atAlice::add);
        byte[] small = "hello".getBytes();
        byte[] big = new byte[200_000];
        new Random(1).nextBytes(big);
        toBob.send(small);
        toBob.send(big);
        fromAlice.send("pong".getBytes());
        assertThat(atBob.poll(5, TimeUnit.SECONDS)).isEqualTo(small);
        assertThat(atBob.poll(5, TimeUnit.SECONDS)).as("chunked and reassembled in order").isEqualTo(big);
        assertThat(atAlice.poll(5, TimeUnit.SECONDS)).isEqualTo("pong".getBytes());
        int capacity = Chunks.payloadCapacity(engine.remoteMaxMessageSize);
        WebRtcCounters a = alice.transport().counters();
        WebRtcCounters b = bob.transport().counters();
        assertThat(a.chunksSent()).isEqualTo(1 + (big.length + capacity - 1) / capacity);
        assertThat(b.chunksReceived()).isEqualTo(a.chunksSent());
        assertThat(a.bytesSent()).isEqualTo(b.bytesReceived()).isEqualTo(small.length + big.length + a.chunksSent());
        assertThat(a.sessionsOffered()).isEqualTo(1);
        assertThat(b.sessionsAnswered()).isEqualTo(1);
        assertThat(a.sessionsOpened()).isEqualTo(1);
        assertThat(a.selectedCandidates()).containsEntry("host", 1L);
        assertThat(engine.channelsCreated).as("JS-D-05: the datagram channel is never opened")
                .allMatch(("aspace/1#0")::equals).hasSize(2);

        CompletableFuture<Boolean> bobClosed = new CompletableFuture<>();
        fromAlice.onClose(() -> bobClosed.complete(true));
        toBob.close();
        assertThat(bobClosed.get(5, TimeUnit.SECONDS)).isTrue();
        await("END(0) to reach Bob", () -> fabric.count("END") >= 1);
        assertThat(fabric.sent.stream().filter(s -> "END".equals(s.signal().kind())).findFirst())
                .hasValueSatisfying(s -> assertThat(s.signal().code()).isEqualTo(RtcSignal.END_NORMAL));
        assertThatThrownBy(() -> toBob.send(small)).isInstanceOf(IOException.class);
        await("sessions to be forgotten", () -> alice.transport().signalling().sessions() == 0
                && bob.transport().signalling().sessions() == 0);
    }

    @Test
    void aForgedAnswerIsRefusedAndTheDialFails() throws Exception {
        Side alice = side(WebRtcOptions.defaults().withDialTimeout(Duration.ofSeconds(5)), false);
        Side bob = side(WebRtcOptions.defaults(), true);
        fabric.tamper = s -> RtcSignal.ANSWER.equals(s.kind())
                ? RtcSignal.answer(s.sid(), s.sdp(), List.of(FakeEngine.randomFingerprint())) : s;
        assertThatThrownBy(() -> alice.transport().dial(WebRtcTransport.SIG, bob.id()))
                .isInstanceOf(IOException.class).hasMessageContaining("fingerprints differ");
        assertThat(alice.transport().counters().failed()).containsEntry("fingerprint", 1L);
        assertThat(bob.accepted()).isEmpty();
        await("END(1) to Bob", () -> fabric.sent.stream().anyMatch(s -> "END".equals(s.signal().kind())
                && s.signal().code() == RtcSignal.END_REFUSED && s.from().equals(alice.id())));
    }

    @Test
    void aForgedOfferIsRefusedWithEndOne() throws Exception {
        Side alice = side(WebRtcOptions.defaults().withDialTimeout(Duration.ofSeconds(3)), false);
        Side bob = side(WebRtcOptions.defaults(), true);
        fabric.tamper = s -> RtcSignal.OFFER.equals(s.kind())
                ? RtcSignal.offer(s.sid(), s.sdp(), List.of(FakeEngine.randomFingerprint())) : s;
        assertThatThrownBy(() -> alice.transport().dial(WebRtcTransport.SIG, bob.id())).isInstanceOf(IOException.class);
        assertThat(bob.transport().counters().failed()).containsEntry("fingerprint", 1L);
        assertThat(fabric.sent.stream().filter(s -> "END".equals(s.signal().kind()) && s.from().equals(bob.id())))
                .anySatisfy(s -> assertThat(s.signal().code()).isEqualTo(RtcSignal.END_REFUSED));
    }

    @Test
    void aDialThatNeverOpensEndsWithTimeoutAndTheSessionExpires() throws Exception {
        engine.neverOpen = true;
        Side alice = side(WebRtcOptions.defaults().withDialTimeout(Duration.ofMillis(600)), false);
        Side bob = side(WebRtcOptions.defaults().withDialTimeout(Duration.ofMillis(600)), true);
        long start = System.nanoTime();
        assertThatThrownBy(() -> alice.transport().dial(WebRtcTransport.SIG, bob.id()))
                .isInstanceOf(IOException.class).hasMessageContaining("no channel within");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
        assertThat(alice.transport().counters().failed()).containsEntry("timeout", 1L);
        await("END(2) to Bob", () -> fabric.sent.stream().anyMatch(s -> "END".equals(s.signal().kind())
                && s.signal().code() == RtcSignal.END_TIMEOUT && s.from().equals(alice.id())));
        await("Bob's pending session to expire too", () -> bob.transport().signalling().sessions() == 0);
        assertThat(alice.transport().signalling().sessions()).isZero();
    }

    @Test
    void aTimedOutDialWhoseEndSignalRoutesBackIntoTheBindingDoesNotNestAnotherDial() throws Exception {
        // The node sends a signal to a member it has no connection to by dialing that member's
        // endpoints, the webrtc one first (WebRtcFleetTest found this: a timed-out dial's END
        // re-entered dial, fifteen seconds per level, until the JVM was killed). The re-entrant
        // dial must fail at once so the node falls through to the relay path for the END.
        engine.neverOpen = true;
        Side alice = side(WebRtcOptions.defaults().withDialTimeout(Duration.ofMillis(400)), false);
        Side bob = side(WebRtcOptions.defaults().withDialTimeout(Duration.ofMillis(400)), true);
        List<String> reentrant = new CopyOnWriteArrayList<>();
        RtcSignalCapability.SignalPipe original = fabric.pipe(alice.id());
        alice.transport().signalling().attach(new RtcSignalCapability.SignalPipe() {
            @Override
            public void onCapability(String capabilityType, java.util.function.BiConsumer<PeerId, byte[]> handler) {
                original.onCapability(capabilityType, handler);
            }

            @Override
            public void send(PeerId to, String capabilityType, byte[] payload) {
                RtcSignal signal = codec.fromBytes(payload, RtcSignal.class);
                if ("END".equals(signal.kind())) {
                    // What PeerNode.connectionTo does for an unconnected target: dial its webrtc endpoint.
                    try {
                        alice.transport().dial(WebRtcTransport.SIG, to);
                        reentrant.add("opened");
                    } catch (IOException e) {
                        reentrant.add(e.getMessage());
                    }
                }
                original.send(to, capabilityType, payload);
            }
        });
        long start = System.nanoTime();
        assertThatThrownBy(() -> alice.transport().dial(WebRtcTransport.SIG, bob.id()))
                .isInstanceOf(IOException.class).hasMessageContaining("no channel within");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).as("one timeout, not a nest of them")
                .isLessThan(Duration.ofSeconds(3));
        assertThat(reentrant).hasSize(1);
        assertThat(reentrant.getFirst()).contains("already in progress");
        await("END(2) to Bob, once", () -> fabric.sent.stream().filter(s -> "END".equals(s.signal().kind())
                && s.from().equals(alice.id())).count() == 1);
        await("the sessions to be forgotten", () -> alice.transport().signalling().sessions() == 0);
    }

    @Test
    void signallingIsBoundedPerPeerPerSessionAndPerBody() throws Exception {
        Side alice = side(WebRtcOptions.defaults().withOffersPerPeerPerMinute(2).withCandidatesPerSession(3)
                .withMaxSignalBytes(2048).withDialTimeout(Duration.ofSeconds(2)), true);
        Side mallory = side(WebRtcOptions.defaults(), false);
        engine.neverOpen = true;
        byte[] sid = new byte[16];
        String sdp = engine.new FakeSession(engine, nullEvents()).sdp("offer");
        List<String> fp = Fingerprints.of(sdp);
        for (int i = 0; i < 5; i++) {
            sid[0] = (byte) i;
            fabric.inject(mallory.id(), alice.id(), codec.toBytes(RtcSignal.offer(sid, sdp, fp)));
        }
        await("the rate to bite", () -> alice.transport().counters().signalsDropped().getOrDefault("rate", 0L) == 3);
        assertThat(alice.transport().counters().signalsDropped()).containsEntry("rate", 3L);
        // The first offer failed at the engine (its token is unknown to the fake), the second too: no session holds.
        // Candidates beyond the per-session bound are dropped; so is a body over the size bound.
        Side carol = side(WebRtcOptions.defaults(), false);
        fabric.dropAll = true; // keep Alice's answers from reaching anyone: the session stays pending
        FakeEngine.FakeSession offerer = engine.new FakeSession(engine, nullEvents());
        engine.byToken.put(offerer.token, offerer);
        byte[] carolSid = new byte[16];
        carolSid[0] = 9;
        fabric.inject(carol.id(), alice.id(), codec.toBytes(RtcSignal.offer(carolSid, offerer.sdp("offer"),
                List.of(offerer.fingerprint))));
        await("Carol's session to be pending", () -> alice.transport().signalling().sessions() == 1);
        for (int i = 0; i < 6; i++) {
            fabric.inject(carol.id(), alice.id(), codec.toBytes(RtcSignal.candidate(carolSid,
                    "candidate:1 1 UDP 1 192.0.2." + i + " 5000 typ host", "0")));
        }
        await("candidates past the bound to be dropped", () ->
                alice.transport().counters().signalsDropped().getOrDefault("candidates", 0L) == 3);
        fabric.inject(carol.id(), alice.id(), new byte[4096]);
        await("an oversize body to be dropped", () ->
                alice.transport().counters().signalsDropped().getOrDefault("size", 0L) == 1);
        fabric.inject(carol.id(), alice.id(), new byte[]{(byte) 0xa0});
        await("a malformed body to be dropped", () ->
                alice.transport().counters().signalsDropped().getOrDefault("malformed", 0L) == 1);
        fabric.inject(carol.id(), alice.id(), codec.toBytes(RtcSignal.end(new byte[16], 0)));
        await("a signal for an unknown session to be dropped", () ->
                alice.transport().counters().signalsDropped().getOrDefault("unknown-session", 0L) == 1);
        fabric.inject(mallory.id(), alice.id(), codec.toBytes(RtcSignal.end(carolSid, 0)));
        await("a signal from the wrong origin to be dropped", () ->
                alice.transport().counters().signalsDropped().getOrDefault("wrong-origin", 0L) == 1);
        await("the pending session to expire at the dial timeout", () -> alice.transport().signalling().sessions() == 0);
        assertThat(alice.transport().counters().failed()).containsEntry("timeout", 1L);
    }

    @Test
    void anOfferFromANonMemberIsIgnoredAndOneWithoutAnAcceptorIsRefused() throws Exception {
        Side alice = side(WebRtcOptions.defaults(), true);
        Side bob = side(WebRtcOptions.defaults(), false);
        Side stranger = side(WebRtcOptions.defaults(), false);
        alice.transport().signalling().members(peer -> !peer.equals(stranger.id()));
        assertThatThrownBy(() -> stranger.transport().dial(WebRtcTransport.SIG, alice.id())).isInstanceOf(IOException.class);
        assertThat(alice.transport().counters().signalsDropped()).containsEntry("non-member", 1L);
        assertThat(alice.transport().counters().sessionsAnswered()).isZero();
        // Bob accepts nothing: an offer to it is answered with END(1) and the dial fails at once.
        long start = System.nanoTime();
        assertThatThrownBy(() -> alice.transport().dial(WebRtcTransport.SIG, bob.id()))
                .isInstanceOf(IOException.class).hasMessageContaining("END(1)");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
        assertThat(bob.transport().counters().signalsDropped()).containsEntry("no-acceptor", 1L);
        assertThat(alice.transport().counters().failed()).containsEntry("refused", 1L);
    }

    @Test
    void theSenderPausesAboveFourMiBResumesAtTheLowEventAndFailsAfterTheBlockedBound() throws Exception {
        // Single-chunk frames (under C), so each send is one atomic chunk and the pause lands
        // between sends, not inside one: pause above 15 KiB buffered, resume below 5 KiB.
        WebRtcOptions options = WebRtcOptions.defaults().withResumeBelow(5_000).withPauseAbove(15_000)
                .withSendBlocked(Duration.ofMillis(700));
        Side alice = side(options, false);
        Side bob = side(options, true);
        TransportConnection toBob = alice.transport().dial(WebRtcTransport.SIG, bob.id());
        TransportConnection fromAlice = bob.accepted().poll(5, TimeUnit.SECONDS);
        LinkedBlockingQueue<byte[]> atBob = new LinkedBlockingQueue<>();
        fromAlice.onReceive(atBob::add);
        engine.stallSends = true;
        byte[] frame = new byte[12_000];
        new Random(5).nextBytes(frame);
        toBob.send(frame); // buffered 0 -> 12 KiB; still at or below pause-above, so it completes
        CompletableFuture<Optional<IOException>> second = CompletableFuture.supplyAsync(() -> {
            try {
                toBob.send(frame); // buffered 12 KiB < 15 KiB at the check, then 24 KiB: completes
                toBob.send(frame); // buffered 24 KiB > 15 KiB at the check: pauses
                return Optional.empty();
            } catch (IOException e) {
                return Optional.of(e);
            }
        });
        Thread.sleep(200);
        assertThat(second).as("paused while the buffer stays full").isNotDone();
        engine.drainAll(); // the channel drains: bufferedamountlow fires and the send resumes
        assertThat(second.get(5, TimeUnit.SECONDS)).isEmpty();
        for (int i = 0; i < 3; i++) {
            assertThat(atBob.poll(5, TimeUnit.SECONDS)).isEqualTo(frame);
        }
        engine.stallSends = true;
        toBob.send(frame); // 0 -> 12 KiB, completes
        toBob.send(frame); // 12 KiB -> 24 KiB, completes
        long start = System.nanoTime();
        assertThatThrownBy(() -> toBob.send(frame)).isInstanceOf(IOException.class).hasMessageContaining("paused longer");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isBetween(Duration.ofMillis(600), Duration.ofSeconds(5));
        assertThat(alice.transport().counters().failed()).containsEntry("blocked", 1L);
    }

    @Test
    void whenBothEndsOfferAtOnceTheLowerPeerIdKeepsItsOffer() throws Exception {
        Side a = side(WebRtcOptions.defaults().withDialTimeout(Duration.ofSeconds(5)), true);
        Side b = side(WebRtcOptions.defaults().withDialTimeout(Duration.ofSeconds(5)), true);
        Side lower = a.id().value().compareTo(b.id().value()) < 0 ? a : b;
        Side higher = lower == a ? b : a;
        fabric.dropAll = true; // hold both offers until both are pending
        CompletableFuture<TransportConnection> fromLower = CompletableFuture.supplyAsync(() -> {
            try {
                return lower.transport().dial(WebRtcTransport.SIG, higher.id());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        CompletableFuture<TransportConnection> fromHigher = CompletableFuture.supplyAsync(() -> {
            try {
                return higher.transport().dial(WebRtcTransport.SIG, lower.id());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        await("both offers to be pending", () -> fabric.count("OFFER") == 2);
        List<FakeFabric.Sent> held = new CopyOnWriteArrayList<>(fabric.sent);
        fabric.dropAll = false;
        for (FakeFabric.Sent s : held) {
            fabric.inject(s.from(), s.to(), codec.toBytes(s.signal()));
        }
        TransportConnection lowerSide = fromLower.get(10, TimeUnit.SECONDS);
        assertThat(lowerSide.attestedPeer()).contains(higher.id());
        assertThatThrownBy(() -> fromHigher.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(RuntimeException.class);
        assertThat(lower.transport().counters().signalsDropped()).containsEntry("tie", 1L);
        assertThat(higher.transport().counters().sessionsAnswered()).isEqualTo(1);
        assertThat(higher.accepted().poll(5, TimeUnit.SECONDS)).isNotNull();
    }

    @Test
    void anUnavailableEngineFailsRegistrationNamingTheArtifact() throws Exception {
        engine.unavailable = Optional.of("the webrtc engine tel.schich:libdatachannel-java cannot load");
        Side a = side(WebRtcOptions.defaults(), false);
        assertThatThrownBy(() -> a.transport().listen(WebRtcTransport.SIG, c -> { }))
                .isInstanceOf(IOException.class).hasMessageContaining("tel.schich:libdatachannel-java");
        assertThatThrownBy(() -> a.transport().dial(WebRtcTransport.SIG, PeerIdentity.generate().peerId()))
                .isInstanceOf(IOException.class).hasMessageContaining("tel.schich:libdatachannel-java");
    }

    @Test
    void iceServersComeFromTheOptionsAndDefaultToNone() {
        assertThat(WebRtcOptions.defaults().iceServers()).isEmpty();
        WebRtcOptions.IceServer turn = new WebRtcOptions.IceServer(List.of("turns:turn.example.com:443?transport=tcp"),
                "user", "p:ss@word");
        assertThat(LibDataChannelEngine.iceUris(List.of(WebRtcOptions.IceServer.of("stun:stun.example.com:3478"), turn)))
                .extracting(Object::toString).containsExactly("stun:stun.example.com:3478",
                        "turns:user:p%3Ass%40word@turn.example.com:443?transport=tcp");
        assertThatThrownBy(() -> WebRtcOptions.IceServer.of("http://x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WebRtcOptions.IceServer(List.of("turn:x"), "u", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WebRtcOptions.defaults().withOffersPerPeerPerMinute(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WebRtcOptions.defaults().withResumeBelow(5L * 1024 * 1024))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static DataChannelEngine.Events nullEvents() {
        return new DataChannelEngine.Events() {
            @Override
            public void localDescription(String sdp, String type) {
            }

            @Override
            public void localCandidate(String candidate, String mid) {
            }

            @Override
            public void gatheringComplete() {
            }

            @Override
            public void open() {
            }

            @Override
            public void message(java.nio.ByteBuffer data) {
            }

            @Override
            public void textMessage() {
            }

            @Override
            public void bufferedAmountLow() {
            }

            @Override
            public void closed(String reason) {
            }

            @Override
            public void failed(String reason) {
            }
        };
    }
}
