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

import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The binding over the real engine (SRAD srad/agentspaces-js.md decision 3, JS-W-01, JS-W-04,
 * JS-W-05, JS-D-02): two bindings in one JVM, signalling over the fake fabric, ICE on the host
 * candidates, DTLS on libdatachannel's per-connection ECDSA certificates. Skips with the engine's
 * own reason where its native library does not load.
 */
@Timeout(120)
class LibDataChannelEngineTest {

    private final FakeFabric fabric = new FakeFabric();
    private final List<WebRtcTransport> transports = new ArrayList<>();

    @AfterEach
    void tearDown() {
        transports.forEach(WebRtcTransport::close);
        fabric.close();
    }

    private WebRtcTransport binding(PeerId id) {
        WebRtcTransport transport = new WebRtcTransport(id, WebRtcOptions.defaults());
        transports.add(transport);
        transport.signalling().attach(fabric.pipe(id));
        return transport;
    }

    @Test
    void twoBindingsOpenAChannelAndExchangeFramesOverLibdatachannel() throws Exception {
        Assumptions.assumeTrue(WebRtcTransport.engineUnavailable().isEmpty(),
                WebRtcTransport.engineUnavailable().orElse(""));
        PeerId aliceId = PeerIdentity.generate().peerId();
        PeerId bobId = PeerIdentity.generate().peerId();
        WebRtcTransport alice = binding(aliceId);
        WebRtcTransport bob = binding(bobId);
        assertThat(alice.engineName()).isEqualTo("libdatachannel-java");
        LinkedBlockingQueue<TransportConnection> accepted = new LinkedBlockingQueue<>();
        bob.listen(WebRtcTransport.SIG, accepted::add);

        TransportConnection toBob = alice.dial(WebRtcTransport.SIG, bobId);
        TransportConnection fromAlice = accepted.poll(15, TimeUnit.SECONDS);
        assertThat(fromAlice).isNotNull();
        assertThat(toBob.attestedPeer()).contains(bobId);
        assertThat(fromAlice.attestedPeer()).contains(aliceId);
        // The fingerprints each side signed are the ones its engine's SDP presented (JS-W-04).
        assertThat(fabric.sent).filteredOn(s -> "OFFER".equals(s.signal().kind()) || "ANSWER".equals(s.signal().kind()))
                .hasSize(2).allSatisfy(s -> {
                    assertThat(s.signal().fp()).hasSize(1).allSatisfy(fp -> assertThat(fp).startsWith("sha-256 "));
                    assertThat(Fingerprints.matches(s.signal().sdp(), s.signal().fp())).isTrue();
                    assertThat(s.signal().sdp()).contains("a=max-message-size");
                });

        LinkedBlockingQueue<byte[]> atBob = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<byte[]> atAlice = new LinkedBlockingQueue<>();
        fromAlice.onReceive(atBob::add);
        toBob.onReceive(atAlice::add);
        byte[] big = new byte[300_000];
        new Random(3).nextBytes(big);
        toBob.send("hello".getBytes());
        toBob.send(big);
        fromAlice.send("pong".getBytes());
        assertThat(atBob.poll(10, TimeUnit.SECONDS)).isEqualTo("hello".getBytes());
        assertThat(atBob.poll(10, TimeUnit.SECONDS)).isEqualTo(big);
        assertThat(atAlice.poll(10, TimeUnit.SECONDS)).isEqualTo("pong".getBytes());
        assertThat(alice.counters().chunksSent()).isGreaterThan(big.length / (16 * 1024));
        assertThat(bob.counters().chunksReceived()).isEqualTo(alice.counters().chunksSent());
        assertThat(alice.counters().sessionsOpened()).isEqualTo(1);
        assertThat(alice.counters().selectedCandidates()).isNotEmpty();

        CompletableFuture<Boolean> closedAtBob = new CompletableFuture<>();
        fromAlice.onClose(() -> closedAtBob.complete(true));
        toBob.close();
        assertThat(closedAtBob.get(10, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> toBob.send(new byte[1])).isInstanceOf(IOException.class);
    }
}
