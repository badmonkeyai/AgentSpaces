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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code webrtc} listener block (SRAD agentspaces-js JS-N-04, JS-W-07, JS-W-08): the binding
 * from properties alone, the {@code Endpoint("webrtc", "sig", priority)} it advertises after the
 * stream endpoints, the attestation posture (it attests, so required attestation admits it), and
 * the ice-server refusal that names the key.
 */
class WebRtcBlockAutoConfigurationTest {

    private final AgentSpacesAutoConfiguration autoConfig = new AgentSpacesAutoConfiguration();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables.reversed()) {
            closeable.close();
        }
    }

    private static int freeTcp() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static AgentSpacesProperties properties() {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setTickMillis(200);
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("fleet");
        group.setFounding("spring-webrtc-v1");
        properties.setGroups(List.of(group));
        return properties;
    }

    private PeerNode node(AgentSpacesProperties properties) {
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity, List.of(), List.of(), List.of());
        closeables.add(node);
        return node;
    }

    @Test
    void theWebrtcBlockAdvertisesTheSigEndpointAfterTheStreamEndpoint() throws Exception {
        AgentSpacesProperties properties = properties();
        properties.setBind("127.0.0.1:" + freeTcp());
        AgentSpacesProperties.Transport.Webrtc webrtc = properties.getTransport().getWebrtc();
        webrtc.setEnabled(true);
        AgentSpacesProperties.Transport.Webrtc.IceServer stun = new AgentSpacesProperties.Transport.Webrtc.IceServer();
        stun.setUrls(List.of("stun:stun.example.com:3478"));
        AgentSpacesProperties.Transport.Webrtc.IceServer turn = new AgentSpacesProperties.Transport.Webrtc.IceServer();
        turn.setUrls(List.of("turns:turn.example.com:443?transport=tcp"));
        turn.setUsername("u");
        turn.setCredential("p");
        webrtc.setIceServers(List.of(stun, turn));

        PeerNode node = node(properties);
        assertThat(node.transportSchemes()).contains("tcp", "webrtc");
        assertThat(node.transportCapabilities("webrtc")).contains(Transport.Capability.ATTESTING,
                Transport.Capability.ENCRYPTED);
        assertThat(node.advertisedEndpoints()).hasSize(2);
        assertThat(node.advertisedEndpoints().get(0).transport()).isIn("tcp", "tls"); // the agentspaces.bind endpoint
        assertThat(node.advertisedEndpoints().get(1))
                .as("webrtc sig advertised after the stream endpoint (JS-W-08)")
                .isEqualTo(new PeerAdvertisement.Endpoint("webrtc", "sig", 1));
    }

    @Test
    void theOffloadSettingsReachTheNodeAndABadValueNamesTheKey() throws Exception {
        AgentSpacesProperties defaults = properties();
        defaults.setBind("127.0.0.1:" + freeTcp());
        assertThat(node(defaults).relayOffload()).as("the SRAD's defaults (JS-W-11)")
                .isEqualTo(new PeerNode.RelayOffload(30, java.time.Duration.ofMinutes(5)));

        AgentSpacesProperties properties = properties();
        properties.setBind("127.0.0.1:" + freeTcp());
        properties.getTransport().getWebrtc().getOffload().setFramesPerMinute(12);
        properties.getTransport().getWebrtc().getOffload().setRetryAfter(java.time.Duration.ofSeconds(90));
        assertThat(node(properties).relayOffload())
                .isEqualTo(new PeerNode.RelayOffload(12, java.time.Duration.ofSeconds(90)));

        AgentSpacesProperties bad = properties();
        bad.setBind("127.0.0.1:" + freeTcp());
        bad.getTransport().getWebrtc().getOffload().setFramesPerMinute(0);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> node(bad))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.webrtc.offload")
                .hasMessageContaining("frames-per-minute");
    }

    @Test
    void theWebrtcBlockSatisfiesRequiredAttestation() throws Exception {
        AgentSpacesProperties properties = properties();
        properties.getTransport().getTls().setRequireAttestation(true);
        properties.getTransport().getWebrtc().setEnabled(true);
        PeerNode node = node(properties);
        assertThat(node.transportSchemes()).contains("webrtc").doesNotContain("tcp");
    }

    @Test
    void aBadIceServerUrlNamesTheKey() {
        AgentSpacesProperties properties = properties();
        AgentSpacesProperties.Transport.Webrtc webrtc = properties.getTransport().getWebrtc();
        webrtc.setEnabled(true);
        AgentSpacesProperties.Transport.Webrtc.IceServer bad = new AgentSpacesProperties.Transport.Webrtc.IceServer();
        bad.setUrls(List.of("http://not-an-ice-server"));
        webrtc.setIceServers(List.of(bad));
        assertThatThrownBy(() -> node(properties)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.webrtc");
    }
}
