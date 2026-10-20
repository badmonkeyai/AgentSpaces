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
import ai.badmonkey.agentspaces.transport.http.WebSocketTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The HTTP listener blocks (SRAD agentspaces-js JS-H-09, JS-H-10, JS-N-04,
 * JS-Q-AVL-01): ws, wss, http, and https from properties alone, the shared
 * ports, the attestation rule, a bean replacing a built binding, and the
 * startup refusals that name their key.
 */
class HttpBlocksAutoConfigurationTest {

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
        group.setFounding("spring-http-blocks-v1");
        properties.setGroups(List.of(group));
        return properties;
    }

    private PeerNode node(AgentSpacesProperties properties, List<Transport> beans) {
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity, beans, List.of(), List.of());
        closeables.add(node);
        return node;
    }

    @Test
    void theFourBlocksListenAndAdvertiseWithWsAndHttpSharingAPort() throws Exception {
        AgentSpacesProperties properties = properties();
        properties.setBind("127.0.0.1:" + freeTcp());
        int plain = freeTcp();
        int secure = freeTcp();
        AgentSpacesProperties.Transport transport = properties.getTransport();
        transport.getWs().setEnabled(true);
        transport.getWs().setBind("127.0.0.1:" + plain);
        transport.getWs().setAllowedOrigins(List.of("http://localhost:8080"));
        transport.getHttp().setEnabled(true);
        transport.getHttp().setBind("127.0.0.1:" + plain);
        transport.getWss().setEnabled(true);
        transport.getWss().setBind("127.0.0.1:" + secure);
        transport.getWss().setAdvertise("fleet.example.com:443/aspace");
        transport.getWss().setPriority(0);
        transport.getHttps().setEnabled(true);
        transport.getHttps().setBind("127.0.0.1:" + secure);

        PeerNode node = node(properties, List.of());

        assertThat(node.transportSchemes()).contains("ws", "wss", "http", "https");
        assertThat(node.advertisedEndpoints()).containsExactly(
                new PeerAdvertisement.Endpoint("tls", properties.getBind(), 0),
                new PeerAdvertisement.Endpoint("ws", "127.0.0.1:" + plain, 1),
                new PeerAdvertisement.Endpoint("wss", "fleet.example.com:443/aspace", 0),
                new PeerAdvertisement.Endpoint("http", "127.0.0.1:" + plain, 3),
                new PeerAdvertisement.Endpoint("https", "127.0.0.1:" + secure, 4));
        assertThat(node.transportCapabilities("wss")).contains(Transport.Capability.ATTESTING);
        assertThat(node.transportCapabilities("https")).contains(Transport.Capability.ENCRYPTED)
                .doesNotContain(Transport.Capability.ATTESTING);
        assertThat(node.transportCapabilities("ws")).doesNotContain(Transport.Capability.ENCRYPTED);
    }

    @Test
    void aWssBlockSatisfiesRequiredAttestationAndThePlaintextBlocksAreRefusedUnderIt() throws Exception {
        AgentSpacesProperties satisfied = properties();
        satisfied.getTransport().getTls().setRequireAttestation(true);
        satisfied.getTransport().getWss().setEnabled(true);
        satisfied.getTransport().getWss().setBind("127.0.0.1:" + freeTcp());
        PeerNode node = node(satisfied, List.of());
        assertThat(node.transportSchemes()).contains("wss").doesNotContain("tcp");

        for (String scheme : new String[]{"ws", "http", "https"}) {
            AgentSpacesProperties refused = properties();
            refused.getTransport().getTls().setRequireAttestation(true);
            refused.getTransport().getWss().setEnabled(true);
            TransportBlocks.httpBlock(refused.getTransport(), scheme).setEnabled(true);
            assertThatThrownBy(() -> node(refused, List.of()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("agentspaces.transport." + scheme)
                    .hasMessageContaining("require-attestation");
        }
    }

    @Test
    void aBeanOfTheSchemeReplacesTheBuiltBindingAndBadOptionsNameTheKey() throws Exception {
        AgentSpacesProperties properties = properties();
        int port = freeTcp();
        properties.getTransport().getWs().setEnabled(true);
        properties.getTransport().getWs().setBind("127.0.0.1:" + port);
        WebSocketTransport bean = new WebSocketTransport();
        closeables.add(bean);
        PeerNode node = node(properties, List.of(bean));
        assertThat(node.advertisedEndpoints()).contains(new PeerAdvertisement.Endpoint("ws", "127.0.0.1:" + port, 0));
        assertThat(bean.boundPort("127.0.0.1:" + port)).isEqualTo(port);

        AgentSpacesProperties bad = properties();
        bad.getTransport().getHttps().setEnabled(true);
        bad.getTransport().getHttps().setAllowedOrigins(List.of("not an origin/path"));
        assertThatThrownBy(() -> node(bad, List.of())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.https");
        AgentSpacesProperties advertiseOnly = properties();
        advertiseOnly.getTransport().getWs().setEnabled(true);
        advertiseOnly.getTransport().getWs().setAdvertise("x:1");
        assertThatThrownBy(() -> node(advertiseOnly, List.of())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.ws.advertise");
    }
}
