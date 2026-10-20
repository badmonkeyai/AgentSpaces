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

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.transport.dtls.DtlsOptions;
import ai.badmonkey.agentspaces.transport.dtls.DtlsTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The per-scheme listener blocks (SRAD dtls-support §3.6, DT-H-01 to DT-H-07,
 * DT-H-11, DT-Q-AVL-01, DT-Q-MNT-02): DTLS from properties alone, the
 * generic attestation rule, bean replacement, and the startup refusals.
 */
class TransportBlocksAutoConfigurationTest {

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

    private static int freeUdp() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static AgentSpacesProperties properties(String seed) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setTickMillis(200);
        AgentSpacesProperties.SpaceDef tasks = new AgentSpacesProperties.SpaceDef();
        tasks.setName("tasks");
        tasks.setSettleWindowMillis(100);
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("fleet");
        group.setFounding("spring-dtls-blocks-v1");
        if (seed != null) {
            group.setSeeds(List.of(seed));
        }
        group.setSpaces(List.of(tasks));
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
    void aDtlsBlockListensAndAdvertisesAfterTheBindEndpoint() throws Exception {
        AgentSpacesProperties properties = properties(null);
        properties.setBind("127.0.0.1:" + freeTcp());
        int udp = freeUdp();
        properties.getTransport().getDtls().setEnabled(true);
        properties.getTransport().getDtls().setBind("127.0.0.1:" + udp);

        PeerNode node = node(properties, List.of());

        // The default profile runs TLS on agentspaces.bind; the DTLS block follows it.
        assertThat(node.transportSchemes()).contains("tls", "dtls");
        assertThat(node.advertisedEndpoints()).containsExactly(
                new PeerAdvertisement.Endpoint("tls", properties.getBind(), 0),
                new PeerAdvertisement.Endpoint("dtls", "127.0.0.1:" + udp, 1));
        assertThat(node.transportCapabilities("dtls")).contains(Transport.Capability.ATTESTING);
    }

    @Test
    void aBlockWithoutABindDialsOnlyAndAPriorityAndAdvertiseApply() throws Exception {
        AgentSpacesProperties dialOnly = properties(null);
        dialOnly.getTransport().getDtls().setEnabled(true);
        PeerNode node = node(dialOnly, List.of());
        assertThat(node.transportSchemes()).contains("dtls");
        assertThat(node.advertisedEndpoints()).isEmpty();

        AgentSpacesProperties explicit = properties(null);
        explicit.setBind("127.0.0.1:" + freeTcp());
        explicit.getTransport().getDtls().setEnabled(true);
        explicit.getTransport().getDtls().setBind("127.0.0.1:" + freeUdp());
        explicit.getTransport().getDtls().setAdvertise("peer-a.fleet.internal:7502");
        explicit.getTransport().getDtls().setPriority(0);
        assertThat(node(explicit, List.of()).advertisedEndpoints())
                .contains(new PeerAdvertisement.Endpoint("dtls", "peer-a.fleet.internal:7502", 0));
    }

    @Test
    void requireAttestationIsSatisfiedByADtlsListenerAlone() throws Exception {
        AgentSpacesProperties properties = properties(null);
        properties.getTransport().getTls().setEnabled(false);
        properties.getTransport().getTls().setRequireAttestation(true);
        properties.getTransport().getDtls().setEnabled(true);
        properties.getTransport().getDtls().setBind("127.0.0.1:" + freeUdp());

        PeerNode node = node(properties, List.of());

        assertThat(node.requiresAttestation()).isTrue();
        assertThat(node.transportSchemes()).containsExactly("dtls");
    }

    @Test
    void theStartupRefusalsNameTheirProperties() throws Exception {
        AgentSpacesProperties unattested = properties(null);
        unattested.getTransport().getTls().setRequireAttestation(true);
        unattested.getTransport().getTls().setEnabled(false);
        unattested.getTransport().getDtls().setEnabled(true);
        unattested.getTransport().getDtls().setAttestation("none");
        assertThatThrownBy(() -> node(unattested, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.dtls.attestation");

        AgentSpacesProperties tcpBind = properties(null);
        tcpBind.setBind("127.0.0.1:" + freeTcp());
        tcpBind.getTransport().getTls().setEnabled(false);
        tcpBind.getTransport().getTls().setRequireAttestation(true);
        tcpBind.getTransport().getDtls().setEnabled(true);
        assertThatThrownBy(() -> node(tcpBind, List.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("agentspaces.bind");

        AgentSpacesProperties advertiseOnly = properties(null);
        advertiseOnly.getTransport().getDtls().setEnabled(true);
        advertiseOnly.getTransport().getDtls().setAdvertise("peer:7502");
        assertThatThrownBy(() -> node(advertiseOnly, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.dtls.advertise");

        AgentSpacesProperties badMode = properties(null);
        badMode.getTransport().getDtls().setAttestation("sometimes");
        assertThatThrownBy(() -> node(badMode, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'identity' or 'none'");

        AgentSpacesProperties badMtu = properties(null);
        badMtu.getTransport().getDtls().setEnabled(true);
        badMtu.getTransport().getDtls().setMtu(100);
        assertThatThrownBy(() -> node(badMtu, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.dtls").hasMessageContaining("mtu");

        AgentSpacesProperties dtls13 = properties(null);
        dtls13.getTransport().getDtls().setEnabled(true);
        dtls13.getTransport().getDtls().setProtocols(List.of("DTLSv1.3"));
        assertThatThrownBy(() -> node(dtls13, List.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("DTLSv1.3");
    }

    @Test
    void aMissingModuleIsNamedAtStartup() {
        assertThat(TransportBlocks.present(TransportBlocks.DTLS_CLASS)).isTrue();
        assertThat(TransportBlocks.present("ai.badmonkey.agentspaces.transport.nothing.Here")).isFalse();
        AgentSpacesProperties.Transport.Listener block = new AgentSpacesProperties.Transport.Listener();
        block.setEnabled(true);
        assertThatThrownBy(() -> TransportBlocks.requireModule("ws", block,
                "ai.badmonkey.agentspaces.transport.nothing.Here", "agentspaces-transport-ws"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.ws.enabled")
                .hasMessageContaining("ai.badmonkey.agentspaces:agentspaces-transport-ws");
    }

    @Test
    void aTransportBeanReplacesTheBlocksBindingAndTheBlockStillListens() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        DtlsTransport bean = new DtlsTransport(identity).options(DtlsOptions.defaults().withWindow(128));
        closeables.add(bean);
        AgentSpacesProperties properties = properties(null);
        int udp = freeUdp();
        properties.getTransport().getDtls().setEnabled(true);
        properties.getTransport().getDtls().setBind("127.0.0.1:" + udp);
        PeerNode node = node(properties, List.of(bean));
        assertThat(node.advertisedEndpoints()).contains(new PeerAdvertisement.Endpoint("dtls", "127.0.0.1:" + udp, 0));
        // The bean listened: its options are now fixed, and they are the bean's.
        assertThatThrownBy(() -> bean.options(DtlsOptions.defaults())).isInstanceOf(IllegalStateException.class);
        assertThat(bean.options().window()).isEqualTo(128);
    }

    @Test
    void withNoBlockAFleetStartsAsToday() throws Exception {
        AgentSpacesProperties properties = properties(null);
        properties.setBind("127.0.0.1:" + freeTcp());
        PeerNode node = node(properties, List.of());
        assertThat(node.transportSchemes()).containsExactlyInAnyOrder("tls", "tcp");
        assertThat(node.advertisedEndpoints()).hasSize(1);
    }

    @Test
    @Timeout(90)
    void twoNodesFormAFleetOverDtlsFromPropertiesAlone() throws Exception {
        int udp = freeUdp();
        AgentSpacesProperties aProps = properties(null);
        aProps.getTransport().getTls().setEnabled(false);
        aProps.getTransport().getTls().setRequireAttestation(true);
        aProps.getTransport().setChannelAuth("attested");
        aProps.getTransport().getDtls().setEnabled(true);
        aProps.getTransport().getDtls().setBind("127.0.0.1:" + udp);
        AgentSpacesProperties bProps = properties("dtls://127.0.0.1:" + udp);
        bProps.getTransport().getTls().setEnabled(false);
        bProps.getTransport().getTls().setRequireAttestation(true);
        bProps.getTransport().getDtls().setEnabled(true);
        bProps.getTransport().getDtls().setBind("127.0.0.1:" + freeUdp());

        Wired a = wire(aProps);
        Wired b = wire(bProps);
        PeerId aPeer = a.identity().peerId();
        PeerId bPeer = b.identity().peerId();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline && !(member(a, bPeer) && member(b, aPeer))) {
            Thread.sleep(200);
        }
        assertThat(member(a, bPeer)).isTrue();
        assertThat(member(b, aPeer)).isTrue();
        assertThat(a.node().channelModes()).containsEntry(bPeer, "attested");
    }

    /** SRAD dtls-support DT-M-01: agentspaces.multicast.seal seals each group's beacon under its content-key. */
    @Test
    @Timeout(60)
    void multicastSealSealsTheBeaconUnderTheGroupContentKey() throws Exception {
        List<byte[]> sent = new java.util.concurrent.CopyOnWriteArrayList<>();
        ai.badmonkey.agentspaces.peering.bootstrap.MulticastBeacon.Datagrams carrier =
                new ai.badmonkey.agentspaces.peering.bootstrap.MulticastBeacon.Datagrams() {
                    @Override
                    public void send(byte[] datagram) {
                        sent.add(datagram);
                    }

                    @Override
                    public void onReceive(java.util.function.Consumer<byte[]> receiver) {
                    }

                    @Override
                    public void close() {
                    }
                };
        AgentSpacesProperties properties = properties(null);
        properties.setBind("127.0.0.1:" + freeTcp());
        properties.getGroups().get(0).setContentKey(java.util.Base64.getEncoder().encodeToString(new byte[32]));
        properties.getMulticast().setSeal(true);
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity, List.of(),
                List.of(builder -> builder.beacon(carrier, Duration.ofMillis(100))), List.of());
        Authorizers authorizers = autoConfig.agentSpacesAuthorizers(properties, identity);
        AgentSpaces spaces = autoConfig.agentSpaces(properties, node, identity, authorizers,
                autoConfig.agentSpacesEmbedder());
        AgentSpacesLifecycle lifecycle = autoConfig.agentSpacesLifecycle(properties, node, spaces, authorizers);
        lifecycle.start();
        closeables.add(lifecycle::stop);
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (sent.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertThat(sent).isNotEmpty()
                .allSatisfy(d -> assertThat(ai.badmonkey.agentspaces.peering.bootstrap.BeaconSeal.isSealed(d)).isTrue());
    }

    /** SRAD dtls-support DT-U-04: the bare UDP block is gated to dev-local or an explicit opt-in. */
    @Test
    void theUdpBlockIsGatedAndRefusedUnderRequiredAttestation() throws Exception {
        AgentSpacesProperties gated = properties(null);
        gated.getTransport().getUdp().setEnabled(true);
        assertThatThrownBy(() -> node(gated, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.transport.udp.allow-plaintext");

        AgentSpacesProperties devLocal = properties(null);
        devLocal.getSecurity().setProfile("dev-local");
        devLocal.getTransport().getUdp().setEnabled(true);
        int udpPort = freeUdp();
        devLocal.getTransport().getUdp().setBind("127.0.0.1:" + udpPort);
        PeerNode node = node(devLocal, List.of());
        assertThat(node.transportSchemes()).contains("udp");
        assertThat(node.advertisedEndpoints()).extracting(PeerAdvertisement.Endpoint::transport).contains("udp");

        AgentSpacesProperties attested = properties(null);
        attested.getTransport().getUdp().setEnabled(true);
        attested.getTransport().getUdp().setAllowPlaintext(true);
        attested.getTransport().getTls().setRequireAttestation(true);
        assertThatThrownBy(() -> node(attested, List.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("agentspaces.transport.udp cannot attest");
    }

    private record Wired(PeerIdentity identity, PeerNode node, AgentSpaces spaces) {
    }

    private Wired wire(AgentSpacesProperties properties) {
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity, List.of(), List.of(), List.of());
        Authorizers authorizers = autoConfig.agentSpacesAuthorizers(properties, identity);
        AgentSpaces spaces = autoConfig.agentSpaces(properties, node, identity, authorizers,
                autoConfig.agentSpacesEmbedder());
        AgentSpacesLifecycle lifecycle = autoConfig.agentSpacesLifecycle(properties, node, spaces, authorizers);
        lifecycle.start();
        closeables.add(lifecycle::stop);
        return new Wired(identity, node, spaces);
    }

    private static boolean member(Wired wired, PeerId peer) {
        return wired.node().group(wired.spaces().group("fleet").id()).orElseThrow()
                .membership().member(peer).isPresent();
    }
}
