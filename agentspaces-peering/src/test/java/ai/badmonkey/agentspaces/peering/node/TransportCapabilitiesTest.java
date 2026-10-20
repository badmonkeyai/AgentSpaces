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

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import ai.badmonkey.agentspaces.test.SimNetwork;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Transport capabilities on the SPI (spec §5.5) and the node-level rule that
 * a node requiring attestation registers attesting bindings alone (§5.6).
 */
class TransportCapabilitiesTest {

    @Test
    void shippedBindingsReportWhatTheyProvide() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        assertThat(new TcpTransport().capabilities())
                .containsExactlyInAnyOrder(Transport.Capability.RELIABLE, Transport.Capability.ORDERED);
        assertThat(new TlsTcpTransport(identity).capabilities())
                .containsExactlyInAnyOrder(Transport.Capability.ENCRYPTED, Transport.Capability.ATTESTING,
                        Transport.Capability.RELIABLE, Transport.Capability.ORDERED);
        assertThat(new SimNetwork().register("a").capabilities())
                .contains(Transport.Capability.ATTESTING)
                .doesNotContain(Transport.Capability.DATAGRAM);
    }

    @Test
    void aNodeRequiringAttestationRefusesANonAttestingBinding() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        try (PeerNode node = PeerNode.builder(identity).requireAttestation(true).build()) {
            assertThatThrownBy(() -> node.transport(new TcpTransport()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'tcp'");
            assertThatThrownBy(() -> node.listen(new TcpTransport(), "127.0.0.1:0"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(node.transportCapabilities("tcp")).isEmpty();

            node.transport(new TlsTcpTransport(identity));
            assertThat(node.transportCapabilities("tls")).contains(Transport.Capability.ATTESTING);
        }
    }

    @Test
    void aNodeNotRequiringAttestationTakesAnyBinding() throws Exception {
        try (PeerNode node = PeerNode.builder(PeerIdentity.generate()).build()) {
            node.transport(new TcpTransport());
            assertThat(node.transportCapabilities("tcp"))
                    .containsExactlyInAnyOrder(Transport.Capability.RELIABLE, Transport.Capability.ORDERED);
        }
    }
}
