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
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.transport.webrtc.RtcSignalCapability;
import ai.badmonkey.agentspaces.transport.webrtc.WebRtcOptions;
import ai.badmonkey.agentspaces.transport.webrtc.WebRtcTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * The {@code webrtc} block (SRAD srad/agentspaces-js.md JS-N-04, JS-W-07, JS-W-10): the binding
 * from its properties, and its signalling capability attached to a group's pipes (JS-N-02).
 * Loaded only when agentspaces-transport-webrtc is on the classpath, as {@link HttpBlocks} is.
 */
final class WebRtcBlocks {

    private WebRtcBlocks() {
    }

    /** The options of a block, range-checked by {@link WebRtcOptions} (bad values name the key). */
    static WebRtcOptions options(AgentSpacesProperties.Transport.Webrtc block) {
        List<WebRtcOptions.IceServer> servers = new ArrayList<>();
        for (AgentSpacesProperties.Transport.Webrtc.IceServer server : block.getIceServers()) {
            servers.add(new WebRtcOptions.IceServer(server.getUrls() == null ? List.of() : server.getUrls(),
                    blank(server.getUsername()) ? null : server.getUsername().trim(),
                    blank(server.getCredential()) ? null : server.getCredential()));
        }
        return WebRtcOptions.defaults()
                .withIceServers(servers)
                .withDialTimeout(block.getDialTimeout())
                .withOffersPerPeerPerMinute(block.getOffersPerPeerPerMinute())
                .withCandidatesPerSession(block.getCandidatesPerSession())
                .withMaxSignalBytes(block.getMaxSignalBytes());
    }

    /** Why the engine cannot run here, naming the artifact, or empty. */
    static Optional<String> engineUnavailable() {
        return WebRtcTransport.engineUnavailable();
    }

    static Transport build(PeerIdentity identity, AgentSpacesProperties.Transport.Webrtc block) {
        return new WebRtcTransport(identity.peerId(), options(block));
    }

    /**
     * Attaches the registered binding's signalling capability to one group's pipes, with the
     * group's membership as the member test (an {@code OFFER} from a non-member is ignored).
     */
    static void attach(PeerNode node, GroupRuntime runtime, AgentSpaces.GroupContext context) {
        node.registeredTransport(WebRtcTransport.SCHEME).ifPresent(transport -> {
            if (!(transport instanceof WebRtcTransport webrtc)) {
                return; // an application bean of another type wires its own signalling
            }
            CapabilityPipes pipes = context.pipes();
            webrtc.signalling().members(peer -> runtime.membership().member(peer).isPresent())
                    .attach(new RtcSignalCapability.SignalPipe() {
                        @Override
                        public void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler) {
                            pipes.onCapability(capabilityType, handler);
                        }

                        @Override
                        public void send(PeerId to, String capabilityType, byte[] payload) {
                            pipes.send(to, capabilityType, payload);
                        }
                    });
        });
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
