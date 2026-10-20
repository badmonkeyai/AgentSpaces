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

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.peering.node.PeerNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The per-scheme listener blocks (SRAD dtls-support §3.6, DT-H-01 to DT-H-08):
 * {@code agentspaces.transport.quic.*} and {@code agentspaces.transport.dtls.*}
 * register their binding, and with a {@code bind} listen and advertise it
 * after the {@code agentspaces.bind} endpoint. An application
 * {@link Transport} bean of the same scheme replaces the binding a block
 * would build, and the block still decides where it listens.
 *
 * <p>The optional modules stay optional: this class names their classes
 * only as strings, and the builders that touch them ({@link DtlsBlocks},
 * {@link QuicBlocks}) load only when a block is enabled and its module is
 * present. Shared by the Spring starter and the Micronaut factory.
 */
final class TransportBlocks {

    static final String DTLS_CLASS = "ai.badmonkey.agentspaces.transport.dtls.DtlsTransport";
    static final String QUIC_CLASS = "ai.badmonkey.agentspaces.transport.quic.QuicTransport";
    static final String UDP_CLASS = "ai.badmonkey.agentspaces.transport.dtls.UdpTransport";
    static final String HTTP_CLASS = "ai.badmonkey.agentspaces.transport.http.WebSocketTransport";
    static final String HTTP_ARTIFACT = "agentspaces-transport-http";
    static final String WEBRTC_CLASS = "ai.badmonkey.agentspaces.transport.webrtc.WebRtcTransport";
    static final String WEBRTC_ARTIFACT = "agentspaces-transport-webrtc";

    /** One enabled block and the binding registered for it. */
    record Block(String scheme, Transport transport, AgentSpacesProperties.Transport.Listener settings,
                 boolean built) {
    }

    private TransportBlocks() {
    }

    /** Whether an enabled block will register a binding that can attest. */
    static boolean anyAttesting(AgentSpacesProperties properties) {
        AgentSpacesProperties.Transport transport = properties.getTransport();
        return transport.getQuic().isEnabled()
                || (transport.getDtls().isEnabled() && !unattested(transport.getDtls()))
                || transport.getWss().isEnabled()
                || transport.getWebrtc().isEnabled(); // attests by signed fingerprint (JS-W-04, decision 5)
    }

    static boolean unattested(AgentSpacesProperties.Transport.Dtls dtls) {
        return "none".equals(attestation(dtls));
    }

    private static String attestation(AgentSpacesProperties.Transport.Dtls dtls) {
        return dtls.getAttestation() == null ? "identity" : dtls.getAttestation().trim().toLowerCase(Locale.ROOT);
    }

    /** Startup refusals that name the property (SRAD DT-H-02, DT-Q-MNT-02). */
    static void validate(AgentSpacesProperties properties, boolean requireAttestation) {
        AgentSpacesProperties.Transport transport = properties.getTransport();
        requireModule("quic", transport.getQuic(), QUIC_CLASS, "agentspaces-transport-quic");
        requireModule("dtls", transport.getDtls(), DTLS_CLASS, "agentspaces-transport-dtls");
        requireModule("udp", transport.getUdp(), UDP_CLASS, "agentspaces-transport-dtls");
        requireModule("ws", transport.getWs(), HTTP_CLASS, HTTP_ARTIFACT);
        requireModule("wss", transport.getWss(), HTTP_CLASS, HTTP_ARTIFACT);
        requireModule("http", transport.getHttp(), HTTP_CLASS, HTTP_ARTIFACT);
        requireModule("https", transport.getHttps(), HTTP_CLASS, HTTP_ARTIFACT);
        requireModule("webrtc", transport.getWebrtc(), WEBRTC_CLASS, WEBRTC_ARTIFACT);
        // JS-N-04: the webrtc block attests, so required attestation admits it; its refusals are
        // an engine that cannot run here (named with the artifact) and ice-server entries that are
        // not stun:, turn:, or turns: urls with a paired username and credential.
        if (transport.getWebrtc().isEnabled()) {
            WebRtcBlocks.engineUnavailable().ifPresent(reason -> {
                throw new IllegalStateException("agentspaces.transport.webrtc.enabled: " + reason);
            });
            try {
                WebRtcBlocks.options(transport.getWebrtc());
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("agentspaces.transport.webrtc: " + e.getMessage(), e);
            }
        }
        // JS-H-09: ws and http follow tcp, https never attests (no client certificate reaches a
        // peer through a proxy), so a node that requires attestation registers none of them.
        for (String scheme : new String[]{"ws", "http", "https"}) {
            if (httpBlock(transport, scheme).isEnabled() && requireAttestation) {
                throw new IllegalStateException("agentspaces.transport." + scheme + " cannot attest, and"
                        + " agentspaces.transport.tls.require-attestation registers attesting transports only;"
                        + " use agentspaces.transport.wss or turn require-attestation off");
            }
        }
        for (String scheme : new String[]{"ws", "wss", "http", "https"}) {
            AgentSpacesProperties.Transport.Http block = httpBlock(transport, scheme);
            if (block.isEnabled()) {
                try {
                    HttpBlocks.options(block);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("agentspaces.transport." + scheme + ": " + e.getMessage(), e);
                }
            }
        }
        AgentSpacesProperties.Transport.Udp udp = transport.getUdp();
        boolean devLocal = "dev-local".equalsIgnoreCase(String.valueOf(properties.getSecurity().getProfile()).trim());
        if (udp.isEnabled() && !devLocal && !udp.isAllowPlaintext()) {
            throw new IllegalStateException("agentspaces.transport.udp is plaintext: it registers only under the"
                    + " dev-local security profile or with agentspaces.transport.udp.allow-plaintext: true");
        }
        if (udp.isEnabled() && requireAttestation) {
            throw new IllegalStateException("agentspaces.transport.udp cannot attest, and"
                    + " agentspaces.transport.tls.require-attestation registers attesting transports only");
        }
        AgentSpacesProperties.Transport.Dtls dtls = transport.getDtls();
        String mode = attestation(dtls);
        if (!mode.equals("identity") && !mode.equals("none")) {
            throw new IllegalStateException("agentspaces.transport.dtls.attestation must be 'identity' or"
                    + " 'none', not '" + dtls.getAttestation() + "'");
        }
        if (dtls.isEnabled() && requireAttestation && mode.equals("none")) {
            throw new IllegalStateException("agentspaces.transport.dtls.attestation: none cannot attest, and"
                    + " agentspaces.transport.tls.require-attestation registers attesting transports only;"
                    + " set attestation to 'identity' or turn require-attestation off");
        }
    }

    static void requireModule(String scheme, AgentSpacesProperties.Transport.Listener block, String className,
                              String artifact) {
        if (!block.isEnabled()) {
            return;
        }
        if (!present(className)) {
            throw new IllegalStateException("agentspaces.transport." + scheme + ".enabled needs"
                    + " ai.badmonkey.agentspaces:" + artifact + " on the classpath");
        }
        if (blank(block.getBind()) && !blank(block.getAdvertise())) {
            throw new IllegalStateException("agentspaces.transport." + scheme + ".advertise needs"
                    + " agentspaces.transport." + scheme + ".bind: a dial-only binding advertises no address");
        }
    }

    static AgentSpacesProperties.Transport.Http httpBlock(AgentSpacesProperties.Transport transport, String scheme) {
        return switch (scheme) {
            case "ws" -> transport.getWs();
            case "wss" -> transport.getWss();
            case "http" -> transport.getHttp();
            default -> transport.getHttps();
        };
    }

    static boolean present(String className) {
        try {
            Class.forName(className, false, TransportBlocks.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /**
     * Builds the binding of each enabled block, taking an application bean of
     * the same scheme out of {@code provided} when there is one.
     */
    static List<Block> build(AgentSpacesProperties properties, PeerIdentity identity, ServingCredential serving,
                             Supplier<ChannelTrust> trust, Map<String, Transport> provided) {
        List<Block> blocks = new ArrayList<>();
        AgentSpacesProperties.Transport transport = properties.getTransport();
        if (transport.getQuic().isEnabled()) {
            Transport bean = provided.remove("quic");
            blocks.add(new Block("quic", bean != null ? bean : QuicBlocks.build(identity, serving, trust),
                    transport.getQuic(), bean == null));
        }
        if (transport.getDtls().isEnabled()) {
            Transport bean = provided.remove("dtls");
            blocks.add(new Block("dtls", bean != null ? bean
                    : DtlsBlocks.build(identity, transport.getDtls(), serving, trust), transport.getDtls(),
                    bean == null));
        }
        if (transport.getUdp().isEnabled()) {
            Transport bean = provided.remove("udp");
            blocks.add(new Block("udp", bean != null ? bean : DtlsBlocks.buildUdp(transport.getDtls()),
                    transport.getUdp(), bean == null));
        }
        for (String scheme : new String[]{"ws", "wss", "http", "https"}) {
            AgentSpacesProperties.Transport.Http block = httpBlock(transport, scheme);
            if (block.isEnabled()) {
                Transport bean = provided.remove(scheme);
                blocks.add(new Block(scheme, bean != null ? bean
                        : HttpBlocks.build(scheme, identity, block, serving, trust), block, bean == null));
            }
        }
        if (transport.getWebrtc().isEnabled()) {
            Transport bean = provided.remove("webrtc");
            blocks.add(new Block("webrtc", bean != null ? bean : WebRtcBlocks.build(identity, transport.getWebrtc()),
                    transport.getWebrtc(), bean == null));
        }
        return blocks;
    }

    /**
     * Listens on, or registers for dialing, each block's binding, after the
     * {@code agentspaces.bind} endpoint; a built binding closes with the node.
     */
    static void register(PeerNode node, List<Block> blocks) {
        for (Block block : blocks) {
            AgentSpacesProperties.Transport.Listener settings = block.settings();
            if (block.built() && block.transport() instanceof AutoCloseable closeable) {
                node.closeWith(closeable);
            }
            // The webrtc binding's one address is "sig": enabled means accepting, advertised as
            // Endpoint("webrtc", "sig", priority) whatever bind says (JS-W-08).
            boolean webrtc = "webrtc".equals(block.scheme());
            if (blank(settings.getBind()) && !webrtc) {
                node.transport(block.transport());
                continue;
            }
            String bind = webrtc ? "sig" : settings.getBind().trim();
            String advertise = webrtc || blank(settings.getAdvertise()) ? bind : settings.getAdvertise().trim();
            int priority = settings.getPriority() != null ? settings.getPriority() : node.advertisedEndpoints().size();
            try {
                node.listen(block.transport(), bind, advertise, priority);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot bind agentspaces.transport." + block.scheme()
                        + ".bind '" + bind + "'", e);
            }
        }
    }

    static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
