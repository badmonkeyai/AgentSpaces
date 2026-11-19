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
package ai.badmonkey.agentspaces.a2a;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The A2A agent card document: the JSON shape A2A clients fetch from
 * {@code /.well-known/agent-card.json}. AgentSpaces {@code AgentCard}s
 * translate mechanically into this form.
 *
 * <p>The card is an A2A 1.0 card (§4.4.1): its endpoints are
 * {@link #supportedInterfaces()}, each with its URL, binding and protocol
 * version, in preference order. A card that also offers an A2A 0.3 interface
 * carries 0.3's top-level {@code protocolVersion}, {@code url} and
 * {@code preferredTransport} as well, describing that interface, so a 0.3
 * client reads it as before; a 1.0 client ignores them as unrecognized fields
 * (§5.7). The gateway serves {@code 1.0} first and {@code 0.3} second, at one
 * URL, and the request's {@code A2A-Version} picks the dialect.
 *
 * <p>The AgentSpaces identity (agent URI, peer id, group, cost hints) rides in
 * {@code capabilities.extensions} as the {@link #IDENTITY_EXTENSION} extension,
 * the place both 0.3 and 1.0 give a card for extension data;
 * {@link #metadata()} reads it back.
 *
 * <p>The record is also the model a fetched card parses into (the platform's
 * A2A import), so every accessor tolerates a card from elsewhere.
 *
 * @param name                the agent's human-readable name
 * @param description         what the agent does
 * @param supportedInterfaces the endpoints, preferred first; at least one
 * @param provider            who operates the agent
 * @param version             the agent's own version string
 * @param capabilities        optional A2A features the endpoints support
 * @param defaultInputModes   media types the agent accepts
 * @param defaultOutputModes  media types the agent produces
 * @param skills              what the agent can do, one entry per skill
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "description", "supportedInterfaces", "provider", "version",
        "capabilities", "defaultInputModes", "defaultOutputModes", "skills",
        "protocolVersion", "url", "preferredTransport"})
@JsonIgnoreProperties(value = {"protocolVersion", "url", "preferredTransport"},
        allowGetters = true, ignoreUnknown = true)
public record A2aAgentCard(
        String name,
        String description,
        List<Interface> supportedInterfaces,
        Provider provider,
        String version,
        Capabilities capabilities,
        List<String> defaultInputModes,
        List<String> defaultOutputModes,
        List<Skill> skills) {

    /** The A2A protocol version the gateway prefers. */
    public static final String PROTOCOL_VERSION = A2aVersion.V1_0.wire();
    /** The older A2A protocol version the gateway still serves at the same URL. */
    public static final String LEGACY_PROTOCOL_VERSION = A2aVersion.V0_3.wire();
    /** The JSON-RPC binding's name in an interface. */
    public static final String JSONRPC = "JSONRPC";
    /** The extension that carries the AgentSpaces identity of a card. */
    public static final String IDENTITY_EXTENSION = "https://badmonkey.ai/agentspaces/a2a/identity/v1";

    public A2aAgentCard {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(description, "description");
        supportedInterfaces = List.copyOf(Objects.requireNonNull(supportedInterfaces,
                "supportedInterfaces"));
        if (supportedInterfaces.isEmpty()) {
            throw new IllegalArgumentException("an A2A card declares at least one interface");
        }
        capabilities = capabilities == null ? Capabilities.none() : capabilities;
        skills = List.copyOf(Objects.requireNonNull(skills, "skills"));
        defaultInputModes = List.copyOf(Objects.requireNonNull(defaultInputModes,
                "defaultInputModes"));
        defaultOutputModes = List.copyOf(Objects.requireNonNull(defaultOutputModes,
                "defaultOutputModes"));
    }

    /**
     * The two JSON-RPC interfaces the gateway serves at one URL: A2A 1.0
     * preferred, then 0.3.
     *
     * @param url the endpoint
     * @return the interfaces, in preference order
     */
    public static List<Interface> jsonRpc(String url) {
        return List.of(new Interface(url, JSONRPC, PROTOCOL_VERSION),
                new Interface(url, JSONRPC, LEGACY_PROTOCOL_VERSION));
    }

    /**
     * The interface a client of this module calls (A2A 1.0 §8.3.2): the first
     * JSON-RPC interface whose version it speaks, in the card's preference order.
     *
     * @return the interface, or empty when the card offers none this module speaks
     */
    public Optional<Interface> select() {
        return supportedInterfaces.stream()
                .filter(i -> JSONRPC.equalsIgnoreCase(i.protocolBinding()))
                .filter(i -> i.version().isPresent())
                .findFirst();
    }

    /** The A2A 0.3 interface, when the card offers one. */
    @JsonIgnore
    public Optional<Interface> legacyInterface() {
        return supportedInterfaces.stream()
                .filter(i -> i.version().filter(A2aVersion.V0_3::equals).isPresent())
                .findFirst();
    }

    /** A2A 0.3's top-level {@code protocolVersion}: the 0.3 interface's, or absent. */
    @JsonProperty("protocolVersion")
    public String protocolVersion() {
        return legacyInterface().map(Interface::protocolVersion).orElse(null);
    }

    /** A2A 0.3's top-level {@code url}: the 0.3 interface's, or absent. */
    @JsonProperty("url")
    public String url() {
        return legacyInterface().map(Interface::url).orElse(null);
    }

    /** A2A 0.3's top-level {@code preferredTransport}: the 0.3 interface's binding, or absent. */
    @JsonProperty("preferredTransport")
    public String preferredTransport() {
        return legacyInterface().map(Interface::protocolBinding).orElse(null);
    }

    /**
     * The AgentSpaces identity the card carries in its
     * {@link #IDENTITY_EXTENSION} extension, as strings; empty for a card
     * from elsewhere.
     */
    @JsonIgnore
    public Map<String, String> metadata() {
        Map<String, String> out = new LinkedHashMap<>();
        for (Extension e : capabilities.extensions()) {
            if (IDENTITY_EXTENSION.equals(e.uri()) && e.params() != null) {
                e.params().forEach((k, v) -> out.put(k, String.valueOf(v)));
            }
        }
        return java.util.Collections.unmodifiableMap(out);
    }

    /**
     * One endpoint of the card (A2A 1.0 {@code AgentInterface}).
     *
     * @param url             where the client sends requests
     * @param protocolBinding {@code JSONRPC}, {@code GRPC} or {@code HTTP+JSON}
     * @param protocolVersion the A2A version spoken there, {@code Major.Minor}
     * @param tenant          the tenant a client sets on every request; may be null
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Interface(String url, String protocolBinding, String protocolVersion,
                            String tenant) {

        public Interface {
            Objects.requireNonNull(url, "url");
            Objects.requireNonNull(protocolBinding, "protocolBinding");
            Objects.requireNonNull(protocolVersion, "protocolVersion");
            tenant = tenant == null || tenant.isBlank() ? null : tenant;
        }

        /** An interface with no tenant. */
        public Interface(String url, String protocolBinding, String protocolVersion) {
            this(url, protocolBinding, protocolVersion, null);
        }

        /** The version this module speaks there, or empty when it speaks none. */
        @JsonIgnore
        public Optional<A2aVersion> version() {
            return A2aVersion.of(protocolVersion);
        }
    }

    /**
     * The operating organization.
     *
     * @param organization the organization name
     * @param url          the organization's URL
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Provider(String organization, String url) {
    }

    /**
     * Optional A2A features (1.0 {@code AgentCapabilities}).
     *
     * @param streaming         server-sent streaming supported
     * @param pushNotifications push notifications supported
     * @param extendedAgentCard an extended card is served to authenticated clients
     * @param extensions        the extensions the agent declares
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Capabilities(boolean streaming, boolean pushNotifications,
                               boolean extendedAgentCard,
                               @JsonInclude(JsonInclude.Include.NON_EMPTY) List<Extension> extensions) {

        public Capabilities {
            extensions = extensions == null ? List.of() : List.copyOf(extensions);
        }

        /** Streaming and push as given, no extended card, no extensions. */
        public Capabilities(boolean streaming, boolean pushNotifications) {
            this(streaming, pushNotifications, false, List.of());
        }

        /** Returns the all-off capability set. */
        public static Capabilities none() {
            return new Capabilities(false, false);
        }

        /**
         * The same capabilities with one more extension.
         *
         * @param extension the extension to declare
         * @return the new capabilities
         */
        public Capabilities with(Extension extension) {
            List<Extension> all = new ArrayList<>(extensions);
            all.add(Objects.requireNonNull(extension, "extension"));
            return new Capabilities(streaming, pushNotifications, extendedAgentCard, all);
        }
    }

    /**
     * One declared extension (A2A {@code AgentExtension}).
     *
     * @param uri         the extension's identifier
     * @param description how the agent uses it
     * @param required    whether a client must understand it
     * @param params      its configuration
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Extension(String uri, String description, boolean required,
                            Map<String, Object> params) {

        public Extension {
            Objects.requireNonNull(uri, "uri");
            params = params == null ? null : Map.copyOf(params);
        }

        /**
         * The AgentSpaces identity extension over the given values.
         *
         * @param identity the agent URI, peer, group and the like
         * @return the extension, not required
         */
        public static Extension identity(Map<String, String> identity) {
            return new Extension(IDENTITY_EXTENSION,
                    "The AgentSpaces identity behind this card: agent URI, peer, group.",
                    false, new LinkedHashMap<>(identity));
        }
    }

    /**
     * One skill: an A2A client's unit of "what can this agent do".
     *
     * @param id          stable skill identifier
     * @param name        human-readable name
     * @param description what the skill does
     * @param tags        searchable tags; the gateway carries the entry schema
     *                    names the agent consumes and produces here
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Skill(String id, String name, String description, List<String> tags) {

        public Skill {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(description, "description");
            tags = List.copyOf(Objects.requireNonNull(tags, "tags"));
        }
    }
}
