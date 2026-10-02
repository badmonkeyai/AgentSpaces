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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The A2A agent card document: the JSON shape A2A
 * clients fetch from {@code /.well-known/agent-card.json}. AgentSpaces
 * {@code AgentCard}s translate mechanically into this form; the
 * AgentSpaces-specific identity (agent URI, peer id, group, schema names)
 * travels in {@link #metadata} so nothing is lost in translation.
 *
 * @param protocolVersion    the A2A protocol version this card follows
 * @param name               the agent's human-readable name
 * @param description        what the agent does
 * @param url                the endpoint an A2A client should talk to
 * @param preferredTransport the transport at {@link #url}
 * @param version            the agent's own version string
 * @param provider           who operates the agent
 * @param capabilities       optional A2A features this endpoint supports
 * @param defaultInputModes  media types the agent accepts
 * @param defaultOutputModes media types the agent produces
 * @param skills             what the agent can do, one entry per skill
 * @param metadata           extension fields (the AgentSpaces identity)
 */
public record A2aAgentCard(
        String protocolVersion,
        String name,
        String description,
        String url,
        String preferredTransport,
        String version,
        Provider provider,
        Capabilities capabilities,
        List<String> defaultInputModes,
        List<String> defaultOutputModes,
        List<Skill> skills,
        Map<String, String> metadata) {

    /** The A2A protocol version the gateway emits. */
    public static final String PROTOCOL_VERSION = "1.0";

    public A2aAgentCard {
        Objects.requireNonNull(protocolVersion, "protocolVersion");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(url, "url");
        skills = List.copyOf(Objects.requireNonNull(skills, "skills"));
        defaultInputModes = List.copyOf(Objects.requireNonNull(defaultInputModes,
                "defaultInputModes"));
        defaultOutputModes = List.copyOf(Objects.requireNonNull(defaultOutputModes,
                "defaultOutputModes"));
        metadata = Map.copyOf(Objects.requireNonNull(metadata, "metadata"));
    }

    /**
     * The operating organization.
     *
     * @param organization the organization name
     * @param url          the organization's URL
     */
    public record Provider(String organization, String url) {
    }

    /**
     * Optional A2A protocol features. The v0.1 gateway is a read-only
     * discovery surface, so everything is off.
     *
     * @param streaming              server-sent streaming supported
     * @param pushNotifications      push notifications supported
     * @param stateTransitionHistory task history supported
     */
    public record Capabilities(boolean streaming, boolean pushNotifications,
                               boolean stateTransitionHistory) {

        /** Returns the all-off capability set. */
        public static Capabilities none() {
            return new Capabilities(false, false, false);
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
    public record Skill(String id, String name, String description, List<String> tags) {

        public Skill {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(description, "description");
            tags = List.copyOf(Objects.requireNonNull(tags, "tags"));
        }
    }
}
