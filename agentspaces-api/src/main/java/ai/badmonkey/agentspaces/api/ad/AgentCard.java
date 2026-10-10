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
package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A capability card describing an agent (spec §6.1): what it can pursue, the entry
 * types it consumes and produces, and how expensive it tends to be. Deliberately
 * close in spirit to A2A Agent Cards so a gateway can translate mechanically. The
 * Embabel extension generates these automatically from {@code @Agent} metadata, so
 * "who can produce a Finding from a ResearchTask?" is answerable with no extra code.
 *
 * @param id          advertisement identifier
 * @param issuer      the hosting peer
 * @param group       the group scope
 * @param issued      issue instant
 * @param ttl         cache time-to-live
 * @param agent       the agent's identity
 * @param description human- and LLM-readable description of what the agent does
 * @param goals       goals the agent can pursue
 * @param consumes    schema names of entry types the agent consumes
 * @param produces    schema names of entry types the agent produces
 * @param costHints   free-form cost hints (tokens, latency, price, …)
 */
public record AgentCard(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        AgentId agent,
        String description,
        List<String> goals,
        List<String> consumes,
        List<String> produces,
        Map<String, String> costHints,
        Map<String, String> spaceBindings,
        // Appended in QA4 A4-7 phase 3 and omitted from the wire when absent, so a
        // card for a peer-signed agent is byte-identical to the v0.1.10 card. When
        // present it is the raw Ed25519 key the peer certified for this agent
        // (SPEC §4.2): readers can see the card's agent is attested, not asserted.
        @com.fasterxml.jackson.annotation.JsonInclude(
                com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        byte[] agentPublicKey,
        // Appended in v0.1.13 (TODO item 6) and omitted from the wire when absent,
        // so a card without declared actions is byte-identical to the v0.1.12 card.
        // When present it pairs each bound method's consumed schemas with what it
        // produces, so readers need not form the cross product of the flat lists.
        @com.fasterxml.jackson.annotation.JsonInclude(
                com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        List<CardAction> actions,
        // Appended in v0.1.13 (TODO-9-10-11 B6), omitted when absent: the peer's
        // certificate for agentPublicKey, so a reader can verify that the card's
        // agent is attested rather than take the key on the card's word. Caches
        // verify it under the card issuer's key at the card's issue time.
        @com.fasterxml.jackson.annotation.JsonInclude(
                com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        ai.badmonkey.agentspaces.api.security.AgentCertificate agentCertificate) implements Advertisement {

    public AgentCard(String id, PeerId issuer, GroupId group, Instant issued, Duration ttl,
                     AgentId agent, String description, List<String> goals,
                     List<String> consumes, List<String> produces, Map<String, String> costHints,
                     Map<String, String> spaceBindings, byte[] agentPublicKey,
                     List<CardAction> actions) {
        this(id, issuer, group, issued, ttl, agent, description, goals, consumes, produces,
                costHints, spaceBindings, agentPublicKey, actions, null);
    }

    /** The most actions one card may declare, which keeps cards inside the ad frame budget. */
    public static final int MAX_ACTIONS = 64;

    public AgentCard(String id, PeerId issuer, GroupId group, Instant issued, Duration ttl,
                     AgentId agent, String description, List<String> goals,
                     List<String> consumes, List<String> produces, Map<String, String> costHints,
                     Map<String, String> spaceBindings, byte[] agentPublicKey) {
        this(id, issuer, group, issued, ttl, agent, description, goals, consumes, produces,
                costHints, spaceBindings, agentPublicKey, null);
    }

    /** The v0.1.10 shape: bindings, no agent key. */
    public AgentCard(String id, PeerId issuer, GroupId group, Instant issued, Duration ttl,
                     AgentId agent, String description, List<String> goals,
                     List<String> consumes, List<String> produces, Map<String, String> costHints,
                     Map<String, String> spaceBindings) {
        this(id, issuer, group, issued, ttl, agent, description, goals, consumes, produces,
                costHints, spaceBindings, null, null);
    }

    /**
     * The pre-v0.1.10 shape, with no space bindings. Kept so cards built by
     * older code and tests keep constructing; a card with no bindings routes
     * exactly as before.
     */
    public AgentCard(String id, PeerId issuer, GroupId group, Instant issued, Duration ttl,
                     AgentId agent, String description, List<String> goals,
                     List<String> consumes, List<String> produces, Map<String, String> costHints) {
        this(id, issuer, group, issued, ttl, agent, description, goals, consumes, produces,
                costHints, Map.of());
    }

    public AgentCard {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(description, "description");
        goals = List.copyOf(Objects.requireNonNull(goals, "goals"));
        consumes = List.copyOf(Objects.requireNonNull(consumes, "consumes"));
        produces = List.copyOf(Objects.requireNonNull(produces, "produces"));
        costHints = java.util.Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(Objects.requireNonNull(costHints, "costHints"))); // signed in iteration order
        spaceBindings = spaceBindings == null ? Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(spaceBindings));
        agentPublicKey = agentPublicKey == null ? null : agentPublicKey.clone();
        if (agentCertificate != null && (agentPublicKey == null
                || !java.util.Arrays.equals(agentPublicKey, agentCertificate.agentPublicKey())
                || !agentCertificate.agent().equals(agent))) {
            throw new IllegalArgumentException("a card's agent certificate must certify exactly"
                    + " its agent and agentPublicKey");
        }
        if (actions != null) {
            if (actions.size() > MAX_ACTIONS) {
                throw new IllegalArgumentException("a card declares at most " + MAX_ACTIONS
                        + " actions: " + actions.size());
            }
            actions = List.copyOf(actions);
            if (actions.stream().map(CardAction::name).distinct().count() != actions.size()) {
                throw new IllegalArgumentException("action names must be unique within a card");
            }
        }
    }

    /** Whether the card's agent signs with a peer-certified key of its own (SPEC §4.2). */
    public boolean attested() {
        return agentPublicKey != null;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof AgentCard that
                && id.equals(that.id) && issuer.equals(that.issuer) && group.equals(that.group)
                && issued.equals(that.issued) && ttl.equals(that.ttl) && agent.equals(that.agent)
                && description.equals(that.description) && goals.equals(that.goals)
                && consumes.equals(that.consumes) && produces.equals(that.produces)
                && costHints.equals(that.costHints) && spaceBindings.equals(that.spaceBindings)
                && java.util.Arrays.equals(agentPublicKey, that.agentPublicKey)
                && Objects.equals(actions, that.actions)
                && Objects.equals(agentCertificate, that.agentCertificate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, issuer, group, issued, ttl, agent, description, goals, consumes,
                produces, costHints, spaceBindings, java.util.Arrays.hashCode(agentPublicKey),
                actions, agentCertificate);
    }

    /**
     * The space a consumed schema is taken from, when the card declares it.
     * SPEC §6.1 (v0.1.10): a card's bindings are space bindings, keyed by the
     * consumed schema name, so a caller can route an input entry to the space
     * the advertised agent actually takes it from without configuring routes.
     *
     * @param consumedSchema a schema name from {@link #consumes()}
     * @return the bound space name, if declared
     */
    public java.util.Optional<String> spaceFor(String consumedSchema) {
        return java.util.Optional.ofNullable(spaceBindings.get(consumedSchema));
    }

    /**
     * The card with its declared actions (v0.1.13), every other field unchanged.
     *
     * @param declared the actions; null clears them
     * @return the card
     */
    public AgentCard withActions(List<CardAction> declared) {
        return new AgentCard(id, issuer, group, issued, ttl, agent, description, goals, consumes,
                produces, costHints, spaceBindings, agentPublicKey, declared, agentCertificate);
    }

    /**
     * The card carrying the peer's certificate for its agent's key (v0.1.13),
     * every other field unchanged; the key must be the certificate's.
     *
     * @param certificate the certificate, or null to clear it
     * @return the card
     */
    public AgentCard withAgentCertificate(ai.badmonkey.agentspaces.api.security.AgentCertificate certificate) {
        return new AgentCard(id, issuer, group, issued, ttl, agent, description, goals, consumes,
                produces, costHints, spaceBindings, agentPublicKey, actions, certificate);
    }

}
