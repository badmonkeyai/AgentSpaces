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

import ai.badmonkey.agentspaces.api.ad.AgentCard;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Translates AgentSpaces {@link AgentCard}s to {@link A2aAgentCard}s. The
 * mapping is mechanical by design (spec §6.1 keeps the two shapes close):
 * goals become skills, consumed and produced entry schema names become skill
 * tags, and the AgentSpaces identity (agent URI, peer, group) rides in the
 * card's metadata so a round trip loses nothing an A2A client can carry.
 */
public final class A2aTranslator {

    private A2aTranslator() {
    }

    /**
     * Translates one card.
     *
     * @param card     the AgentSpaces card
     * @param baseUrl  the gateway's externally visible base URL, no trailing
     *                 slash (e.g. {@code http://gateway:8080})
     * @param provider the operating organization named on the A2A card
     * @return the A2A card
     */
    public static A2aAgentCard translate(AgentCard card, String baseUrl, String provider) {
        return translate(card, baseUrl, provider, A2aAgentCard.Capabilities.none());
    }

    /**
     * Translates one card with explicit endpoint capabilities.
     *
     * @param card         the AgentSpaces card
     * @param baseUrl      the gateway's externally visible base URL
     * @param provider     the operating organization named on the A2A card
     * @param capabilities the A2A features the serving endpoint supports
     * @return the A2A card
     */
    public static A2aAgentCard translate(AgentCard card, String baseUrl, String provider,
                                         A2aAgentCard.Capabilities capabilities) {
        Objects.requireNonNull(card, "card");
        Objects.requireNonNull(baseUrl, "baseUrl");
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(capabilities, "capabilities");

        String agentName = card.agent().localName();
        List<String> tags = new ArrayList<>();

        card.consumes().forEach(schema -> tags.add("consumes:" + schema));
        card.produces().forEach(schema -> tags.add("produces:" + schema));

        List<A2aAgentCard.Skill> skills = new ArrayList<>();

        // SPEC §6.1 v0.1.13: a card that declares its actions offers one skill
        // per invocable action, each with its own description and schemas; the
        // agent's goals stay as tags. An older card keeps one skill per goal.
        if (card.actions() != null) {
            for (ai.badmonkey.agentspaces.api.ad.CardAction action : card.actions()) {
                if (!action.invocable()) {
                    continue;
                }
                List<String> actionTags = new ArrayList<>();
                action.consumes().forEach(schema -> actionTags.add("consumes:" + schema));
                action.produces().forEach(schema -> actionTags.add("produces:" + schema));
                card.goals().forEach(goal -> actionTags.add("goal:" + goal));
                skills.add(new A2aAgentCard.Skill(agentName + "/" + action.name(), action.name(),
                        action.description().isBlank() ? card.description() : action.description(),
                        List.copyOf(actionTags)));
            }
        }

        for (String goal : skills.isEmpty() ? card.goals() : List.<String>of()) {
            skills.add(new A2aAgentCard.Skill(agentName + "/" + goal, goal,
                    "Pursues the '" + goal + "' goal: " + card.description(),
                    List.copyOf(tags)));
        }

        if (skills.isEmpty()) {
            skills.add(new A2aAgentCard.Skill(agentName + "/default", agentName,
                    card.description(), List.copyOf(tags)));
        }

        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("agentspaces.id", card.id());
        metadata.put("agentspaces.agent", card.agent().encoded());
        metadata.put("agentspaces.peer", card.issuer().value());
        metadata.put("agentspaces.group", card.group().value());
        
        card.costHints().forEach((k, v) -> metadata.put("agentspaces.cost." + k, v));
        return new A2aAgentCard(
                A2aAgentCard.PROTOCOL_VERSION,
                agentName,
                card.description(),
                baseUrl + "/agents/" + agentName,
                "JSONRPC",
                "0.1",
                new A2aAgentCard.Provider(provider, baseUrl),
                capabilities,
                List.of("application/json"),
                List.of("application/json"),
                skills,
                metadata);
    }
}
