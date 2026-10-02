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
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class A2aTranslatorTest {

    private static AgentCard card(List<String> goals) {
        PeerId peer = PeerId.fromPublicKey("test-key".getBytes(StandardCharsets.UTF_8));
        GroupId group = GroupId.fromFounding("g".getBytes(StandardCharsets.UTF_8));
        return new AgentCard("aspace://" + group.value() + "/agent/researcher",
                peer, group, Instant.parse("2026-01-01T00:00:00Z"),
                Duration.ofMinutes(15), new AgentId(peer, "researcher"),
                "Researches topics and writes findings", goals,
                List.of("com.example.ResearchTask#v1"),
                List.of("com.example.Finding#v1"),
                Map.of("model", "mini", "priceTier", "low"));
    }

    @Test
    void goalsBecomeSkillsAndSchemasBecomeTags() {
        A2aAgentCard a2a = A2aTranslator.translate(
                card(List.of("research", "summarize")), "http://gw:8080", "Acme");

        assertThat(a2a.protocolVersion()).isEqualTo(A2aAgentCard.PROTOCOL_VERSION);
        assertThat(a2a.name()).isEqualTo("researcher");
        assertThat(a2a.url()).isEqualTo("http://gw:8080/agents/researcher");
        assertThat(a2a.provider().organization()).isEqualTo("Acme");
        assertThat(a2a.skills()).extracting(A2aAgentCard.Skill::name)
                .containsExactly("research", "summarize");
        assertThat(a2a.skills().get(0).tags()).containsExactlyInAnyOrder(
                "consumes:com.example.ResearchTask#v1",
                "produces:com.example.Finding#v1");
    }

    @Test
    void theAgentSpacesIdentitySurvivesInMetadata() {
        A2aAgentCard a2a = A2aTranslator.translate(
                card(List.of("research")), "http://gw:8080", "Acme");

        assertThat(a2a.metadata())
                .containsKeys("agentspaces.id", "agentspaces.agent",
                        "agentspaces.peer", "agentspaces.group")
                .containsEntry("agentspaces.cost.model", "mini")
                .containsEntry("agentspaces.cost.priceTier", "low");
        assertThat(a2a.metadata().get("agentspaces.agent")).endsWith("/researcher");
    }

    @Test
    void aGoallessAgentStillPresentsOneSkill() {
        A2aAgentCard a2a = A2aTranslator.translate(
                card(List.of()), "http://gw:8080", "Acme");

        assertThat(a2a.skills()).hasSize(1);
        assertThat(a2a.skills().get(0).id()).isEqualTo("researcher/default");
        assertThat(a2a.skills().get(0).description())
                .isEqualTo("Researches topics and writes findings");
    }

    /** TECH-SPEC §9.4: every A2A card field the gateway emits is pinned, so translation is mechanical and stable. */
    @Test
    void everyA2aFieldIsPinned() {
        AgentCard source = card(List.of("research"));
        A2aAgentCard a2a = A2aTranslator.translate(source, "http://gw:8080", "Acme");

        assertThat(a2a.protocolVersion()).isEqualTo("1.0");
        assertThat(a2a.name()).isEqualTo("researcher");
        assertThat(a2a.description()).isEqualTo("Researches topics and writes findings");
        assertThat(a2a.url()).isEqualTo("http://gw:8080/agents/researcher");
        assertThat(a2a.preferredTransport()).isEqualTo("JSONRPC");
        assertThat(a2a.version()).isEqualTo("0.1");
        assertThat(a2a.provider()).isEqualTo(new A2aAgentCard.Provider("Acme", "http://gw:8080"));
        assertThat(a2a.capabilities()).isEqualTo(A2aAgentCard.Capabilities.none());
        assertThat(a2a.defaultInputModes()).containsExactly("application/json");
        assertThat(a2a.defaultOutputModes()).containsExactly("application/json");
        assertThat(a2a.skills()).hasSize(1);
        A2aAgentCard.Skill skill = a2a.skills().get(0);
        assertThat(skill.id()).isEqualTo("researcher/research");
        assertThat(skill.name()).isEqualTo("research");
        assertThat(skill.description())
                .isEqualTo("Pursues the 'research' goal: Researches topics and writes findings");
        assertThat(a2a.metadata()).containsOnlyKeys("agentspaces.id", "agentspaces.agent",
                "agentspaces.peer", "agentspaces.group", "agentspaces.cost.model",
                "agentspaces.cost.priceTier");
        assertThat(a2a.metadata())
                .containsEntry("agentspaces.id", source.id())
                .containsEntry("agentspaces.agent", source.agent().encoded())
                .containsEntry("agentspaces.peer", source.issuer().value())
                .containsEntry("agentspaces.group", source.group().value());

        // Explicit endpoint capabilities pass straight through.
        A2aAgentCard.Capabilities streaming = new A2aAgentCard.Capabilities(true, true, false);
        assertThat(A2aTranslator.translate(source, "http://gw:8080", "Acme", streaming)
                .capabilities()).isEqualTo(streaming);
    }

    /** TECH-SPEC §9.4: every skill of one agent carries the same schema tags. */
    @Test
    void tagsAreStableAcrossSkills() {
        A2aAgentCard a2a = A2aTranslator.translate(
                card(List.of("research", "summarize", "review")), "http://gw:8080", "Acme");
        assertThat(a2a.skills()).hasSize(3);
        List<String> expected = List.of("consumes:com.example.ResearchTask#v1",
                "produces:com.example.Finding#v1");
        assertThat(a2a.skills()).allSatisfy(skill ->
                assertThat(skill.tags()).containsExactlyElementsOf(expected));
        assertThat(a2a.skills()).extracting(A2aAgentCard.Skill::id)
                .containsExactly("researcher/research", "researcher/summarize", "researcher/review");
    }

    /** SPEC §6.1 v0.1.13 (TODO item 6): a card that declares its actions offers one skill per invocable action, with the action's own description and schemas, goals carried as tags; a ballot action is not a skill. */
    @Test
    void declaredActionsBecomeOneSkillEach() {
        AgentCard card = card(List.of("research")).withActions(List.of(
                new ai.badmonkey.agentspaces.api.ad.CardAction("summarize", "Summarizes one document",
                        List.of("com.example.Document#v1"), List.of("com.example.Summary#v1"),
                        "work", ai.badmonkey.agentspaces.api.ad.CardAction.TAKE),
                new ai.badmonkey.agentspaces.api.ad.CardAction("translate", "",
                        List.of("com.example.Text#v1"), List.of("com.example.Translation#v1"),
                        "work", ai.badmonkey.agentspaces.api.ad.CardAction.NOTIFY),
                new ai.badmonkey.agentspaces.api.ad.CardAction("vote", "",
                        List.of("Proposal#v1"), List.of("Ballot#v1"),
                        "votes", ai.badmonkey.agentspaces.api.ad.CardAction.BALLOT)));

        A2aAgentCard a2a = A2aTranslator.translate(card, "http://gw:8080", "Acme");

        assertThat(a2a.skills()).extracting(A2aAgentCard.Skill::id)
                .containsExactly("researcher/summarize", "researcher/translate");
        assertThat(a2a.skills().get(0).description()).isEqualTo("Summarizes one document");
        assertThat(a2a.skills().get(0).tags()).containsExactlyInAnyOrder(
                "consumes:com.example.Document#v1", "produces:com.example.Summary#v1",
                "goal:research");
        assertThat(a2a.skills().get(1).description())
                .as("an undescribed action falls back to the card description")
                .isEqualTo("Researches topics and writes findings");
    }
}
