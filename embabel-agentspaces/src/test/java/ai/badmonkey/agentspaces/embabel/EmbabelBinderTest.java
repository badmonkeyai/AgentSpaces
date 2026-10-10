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
package ai.badmonkey.agentspaces.embabel;

import com.embabel.agent.api.annotation.AchievesGoal;
import com.embabel.agent.api.annotation.Action;
import com.embabel.agent.api.annotation.Agent;
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.space.local.NamespaceSchemaRegistry;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.spring.SpaceAgent;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reflective Embabel bridge, exercised against test doubles that carry
 * Embabel's fully qualified annotation names: exactly what the adapter sees
 * when a real Embabel application runs with this module on the classpath.
 */
class EmbabelBinderTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private PeerIdentity identity;
    private PeerNode node;
    private AgentSpaces spaces;
    private DiscoveryService discovery;
    private EmbabelBinder binder;
    private GroupRuntime runtime;

    @BeforeEach
    void setUp() throws IOException {
        identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        node.listen(network.register("a"), "a");
        GroupId groupId = GroupId.of("zEmbabel");
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zEmbabel",
                PeerIdentity.generate().peerId(), groupId, java.time.Instant.EPOCH,
                Duration.ofDays(1), "fleet", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        runtime = node.joinGroup(groupAd,
                GroupMembership.Config.defaults(), List.of());
        discovery = new DiscoveryService(runtime, new AdCache(codec, clock),
                codec, identity.peerId());
        spaces = new AgentSpaces(identity, clock);
        spaces.register("fleet", groupId, runtime, discovery);
        binder = new EmbabelBinder(spaces, identity, clock, List.of("fleet"));
    }

    @AfterEach
    void tearDown() {
        spaces.close();
        node.close();
    }

    @Agent(name = "researcher", description = "Researches topics from the shared task space")
    public static class EmbabelResearcher {

        @Action
        @AchievesGoal(description = "produce findings")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }
    }

    @Agent(name = "desk", description = "A two-action Embabel desk")
    public static class TwoActionDesk {

        @Action(description = "Researches one topic")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }

        @Action
        public TaskEntry followUp(FindingEntry finding) {
            return new TaskEntry(finding.topic(), 1);
        }
    }

    @Agent(description = "Uses the class name")
    public static class UnnamedAgent {

        @Action
        public void act(TaskEntry task) {
        }
    }

    @AgentSpec(name = "explicit", description = "Chose its own card")
    @Agent(description = "Should be ignored")
    public static class ExplicitlyCarded {
    }

    public static class NotAnAgent {
    }

    /** Carries @AgentSpec only through the composed Spring stereotype. */
    @SpaceAgent(name = "stereotyped", description = "Chose the stereotype card")
    @Agent(description = "Should be ignored too")
    public static class Stereotyped {
    }

    @Test
    void embabelMetadataBecomesAPublishedAgentCard() {
        assertThat(binder.bind(new EmbabelResearcher())).hasValueSatisfying(card -> {
            assertThat(card.agent().localName()).isEqualTo("researcher");
            assertThat(card.description())
                    .isEqualTo("Researches topics from the shared task space");
            assertThat(card.goals()).containsExactly("produce findings");
            assertThat(card.consumes())
                    .containsExactly(TaskEntry.class.getName() + "#v1");
            assertThat(card.produces())
                    .containsExactly(FindingEntry.class.getName() + "#v1");
            assertThat(card.costHints()).containsEntry("framework", "embabel");
        });

        List<AgentCard> found = discovery.find(AgentCard.class,
                card -> "researcher".equals(card.agent().localName()));
        assertThat(found).hasSize(1);
    }

    @Test
    void agentsWithoutANameUseTheDecapitalizedClassName() {
        assertThat(binder.bind(new UnnamedAgent())).hasValueSatisfying(card ->
                assertThat(card.agent().localName()).isEqualTo("unnamedAgent"));
    }

    @Test
    void explicitAgentSpecBeansAndPlainBeansAreSkipped() {
        assertThat(binder.bind(new ExplicitlyCarded())).isEmpty();
        assertThat(binder.bind(new NotAnAgent())).isEmpty();
        assertThat(discovery.find(AgentCard.class, card -> true)).isEmpty();
    }

    @Test
    void presenceDetectionFollowsTheClassLoader() throws IOException {
        assertThat(EmbabelIntrospector.embabelPresent(getClass().getClassLoader()))
                .isTrue();
        try (URLClassLoader empty = new URLClassLoader(new URL[0], null)) {
            assertThat(EmbabelIntrospector.embabelPresent(empty)).isFalse();
        }
    }

    /** SPEC §10.3/§10.4: a @SpaceAgent stereotype bean already has the core card; the Embabel binder must not double-card it. */
    @Test
    void aSpaceAgentStereotypeBeanIsSkippedByTheEmbabelBinder() {
        assertThat(binder.bind(new Stereotyped())).isEmpty();
        assertThat(discovery.find(AgentCard.class, card -> true)).isEmpty();
    }

    /** An Embabel action that is also a space worker: the card learns which space it takes from. */
    @Agent(name = "spaceWorker", description = "Takes tasks from the work space")
    public static class SpaceBoundEmbabelWorker {

        @Action
        @ai.badmonkey.agentspaces.agent.annotation.SpaceTake(space = "work")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }
    }

    /** SPEC §10.4: an Embabel agent's card is published into every joined group, not only the first. */
    @Test
    void embabelCardsReachEveryJoinedGroup() throws IOException {
        // A second group on the same node, with its own discovery and ad-cache.
        GroupId secondId = GroupId.of("zEmbabelTwo");
        GroupAdvertisement secondAd = new GroupAdvertisement("aspace://zEmbabelTwo",
                PeerIdentity.generate().peerId(), secondId, java.time.Instant.EPOCH,
                Duration.ofDays(1), "fleet2", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        GroupRuntime second = node.joinGroup(secondAd, GroupMembership.Config.defaults(), List.of());
        DiscoveryService secondDiscovery = new DiscoveryService(second,
                new AdCache(codec, clock), codec, identity.peerId());
        spaces.register("fleet2", secondId, second, secondDiscovery);
        EmbabelBinder both = new EmbabelBinder(spaces, identity, clock, List.of("fleet", "fleet2"));

        List<AgentCard> cards = both.bindAll(new EmbabelResearcher());
        assertThat(cards).hasSize(2);
        assertThat(cards).extracting(AgentCard::group).containsExactly(runtime.id(), secondId);
        assertThat(both.bind(new UnnamedAgent())).hasValueSatisfying(card ->
                assertThat(card.group()).as("bind() returns the first group's card")
                        .isEqualTo(runtime.id()));

        for (DiscoveryService each : List.of(discovery, secondDiscovery)) {
            List<AgentCard> found = each.find(AgentCard.class,
                    card -> "researcher".equals(card.agent().localName()));
            assertThat(found).hasSize(1);
            assertThat(found.get(0).consumes()).containsExactly(TaskEntry.class.getName() + "#v1");
        }
        assertThat(discovery.find(AgentCard.class, card -> true))
                .extracting(AgentCard::group).containsOnly(runtime.id());
        assertThat(secondDiscovery.find(AgentCard.class, card -> true))
                .extracting(AgentCard::group).containsOnly(secondId);

        // Both groups' cards stay fresh through the one facade refresh.
        clock.advance(Duration.ofMinutes(16));
        assertThat(discovery.find(AgentCard.class, card -> true)).isEmpty();
        assertThat(secondDiscovery.find(AgentCard.class, card -> true)).isEmpty();
        spaces.refreshCards();
        assertThat(discovery.find(AgentCard.class, card -> true)).hasSize(2);
        assertThat(secondDiscovery.find(AgentCard.class, card -> true)).hasSize(2);
    }

    /** SPEC §6.1/§10.4: an Embabel card binds each consumed type to the space its @SpaceTake action takes it from, when known. */
    @Test
    void embabelCardsDeclareSpaceBindingsFromSpaceTakeActions() {
        LocalSpace work = LocalSpace.builder("work", identity.agent("host")).build();
        LocalSpace other = LocalSpace.builder("other", identity.agent("host")).build();
        try {
            spaces.group("fleet").space("work", work).space("other", other);
            AgentCard card = binder.bind(new SpaceBoundEmbabelWorker()).orElseThrow();
            assertThat(card.spaceFor(TaskEntry.class.getName() + "#v1")).contains("work");
            // A plain @Action without a space annotation declares no binding.
            AgentCard plain = binder.bind(new EmbabelResearcher()).orElseThrow();
            assertThat(plain.spaceBindings()).isEmpty();
        } finally {
            work.close();
            other.close();
        }
    }

    /** SPEC §10.4: Embabel-derived cards are leased and auto-refreshed like every bound card. */
    @Test
    void embabelCardsStayAliveAcrossRefresh() {
        AgentCard published = binder.bind(new EmbabelResearcher()).orElseThrow();

        clock.advance(Duration.ofMinutes(16)); // past the 15-minute TTL
        assertThat(discovery.find(AgentCard.class, card -> true)).isEmpty();

        spaces.refreshCards();
        List<AgentCard> alive = discovery.find(AgentCard.class, card -> true);
        assertThat(alive).hasSize(1);
        assertThat(alive.get(0).id()).isEqualTo(published.id());
        assertThat(alive.get(0).issued()).isEqualTo(clock.instant());
        assertThat(alive.get(0).costHints()).containsEntry("framework", "embabel");
        assertThat(alive.get(0).consumes()).isEqualTo(published.consumes());
    }

    /** SPEC §6.1 v0.1.13 (TODO item 6): each Embabel {@code @Action} is a declared card action, with its own description and its own consumed and produced types. */
    @Test
    void eachEmbabelActionIsADeclaredCardAction() {
        AgentCard card = binder.bind(new TwoActionDesk()).orElseThrow();
        assertThat(card.actions()).extracting(ai.badmonkey.agentspaces.api.ad.CardAction::name)
                .containsExactly("followUp", "research");
        ai.badmonkey.agentspaces.api.ad.CardAction research = card.actions().get(1);
        assertThat(research.description()).isEqualTo("Researches one topic");
        assertThat(research.kind()).isEqualTo(ai.badmonkey.agentspaces.api.ad.CardAction.EMBABEL_ACTION);
        assertThat(research.consumes()).containsExactly(TaskEntry.class.getName() + "#v1");
        assertThat(research.produces()).containsExactly(FindingEntry.class.getName() + "#v1");
        assertThat(card.actions().get(0).consumes()).containsExactly(FindingEntry.class.getName() + "#v1");
    }

    /** ISSUE-WorkflowShape §7.1 item 1: the Embabel binder names cards from the group's registry, or from one it is given. */
    @Test
    void embabelCardsNameThroughTheGroupsRegistryOrAnExplicitOne() {
        String ns = "https://example.org/test#";
        SchemaRegistry shared =
                NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.test", ns));
        spaces.group("fleet").schemaRegistry(shared);

        AgentCard card = binder.bind(new TwoActionDesk()).orElseThrow();
        assertThat(card.consumes())
                .containsExactlyInAnyOrder(ns + "TaskEntry", ns + "FindingEntry");
        assertThat(card.produces())
                .containsExactlyInAnyOrder(ns + "FindingEntry", ns + "TaskEntry");
        assertThat(card.actions()).allSatisfy(action -> {
            assertThat(action.consumes()).allMatch(name -> name.startsWith(ns));
            assertThat(action.produces()).allMatch(name -> name.startsWith(ns));
        });

        SchemaRegistry own = NamespaceSchemaRegistry.of(
                Map.of("ai.badmonkey.agentspaces.test", "https://example.org/own#"));
        EmbabelBinder explicit = new EmbabelBinder(spaces, identity, clock, List.of("fleet"), own);
        assertThat(explicit.bind(new EmbabelResearcher()).orElseThrow().consumes())
                .containsExactly("https://example.org/own#TaskEntry");
    }
}
