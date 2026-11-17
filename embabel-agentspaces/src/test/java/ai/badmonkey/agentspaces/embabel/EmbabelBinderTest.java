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
    private EmbabelDeclarationReader reader;
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
        reader = new EmbabelDeclarationReader().register();
    }

    /** The core binder's card for one bean, bound through the facade (review §7.2). */
    private java.util.Optional<AgentCard> bindOne(Object bean) {
        List<ai.badmonkey.agentspaces.agent.AgentBinder.Bound> bound = spaces.bind(bean);
        return bound.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(bound.get(0).card());
    }

    @AfterEach
    void tearDown() {
        ai.badmonkey.agentspaces.agent.AgentBinder.unregisterDeclarationReader(EmbabelDeclarationReader.class);
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

        @Action(description = "Researches one topic", pre = {"topic-chosen"}, post = {"findings-ready"}, cost = 2.0)
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

    /** Review §7.2: an Embabel @Agent bean gets one card from the core binder, named by @Agent(name), with the framework hint. */
    @Test
    void embabelMetadataBecomesAPublishedAgentCard() {
        assertThat(bindOne(new EmbabelResearcher())).hasValueSatisfying(card -> {
            assertThat(card.agent().localName()).isEqualTo("researcher");
            assertThat(card.description()).isEqualTo("Researches topics from the shared task space");
            assertThat(card.goals()).containsExactly("produce findings");
            assertThat(card.consumes()).containsExactly(TaskEntry.class.getName() + "#v1");
            assertThat(card.produces()).containsExactly(FindingEntry.class.getName() + "#v1");
            assertThat(card.costHints()).containsEntry("framework", "embabel");
        });
        List<AgentCard> found = discovery.find(AgentCard.class, card -> "researcher".equals(card.agent().localName()));
        assertThat(found).as("one card, not two").hasSize(1);
        assertThat(discovery.find(AgentCard.class, card -> true)).hasSize(1);
    }

    @Test
    void agentsWithoutANameUseTheDecapitalizedClassName() {
        assertThat(bindOne(new UnnamedAgent())).hasValueSatisfying(card ->
                assertThat(card.agent().localName()).isEqualTo("unnamedAgent"));
    }

    /** Review §7.2 (X19): an @AgentSpec or stereotype name wins over @Agent; a plain bean is no agent. */
    @Test
    void explicitAgentSpecBeansKeepTheirNameAndPlainBeansAreSkipped() {
        assertThat(bindOne(new ExplicitlyCarded())).hasValueSatisfying(card ->
                assertThat(card.agent().localName()).isEqualTo("explicit"));
        assertThat(bindOne(new Stereotyped())).hasValueSatisfying(card ->
                assertThat(card.agent().localName()).isEqualTo("stereotyped"));
        assertThat(bindOne(new NotAnAgent())).isEmpty();
        assertThat(discovery.find(AgentCard.class, card -> true)).hasSize(2);
    }

    @Test
    void presenceDetectionFollowsTheClassLoader() throws IOException {
        assertThat(EmbabelIntrospector.embabelPresent(getClass().getClassLoader())).isTrue();
        try (URLClassLoader empty = new URLClassLoader(new URL[0], null)) {
            assertThat(EmbabelIntrospector.embabelPresent(empty)).isFalse();
        }
    }

    /** Review §7.2: the deprecated binder binds through the facade and returns the core card; it makes no second card. */
    @Test
    @SuppressWarnings("removal")
    void theDeprecatedBinderReturnsTheCoreCard() {
        EmbabelBinder binder = new EmbabelBinder(spaces, identity, clock, List.of("fleet"));
        assertThat(binder.bind(new EmbabelResearcher())).hasValueSatisfying(card ->
                assertThat(card.agent().localName()).isEqualTo("researcher"));
        assertThat(binder.bind(new NotAnAgent())).isEmpty();
        assertThat(discovery.find(AgentCard.class, card -> true)).hasSize(1);
    }

    /** An Embabel action that is also a space worker: the one card learns which space it takes from. */
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
        GroupId secondId = GroupId.of("zEmbabelTwo");
        GroupAdvertisement secondAd = new GroupAdvertisement("aspace://zEmbabelTwo",
                PeerIdentity.generate().peerId(), secondId, java.time.Instant.EPOCH,
                Duration.ofDays(1), "fleet2", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        GroupRuntime second = node.joinGroup(secondAd, GroupMembership.Config.defaults(), List.of());
        DiscoveryService secondDiscovery = new DiscoveryService(second, new AdCache(codec, clock), codec, identity.peerId());
        spaces.register("fleet2", secondId, second, secondDiscovery);

        List<ai.badmonkey.agentspaces.agent.AgentBinder.Bound> bound = spaces.bind(new EmbabelResearcher());
        assertThat(bound).hasSize(2);
        assertThat(bound).extracting(b -> b.card().group()).containsExactlyInAnyOrder(runtime.id(), secondId);
        for (DiscoveryService each : List.of(discovery, secondDiscovery)) {
            List<AgentCard> found = each.find(AgentCard.class, card -> "researcher".equals(card.agent().localName()));
            assertThat(found).hasSize(1);
            assertThat(found.get(0).consumes()).containsExactly(TaskEntry.class.getName() + "#v1");
        }

        // Both groups' cards stay fresh through the one facade refresh.
        clock.advance(Duration.ofMinutes(16));
        assertThat(discovery.find(AgentCard.class, card -> true)).isEmpty();
        assertThat(secondDiscovery.find(AgentCard.class, card -> true)).isEmpty();
        spaces.refreshCards();
        assertThat(discovery.find(AgentCard.class, card -> true)).hasSize(1);
        assertThat(secondDiscovery.find(AgentCard.class, card -> true)).hasSize(1);
    }

    /** SPEC §6.1/§10.4: the card binds each consumed type to the space its @SpaceTake action takes it from. */
    @Test
    void embabelCardsDeclareSpaceBindingsFromSpaceTakeActions() {
        LocalSpace work = LocalSpace.builder("work", identity.agent("host")).build();
        LocalSpace other = LocalSpace.builder("other", identity.agent("host")).build();
        try {
            spaces.group("fleet").space("work", work).space("other", other);
            AgentCard card = bindOne(new SpaceBoundEmbabelWorker()).orElseThrow();
            assertThat(card.spaceFor(TaskEntry.class.getName() + "#v1")).contains("work");
            assertThat(card.actions()).hasSize(1);
            assertThat(card.actions().get(0).kind()).isEqualTo(ai.badmonkey.agentspaces.api.ad.CardAction.TAKE);
            // Review §7.3 (X46): a plain @Action in a two-space group with no action space is local, not served.
            AgentCard plain = bindOne(new EmbabelResearcher()).orElseThrow();
            assertThat(plain.spaceBindings()).isEmpty();
            assertThat(plain.actions()).hasSize(1);
            assertThat(plain.actions().get(0).kind()).isEqualTo(ai.badmonkey.agentspaces.api.ad.CardAction.LOCAL);
        } finally {
            work.close();
            other.close();
        }
    }

    /** SPEC §10.4: Embabel-derived cards are leased and auto-refreshed like every bound card. */
    @Test
    void embabelCardsStayAliveAcrossRefresh() {
        AgentCard published = bindOne(new EmbabelResearcher()).orElseThrow();
        clock.advance(Duration.ofMinutes(16));
        assertThat(discovery.find(AgentCard.class, card -> true)).isEmpty();
        spaces.refreshCards();
        List<AgentCard> alive = discovery.find(AgentCard.class, card -> true);
        assertThat(alive).hasSize(1);
        assertThat(alive.get(0).id()).isEqualTo(published.id());
        assertThat(alive.get(0).issued()).isEqualTo(clock.instant());
        assertThat(alive.get(0).costHints()).containsEntry("framework", "embabel");
        assertThat(alive.get(0).consumes()).isEqualTo(published.consumes());
    }

    /**
     * Review §7.3: in a one-space group every map-of-1 @Action is an implied take, advertised as an embabel-action
     * with its space and served by writing the entry tagged aspace.action=&lt;agent&gt;.&lt;method&gt;; an @Action with a
     * precondition is local; the contract rides along.
     */
    @Test
    void mapOfOneActionsAreImpliedTakesAndPreconditionedOnesAreLocal() {
        LocalSpace work = LocalSpace.builder("work", identity.agent("host")).build();
        try {
            spaces.group("fleet").space("work", work);
            AgentCard card = bindOne(new TwoActionDesk()).orElseThrow();
            assertThat(card.actions()).extracting(ai.badmonkey.agentspaces.api.ad.CardAction::name)
                    .containsExactly("followUp", "research");
            ai.badmonkey.agentspaces.api.ad.CardAction followUp = card.actions().get(0);
            assertThat(followUp.kind()).isEqualTo(ai.badmonkey.agentspaces.api.ad.CardAction.EMBABEL_ACTION);
            assertThat(followUp.space()).isEqualTo("work");
            assertThat(followUp.consumes()).containsExactly(FindingEntry.class.getName() + "#v1");
            assertThat(followUp.produces()).containsExactly(TaskEntry.class.getName() + "#v1");
            ai.badmonkey.agentspaces.api.ad.CardAction research = card.actions().get(1);
            assertThat(research.kind()).as("pre on local state: not served by a direct take")
                    .isEqualTo(ai.badmonkey.agentspaces.api.ad.CardAction.LOCAL);
            assertThat(research.space()).isNull();
            assertThat(research.contract()).isNotNull();
            assertThat(research.contract().pre()).containsExactly("topic-chosen");
            assertThat(research.contract().post()).containsExactly("findings-ready");
            assertThat(research.contract().cost()).isEqualTo("2.0");
            assertThat(research.description()).isEqualTo("Researches one topic");

            // The implied take serves only an entry addressed to it (X41): an untagged Finding stays.
            work.write(new FindingEntry("untagged", "x"), ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(1)));
            work.write(new FindingEntry("addressed", "y"), ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(1)),
                    Map.of("aspace.action", "desk.followUp"));
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (work.readAll(ai.badmonkey.agentspaces.api.space.Template.of(TaskEntry.class), 10).isEmpty()
                    && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(work.readAll(ai.badmonkey.agentspaces.api.space.Template.of(TaskEntry.class), 10))
                    .extracting(TaskEntry::topic).containsExactly("addressed");
            assertThat(work.readAll(ai.badmonkey.agentspaces.api.space.Template.of(FindingEntry.class), 10))
                    .extracting(FindingEntry::topic).containsExactly("untagged");
        } finally {
            work.close();
        }
    }

    /** Review §7.4: a worker that completes into another space binds its produced schema to that space on the card. */
    @Agent(name = "reporter", description = "Takes from tasks, writes to reports")
    public static class ResultElsewhere {

        @Action
        @ai.badmonkey.agentspaces.agent.annotation.SpaceTake(space = "work", resultSpace = "reports")
        public FindingEntry report(TaskEntry task) {
            return new FindingEntry(task.topic(), "reported");
        }
    }

    @Test
    void theResultSpaceRidesTheCardAsAProducedBinding() {
        LocalSpace work = LocalSpace.builder("work", identity.agent("host")).build();
        LocalSpace reports = LocalSpace.builder("reports", identity.agent("host")).build();
        try {
            spaces.group("fleet").space("work", work).space("reports", reports);
            AgentCard card = bindOne(new ResultElsewhere()).orElseThrow();
            assertThat(card.spaceFor(TaskEntry.class.getName() + "#v1")).contains("work");
            assertThat(card.spaceFor(FindingEntry.class.getName() + "#v1")).contains("reports");
            AgentCard same = bindOne(new SpaceBoundEmbabelWorker()).orElseThrow();
            assertThat(same.spaceFor(FindingEntry.class.getName() + "#v1")).as("same space: no produced binding").isEmpty();
        } finally {
            work.close();
            reports.close();
        }
    }

    /** A different class under the researcher's name. */
    @Agent(name = "researcher", description = "Another researcher")
    public static class ImpostorResearcher {
        @Action
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "impostor");
        }
    }

    /**
     * Review §7.2 (X20): two different classes under one name on one peer would flap one card id between two
     * cards; the second refuses to bind. A second instance of the same class publishes the same card and may.
     */
    @Test
    void aDifferentClassUnderTheSameNameRefusesToBind() {
        bindOne(new EmbabelResearcher()).orElseThrow();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> spaces.bind(new ImpostorResearcher()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("researcher")
                .hasMessageContaining("ImpostorResearcher");
        assertThat(spaces.bind(new EmbabelResearcher())).as("the same class again is allowed").hasSize(1);
    }

    /** Review §7.2: a configured reader names the card by the class instead; card-name: class. */
    @Test
    void cardNameClassKeepsTheClassDerivedName() {
        new EmbabelDeclarationReader(new EmbabelDeclarationReader.Settings(false, true, "", "1m",
                EmbabelDeclarationReader.Unserved.LOCAL, false)).register();
        assertThat(bindOne(new EmbabelResearcher())).hasValueSatisfying(card ->
                assertThat(card.agent().localName()).isEqualTo("embabelResearcher"));
    }

    /** Review §7.3: serve-actions false and advertise-unserved omit / invocable. */
    @Test
    void unservedActionsFollowTheSettings() {
        LocalSpace work = LocalSpace.builder("work", identity.agent("host")).build();
        try {
            spaces.group("fleet").space("work", work);
            new EmbabelDeclarationReader(new EmbabelDeclarationReader.Settings(true, false, "", "1m",
                    EmbabelDeclarationReader.Unserved.INVOCABLE, false)).register();
            AgentCard invocable = bindOne(new EmbabelResearcher()).orElseThrow();
            assertThat(invocable.actions()).hasSize(1);
            assertThat(invocable.actions().get(0).kind()).isEqualTo(ai.badmonkey.agentspaces.api.ad.CardAction.EMBABEL_ACTION);
            assertThat(invocable.actions().get(0).space()).as("advertised as before: no space, a caller times out").isNull();

            new EmbabelDeclarationReader(new EmbabelDeclarationReader.Settings(true, false, "", "1m",
                    EmbabelDeclarationReader.Unserved.OMIT, false)).register();
            AgentCard omitted = bindOne(new UnnamedAgent()).orElseThrow();
            assertThat(omitted.actions()).isNull();
        } finally {
            work.close();
        }
    }

    /** ISSUE-WorkflowShape §7.1 item 1: the card is named from the group's registry. */
    @Test
    void embabelCardsNameThroughTheGroupsRegistry() {
        String ns = "https://example.org/test#";
        SchemaRegistry shared = NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.test", ns));
        spaces.group("fleet").schemaRegistry(shared);
        AgentCard card = bindOne(new EmbabelResearcher()).orElseThrow();
        assertThat(card.consumes()).containsExactly(ns + "TaskEntry");
        assertThat(card.produces()).containsExactly(ns + "FindingEntry");
    }
}
