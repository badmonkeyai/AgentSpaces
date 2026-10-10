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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.space.local.NamespaceSchemaRegistry;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Card lifecycle under a deterministic clock (spec §10.4): bound agents' cards
 * are leased and re-published with a fresh issue time by {@code refreshCards},
 * so they never age out of the ad-cache while the agent stays bound.
 */
class AgentBinderCardsTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zCards");
    private PeerIdentity identity;
    private PeerNode node;
    private DiscoveryService discovery;
    private LocalSpace tasks;
    private AgentBinder binder;

    @AgentSpec(name = "researcher", description = "Researches topics")
    public static class Researcher {
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }
    }

    /** A two-action agent: a described take and an undescribed reaction. */
    @AgentSpec(name = "desk", description = "A research desk")
    public static class Desk {
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S", description = "Researches a topic in depth")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }

        @ai.badmonkey.agentspaces.agent.annotation.SpaceNotify(space = "tasks")
        public void audit(FindingEntry finding) {
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        node.listen(network.register("a"), "a");
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zCards",
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(1), "cards", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        tasks = LocalSpace.builder("tasks", identity.agent("host")).build();
        binder = new AgentBinder(identity, groupId, discovery, clock);
        binder.space("tasks", tasks);
    }

    @AfterEach
    void tearDown() {
        binder.close();
        tasks.close();
        node.close();
    }

    /** SPEC §10.4: refreshCards re-publishes a bound agent's card with a newer issue time. */
    @Test
    void refreshCardsRepublishesWithANewerIssueTime() {
        AgentBinder.Bound bound = binder.bind(new Researcher());
        Instant first = bound.card().issued();
        assertThat(discovery.find(AgentCard.class, c -> true)).hasSize(1);

        clock.advance(Duration.ofMinutes(1));
        binder.refreshCards();

        assertThat(bound.card().issued()).isAfter(first);
        List<AgentCard> cached = discovery.find(AgentCard.class, c -> true);
        assertThat(cached).hasSize(1);
        assertThat(cached.get(0).issued()).isEqualTo(bound.card().issued());
        assertThat(cached.get(0).id()).isEqualTo(bound.card().id());
    }

    /** SPEC §4.2 v0.1.13: once a renewing identity renews, refreshCards publishes the card with the new certificate. */
    @Test
    void refreshCardsCarriesTheRenewedCertificate() {
        try (AgentBinder renewing = new AgentBinder(identity, groupId, discovery, clock, "cards",
                name -> identity.renewingSubordinate(name, Duration.ofHours(1), clock))) {
            renewing.space("tasks", tasks);
            AgentBinder.Bound bound = renewing.bind(new Researcher());
            Instant first = bound.card().agentCertificate().issued();

            clock.advance(Duration.ofMinutes(40)); // past half the TTL: due for renewal
            renewing.refreshCards();

            assertThat(bound.card().agentCertificate().issued())
                    .as("the renewed certificate, not the first").isEqualTo(clock.instant()).isAfter(first);
            assertThat(discovery.find(AgentCard.class, c -> true)).singleElement()
                    .satisfies(c -> assertThat(c.agentCertificate().issued()).isEqualTo(clock.instant()));
        }
    }

    /** SPEC §10.4: without refresh a card ages out at its TTL; a refresh keeps it alive. */
    @Test
    void anUnrefreshedCardAgesOutButARefreshedOneStaysCached() {
        binder.bind(new Researcher());
        clock.advance(Duration.ofMinutes(16)); // past the 15-minute card TTL
        assertThat(discovery.find(AgentCard.class, c -> true)).isEmpty();

        binder.refreshCards();
        assertThat(discovery.find(AgentCard.class, c -> true)).hasSize(1);
    }

    /** SPEC §6.1 v0.1.13 (TODO item 6): each bound method is a declared action with its own schemas, space, kind, and description, sorted by name; and a refreshed card keeps its actions (review M-2: refreshCards used to rebuild cards field by field). */
    @Test
    void eachBoundMethodIsADeclaredActionThatSurvivesRefresh() {
        AgentBinder.Bound bound = binder.bind(new Desk());
        List<ai.badmonkey.agentspaces.api.ad.CardAction> actions = bound.card().actions();
        assertThat(actions).extracting(ai.badmonkey.agentspaces.api.ad.CardAction::name)
                .containsExactly("audit", "research");
        ai.badmonkey.agentspaces.api.ad.CardAction research = actions.get(1);
        assertThat(research.kind()).isEqualTo(ai.badmonkey.agentspaces.api.ad.CardAction.TAKE);
        assertThat(research.space()).isEqualTo("tasks");
        assertThat(research.description()).isEqualTo("Researches a topic in depth");
        assertThat(research.consumes()).containsExactly(TaskEntry.class.getName() + "#v1");
        assertThat(research.produces()).containsExactly(FindingEntry.class.getName() + "#v1");
        ai.badmonkey.agentspaces.api.ad.CardAction audit = actions.get(0);
        assertThat(audit.kind()).isEqualTo(ai.badmonkey.agentspaces.api.ad.CardAction.NOTIFY);
        assertThat(audit.produces()).as("a void reaction produces nothing").isEmpty();

        clock.advance(Duration.ofMinutes(1));
        binder.refreshCards();
        assertThat(bound.card().actions()).isEqualTo(actions);
        assertThat(discovery.find(AgentCard.class, c -> true).get(0).actions()).isEqualTo(actions);
    }

    /** ISSUE-WorkflowShape §7.1 item 1 / §14 item 1: a binder given the namespace registry publishes IRI names on consumes, produces, spaceBindings, and each CardAction. */
    @Test
    void aNamespaceRegistryPublishesIriNamesOnEveryCardField() {
        String ns = "https://example.org/test#";
        SchemaRegistry schemas =
                NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.test", ns));
        try (AgentBinder named = new AgentBinder(identity, groupId, discovery, clock)) {
            assertThat(named.schemas(schemas)).isSameAs(named);
            assertThat(named.schemas()).isSameAs(schemas);
            named.space("tasks", tasks);
            AgentCard card = named.bind(new Desk()).card();

            assertThat(card.consumes())
                    .containsExactlyInAnyOrder(ns + "TaskEntry", ns + "FindingEntry");
            assertThat(card.produces()).containsExactly(ns + "FindingEntry");
            assertThat(card.spaceBindings()).containsEntry(ns + "TaskEntry", "tasks")
                    .containsEntry(ns + "FindingEntry", "tasks");
            assertThat(card.actions()).extracting(CardAction::name)
                    .containsExactly("audit", "research");
            CardAction research = card.actions().get(1);
            assertThat(research.consumes()).containsExactly(ns + "TaskEntry");
            assertThat(research.produces()).containsExactly(ns + "FindingEntry");
            assertThat(card.actions().get(0).consumes()).containsExactly(ns + "FindingEntry");
            assertThat(discovery.find(AgentCard.class, c -> c.agent().equals(card.agent())))
                    .singleElement().satisfies(published ->
                            assertThat(published.consumes()).isEqualTo(card.consumes()));
        }
    }

    /** ISSUE-WorkflowShape §9.1: the registry is fixed by the first bind, as the identity factory is meant to be. */
    @Test
    void theRegistryCannotChangeAfterTheFirstBind() {
        SchemaRegistry schemas = NamespaceSchemaRegistry.of(
                Map.of("ai.badmonkey.agentspaces.test", "https://example.org/test#"));
        binder.bind(new Researcher());
        assertThatThrownBy(() -> binder.schemas(schemas))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bind");
        assertThat(binder.schemas()).as("the default registry stays").isNotSameAs(schemas);
    }

    /**
     * ISSUE-WorkflowShape §8.1: a space and a binder sharing one registry agree
     * on names by construction. The space names what it writes through the
     * registry, the card names what it consumes through the same registry, and
     * the worker still takes and completes under the IRI names.
     */
    @Test
    void aSpaceAndABinderSharingTheRegistryAgreeOnNames() {
        String ns = "https://example.org/test#";
        SchemaRegistry shared =
                NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.test", ns));
        try (LocalSpace work = LocalSpace.builder("tasks", identity.agent("host"))
                     .schemaRegistry(shared).build();
             AgentBinder named = new AgentBinder(identity, groupId, discovery, clock)
                     .schemas(shared)) {
            named.space("tasks", work);
            AgentCard card = named.bind(new Researcher()).card();

            work.write(new TaskEntry("agreement", 1), Lease.of(Duration.ofMinutes(5)));
            assertThat(work.read(Template.of(FindingEntry.class), Duration.ofSeconds(10)))
                    .hasValueSatisfying(f -> assertThat(f.topic()).isEqualTo("agreement"));

            // The space registered both types as it wrote them; the card's names
            // are those registrations, not a parallel guess.
            assertThat(card.consumes()).containsExactly(shared.schemaNameOf(TaskEntry.class));
            assertThat(card.produces()).containsExactly(shared.schemaNameOf(FindingEntry.class));
            assertThat(shared.schemaNameOf(TaskEntry.class)).isEqualTo(ns + "TaskEntry");
            assertThat(work.readAll(Template.of(FindingEntry.class), 10)).hasSize(1);
        }
    }
}
