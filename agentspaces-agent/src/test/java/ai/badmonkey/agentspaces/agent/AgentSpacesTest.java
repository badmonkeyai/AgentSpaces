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
import ai.badmonkey.agentspaces.agent.annotation.ProvidesCapability;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.capability.AggregateClient;
import ai.badmonkey.agentspaces.agent.capability.VoteClient;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
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
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The fluent facade (spec §10.2) over local wiring, plus its routing and capability surface (§10.3 to §10.5). */
class AgentSpacesTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final PeerIdentity identity = PeerIdentity.generate();
    private final AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system());
    private final LocalSpace tasks = LocalSpace
            .builder("tasks", identity.agent("host")).build();
    private final LocalSpace findings = LocalSpace
            .builder("findings", identity.agent("host")).build();

    // The networked fixture: nodes on one in-memory network under a shared test clock.
    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        spaces.close();
        tasks.close();
        findings.close();
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        nodes.forEach(PeerNode::close);
    }

    @AgentSpec(name = "researcher", description = "Researches topics", goals = {"research"})
    public static class Researcher {

        @SpaceTake(space = "tasks", lease = "PT10M", pollTimeout = "PT0.2S",
                resultSpace = "findings")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done: " + task.topic());
        }
    }

    @Test
    void theFluentChainNavigatesGroupsAndSpaces() {
        spaces.register("research-fleet", GroupId.of("zFluent"), null, null)
                .space("tasks", tasks)
                .space("findings", findings);

        spaces.group("research-fleet").space("tasks")
                .write(new TaskEntry("agentic memory", 3), Lease.of(Duration.ofMinutes(30)));

        assertThat(spaces.group("research-fleet").space("tasks")
                .read(Template.of(TaskEntry.class)))
                .contains(new TaskEntry("agentic memory", 3));
        assertThat(spaces.groupNames()).containsExactly("research-fleet");
        assertThat(spaces.group("research-fleet").spaceNames())
                .containsExactlyInAnyOrder("tasks", "findings");
    }

    @Test
    void bindingThroughTheGroupContextRunsTheWorkerIdiom() {
        AgentSpaces.GroupContext fleet =
                spaces.register("fleet", GroupId.of("zFluentBind"), null, null)
                        .space("tasks", tasks)
                        .space("findings", findings);
        fleet.bind(new Researcher());

        fleet.space("tasks").write(new TaskEntry("bind me", 1),
                Lease.of(Duration.ofMinutes(10)));

        assertThat(fleet.space("findings").read(
                Template.of(FindingEntry.class), Duration.ofSeconds(5)))
                .hasValueSatisfying(finding ->
                        assertThat(finding.summary()).isEqualTo("done: bind me"));
    }

    @Test
    void unknownNamesFailWithHelpfulMessages() {
        spaces.register("fleet", GroupId.of("zFluentErr"), null, null)
                .space("tasks", tasks);

        assertThatThrownBy(() -> spaces.group("nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fleet");
        assertThatThrownBy(() -> spaces.group("fleet").space("nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tasks");
        assertThatThrownBy(() -> spaces.group("fleet").discovery())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void duplicateRegistrationsAreRejected() {
        AgentSpaces.GroupContext fleet =
                spaces.register("fleet", GroupId.of("zFluentDup"), null, null)
                        .space("tasks", tasks);

        assertThatThrownBy(() -> spaces.register("fleet", GroupId.of("zFluentDup"),
                null, null)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> fleet.space("tasks", findings))
                .isInstanceOf(IllegalStateException.class);
    }

    // ------------------------------------------- binding by group name (§10.3)

    /** Two take methods on one bean, each addressed to a different group by name. */
    public static class TwoGroupWorker {
        final CopyOnWriteArrayList<String> alpha = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> beta = new CopyOnWriteArrayList<>();

        @SpaceTake(group = "alpha", space = "tasks", pollTimeout = "PT0.2S")
        public void alpha(TaskEntry task) {
            alpha.add(task.topic());
        }

        @SpaceTake(group = "beta", space = "tasks", pollTimeout = "PT0.2S")
        public void beta(TaskEntry task) {
            beta.add(task.topic());
        }
    }

    /** SPEC §10.3: @SpaceTake(group = ...) binds each method into the named group only, even when the spaces share a name. */
    @Test
    @Timeout(30)
    void aBeanMayBindMethodsIntoDifferentGroupsByName() throws Exception {
        LocalSpace alphaTasks = local("tasks");
        LocalSpace betaTasks = local("tasks");
        spaces.register("alpha", GroupId.of("zAlpha"), null, null).space("tasks", alphaTasks);
        spaces.register("beta", GroupId.of("zBeta"), null, null).space("tasks", betaTasks);
        TwoGroupWorker worker = new TwoGroupWorker();

        List<AgentBinder.Bound> bound = spaces.bind(worker);
        assertThat(bound).hasSize(2);
        assertThat(bound).extracting(handle -> handle.card().group())
                .containsExactlyInAnyOrder(GroupId.of("zAlpha"), GroupId.of("zBeta"));

        alphaTasks.write(new TaskEntry("for-alpha", 1), HOUR);
        betaTasks.write(new TaskEntry("for-beta", 1), HOUR);
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while ((worker.alpha.isEmpty() || worker.beta.isEmpty()) && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        Thread.sleep(300); // give a wrongly routed method every chance to misfire
        assertThat(worker.alpha).containsExactly("for-alpha");
        assertThat(worker.beta).containsExactly("for-beta");

        // A method naming a group nobody joined is a wiring mistake, surfaced at bind.
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @SpaceTake(group = "gamma", space = "tasks")
            public void gamma(TaskEntry task) {
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("gamma");
    }

    // ------------------------------------------ cards into every group (§10.4)

    /** SPEC §10.4: the facade binds into every joined group whose spaces satisfy the bean and publishes a card per group. */
    @Test
    void bindingPublishesACardIntoEveryJoinedGroup() throws IOException {
        Node node = node("solo");
        AgentSpaces.GroupContext alpha = join(node, "alpha", "zCardsAlpha");
        AgentSpaces.GroupContext beta = join(node, "beta", "zCardsBeta");
        alpha.space("tasks", local("tasks", node.identity()));
        beta.space("tasks", local("tasks", node.identity()));
        AgentSpaces.GroupContext noTasks = join(node, "gamma", "zCardsGamma");
        noTasks.space("other", local("other", node.identity()));

        List<AgentBinder.Bound> bound = node.spaces().bind(new TasksResearcher());
        assertThat(bound).hasSize(2);

        String taskSchema = TaskEntry.class.getName() + "#v1";
        for (AgentSpaces.GroupContext group : List.of(alpha, beta)) {
            List<AgentCard> cards = group.discovery().find(AgentCard.class, card -> true);
            assertThat(cards).as(group.name()).hasSize(1);
            assertThat(cards.get(0).group()).isEqualTo(group.id());
            assertThat(cards.get(0).agent().localName()).isEqualTo("tasksResearcher");
            assertThat(cards.get(0).spaceFor(taskSchema)).contains("tasks");
        }
        assertThat(noTasks.discovery().find(AgentCard.class, card -> true))
                .as("a group without the bean's space gets no card").isEmpty();
    }

    /** Takes tasks from the "tasks" space, wherever a group registers one. */
    @AgentSpec(description = "Works the tasks space")
    public static class TasksResearcher {
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }
    }

    // ------------------------------------------- typed capability clients (§10.5)

    /** SPEC §10.5: capability(Class) resolves one typed client per group from the registered factories. */
    @Test
    void typedCapabilityClientsResolvePerGroup() throws IOException {
        Node node = node("caps");
        AgentSpaces.GroupContext alpha = join(node, "alpha", "zCapAlpha");
        AgentSpaces.GroupContext beta = join(node, "beta", "zCapBeta");
        LocalSpace alphaVotes = local("votes", node.identity());
        LocalSpace betaVotes = local("votes", node.identity());
        alpha.space("votes", alphaVotes, node.identity().agent("host"))
                .provide(vote(alphaVotes, node));
        beta.space("votes", betaVotes, node.identity().agent("host"))
                .provide(vote(betaVotes, node));

        assertThat(node.spaces().clientTypes())
                .contains(VoteClient.class, AggregateClient.class);
        VoteClient alphaVote = alpha.capability(VoteClient.class);
        VoteClient betaVote = beta.capability(VoteClient.class);
        assertThat(alpha.capability(VoteClient.class)).isSameAs(alphaVote);
        assertThat(betaVote).isNotSameAs(alphaVote);
        assertThat(alphaVote.capability()).isSameAs(alpha.provider(VoteCapability.TYPE).orElseThrow());

        alphaVote.propose("p1", "Adopt AUCTION?", List.of("yes", "no"), 1, HOUR);
        assertThat(alphaVote.proposal("p1")).isPresent();
        assertThat(betaVote.proposal("p1")).as("groups are isolated").isEmpty();
        alphaVote.castBallot("p1", "yes", HOUR);
        assertThat(alphaVote.decision("p1")).hasValueSatisfying(decision ->
                assertThat(decision.winner()).isEqualTo("yes"));

        assertThat(alpha.capabilities().providersOf(VoteCapability.TYPE)).hasSize(1);
        assertThat(alphaVote.providers()).hasSize(1);
        assertThatThrownBy(() -> alpha.capability(String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VoteClient");
    }

    /**
     * QA4 A4-1: {@code @ProvidesCapability} on a class that does not implement
     * {@code CapabilityProvider} used to be ignored in silence — the branch that
     * registers it is guarded on the interface, and a bean that is not also an
     * agent then fell through to an empty result. The application booted green,
     * the capability was never advertised, and nothing said why. Every other
     * annotation fails fast; so must this one.
     */
    @Test
    void providesCapabilityWithoutTheInterfaceFailsFast() throws IOException {
        Node node = node("caps");
        join(node, "alpha", "zCapAlpha");

        assertThatThrownBy(() -> node.spaces().bind(new NotAProvider()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NotAProvider")
                .hasMessageContaining("CapabilityProvider");
    }

    /** Annotated, but does not implement the interface the annotation promises. */
    @ProvidesCapability("aspace:cap/broken")
    public static class NotAProvider {
    }

    /** A capability bean: wraps the vote capability and advertises it. */
    @ProvidesCapability(VoteCapability.TYPE)
    public static class VoteService implements CapabilityProvider {
        private final VoteCapability delegate;

        VoteService(VoteCapability delegate) {
            this.delegate = delegate;
        }

        @Override
        public String capabilityType() {
            return delegate.capabilityType();
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            return delegate.describe(group);
        }
    }

    /** SPEC §10.5: a @ProvidesCapability bean is registered and advertised; a consumer's typed client votes with it across peers. */
    @Test
    @Timeout(60)
    void aProvidesCapabilityBeanIsRegisteredAndAdvertised() throws IOException {
        Node provider = node("provider");
        Node consumer = node("consumer", "provider");
        AgentSpaces.GroupContext providerFleet = join(provider, "fleet", "zVoteFleet");
        AgentSpaces.GroupContext consumerFleet = join(consumer, "fleet", "zVoteFleet");
        // The provider wires its capability runtime explicitly; the consumer lets
        // the facade create one on first use.
        provider.spaces().register("fleet", new CapabilityRuntime(providerFleet.runtime(),
                providerFleet.discovery(), provider.identity()));
        ReplicatedSpace providerVotes = replicated(providerFleet, "votes");
        ReplicatedSpace consumerVotes = replicated(consumerFleet, "votes");
        providerFleet.space("votes", providerVotes, provider.identity().agent("voter"));
        consumerFleet.space("votes", consumerVotes, consumer.identity().agent("voter"));
        tick(4);

        VoteService service = new VoteService(new VoteCapability(providerVotes,
                provider.identity().agent("voter"), provider.identity().peerId(), clock));
        assertThat(provider.spaces().bind(service)).as("a pure provider binds no agent").isEmpty();
        assertThat(providerFleet.provider(VoteCapability.TYPE)).contains(service);
        tick(2);

        List<CapabilityAdvertisement> seen =
                consumerFleet.capabilities().providersOf(VoteCapability.TYPE);
        assertThat(seen).hasSize(1);
        assertThat(seen.get(0).issuer()).isEqualTo(provider.identity().peerId());
        assertThat(seen.get(0).binding()).isEqualTo("space:votes");

        // The consumer's client is built over the advertised space binding.
        VoteClient consumerVote = consumerFleet.capability(VoteClient.class);
        consumerVote.propose("ship", "Ship v0.1.10?", List.of("approve", "reject"), 2, HOUR);
        VoteClient providerVote = providerFleet.capability(VoteClient.class);
        assertThat(providerVote.proposal("ship")).isPresent();

        providerVote.castBallot("ship", "approve", HOUR);
        assertThat(consumerVote.decision("ship")).as("quorum of two, one ballot").isEmpty();
        consumerVote.castBallot("ship", "approve", HOUR);
        for (VoteClient client : List.of(consumerVote, providerVote)) {
            assertThat(client.decision("ship")).hasValueSatisfying(decision -> {
                assertThat(decision.winner()).isEqualTo("approve");
                assertThat(decision.tally()).containsEntry("approve", 2);
            });
        }
    }

    /** SPEC §8/§10.5: refreshCards also re-issues every capability advertisement, so it never lapses while the host runs. */
    @Test
    void refreshAlsoRenewsCapabilityAdvertisements() throws IOException {
        Node node = node("refresh");
        AgentSpaces.GroupContext fleet = join(node, "fleet", "zRefreshCaps");
        LocalSpace votes = local("votes", node.identity());
        fleet.space("votes", votes, node.identity().agent("host")).provide(vote(votes, node));
        assertThat(fleet.capabilities().providersOf(VoteCapability.TYPE)).hasSize(1);

        clock.advance(Duration.ofMinutes(16)); // past the 15-minute TTL, no refresh
        assertThat(fleet.capabilities().providersOf(VoteCapability.TYPE)).isEmpty();

        node.spaces().refreshCards();
        List<CapabilityAdvertisement> alive = fleet.capabilities().providersOf(VoteCapability.TYPE);
        assertThat(alive).hasSize(1);
        assertThat(alive.get(0).issued()).isEqualTo(clock.instant());
    }

    // ------------------------------------------------------------------ fixture

    private record Node(PeerNode node, PeerIdentity identity, AgentSpaces spaces) {
    }

    private LocalSpace local(String name) {
        return local(name, identity);
    }

    /** A local space whose writes are attributed to {@code owner}'s "host" agent. */
    private LocalSpace local(String name, PeerIdentity owner) {
        LocalSpace space = LocalSpace.builder(name, owner.agent("host")).build();
        closeables.add(space);
        return space;
    }

    private ReplicatedSpace replicated(AgentSpaces.GroupContext group, String name) {
        ReplicatedSpace space = ReplicatedSpace.builder(group.runtime(), name,
                        group.identity(), "voter")
                .clock(clock).settleWindow(Duration.ZERO).build();
        closeables.add(space);
        return space;
    }

    private VoteCapability vote(LocalSpace votes, Node node) {
        return new VoteCapability(votes, node.identity().agent("host"),
                node.identity().peerId(), clock);
    }

    private Node node(String address, String... seedAddresses) throws IOException {
        PeerIdentity nodeIdentity = PeerIdentity.generate();
        PeerNode peerNode = PeerNode.builder(nodeIdentity).clock(clock)
                .randomSeed(nodes.size() + 1).build();
        peerNode.listen(network.register(address), address);
        nodes.add(peerNode);
        AgentSpaces facade = new AgentSpaces(nodeIdentity, clock);
        closeables.add(0, facade); // close facades before their spaces and nodes
        Node node = new Node(peerNode, nodeIdentity, facade);
        seeds.put(node, List.of(seedAddresses));
        return node;
    }

    private final java.util.Map<Node, List<String>> seeds = new java.util.HashMap<>();

    private AgentSpaces.GroupContext join(Node node, String groupName, String groupIdValue) {
        GroupId groupId = GroupId.of(groupIdValue);
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupIdValue,
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(1), groupName, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds.get(node)) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", seed, 0));
        }
        GroupRuntime runtime = node.node().joinGroup(groupAd,
                GroupMembership.Config.defaults(), endpoints);
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, clock), codec, node.identity().peerId());
        return node.spaces().register(groupName, groupId, runtime, discovery);
    }

    private void tick(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
        }
    }

    /** ISSUE-WorkflowShape §9.1: the facade's registry reaches every group registered after it; a group may still choose its own. */
    @Test
    void theFacadeRegistryReachesEveryGroupRegisteredAfterIt() {
        String ns = "https://example.org/test#";
        SchemaRegistry shared =
                NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.test", ns));
        AgentSpaces.GroupContext before = spaces.register("before", GroupId.of("zBefore"), null, null)
                .space("tasks", tasks).space("findings", findings);
        assertThat(spaces.schemaRegistry()).isEmpty();
        assertThat(before.schemaRegistry()).as("defaults to the binder's")
                .isSameAs(before.binder().schemas());

        assertThat(spaces.schemaRegistry(shared)).isSameAs(spaces);
        assertThat(spaces.schemaRegistry()).contains(shared);
        AgentSpaces.GroupContext after = spaces.register("after", GroupId.of("zAfter"), null, null)
                .space("tasks", tasks).space("findings", findings);
        assertThat(after.schemaRegistry()).isSameAs(shared);
        assertThat(after.binder().schemas()).isSameAs(shared);
        assertThat(before.schemaRegistry()).as("already registered groups keep theirs")
                .isNotSameAs(shared);

        assertThat(after.bind(new Researcher()).card().consumes())
                .containsExactly(ns + "TaskEntry");
        assertThat(before.bind(new Researcher()).card().consumes())
                .containsExactly(TaskEntry.class.getName() + "#v1");

        // A group's own setter wins over the facade default, until its first bind.
        SchemaRegistry own = NamespaceSchemaRegistry.of(
                Map.of("ai.badmonkey.agentspaces.test", "https://example.org/own#"));
        AgentSpaces.GroupContext chosen = spaces.register("chosen", GroupId.of("zChosen"), null, null)
                .schemaRegistry(own);
        assertThat(chosen.schemaRegistry()).isSameAs(own);
        assertThatThrownBy(() -> after.schemaRegistry(own))
                .isInstanceOf(IllegalStateException.class);
    }
}
