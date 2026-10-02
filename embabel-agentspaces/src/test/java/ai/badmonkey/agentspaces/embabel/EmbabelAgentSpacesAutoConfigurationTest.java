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

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.spring.AgentSpacesProperties;
import ai.badmonkey.agentspaces.test.SimNetwork;
import com.embabel.agent.api.annotation.Action;
import com.embabel.agent.api.annotation.Agent;
import com.embabel.agent.core.AgentPlatform;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Embabel auto-configuration as plain objects, the way Spring calls it
 * (spec §10.4 and §10.6): the card post-processor publishes for {@code @Agent}
 * beans, and the remote-actions bridge bean follows the
 * {@code agentspaces.embabel.*} properties.
 */
class EmbabelAgentSpacesAutoConfigurationTest {

    /** A request the remote fleet serves. */
    public record Task(String topic, int priority) {
    }

    /** The correlated result. */
    public record Finding(String topic, String summary) {
    }

    @Agent(name = "planner", description = "Drafts tasks")
    public static class EmbabelPlanner {
        @Action
        public Task draft(String brief) {
            return new Task(brief, 1);
        }
    }

    public static class PlainBean {
    }

    private final EmbabelAgentSpacesAutoConfiguration autoConfig =
            new EmbabelAgentSpacesAutoConfiguration();
    // The auto-configuration binds cards on the system clock, so the cache must too.
    private final InstantSource clock = InstantSource.system();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zEmbabelAuto");
    private PeerIdentity identity;
    private PeerNode node;
    private AgentSpaces spaces;
    private DiscoveryService discovery;
    private LocalSpace work;

    @BeforeEach
    void setUp() throws IOException {
        identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).randomSeed(1).build();
        node.listen(network.register("a"), "a");
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zEmbabelAuto",
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(1), "fleet", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        work = LocalSpace.builder("work", identity.agent("host")).build();
        spaces = new AgentSpaces(identity, clock);
        spaces.register("fleet", groupId, runtime, discovery).space("work", work);
    }

    @AfterEach
    void tearDown() {
        spaces.close();
        work.close();
        node.close();
    }

    private static AgentSpacesProperties fleetProperties() {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("fleet");
        properties.setGroups(List.of(group));
        return properties;
    }

    /** SPEC §10.4: the post-processor publishes a card for every @Agent bean and passes others through. */
    @Test
    void thePostProcessorPublishesEmbabelCards() {
        EmbabelAgentSpacesAutoConfiguration.EmbabelBindingPostProcessor processor =
                autoConfig.embabelBindingPostProcessor(fleetProperties(), spaces, identity);
        PlainBean plain = new PlainBean();
        assertThat(processor.postProcessAfterInitialization(plain, "plain")).isSameAs(plain);
        EmbabelPlanner planner = new EmbabelPlanner();
        assertThat(processor.postProcessAfterInitialization(planner, "planner")).isSameAs(planner);

        List<AgentCard> cards = discovery.find(AgentCard.class, card -> true);
        assertThat(cards).hasSize(1);
        assertThat(cards.get(0).agent().localName()).isEqualTo("planner");
        assertThat(cards.get(0).produces()).containsExactly(Task.class.getName() + "#v1");
    }

    /** SPEC §10.6: the bridge is enabled by default and switched off by agentspaces.embabel.remote-actions-enabled. */
    /** SPEC §10.6: the bridge bean is conditional on remote-actions-enabled (default on), so a disabled bridge is absent, not null. */
    @Test
    void theBridgeBeanFollowsTheEnabledProperty() throws Exception {
        assertThat(autoConfig.embabelRemoteActions(fleetProperties(), spaces, identity)).isNotNull();

        ConditionalOnProperty condition = EmbabelAgentSpacesAutoConfiguration.class
                .getMethod("embabelRemoteActions", AgentSpacesProperties.class,
                        AgentSpaces.class, PeerIdentity.class)
                .getAnnotation(ConditionalOnProperty.class);
        assertThat(condition).isNotNull();
        assertThat(condition.prefix()).isEqualTo("agentspaces.embabel");
        assertThat(condition.name()).containsExactly("remote-actions-enabled");
        assertThat(condition.havingValue()).isEqualTo("true");
        assertThat(condition.matchIfMissing()).isTrue();
        assertThat(new AgentSpacesProperties().getEmbabel().isRemoteActionsEnabled()).isTrue();

        // Nothing to bridge over without a group.
        assertThat(autoConfig.embabelRemoteActions(new AgentSpacesProperties(), spaces, identity))
                .isNull();
    }

    /** Publishes a foreign researcher card so the bridge has one remote action. */
    private void publishForeignResearcher() {
        PeerIdentity foreign = PeerIdentity.generate();
        AgentCard card = new AgentCard("aspace://" + groupId.value() + "/agent/researcher",
                foreign.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                foreign.agent("researcher"), "Researches topics", List.of(),
                List.of(Task.class.getName() + "#v1"), List.of(Finding.class.getName() + "#v1"),
                Map.of());
        discovery.publish(new AdvertisementSigner().sign(card, foreign));
    }

    /** SPEC §10.6: the deployer lifecycle catches the AgentPlatform bean and deploys the generated agent onto it at start. */
    @Test
    @Timeout(30)
    void theDeployerLifecycleDeploysToThePlatformBean() throws Exception {
        publishForeignResearcher();
        AgentSpacesProperties properties = fleetProperties();
        properties.getEmbabel().setDeployPollMillis(100);
        EmbabelRemoteActions bridge = autoConfig.embabelRemoteActions(properties, spaces, identity);
        EmbabelRemoteActionsDeployerLifecycle lifecycle =
                autoConfig.embabelRemoteActionsDeployer(properties, Optional.of(bridge));
        assertThat(lifecycle.deployer()).isPresent();

        List<Object> deployed = new ArrayList<>();
        AgentPlatform platform = deployed::add;
        PlainBean plain = new PlainBean();
        assertThat(lifecycle.postProcessAfterInitialization(plain, "plain")).isSameAs(plain);
        assertThat(lifecycle.postProcessAfterInitialization(platform, "agentPlatform"))
                .isSameAs(platform);
        assertThat(deployed).as("nothing deploys before the context starts").isEmpty();

        lifecycle.start();
        assertThat(lifecycle.isRunning()).isTrue();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (deployed.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(deployed).hasSize(1);
        assertThat(deployed.get(0).getClass().getAnnotation(Agent.class).name())
                .isEqualTo("remoteFleet");
        assertThat(deployed.get(0).getClass().getMethod("researcher_Task", Task.class))
                .isNotNull();
        assertThat(lifecycle.deployer().orElseThrow().deployments()).isEqualTo(1);

        lifecycle.stop();
        assertThat(lifecycle.isRunning()).isFalse();
        assertThat(lifecycle.deployer().orElseThrow().isRunning()).isFalse();
    }

    /** SPEC §10.6: agentspaces.embabel.auto-deploy (default on) gates the deployer bean, and a disabled bridge leaves it inert. */
    @Test
    void autoDeployCanBeDisabled() throws Exception {
        ConditionalOnProperty condition = EmbabelAgentSpacesAutoConfiguration.class
                .getMethod("embabelRemoteActionsDeployer",
                        org.springframework.beans.factory.ObjectProvider.class,
                        org.springframework.beans.factory.ObjectProvider.class)
                .getAnnotation(ConditionalOnProperty.class);
        assertThat(condition).isNotNull();
        assertThat(condition.prefix()).isEqualTo("agentspaces.embabel");
        assertThat(condition.name()).containsExactly("auto-deploy");
        assertThat(condition.havingValue()).isEqualTo("true");
        assertThat(condition.matchIfMissing()).isTrue();

        AgentSpacesProperties properties = fleetProperties();
        assertThat(properties.getEmbabel().isAutoDeploy()).as("on by default").isTrue();
        properties.getEmbabel().setAutoDeploy(false);
        assertThat(properties.getEmbabel().isAutoDeploy()).isFalse();

        // remote-actions-enabled=false leaves no bridge: the lifecycle stays inert.
        EmbabelRemoteActionsDeployerLifecycle inert =
                autoConfig.embabelRemoteActionsDeployer(properties, Optional.empty());
        assertThat(inert.deployer()).isEmpty();
        List<Object> deployed = new ArrayList<>();
        AgentPlatform platform = deployed::add;
        assertThat(inert.postProcessAfterInitialization(platform, "agentPlatform"))
                .isSameAs(platform);
        inert.start();
        assertThat(inert.isRunning()).isFalse();
        assertThat(deployed).isEmpty();
        inert.stop();

        assertThat(EmbabelRemoteActionsDeployerLifecycle.isAgentPlatform(platform)).isTrue();
        assertThat(EmbabelRemoteActionsDeployerLifecycle.isAgentPlatform(new PlainBean()))
                .isFalse();
    }

    /** SPEC §10.6: agent name, description and remote-action timeout come from agentspaces.embabel.*. */
    @Test
    @Timeout(30)
    void theBridgeCarriesTheConfiguredNameDescriptionAndTimeout() throws Exception {
        AgentSpacesProperties properties = fleetProperties();
        properties.getEmbabel().setRemoteAgentName("fleetSkills");
        properties.getEmbabel().setRemoteAgentDescription("What the fleet advertises");
        properties.getEmbabel().setRemoteActionTimeoutMillis(300);

        PeerIdentity foreign = PeerIdentity.generate();
        AgentCard card = new AgentCard("aspace://" + groupId.value() + "/agent/researcher",
                foreign.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                foreign.agent("researcher"), "Researches topics", List.of(),
                List.of(Task.class.getName() + "#v1"), List.of(Finding.class.getName() + "#v1"),
                Map.of());
        discovery.publish(new AdvertisementSigner().sign(card, foreign));

        EmbabelRemoteActions bridge = autoConfig.embabelRemoteActions(properties, spaces, identity);
        Object agent = bridge.agentInstance().orElseThrow();
        Agent annotation = agent.getClass().getAnnotation(Agent.class);
        assertThat(annotation.name()).isEqualTo("fleetSkills");
        assertThat(annotation.description()).isEqualTo("What the fleet advertises");

        Method action = agent.getClass().getMethod("researcher_Task", Task.class);
        assertThatThrownBy(() -> action.invoke(agent, new Task("unserved", 1)))
                .isInstanceOf(InvocationTargetException.class)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PT0.3S");
        assertThat(work.readAll(Template.of(Task.class), 10)).extracting(Task::topic)
                .contains("unserved");
    }
}
