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
package ai.badmonkey.agentspaces.it.embabel;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.embabel.EmbabelRemoteActions;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import com.embabel.agent.api.invocation.AgentInvocation;
import com.embabel.agent.core.AgentPlatform;
import com.embabel.agent.spi.support.springai.SpringAiLlmService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The first in-repo test against a real Embabel artifact. A worker on a plain
 * AgentSpaces peer advertises "Brief in, Summary out"; a Spring Boot 4.1
 * application with Embabel 1.5 and the extension joins the same group, the
 * extension deploys its generated fleet agent onto the real
 * {@link AgentPlatform} (through Embabel's own {@code AgentMetadataReader}), and
 * Embabel's GOAP planner reaches the {@code Summary} goal by invoking that
 * action, which the remote worker performs.
 */
class EmbabelFleetPlanningTest {

    private static final String FOUNDING = "embabel-it-fleet-v1";

    /** The task the remote worker consumes. */
    public record Brief(String topic) {
    }

    /** The result the remote worker produces, and the planner's goal type. */
    public record Summary(String topic, String text) {
    }

    /** The remote worker: a plain annotated object on a plain peer. */
    @AgentSpec(name = "summarizer", description = "Summarizes briefs",
            goals = {"produce a summary"})
    public static class Summarizer {
        @SpaceTake(space = "work", pollTimeout = "PT0.2S")
        public Summary summarize(Brief brief) {
            return new Summary(brief.topic(), "summary of " + brief.topic());
        }
    }

    /** The Embabel application: its only agent arrives from the fleet. */
    @SpringBootApplication
    public static class PlannerApp {

        /** A model the platform can resolve; the fleet plan never calls it. */
        @Bean
        SpringAiLlmService scriptedLlm() {
            ChatModel model = new ChatModel() {
                @Override
                public ChatResponse call(Prompt prompt) {
                    return new ChatResponse(List.of(new Generation(new AssistantMessage("{}"))));
                }
            };
            return new SpringAiLlmService("scripted", "test", model);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private record WorkerPeer(PeerNode node, ReplicatedSpace work, AgentBinder binder)
            implements AutoCloseable {
        @Override
        public void close() {
            binder.close();
            work.close();
            node.close();
        }
    }

    private static WorkerPeer startWorker(int port) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement group = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365),
                "planner-fleet", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        GroupRuntime runtime = node.joinGroup(group,
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                List.<PeerAdvertisement.Endpoint>of());
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        ReplicatedSpace work = ReplicatedSpace.builder(runtime, "work", identity, "summarizer")
                .settleWindow(Duration.ofMillis(100))
                .build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), discovery,
                InstantSource.system());
        binder.space("work", work);
        node.startTicking(Duration.ofMillis(200));
        binder.bind(new Summarizer());
        return new WorkerPeer(node, work, binder);
    }

    @Test
    @Timeout(180)
    void embabelsPlannerReachesItsGoalThroughAFleetAction() throws Exception {
        int workerPort = freePort();
        try (WorkerPeer worker = startWorker(workerPort);
             ConfigurableApplicationContext context = new SpringApplicationBuilder(PlannerApp.class)
                     .properties(Map.of(
                             "spring.main.web-application-type", "none",
                             "agentspaces.security.profile", "dev-local",
                             "agentspaces.bind", "127.0.0.1:" + freePort(),
                             "agentspaces.tick-millis", "200",
                             "agentspaces.embabel.deploy-poll-millis", "250",
                             "agentspaces.groups[0].name", "planner-fleet",
                             "agentspaces.groups[0].founding", FOUNDING,
                             "agentspaces.groups[0].seeds[0]", "127.0.0.1:" + workerPort,
                             "agentspaces.groups[0].spaces[0].name", "work"))
                     // Embabel ships its own default-llm, which outranks the
                     // builder's default properties; a command-line argument
                     // outranks both.
                     .run("--embabel.models.default-llm=scripted")) {
            AgentPlatform platform = context.getBean(AgentPlatform.class);
            EmbabelRemoteActions bridge = context.getBean(EmbabelRemoteActions.class);

            // The worker's card reaches the application's discovery cache over TCP.
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (bridge.actions().available().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            assertThat(bridge.actions().available()).as("the fleet action is discovered")
                    .isNotEmpty();

            // The deployer puts the generated fleet agent onto the real platform.
            deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (platform.agents().stream().noneMatch(a -> a.getName().equals(bridge.agentName()))
                    && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            assertThat(platform.agents()).as("agents on the platform")
                    .anySatisfy(agent -> assertThat(agent.getName()).isEqualTo(bridge.agentName()));

            // Embabel's planner plans Brief -> Summary and runs the fleet action.
            Summary summary = AgentInvocation.create(platform, Summary.class)
                    .invoke(new Brief("tuple spaces"));
            assertThat(summary.text()).isEqualTo("summary of tuple spaces");
        }
    }
}
