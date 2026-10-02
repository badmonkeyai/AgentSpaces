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
package ai.badmonkey.agentspaces.it;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.it.FleetItApp.ItFinding;
import ai.badmonkey.agentspaces.it.FleetItApp.ItTask;
import ai.badmonkey.agentspaces.it.FleetItApp.Researcher;
import ai.badmonkey.agentspaces.console.FleetConsoleServer;
import ai.badmonkey.agentspaces.spring.Authorizers;
import ai.badmonkey.agentspaces.spring.DirectiveGates;
import ai.badmonkey.agentspaces.spring.AgentSpacesConsoleLifecycle;
import ai.badmonkey.agentspaces.spring.AgentSpacesLifecycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The release gate (docs/BUILD-ENVIRONMENTS.md): real Spring Boot boots the
 * starter. What the in-repo unit tests cannot prove, and these tests do, is
 * that Spring itself honors the stub-compiled annotations: the properties
 * bind, the auto-configuration loads through the imports file, the lifecycle
 * starts with the context, the post-processor sees every bean, and
 * {@code @ConditionalOnMissingBean} backs off when the application declares
 * its own bean.
 */
class SpringFleetIntegrationTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static ConfigurableApplicationContext boot(Class<?> app, int port,
                                                       int seedPort) {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(app)
                .properties(Map.of(
                        "spring.main.web-application-type", "none",
                        "agentspaces.bind", "127.0.0.1:" + port,
                        "agentspaces.tick-millis", "200",
                        "agentspaces.groups[0].name", "it-fleet",
                        "agentspaces.groups[0].founding", "spring-it-fleet-v1",
                        "agentspaces.groups[0].spaces[0].name", "tasks",
                        "agentspaces.groups[0].spaces[0].settle-window-millis", "100",
                        "agentspaces.groups[0].spaces[1].name", "findings",
                        "agentspaces.groups[0].spaces[1].settle-window-millis", "100"));
        if (seedPort > 0) {
            builder.properties(Map.of(
                    "agentspaces.groups[0].seeds[0]", "127.0.0.1:" + seedPort));
        }
        return builder.run();
    }

    @Test
    @Timeout(120)
    void theStarterBootsAndTheAnnotatedBeanWorksTheFleet() throws Exception {
        try (ConfigurableApplicationContext context =
                     boot(FleetItApp.class, freePort(), 0)) {
            AgentSpaces spaces = context.getBean(AgentSpaces.class);
            assertThat(context.getBean(AgentSpacesLifecycle.class).isRunning()).isTrue();
            assertThat(spaces.groupNames()).containsExactly("it-fleet");
            // TODO-EFG §4 / TODO item 6: the default (mtls) profile exposes a
            // membership-rooted Authorizer bean, one per group behind the holder.
            assertThat(context.getBean(Authorizer.class))
                    .isInstanceOf(ai.badmonkey.agentspaces.peering.membership.GroupScopedAuthorizer.class)
                    .isSameAs(context.getBean(Authorizers.class).forGroup("it-fleet"));
            assertThat(context.getBean(Authorizers.class).oidc()).isEmpty();
            assertThat(context.getBeanNamesForType(DirectiveGates.class)).hasSize(1);

            spaces.group("it-fleet").space("tasks")
                    .write(new ItTask("spring boot wiring", 1),
                            Lease.of(Duration.ofMinutes(10)));

            assertThat(spaces.group("it-fleet").space("findings")
                    .read(Template.of(ItFinding.class), Duration.ofSeconds(20)))
                    .hasValueSatisfying(finding -> assertThat(finding.summary())
                            .isEqualTo("done: spring boot wiring"));
            assertThat(context.getBean(Researcher.class).worked)
                    .containsExactly("spring boot wiring");

            // The bound bean's card published through the group's discovery.
            assertThat(spaces.group("it-fleet").discovery().find(AgentCard.class,
                    card -> "it-researcher".equals(card.agent().localName())))
                    .hasSize(1);
            // SPEC §10.5: the booted starter ships its capability providers,
            // advertised under this node's own PeerId.
            assertThat(spaces.group("it-fleet").discovery()
                    .find(CapabilityAdvertisement.class, ad -> true))
                    .isNotEmpty()
                    .allSatisfy(ad -> assertThat(ad.issuer())
                            .isEqualTo(context.getBean(PeerIdentity.class).peerId()))
                    .extracting(CapabilityAdvertisement::capabilityType)
                    .contains("aspace:cap/vote", "aspace:cap/aggregate");
        }
    }

    @Test
    @Timeout(180)
    void twoSpringApplicationsFormOneFleetOverTcp() throws Exception {
        int seedPort = freePort();
        try (ConfigurableApplicationContext first =
                     boot(WriterOnlyApp.class, seedPort, 0);
             ConfigurableApplicationContext second =
                     boot(FleetItApp.class, freePort(), seedPort)) {
            AgentSpaces writer = first.getBean(AgentSpaces.class);

            writer.group("it-fleet").space("tasks")
                    .write(new ItTask("cross-context", 2),
                            Lease.of(Duration.ofMinutes(10)));

            // The second app's researcher takes the task; the finding
            // replicates back to the first app's replica.
            assertThat(writer.group("it-fleet").space("findings")
                    .read(Template.of(ItFinding.class), Duration.ofSeconds(60)))
                    .hasValueSatisfying(finding -> assertThat(finding.summary())
                            .isEqualTo("done: cross-context"));
            assertThat(second.getBean(Researcher.class).worked)
                    .containsExactly("cross-context");
        }
    }

    @Test
    @Timeout(120)
    void theConsoleServesWhenEnabledAndStaysOffOtherwise() throws Exception {
        // Off by default: the conditional keeps every console bean out.
        try (ConfigurableApplicationContext context =
                     boot(FleetItApp.class, freePort(), 0)) {
            assertThat(context.getBeanNamesForType(FleetConsoleServer.class)).isEmpty();
        }

        try (ConfigurableApplicationContext context =
                     new SpringApplicationBuilder(FleetItApp.class)
                             .properties(Map.of(
                                     "spring.main.web-application-type", "none",
                                     "agentspaces.bind", "127.0.0.1:" + freePort(),
                                     "agentspaces.tick-millis", "200",
                                     "agentspaces.groups[0].name", "it-fleet",
                                     "agentspaces.groups[0].founding", "spring-it-fleet-v1",
                                     "agentspaces.groups[0].spaces[0].name", "tasks",
                                     "agentspaces.groups[0].spaces[1].name", "findings",
                                     "agentspaces.console.enabled", "true",
                                     "agentspaces.console.port", "0"))
                             .run()) {
            int port = context.getBean(AgentSpacesConsoleLifecycle.class).boundPort();
            AgentSpaces spaces = context.getBean(AgentSpaces.class);
            spaces.group("it-fleet").space("tasks")
                    .write(new ItTask("watched by the console", 1),
                            Lease.of(Duration.ofMinutes(10)));
            assertThat(spaces.group("it-fleet").space("findings")
                    .read(Template.of(ItFinding.class), Duration.ofSeconds(20)))
                    .isPresent();

            HttpClient http = HttpClient.newHttpClient();
            String overview = http.send(HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/overview"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            assertThat(overview)
                    .contains("\"fleet\":\"it-fleet\"")
                    .contains("\"written\":")
                    .contains("it-researcher");
            String panels = http.send(HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/panels"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            assertThat(panels).contains("it-panel");
            String ui = http.send(HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + port + "/"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            assertThat(ui).contains("it-fleet").contains("EventSource");
        }
    }

    @Test
    @Timeout(120)
    void anApplicationDeclaredIdentityBacksTheAutoConfigurationOff() throws Exception {
        try (ConfigurableApplicationContext context =
                     boot(IdentityOverrideApp.class, freePort(), 0)) {
            assertThat(context.getBean(PeerIdentity.class))
                    .isSameAs(IdentityOverrideApp.FIXED);
            // The rest of the chain still wired on top of the overridden bean.
            AgentSpaces spaces = context.getBean(AgentSpaces.class);
            assertThat(spaces.group("it-fleet").spaceNames())
                    .containsExactlyInAnyOrder("tasks", "findings");
        }
    }

    /** An app with no agent beans: it only writes and reads. */
    @Configuration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    static class WriterOnlyApp {
    }

    /** An app that declares its own identity bean. */
    @Configuration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    static class IdentityOverrideApp {

        static final PeerIdentity FIXED = PeerIdentity.generate();

        @Bean
        PeerIdentity peerIdentity() {
            return FIXED;
        }
    }
}
