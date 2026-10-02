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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.auth.oidc.BearerJwtValidator;
import ai.badmonkey.agentspaces.console.ConsoleCommand;
import ai.badmonkey.agentspaces.console.ConsolePanel;
import ai.badmonkey.agentspaces.console.ConsoleView;
import ai.badmonkey.agentspaces.console.FleetCommander;
import ai.badmonkey.agentspaces.console.FleetConsoleServer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * Serves the fleet console when {@code agentspaces.console.enabled=true}
 * (spec's "the console is one more read-only peer": everything shown derives
 * from this peer's replicas, membership, and ad cache). The default view
 * watches every configured space generically; {@link ConsoleViewCustomizer}
 * beans refine it with the application's entry types and worker attribution,
 * and every {@link ConsolePanel} bean becomes a section on the served UI and
 * a document under {@code /api/v1/panels}. Each bean here is
 * {@code @ConditionalOnMissingBean}, so an application replaces any piece by
 * declaring its own.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "agentspaces.console", name = "enabled",
        havingValue = "true")
public class AgentSpacesConsoleAutoConfiguration {

    /**
     * The console model over the configured fabric: first group's membership
     * and discovery, every configured space watched generically, refined by
     * every {@link ConsoleViewCustomizer} bean in order.
     *
     * @param spaces      the fabric facade
     * @param node        the peer node, for per-connection channel modes
     * @param customizers the application's refinements, possibly empty
     * @return the console view
     */
    @Bean
    @ConditionalOnMissingBean
    public ConsoleView agentSpacesConsoleView(AgentSpaces spaces,
                                              ai.badmonkey.agentspaces.peering.node.PeerNode node,
                                              List<ConsoleViewCustomizer> customizers) {
        ConsoleView.Builder builder = ConsoleView.builder();
        builder.channelModes(node::channelModes);
        boolean first = true;
        for (String groupName : spaces.groupNames()) {
            AgentSpaces.GroupContext group = spaces.group(groupName);
            if (first) {
                builder.members(() -> group.runtime().membership().allMembers());
                builder.discovery(group.discovery());
                first = false;
            }
            for (String spaceName : group.spaceNames()) {
                builder.space(spaceName, group.space(spaceName), Object.class);
            }
        }
        for (ConsoleViewCustomizer customizer : customizers) {
            customizer.customize(builder, spaces);
        }
        return builder.build();
    }

    /**
     * The console server, its fleet name defaulting to the first configured
     * group's name.
     *
     * @param properties the starter configuration
     * @param view       the console view
     * @param panels     every panel bean in the context, possibly empty
     * @return the server, not yet started
     */
    /**
     * The C2 executor, present only when {@code agentspaces.console.command.enabled=true}.
     * It resolves spaces on the first configured group (so the control and
     * audit spaces must be declared among that group's spaces), collects every
     * {@link ConsoleCommand} bean, and records commands in the console view.
     *
     * @param properties the starter configuration
     * @param spaces     the fabric facade
     * @param view       the console view
     * @param commands   the application's command beans, possibly empty
     * @return the commander
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "agentspaces.console.command", name = "enabled",
            havingValue = "true")
    public FleetCommander agentSpacesCommander(AgentSpacesProperties properties,
                                               AgentSpaces spaces, ConsoleView view,
                                               List<ConsoleCommand> commands) {
        String firstGroup = spaces.groupNames().iterator().next();
        AgentSpaces.GroupContext group = spaces.group(firstGroup);
        AgentSpacesProperties.Command config = properties.getConsole().getCommand();
        return FleetCommander.builder(name ->
                        group.spaceNames().contains(name) ? group.space(name) : (Space) null)
                .controlSpace(config.getControlSpace())
                .auditSpace(group.spaceNames().contains(config.getAuditSpace())
                        ? config.getAuditSpace() : null)
                .view(view)
                .commands(commands)
                .build();
    }

    /**
     * The console server, its fleet name defaulting to the first configured
     * group's name. Command-and-control is attached when a {@link FleetCommander}
     * bean is present and the command routes have a gate: the static
     * {@code agentspaces.console.command.token} when set, otherwise — under a
     * configured {@code agentspaces.security.oidc.*} — a
     * {@link BearerJwtValidator} over the identity provider's keys requiring
     * {@code agentspaces.console.command.required-scope} (TODO-EFG §5). A
     * static token always wins when both are present.
     *
     * @param properties the starter configuration
     * @param view       the console view
     * @param panels     every panel bean in the context, possibly empty
     * @param commanders the commander bean when C2 is enabled, else empty
     * @return the server, not yet started
     */
    @Bean
    @ConditionalOnMissingBean
    public FleetConsoleServer agentSpacesConsoleServer(AgentSpacesProperties properties,
                                                       ConsoleView view,
                                                       List<ConsolePanel> panels,
                                                       List<FleetCommander> commanders) {
        String fleetName = properties.getConsole().getFleetName();
        if (fleetName.isBlank()) {
            fleetName = properties.getGroups().isEmpty() ? "AgentSpaces"
                    : properties.getGroups().get(0).getName();
        }
        FleetConsoleServer.Builder builder = FleetConsoleServer.builder(view)
                .fleetName(fleetName)
                .panels(panels);
        AgentSpacesProperties.Command command = properties.getConsole().getCommand();
        String token = command.getToken();
        AgentSpacesProperties.Security.Oidc oidc = properties.getSecurity().getOidc();
        if (!commanders.isEmpty() && token != null && !token.isBlank()) {
            builder.commandAndControl(commanders.get(0), token);
        } else if (!commanders.isEmpty() && oidc.isConfigured()) {
            builder.commandAndControl(commanders.get(0), BearerJwtValidator.fromJwks(
                    oidc.getIssuer().trim(), oidc.getAudience().trim(),
                    Authorizers.jwksUrl(oidc.getJwksUrl())));
            String scope = command.getRequiredScope();
            if (scope != null && !scope.isBlank()) {
                builder.requiredScope(scope.trim());
            }
        }
        return builder.build();
    }

    /**
     * Starts the console server once the fabric is up and stops it first on
     * the way down.
     *
     * @param properties the starter configuration
     * @param server     the console server
     * @return the lifecycle
     */
    @Bean
    @ConditionalOnMissingBean
    public AgentSpacesConsoleLifecycle agentSpacesConsoleLifecycle(
            AgentSpacesProperties properties, FleetConsoleServer server) {
        return new AgentSpacesConsoleLifecycle(server,
                properties.getConsole().getPort());
    }
}
