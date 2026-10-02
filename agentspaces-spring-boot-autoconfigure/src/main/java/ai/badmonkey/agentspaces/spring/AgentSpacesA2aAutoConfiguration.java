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

import ai.badmonkey.agentspaces.a2a.A2aGateway;
import ai.badmonkey.agentspaces.a2a.A2aTaskBinding;
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.auth.oidc.BearerJwtValidator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

/**
 * Serves the fleet's A2A gateway when {@code agentspaces.a2a.enabled=true}
 * (TODO item 7, TECH-SPEC §9.4): the fleet's AgentCards as A2A Agent Cards and
 * A2A JSON-RPC tasks bound to {@code agentspaces.a2a.space}, gated by the same
 * bearer rule the console uses. A static {@code token} wins; otherwise, with
 * {@code agentspaces.security.oidc.*} configured, a {@link BearerJwtValidator}
 * over the identity provider's keys requiring {@code required-scope};
 * otherwise the gateway is ungated, which startup refuses unless {@code bind}
 * is a loopback address. Each bean is {@code @ConditionalOnMissingBean}.
 */
@AutoConfiguration
@ConditionalOnClass(name = "ai.badmonkey.agentspaces.a2a.A2aGateway")
@ConditionalOnProperty(prefix = "agentspaces.a2a", name = "enabled", havingValue = "true")
public class AgentSpacesA2aAutoConfiguration {

    /**
     * The gateway over the configured group's cards and task space.
     *
     * @param properties the starter configuration
     * @param spaces     the fabric facade
     * @return the gateway, not yet listening
     */
    @Bean
    @ConditionalOnMissingBean
    public A2aGateway agentSpacesA2aGateway(AgentSpacesProperties properties, AgentSpaces spaces) {
        AgentSpacesProperties.A2a config = properties.getA2a();
        String groupName = config.getGroup() == null || config.getGroup().isBlank()
                ? firstGroup(properties) : config.getGroup().trim();
        if (config.getSpace() == null || config.getSpace().isBlank()) {
            throw new IllegalStateException("agentspaces.a2a.space must name the task space"
                    + " A2A tasks are written into (group '" + groupName + "' has "
                    + spaces.group(groupName).spaceNames() + ")");
        }
        AgentSpaces.GroupContext group = spaces.group(groupName);
        String fleetName = config.getFleetName() == null || config.getFleetName().isBlank()
                ? groupName : config.getFleetName();
        A2aGateway gateway = new A2aGateway(fleetName, config.getProvider(),
                () -> group.discovery().find(AgentCard.class, card -> true))
                .taskBinding(new A2aTaskBinding(group.space(config.getSpace().trim()),
                        Lease.of(Duration.ofMillis(config.getTaskLeaseMillis())),
                        Lease.of(Duration.ofMillis(config.getResultLeaseMillis())),
                        group.clock()));
        if (config.getExternalBaseUrl() != null && !config.getExternalBaseUrl().isBlank()) {
            gateway.externalBaseUrl(config.getExternalBaseUrl().trim());
        }
        for (String host : config.getAllowedWebhookHosts()) {
            gateway.allowWebhookHost(host);
        }
        AgentSpacesProperties.Security.Oidc oidc = properties.getSecurity().getOidc();
        String token = config.getToken();
        if (token != null && !token.isBlank()) {
            gateway.operatorToken(token);
        } else if (oidc.isConfigured()) {
            gateway.bearer(BearerJwtValidator.fromJwks(oidc.getIssuer().trim(),
                    oidc.getAudience().trim(), Authorizers.jwksUrl(oidc.getJwksUrl())));
            String scope = config.getRequiredScope();
            if (scope != null && !scope.isBlank()) {
                gateway.requiredScope(scope.trim());
            }
        } else if (!loopback(config.getBind())) {
            throw new IllegalStateException("agentspaces.a2a.bind '" + config.getBind()
                    + "' is routable but no agentspaces.a2a.token or agentspaces.security.oidc"
                    + " gate is configured; bind 127.0.0.1 or configure a gate");
        }
        return gateway;
    }

    /**
     * Starts the gateway once the fabric is up and stops it first on the way down.
     *
     * @param properties the starter configuration
     * @param gateway    the gateway
     * @return the lifecycle
     */
    @Bean
    @ConditionalOnMissingBean
    public AgentSpacesA2aLifecycle agentSpacesA2aLifecycle(AgentSpacesProperties properties,
                                                           A2aGateway gateway) {
        return new AgentSpacesA2aLifecycle(gateway, properties.getA2a().getBind(),
                properties.getA2a().getPort());
    }

    private static String firstGroup(AgentSpacesProperties properties) {
        if (properties.getGroups().isEmpty()) {
            throw new IllegalStateException("agentspaces.a2a needs a configured group");
        }
        return properties.getGroups().get(0).getName();
    }

    /** Whether an address resolves to the loopback interface. */
    static boolean loopback(String bind) {
        try {
            return InetAddress.getByName(bind == null || bind.isBlank() ? "127.0.0.1" : bind.trim())
                    .isLoopbackAddress();
        } catch (UnknownHostException e) {
            throw new IllegalStateException("agentspaces.a2a.bind is not a resolvable address: "
                    + bind, e);
        }
    }
}
