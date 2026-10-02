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
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.spring.AgentSpacesProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Activates the Embabel bridge when Embabel is on the application classpath
 * (spec §10): every {@code @Agent} bean's metadata becomes a published
 * AgentCard in every joined group (§10.4), with worker loops still declared
 * through {@code @SpaceTake} on actions and bound by the core starter; the
 * fleet's cards come back as a generated planner agent (§10.6) that deploys
 * onto the application's {@code AgentPlatform} bean automatically. Without
 * Embabel present this configuration never loads, so the dependency stays
 * one-directional.
 */
@AutoConfiguration
@ConditionalOnClass(name = "com.embabel.agent.api.annotation.Agent")
public class EmbabelAgentSpacesAutoConfiguration {

    /**
     * The post-processor publishing Embabel-derived cards.
     *
     * @param properties the starter configuration, for group order
     * @param spaces     the facade
     * @param identity   the peer identity
     * @return the post-processor
     */
    @Bean
    @ConditionalOnMissingBean
    public static EmbabelBindingPostProcessor embabelBindingPostProcessor(
            ObjectProvider<AgentSpacesProperties> properties, ObjectProvider<AgentSpaces> spaces,
            ObjectProvider<PeerIdentity> identity) {
        return new EmbabelBindingPostProcessor(() -> binder(properties.getObject(), spaces.getObject(),
                identity.getObject()));
    }

    /**
     * The post-processor over already built beans, for programmatic use.
     *
     * @param properties the starter configuration, for group order
     * @param spaces     the facade
     * @param identity   the peer identity
     * @return the post-processor
     */
    public static EmbabelBindingPostProcessor embabelBindingPostProcessor(
            AgentSpacesProperties properties, AgentSpaces spaces, PeerIdentity identity) {
        EmbabelBinder binder = binder(properties, spaces, identity);
        return new EmbabelBindingPostProcessor(() -> binder);
    }

    private static EmbabelBinder binder(AgentSpacesProperties properties, AgentSpaces spaces,
                                        PeerIdentity identity) {
        List<String> order = new ArrayList<>();
        properties.getGroups().forEach(group -> order.add(group.getName()));
        return new EmbabelBinder(spaces, identity, InstantSource.system(), order);
    }

    /**
     * The remote-action bridge: the fleet's discovered AgentCards as a
     * generated {@code @Agent} the GOAP planner plans over (spec §10.6). The
     * bean builds over the first configured group with the default routing (a
     * one-space group needs none); define your own {@code EmbabelRemoteActions}
     * bean to route several spaces or tune correlation. Absent entirely under
     * {@code agentspaces.embabel.remote-actions-enabled=false}, so an
     * {@code @Autowired} consumer sees a clean missing bean rather than a
     * null. Deployment onto the platform bean is automatic through
     * {@link #embabelRemoteActionsDeployer}; to drive it by hand instead, set
     * {@code agentspaces.embabel.auto-deploy=false} and call
     * {@code bridge.deployTo(agentPlatform)} on your own cadence.
     *
     * @param properties the starter configuration
     * @param spaces     the facade
     * @param identity   the peer identity
     * @return the bridge, or null when no group is configured to bridge over
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "agentspaces.embabel", name = "remote-actions-enabled",
            havingValue = "true", matchIfMissing = true)
    public EmbabelRemoteActions embabelRemoteActions(
            AgentSpacesProperties properties, AgentSpaces spaces, PeerIdentity identity) {
        AgentSpacesProperties.Embabel embabel = properties.getEmbabel();
        if (properties.getGroups().isEmpty()) {
            return null;
        }
        ai.badmonkey.agentspaces.agent.remote.RemoteActions remote =
                ai.badmonkey.agentspaces.agent.remote.RemoteActions.over(
                        spaces.group(properties.getGroups().get(0).getName()),
                        identity.peerId());
        return new EmbabelRemoteActions(remote,
                java.time.Duration.ofMillis(embabel.getRemoteActionTimeoutMillis()),
                embabel.getRemoteAgentName(), embabel.getRemoteAgentDescription());
    }

    /**
     * The auto-deployer (spec §10.6): a {@code SmartLifecycle} that catches the
     * application's Embabel {@code AgentPlatform} bean as it initializes and,
     * from context start, deploys the generated remote-fleet agent onto it —
     * redeploying as the fleet's advertised actions change, on the
     * {@code deploy-poll-millis} cadence. Off under
     * {@code agentspaces.embabel.auto-deploy=false}; inert when the bridge
     * bean itself is disabled.
     *
     * @param properties the starter configuration
     * @param bridge     the remote-action bridge, when enabled
     * @return the lifecycle
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "agentspaces.embabel", name = "auto-deploy",
            havingValue = "true", matchIfMissing = true)
    public static EmbabelRemoteActionsDeployerLifecycle embabelRemoteActionsDeployer(
            ObjectProvider<AgentSpacesProperties> properties, ObjectProvider<EmbabelRemoteActions> bridge) {
        return new EmbabelRemoteActionsDeployerLifecycle(() -> deployer(properties.getObject(),
                Optional.ofNullable(bridge.getIfAvailable())));
    }

    /**
     * The auto-deployer over an already built bridge, for programmatic use.
     *
     * @param properties the starter configuration
     * @param bridge     the remote-action bridge, when enabled
     * @return the lifecycle
     */
    public static EmbabelRemoteActionsDeployerLifecycle embabelRemoteActionsDeployer(
            AgentSpacesProperties properties, Optional<EmbabelRemoteActions> bridge) {
        return new EmbabelRemoteActionsDeployerLifecycle(deployer(properties, bridge));
    }

    private static Optional<EmbabelRemoteActionsDeployer> deployer(AgentSpacesProperties properties,
                                                                  Optional<EmbabelRemoteActions> bridge) {
        Duration poll = Duration.ofMillis(properties.getEmbabel().getDeployPollMillis());
        return bridge.map(remote -> new EmbabelRemoteActionsDeployer(remote, poll));
    }

    /** Publishes an Embabel-derived card for every {@code @Agent} bean, into every group (§10.4). */
    public static class EmbabelBindingPostProcessor implements BeanPostProcessor {

        private final java.util.function.Supplier<EmbabelBinder> binder;
        private volatile EmbabelBinder resolved;

        /**
         * Creates the post-processor.
         *
         * @param binder the Embabel binder
         */
        public EmbabelBindingPostProcessor(EmbabelBinder binder) {
            java.util.Objects.requireNonNull(binder, "binder");
            this.binder = () -> binder;
        }

        /**
         * Creates the post-processor over a binder resolved on the first
         * {@code @Agent} bean, so creating the post-processor does not build
         * the fabric before the context's other post-processors register.
         *
         * @param binder supplies the Embabel binder
         */
        public EmbabelBindingPostProcessor(java.util.function.Supplier<EmbabelBinder> binder) {
            this.binder = java.util.Objects.requireNonNull(binder, "binder");
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (EmbabelIntrospector.introspect(bean.getClass()).isEmpty()) {
                return bean; // not an Embabel @Agent: nothing to publish, no fabric needed
            }
            EmbabelBinder current = resolved;
            if (current == null) {
                synchronized (this) {
                    if (resolved == null) {
                        resolved = binder.get();
                    }
                    current = resolved;
                }
            }
            current.bindAll(bean);
            return bean;
        }
    }
}
