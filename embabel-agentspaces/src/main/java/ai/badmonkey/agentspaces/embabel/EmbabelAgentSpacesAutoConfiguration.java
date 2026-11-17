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
import ai.badmonkey.agentspaces.agent.remote.RemoteAction;
import ai.badmonkey.agentspaces.agent.remote.RemoteActions;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Activates the Embabel bridge when Embabel is on the application classpath (spec §10). Outbound, every
 * {@code @Agent} bean gets <em>one</em> card from the core binder, named by {@code @Agent(name)}, through the
 * {@link EmbabelDeclarationReader} this configuration registers with the settings under
 * {@code agentspaces.embabel} (review §7.2, §7.3); worker loops are the core starter's, explicit or implied.
 * Inbound, the fleet's cards come back as a generated planner agent (§10.6) over the configured groups
 * (review §7.7) that deploys onto the application's {@code AgentPlatform} bean automatically (review §7.9).
 * Without Embabel present this configuration never loads, so the dependency stays one-directional.
 */
@AutoConfiguration
@ConditionalOnClass(name = "com.embabel.agent.api.annotation.Agent")
public class EmbabelAgentSpacesAutoConfiguration {

    /**
     * The post-processor that registers the configured declaration reader before any bean binds. The card
     * publishing it once did is the core binder's now (review §7.2); the bean stays so an application that
     * named it keeps working.
     *
     * @param properties the starter configuration
     * @param spaces     the facade
     * @param identity   the peer identity
     * @return the post-processor
     */
    @Bean
    @ConditionalOnMissingBean
    public static EmbabelBindingPostProcessor embabelBindingPostProcessor(
            ObjectProvider<AgentSpacesProperties> properties, ObjectProvider<AgentSpaces> spaces,
            ObjectProvider<PeerIdentity> identity) {
        return new EmbabelBindingPostProcessor(() -> readerSettings(properties.getObject()), true)
                .invoker(() -> takeInvoker(properties.getObject()));
    }

    /** The take invoker {@code agentspaces.embabel.take-mode} asks for (review §7.8), or empty under {@code direct}. */
    public static Optional<EmbabelTakeInvoker> takeInvoker(AgentSpacesProperties properties) {
        AgentSpacesProperties.Embabel embabel = properties.getEmbabel();
        String mode = lower(embabel.getTakeMode());
        if ("direct".equals(mode)) {
            return Optional.empty();
        }
        Duration maxRun = Duration.parse(embabel.getServedMaxRun());
        return Optional.of(new EmbabelTakeInvoker("platform".equals(mode)
                ? EmbabelTakeInvoker.Mode.PLATFORM : EmbabelTakeInvoker.Mode.AUTO, maxRun));
    }

    /**
     * The post-processor over already built beans, for programmatic use.
     *
     * @param properties the starter configuration
     * @param spaces     the facade
     * @param identity   the peer identity
     * @return the post-processor
     */
    public static EmbabelBindingPostProcessor embabelBindingPostProcessor(
            AgentSpacesProperties properties, AgentSpaces spaces, PeerIdentity identity) {
        EmbabelDeclarationReader.Settings settings = readerSettings(properties);
        return new EmbabelBindingPostProcessor(() -> settings, true).invoker(() -> takeInvoker(properties));
    }

    /** The reader's settings from {@code agentspaces.embabel} (review §7.2, §7.3, §7.8). */
    public static EmbabelDeclarationReader.Settings readerSettings(AgentSpacesProperties properties) {
        AgentSpacesProperties.Embabel embabel = properties.getEmbabel();
        EmbabelDeclarationReader.Unserved unserved = switch (lower(embabel.getAdvertiseUnserved())) {
            case "invocable" -> EmbabelDeclarationReader.Unserved.INVOCABLE;
            case "omit" -> EmbabelDeclarationReader.Unserved.OMIT;
            default -> EmbabelDeclarationReader.Unserved.LOCAL;
        };
        boolean platformInvoker = !"direct".equals(lower(embabel.getTakeMode()));
        return new EmbabelDeclarationReader.Settings(!"class".equals(lower(embabel.getCardName())),
                embabel.isServeActions(), embabel.getActionSpace(), embabel.getActionLease(), unserved,
                platformInvoker);
    }

    private static String lower(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * The remote-action bridge: the fleet's discovered AgentCards as a generated {@code @Agent} the GOAP
     * planner plans over (spec §10.6). The bean spans the groups {@code agentspaces.embabel.groups} names
     * (the first configured group by default, {@code *} for every group; review §7.7), with the default
     * routing; define your own {@code EmbabelRemoteActions} bean to tune routes or correlation. Absent
     * entirely under {@code agentspaces.embabel.remote-actions-enabled=false}.
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
        List<RemoteActions> registries = new ArrayList<>();
        for (String group : bridgedGroups(properties)) {
            registries.add(RemoteActions.over(spaces.group(group), identity.peerId()));
        }
        return new EmbabelRemoteActions(registries, Duration.ofMillis(embabel.getRemoteActionTimeoutMillis()),
                embabel.getRemoteAgentName(), embabel.getRemoteAgentDescription(), attributes(embabel));
    }

    /** The groups the default bridge spans (review §7.7): the listed ones, every one under {@code *}, else the first. */
    static List<String> bridgedGroups(AgentSpacesProperties properties) {
        List<String> configured = new ArrayList<>();
        properties.getGroups().forEach(group -> configured.add(group.getName()));
        List<String> wanted = properties.getEmbabel().getGroups();
        if (wanted == null || wanted.isEmpty()) {
            if (configured.size() > 1) {
                System.getLogger(EmbabelAgentSpacesAutoConfiguration.class.getName()).log(System.Logger.Level.INFO,
                        "the Embabel bridge spans the first group '" + configured.get(0) + "' of " + configured
                                + "; set agentspaces.embabel.groups to span more");
            }
            return List.of(configured.get(0));
        }
        if (wanted.size() == 1 && "*".equals(wanted.get(0).trim())) {
            return configured;
        }
        for (String name : wanted) {
            if (!configured.contains(name)) {
                throw new IllegalArgumentException("agentspaces.embabel.groups names '" + name
                        + "', which is not a configured group; groups: " + configured);
            }
        }
        return List.copyOf(wanted);
    }

    /** The generated actions' behaviour from {@code agentspaces.embabel} (review §7.5, §7.6). */
    static EmbabelRemoteActions.Attributes attributes(AgentSpacesProperties.Embabel embabel) {
        boolean fireOnce = !"default".equals(lower(embabel.getRemoteActionRetry()));
        RemoteAction.OnTimeout onTimeout = "leave".equals(lower(embabel.getOnTimeout()))
                ? RemoteAction.OnTimeout.LEAVE : RemoteAction.OnTimeout.WITHDRAW;
        EmbabelRemoteActions.CarryConditions conditions = switch (lower(embabel.getCarryConditions())) {
            case "none" -> EmbabelRemoteActions.CarryConditions.NONE;
            case "pre-post", "pre_post", "prepost" -> EmbabelRemoteActions.CarryConditions.PRE_POST;
            default -> EmbabelRemoteActions.CarryConditions.PRE;
        };
        return new EmbabelRemoteActions.Attributes(fireOnce, onTimeout, embabel.getRemoteActionDefaultCost(),
                conditions);
    }

    /**
     * The auto-deployer (spec §10.6): a {@code SmartLifecycle} that catches the application's Embabel
     * {@code AgentPlatform} bean as it initializes and, from context start, deploys every bridge bean's
     * generated agent onto it, redeploying as the fleet's capabilities change (review §7.9). Off under
     * {@code agentspaces.embabel.auto-deploy=false}; inert when no bridge bean exists. Two bridges sharing an
     * agent name refuse to start, because the platform keeps one agent per name (review §7.7).
     *
     * @param properties the starter configuration
     * @param bridges    the remote-action bridges, when enabled
     * @return the lifecycle
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "agentspaces.embabel", name = "auto-deploy",
            havingValue = "true", matchIfMissing = true)
    public static EmbabelRemoteActionsDeployerLifecycle embabelRemoteActionsDeployer(
            ObjectProvider<AgentSpacesProperties> properties, ObjectProvider<EmbabelRemoteActions> bridges) {
        return new EmbabelRemoteActionsDeployerLifecycle(() -> deployers(properties.getObject(),
                bridges.orderedStream().filter(Objects::nonNull).toList()));
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
        List<EmbabelRemoteActions> bridges = bridge.map(List::of).orElse(List.of());
        return new EmbabelRemoteActionsDeployerLifecycle(() -> deployers(properties, bridges));
    }

    static List<EmbabelRemoteActionsDeployer> deployers(AgentSpacesProperties properties,
                                                        List<EmbabelRemoteActions> bridges) {
        AgentSpacesProperties.Embabel embabel = properties.getEmbabel();
        Set<String> names = new HashSet<>();
        List<EmbabelRemoteActionsDeployer> deployers = new ArrayList<>();
        for (EmbabelRemoteActions bridge : bridges) {
            if (!names.add(bridge.agentName())) {
                throw new IllegalStateException("two Embabel bridges share the agent name '" + bridge.agentName()
                        + "'; the platform keeps one agent per name, so give each bridge its own");
            }
            deployers.add(new EmbabelRemoteActionsDeployer(bridge, Duration.ofMillis(embabel.getDeployPollMillis()))
                    .absenceGrace(Duration.ofMillis(embabel.getAbsenceGraceMillis()))
                    .debounce(Duration.ofMillis(embabel.getDeployDebounceMillis()))
                    .minInterval(Duration.ofMillis(embabel.getDeployMinIntervalMillis())));
        }
        return deployers;
    }

    /**
     * Registers the configured {@link EmbabelDeclarationReader} before the first bean initializes, so the
     * core binder's card for every {@code @Agent} bean carries the Embabel declaration (review §7.2). The
     * card publishing this post-processor once did itself is gone: the core post-processor binds the bean.
     */
    public static class EmbabelBindingPostProcessor implements BeanPostProcessor {

        private final Supplier<EmbabelDeclarationReader.Settings> settings;
        private volatile Supplier<Optional<EmbabelTakeInvoker>> invoker = Optional::empty;
        private volatile boolean registered;

        /** The take invoker to register beside the reader (review §7.8). */
        EmbabelBindingPostProcessor invoker(Supplier<Optional<EmbabelTakeInvoker>> invoker) {
            this.invoker = Objects.requireNonNull(invoker, "invoker");
            return this;
        }

        /**
         * Creates the post-processor over a binder; kept for applications that built one by hand. The binder
         * is deprecated and publishes nothing; the reader is registered with the default settings.
         *
         * @param binder the Embabel binder
         */
        @Deprecated(since = "0.7.0", forRemoval = true)
        public EmbabelBindingPostProcessor(EmbabelBinder binder) {
            Objects.requireNonNull(binder, "binder");
            this.settings = EmbabelDeclarationReader.Settings::defaults;
        }

        /**
         * Creates the post-processor over a lazily resolved binder; kept for applications that built one by hand.
         *
         * @param binder supplies the Embabel binder
         */
        @Deprecated(since = "0.7.0", forRemoval = true)
        public EmbabelBindingPostProcessor(Supplier<EmbabelBinder> binder) {
            Objects.requireNonNull(binder, "binder");
            this.settings = EmbabelDeclarationReader.Settings::defaults;
        }

        /**
         * Creates the post-processor that registers the reader with the given settings on the first bean.
         *
         * @param settings supplies the reader settings
         */
        public EmbabelBindingPostProcessor(Supplier<EmbabelDeclarationReader.Settings> settings, boolean marker) {
            this.settings = Objects.requireNonNull(settings, "settings");
        }

        /** Registers the configured reader now; the post-processor does so itself before the first bean initializes. */
        public void register() {
            if (!registered) {
                synchronized (this) {
                    if (!registered) {
                        new EmbabelDeclarationReader(settings.get()).register();
                        invoker.get().ifPresent(ai.badmonkey.agentspaces.agent.AgentBinder::registerTakeInvoker);
                        registered = true;
                    }
                }
            }
        }

        @Override
        public Object postProcessBeforeInitialization(Object bean, String beanName) {
            // Only when an @Agent bean is about to initialize: the settings come from the properties bean, which
            // must not be asked for while Spring is still creating it (a circular reference otherwise).
            if (!registered && EmbabelIntrospector.introspect(bean.getClass()).isPresent()) {
                register();
            }
            return bean;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (EmbabelRemoteActionsDeployerLifecycle.isAgentPlatform(bean)) {
                EmbabelTakeInvoker.attach(bean); // served tasks run on this platform (review §7.8)
            }
            return bean; // the core post-processor binds the bean; one card, from the core binder
        }
    }
}
