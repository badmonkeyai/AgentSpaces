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

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.SmartLifecycle;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Deploys the generated remote-fleet agent onto the application's Embabel
 * {@code AgentPlatform} bean automatically (spec §10.6). The class doubles as
 * a {@link BeanPostProcessor} so it sees the platform bean the moment Spring
 * initializes it — detected reflectively by type name, exactly as the bridge
 * itself reaches the platform, so the module still compiles with no Embabel
 * artifact — and hands it to the {@link EmbabelRemoteActionsDeployer}; on
 * context start the deployer's watcher begins polling the fleet's cards and
 * (re)deploys whenever the advertised action set changes, and on context stop
 * it stops. Built with no bridge (the remote-actions bridge disabled) the
 * lifecycle is inert.
 */
public final class EmbabelRemoteActionsDeployerLifecycle
        implements SmartLifecycle, BeanPostProcessor {

    /** The Embabel platform type the deployer looks for, by name. */
    public static final String AGENT_PLATFORM_TYPE = "com.embabel.agent.core.AgentPlatform";

    private final Supplier<Optional<EmbabelRemoteActionsDeployer>> source;
    private volatile Optional<EmbabelRemoteActionsDeployer> resolved;
    private volatile boolean running;

    /**
     * Creates the lifecycle.
     *
     * @param deployer the deployer to drive, or empty for an inert lifecycle
     */
    public EmbabelRemoteActionsDeployerLifecycle(Optional<EmbabelRemoteActionsDeployer> deployer) {
        Optional<EmbabelRemoteActionsDeployer> fixed = Objects.requireNonNull(deployer, "deployer");
        this.source = () -> fixed;
    }

    /**
     * Creates the lifecycle over a deployer resolved lazily, when the platform
     * bean appears or the context starts: as a post-processor this bean is
     * created early, and resolving the bridge then would build the fabric
     * before the context's other post-processors are registered.
     *
     * @param deployer supplies the deployer, or empty for an inert lifecycle
     */
    public EmbabelRemoteActionsDeployerLifecycle(Supplier<Optional<EmbabelRemoteActionsDeployer>> deployer) {
        this.source = Objects.requireNonNull(deployer, "deployer");
    }

    /** Returns the deployer this lifecycle drives, when the bridge is enabled. */
    public Optional<EmbabelRemoteActionsDeployer> deployer() {
        Optional<EmbabelRemoteActionsDeployer> current = resolved;
        if (current == null) {
            synchronized (this) {
                if (resolved == null) {
                    resolved = source.get();
                }
                current = resolved;
            }
        }
        return current;
    }

    /**
     * Whether a bean is an Embabel {@code AgentPlatform}: any supertype or
     * interface in its hierarchy carries the platform's type name.
     *
     * @param bean the bean
     * @return true when the bean is a platform
     */
    public static boolean isAgentPlatform(Object bean) {
        for (Class<?> at = bean.getClass(); at != null; at = at.getSuperclass()) {
            if (namedPlatform(at)) {
                return true;
            }
        }
        return false;
    }

    private static boolean namedPlatform(Class<?> type) {
        if (AGENT_PLATFORM_TYPE.equals(type.getName())) {
            return true;
        }
        for (Class<?> implemented : type.getInterfaces()) {
            if (namedPlatform(implemented)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (isAgentPlatform(bean)) {
            deployer().ifPresent(deployer -> deployer.deployWhenReady(bean));
        }
        return bean;
    }

    @Override
    public synchronized void start() {
        if (running || deployer().isEmpty()) {
            return;
        }
        deployer().get().start();
        running = true;
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        deployer().ifPresent(EmbabelRemoteActionsDeployer::stop);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // After the fabric lifecycle: the fleet's cards flow before we watch them.
        return Integer.MAX_VALUE - 512;
    }
}
