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

import ai.badmonkey.agentspaces.agent.remote.RemoteAction;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps the Embabel platform's view of the fleet current (spec §10.6,
 * auto-deploy): once {@link #start() started} with a platform handed to
 * {@link #deployWhenReady(Object)}, it watches the bridge's remote-action
 * registry and deploys a freshly generated {@code @Agent} through
 * {@link EmbabelRemoteActions#deployTo(Object)} whenever the set of actions
 * changes — the first time cards arrive, when a new card brings a new
 * capability, and when an expired card takes one away. Card refreshes that
 * change nothing but the issue time do not redeploy.
 *
 * <p>Framework-neutral: no Spring type appears here. The Spring starter wraps
 * it in a {@code SmartLifecycle} that calls {@link #deployWhenReady} with the
 * {@code AgentPlatform} bean and then {@link #start()}; any other host calls
 * the same two methods. Detection is by polling the registry on a virtual
 * thread at the configured interval, which is the ad-cache's own view of card
 * arrival and expiry.
 *
 * <p>Embabel's platform has no undeploy in the surface this module reaches, so
 * an action that disappears is removed from the <em>next</em> generated agent
 * while the previously deployed generation stays registered until the
 * platform restarts; invoking one of its vanished actions times out and
 * throws, which the planner treats as the action failing.
 */
public final class EmbabelRemoteActionsDeployer implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(EmbabelRemoteActionsDeployer.class.getName());

    private final EmbabelRemoteActions bridge;
    private final Duration pollInterval;
    private final AtomicInteger deployments = new AtomicInteger();
    private volatile Object platform;
    private volatile String deployedSignature = "";
    private volatile Thread watcher;

    /**
     * Creates the deployer.
     *
     * @param bridge       the remote-actions bridge that generates the agent
     * @param pollInterval how often to check the registry for changes
     */
    public EmbabelRemoteActionsDeployer(EmbabelRemoteActions bridge, Duration pollInterval) {
        this.bridge = Objects.requireNonNull(bridge, "bridge");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive: " + pollInterval);
        }
    }

    /**
     * Names the platform to deploy onto (Embabel's {@code AgentPlatform}, typed
     * as Object so this module needs no Embabel artifact). May be called before
     * or after {@link #start()}; when the watcher is already running, the
     * current action set is deployed at once.
     *
     * @param agentPlatform the platform
     * @return this deployer
     */
    public EmbabelRemoteActionsDeployer deployWhenReady(Object agentPlatform) {
        this.platform = Objects.requireNonNull(agentPlatform, "agentPlatform");
        if (isRunning()) {
            checkNow();
        }
        return this;
    }

    /**
     * Starts watching the registry. Idempotent. Nothing deploys until a
     * platform is known through {@link #deployWhenReady(Object)}.
     */
    public synchronized void start() {
        if (watcher != null) {
            return;
        }
        watcher = Thread.ofVirtual().name("embabel-remote-actions-deployer").start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    checkNow();
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "remote-action deploy check failed", e);
                }
                try {
                    Thread.sleep(pollInterval.toMillis());
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
    }

    /** Stops watching. Idempotent; already deployed generations stay deployed. */
    public synchronized void stop() {
        Thread current = watcher;
        watcher = null;
        if (current != null) {
            current.interrupt();
        }
    }

    /** Returns whether the watcher is running. */
    public boolean isRunning() {
        return watcher != null;
    }

    /** Returns how many generations have been deployed so far. */
    public int deployments() {
        return deployments.get();
    }

    /**
     * Compares the registry with the last deployed generation and deploys a
     * new one when the action set changed and a platform is known. Hosts that
     * drive their own cadence may call this instead of {@link #start()}.
     *
     * @return {@code true} when a new generation was deployed
     */
    public boolean checkNow() {
        Object target = platform;
        if (target == null) {
            return false;
        }
        List<RemoteAction> available = bridge.actions().available();
        String signature = signature(available);
        if (signature.equals(deployedSignature)) {
            return false;
        }
        if (available.isEmpty()) {
            // Nothing to generate; remember the empty set so a re-arrival deploys.
            deployedSignature = signature;
            return false;
        }
        synchronized (this) {
            if (signature.equals(deployedSignature)) {
                return false;
            }
            boolean deployed = bridge.deployTo(target);
            if (deployed) {
                deployedSignature = signature;
                deployments.incrementAndGet();
                LOG.log(System.Logger.Level.INFO, "deployed remote fleet agent '"
                        + bridge.agentName() + "' with " + available.size() + " action(s)");
            }
            return deployed;
        }
    }

    @Override
    public void close() {
        stop();
    }

    /** The action set's identity: who offers what, independent of card issue time. */
    private static String signature(List<RemoteAction> actions) {
        TreeSet<String> keys = new TreeSet<>();
        for (RemoteAction action : actions) {
            keys.add(action.card().issuer().value() + "|" + action.card().agent().encoded()
                    + "|" + action.inputType().getName() + "|" + action.outputType().getName());
        }
        return String.join("\n", keys);
    }
}
