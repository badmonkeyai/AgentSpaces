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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps the Embabel platform's view of the fleet current (spec §10.6, auto-deploy): once {@link #start()
 * started} with a platform handed to {@link #deployWhenReady(Object)}, it watches the bridge's registries and
 * deploys a freshly generated {@code @Agent} whenever the set of capabilities changes.
 *
 * <p>Review §7.9: a poll that finds the ad-caches' generation counter unmoved and no card expired costs two
 * reads; a changed set deploys after {@code debounce} of quiet and at least {@code minInterval} after the
 * previous generation (the first generation deploys at once); an action whose card vanished stays for
 * {@code absenceGrace} before it leaves, so a late refresh never flaps the planner; when every action has
 * lapsed a vacant generation replaces the last one so dead actions leave the platform; the first deploy
 * refuses a platform that already holds an agent under the bridge's name, which would be a user's own; and
 * every deploy is read back, with one ERROR and a counter when the platform kept more than one generation.
 *
 * <p>Framework-neutral: no Spring type appears here. The Spring starter wraps it in a {@code SmartLifecycle}.
 */
public final class EmbabelRemoteActionsDeployer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(EmbabelRemoteActionsDeployer.class.getName());

    private final EmbabelRemoteActions bridge;
    private final Duration pollInterval;
    private volatile Duration absenceGrace = Duration.ofSeconds(30);
    private volatile Duration debounce = Duration.ofMillis(250);
    private volatile Duration minInterval = Duration.ofSeconds(5);
    private final AtomicInteger deployments = new AtomicInteger();
    private final AtomicInteger generationsRetained = new AtomicInteger();
    private volatile Object platform;
    private volatile String deployedSignature = "";
    private volatile Thread watcher;
    private volatile Instant lastDeployAt;
    private volatile long lastGeneration = -1;
    private volatile Instant nextExpiry;
    private volatile String pendingSignature;
    private volatile Instant pendingSince;
    /** The last action seen per capability key, and when (review §7.9: the absence grace). */
    private final Map<String, RemoteAction> lastSeen = new HashMap<>();
    private final Map<String, Instant> lastSeenAt = new HashMap<>();

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

    /** How long a vanished action stays deployed; zero removes it at the next check. */
    public EmbabelRemoteActionsDeployer absenceGrace(Duration grace) {
        this.absenceGrace = requireNonNegative(grace, "absenceGrace");
        return this;
    }

    /** Quiet time a changed set must hold before it deploys; the first generation ignores it. */
    public EmbabelRemoteActionsDeployer debounce(Duration debounce) {
        this.debounce = requireNonNegative(debounce, "debounce");
        return this;
    }

    /** Least time between two generations; the first generation ignores it. */
    public EmbabelRemoteActionsDeployer minInterval(Duration minInterval) {
        this.minInterval = requireNonNegative(minInterval, "minInterval");
        return this;
    }

    private static Duration requireNonNegative(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative: " + value);
        }
        return value;
    }

    /**
     * Names the platform to deploy onto. May be called before or after {@link #start()}; when the watcher is
     * already running, the current action set is deployed at once.
     *
     * @param agentPlatform the platform
     * @return this deployer
     * @throws IllegalStateException when the platform already holds an agent under the bridge's name (review
     *                               §7.9, X2): a user's own agent, which a deploy would silently replace
     */
    public EmbabelRemoteActionsDeployer deployWhenReady(Object agentPlatform) {
        Objects.requireNonNull(agentPlatform, "agentPlatform");
        if (deployments.get() == 0 && EmbabelRemoteActions.deployedNames(agentPlatform).contains(bridge.agentName())) {
            throw new IllegalStateException("the platform already holds an agent named '" + bridge.agentName()
                    + "'; the fleet bridge would replace it: rename the bridge (agentspaces.embabel.remote-agent-name)");
        }
        this.platform = agentPlatform;
        if (isRunning()) {
            checkNow();
        }
        return this;
    }

    /** Starts watching the registry. Idempotent. Nothing deploys until a platform is known. */
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

    /** Stops watching. Idempotent; the deployed generation stays deployed. */
    public synchronized void stop() {
        Thread current = watcher;
        watcher = null;
        if (current != null) {
            current.interrupt();
        }
    }

    public boolean isRunning() {
        return watcher != null;
    }

    /** Returns how many generations have been deployed so far, the vacant one included. */
    public int deployments() {
        return deployments.get();
    }

    /** How many deploys the platform answered with more than one agent under the bridge's name (review §7.9, X3). */
    public int generationsRetained() {
        return generationsRetained.get();
    }

    /**
     * Compares the registries with the deployed generation and deploys a new one when the capability set
     * changed, the rate limits allow, and a platform is known. Hosts that drive their own cadence may call this
     * instead of {@link #start()}.
     *
     * @return {@code true} when a new generation was deployed
     */
    public boolean checkNow() {
        Object target = platform;
        if (target == null) {
            return false;
        }
        Instant now = Instant.now();
        long generation = bridge.generation();
        boolean cacheMoved = generation != lastGeneration;
        boolean expired = nextExpiry != null && !now.isBefore(nextExpiry);
        boolean graceRunning = !lastSeenAt.isEmpty() && !absenceGrace.isZero();
        if (!cacheMoved && !expired && pendingSignature == null && !graceRunning) {
            return false; // nothing changed since the last look: two reads, no recomputation
        }
        lastGeneration = generation;
        nextExpiry = bridge.nextExpiry().orElse(null);

        List<RemoteAction> effective = effectiveSet(bridge.available(), now);
        String signature = signature(effective);
        if (signature.equals(deployedSignature)) {
            pendingSignature = null;
            return false;
        }
        boolean first = deployments.get() == 0;
        if (!first) {
            if (!signature.equals(pendingSignature)) {
                pendingSignature = signature;
                pendingSince = now;
            }
            if (now.isBefore(pendingSince.plus(debounce))
                    || (lastDeployAt != null && now.isBefore(lastDeployAt.plus(minInterval)))) {
                return false; // not yet quiet, or too soon after the last generation
            }
        }
        synchronized (this) {
            if (signature.equals(deployedSignature)) {
                pendingSignature = null;
                return false;
            }
            boolean deployed;
            if (effective.isEmpty()) {
                if (first) {
                    deployedSignature = signature; // nothing ever deployed: remember the empty set
                    return false;
                }
                deployed = bridge.deployVacant(target);
            } else {
                deployed = bridge.deployTo(target, effective);
            }
            if (deployed) {
                deployedSignature = signature;
                pendingSignature = null;
                lastDeployAt = now;
                deployments.incrementAndGet();
                LOG.log(System.Logger.Level.INFO, "deployed remote fleet agent '" + bridge.agentName() + "' with "
                        + effective.size() + " action(s)");
                readBack(target, effective.size());
            }
            return deployed;
        }
    }

    /** The current actions plus those absent for less than the grace (review §7.9, X5). */
    private List<RemoteAction> effectiveSet(List<RemoteAction> current, Instant now) {
        synchronized (lastSeen) {
            for (RemoteAction action : current) {
                String key = EmbabelRemoteActions.capabilityKey(action);
                lastSeen.put(key, action);
                lastSeenAt.put(key, now);
            }
            List<RemoteAction> effective = new ArrayList<>(current);
            TreeSet<String> present = new TreeSet<>();
            current.forEach(a -> present.add(EmbabelRemoteActions.capabilityKey(a)));
            for (var entry : new ArrayList<>(lastSeenAt.entrySet())) {
                if (present.contains(entry.getKey())) {
                    continue;
                }
                if (now.isBefore(entry.getValue().plus(absenceGrace))) {
                    effective.add(lastSeen.get(entry.getKey()));
                } else {
                    lastSeen.remove(entry.getKey());
                    lastSeenAt.remove(entry.getKey());
                }
            }
            return effective;
        }
    }

    /** Reads the platform back after a deploy (review §7.9, X3): exactly one agent should carry the bridge's name. */
    private void readBack(Object target, int actions) {
        List<String> names = EmbabelRemoteActions.deployedNames(target);
        if (names.isEmpty()) {
            return; // a platform without agents(): nothing to read back
        }
        long held = names.stream().filter(bridge.agentName()::equals).count();
        if (held != 1) {
            generationsRetained.incrementAndGet();
            LOG.log(System.Logger.Level.ERROR, "the platform holds " + held + " agents named '" + bridge.agentName()
                    + "' after a deploy of " + actions + " action(s); it does not replace by name, so earlier"
                    + " generations stay reachable until it restarts");
        }
    }

    @Override
    public void close() {
        stop();
    }

    /** The capability set's identity (review §7.7): what is offered and where, never who offers it or when. */
    private static String signature(List<RemoteAction> actions) {
        TreeSet<String> keys = new TreeSet<>();
        for (RemoteAction action : actions) {
            keys.add(EmbabelRemoteActions.capabilityKey(action));
        }
        return String.join("\n", keys);
    }
}
