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
package ai.badmonkey.agentspaces.peering.bootstrap;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;
import ai.badmonkey.agentspaces.api.spi.LocatePolicy;
import ai.badmonkey.agentspaces.common.id.GroupId;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * The ordered composition of {@link BeaconLocator}s for one group. Asks each
 * locator with its own deadline on a virtual thread, merges endpoints in
 * chain order (deduplicated, re-prioritised by position), takes the first
 * founding, accepts a GroupId claim only from locators the operator named,
 * sets the hint's time to live to the shortest contributor's, expands names
 * to addresses last, and keeps a trace of what each locator did. A locator
 * that throws or times out contributes nothing and never stops the rest.
 */
public final class BootstrapChain implements BeaconLocator {

    /** The chain's name in traces. */
    public static final String NAME = "chain";

    /**
     * What one locator did in the last locate.
     *
     * @param locator   the locator's name
     * @param millis    how long it took
     * @param endpoints how many endpoints it returned
     * @param evidence  the hint's evidence, or empty
     * @param error     the error, or null
     */
    public record Trace(String locator, long millis, int endpoints, String evidence, String error) {
    }

    private final List<BeaconLocator> locators;
    private final boolean firstWins;
    private final Duration timeout;
    private final Set<String> acceptGroupIdFrom;
    private final Function<String, List<InetAddress>> resolver;
    private final LocatePolicy policy;
    private volatile List<Trace> lastTrace = List.of();

    private BootstrapChain(Builder builder) {
        this.locators = List.copyOf(builder.locators);
        this.firstWins = builder.firstWins;
        this.timeout = builder.timeout;
        this.acceptGroupIdFrom = Set.copyOf(builder.acceptGroupIdFrom);
        this.resolver = builder.resolver;
        this.policy = builder.policy;
    }

    @Override
    public LocatePolicy policy() {
        return policy;
    }

    /** Starts a chain. */
    public static Builder builder() {
        return new Builder();
    }

    /** The locators, in order. */
    public List<BeaconLocator> locators() {
        return locators;
    }

    /** The trace of the last locate: one entry per locator asked. */
    public List<Trace> lastTrace() {
        return lastTrace;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public BeaconHint locate(BeaconRequest request) {
        Objects.requireNonNull(request, "request");
        List<Trace> trace = new ArrayList<>();
        List<BeaconHint> hints = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<BeaconLocator> asked = new ArrayList<>();
        for (BeaconLocator locator : locators) {
            if (locator.appliesTo(request.groupName())) {
                asked.add(locator);
            }
        }
        if (firstWins) {
            for (BeaconLocator locator : asked) {
                BeaconHint hint = ask(locator, request, trace);
                if (hint != null) {
                    hints.add(hint);
                    names.add(locator.name());
                    if (!hint.endpoints().isEmpty()) {
                        break;
                    }
                }
            }
        } else {
            List<Future<BeaconHint>> futures = new ArrayList<>();
            try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                for (BeaconLocator locator : asked) {
                    futures.add(executor.submit(() -> locator.locate(request)));
                }
                for (int i = 0; i < asked.size(); i++) {
                    BeaconHint hint = await(asked.get(i), futures.get(i), trace);
                    if (hint != null) {
                        hints.add(hint);
                        names.add(asked.get(i).name());
                    }
                }
            }
        }
        lastTrace = List.copyOf(trace);
        return merge(request, hints, names);
    }

    private BeaconHint ask(BeaconLocator locator, BeaconRequest request, List<Trace> trace) {
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            return await(locator, executor.submit(() -> locator.locate(request)), trace);
        }
    }

    private BeaconHint await(BeaconLocator locator, Future<BeaconHint> future, List<Trace> trace) {
        long started = System.nanoTime();
        try {
            BeaconHint hint = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (hint == null) {
                hint = BeaconHint.none();
            }
            trace.add(new Trace(locator.name(), elapsed(started), hint.endpoints().size(), hint.evidence(), null));
            return hint;
        } catch (TimeoutException e) {
            future.cancel(true);
            trace.add(new Trace(locator.name(), elapsed(started), 0, "", "timeout after " + timeout.toMillis() + " ms"));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            trace.add(new Trace(locator.name(), elapsed(started), 0, "", cause.toString()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            trace.add(new Trace(locator.name(), elapsed(started), 0, "", "interrupted"));
        }
        return null;
    }

    private static long elapsed(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000L;
    }

    private BeaconHint merge(BeaconRequest request, List<BeaconHint> hints, List<String> names) {
        Set<String> seen = new LinkedHashSet<>();
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        Optional<GroupId> groupId = request.groupId();
        Optional<byte[]> founding = Optional.empty();
        Map<String, String> parameters = new LinkedHashMap<>();
        Duration ttl = null;
        List<String> evidence = new ArrayList<>();
        for (int i = 0; i < hints.size(); i++) {
            BeaconHint hint = hints.get(i);
            if (hint.isEmpty()) {
                continue;
            }
            for (PeerAdvertisement.Endpoint endpoint : byPriority(hint.endpoints())) {
                if (seen.add(endpoint.transport() + "://" + endpoint.address())) {
                    endpoints.add(new PeerAdvertisement.Endpoint(endpoint.transport(), endpoint.address(),
                            endpoints.size()));
                }
            }
            if (founding.isEmpty() && hint.founding().isPresent()) {
                founding = hint.founding();
            }
            if (groupId.isEmpty() && hint.groupId().isPresent() && acceptGroupIdFrom.contains(names.get(i))) {
                groupId = hint.groupId();
            }
            hint.parameters().forEach(parameters::putIfAbsent);
            if (!hint.ttl().isZero() && (ttl == null || hint.ttl().compareTo(ttl) < 0)) {
                ttl = hint.ttl();
            }
            evidence.add(names.get(i) + ": " + hint.evidence());
        }
        if (endpoints.isEmpty() && founding.isEmpty()) {
            return BeaconHint.none();
        }
        if (resolver != null) {
            endpoints = DnsExpansion.expand(endpoints, resolver);
        }
        return new BeaconHint(endpoints, groupId, founding, parameters,
                ttl == null ? BeaconHint.DEFAULT_TTL : ttl, String.join("; ", evidence));
    }

    private static List<PeerAdvertisement.Endpoint> byPriority(List<PeerAdvertisement.Endpoint> endpoints) {
        List<PeerAdvertisement.Endpoint> sorted = new ArrayList<>(endpoints);
        sorted.sort(java.util.Comparator.comparingInt(PeerAdvertisement.Endpoint::priority));
        return sorted;
    }

    /** Builds a chain. */
    public static final class Builder {
        private final List<BeaconLocator> locators = new ArrayList<>();
        private boolean firstWins;
        private Duration timeout = Duration.ofSeconds(3);
        private final Set<String> acceptGroupIdFrom = new LinkedHashSet<>(Set.of("url"));
        private Function<String, List<InetAddress>> resolver = DnsExpansion.PLATFORM;
        private LocatePolicy policy = LocatePolicy.DEFAULT;

        private Builder() {
        }

        /** How the node paces, requires, and traces this chain's answers (default: the node's own settings). */
        public Builder locatePolicy(LocatePolicy value) {
            this.policy = Objects.requireNonNull(value, "policy");
            return this;
        }

        /** Appends a locator. */
        public Builder add(BeaconLocator locator) {
            locators.add(Objects.requireNonNull(locator, "locator"));
            return this;
        }

        /** Appends locators in order. */
        public Builder addAll(List<? extends BeaconLocator> more) {
            more.forEach(this::add);
            return this;
        }

        /** Stop at the first locator that returned endpoints, asking them one at a time. */
        public Builder firstWins() {
            this.firstWins = true;
            return this;
        }

        /** The deadline per locator (default 3 s). */
        public Builder timeout(Duration perLocator) {
            this.timeout = Objects.requireNonNull(perLocator, "perLocator");
            return this;
        }

        /** The locator names whose GroupId claim is honoured when the request has none (default {@code url}). */
        public Builder acceptGroupIdFrom(Set<String> names) {
            acceptGroupIdFrom.clear();
            acceptGroupIdFrom.addAll(names);
            return this;
        }

        /** The resolver that expands names to addresses as the last step; null to keep names. */
        public Builder expandNames(Function<String, List<InetAddress>> resolver) {
            this.resolver = resolver;
            return this;
        }

        /** Builds the chain. */
        public BootstrapChain build() {
            return new BootstrapChain(this);
        }
    }
}
