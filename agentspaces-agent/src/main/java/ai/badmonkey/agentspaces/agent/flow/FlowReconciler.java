/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.agent.flow;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * The flow reconciler (ISSUE-FlowManifest FR-2, FR-3, FR-7, FR-9, FR-11): watches the group's {@code flow-control}
 * space for manifests from an issuer the authorizer permits as a directive issuer, keeps the newest version per
 * flow, and on every change rebinds the stages that name this peer: the previous binding of the agent is drained
 * (closed, so in-flight takes finish or lapse) and the agent is bound again from the catalogue with the manifest's
 * attribute overrides and the resolved assets, under the tags {@code flow} and {@code flowVersion}. A binding
 * whose class the catalogue lacks or whose asset no card serves is a conformance failure with the reason; the
 * hints {@code flowVersion} and {@code flowConformance} say where this peer stands.
 */
public final class FlowReconciler implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(FlowReconciler.class.getName());
    private static final Duration SUBSCRIPTION = Duration.ofHours(1);

    /** Where this peer stands: the version per flow, the stages bound, the failures with reasons. */
    public record Conformance(Map<String, Long> versions, List<String> bound, Map<String, String> failures) {
        public Conformance {
            versions = versions == null ? Map.of() : Map.copyOf(versions);
            bound = bound == null ? List.of() : List.copyOf(bound);
            failures = failures == null ? Map.of() : Map.copyOf(failures);
        }
    }

    private final Space flowControl;
    private final String self;
    private final Map<String, String> selfHints;
    private final AgentBinder binder;
    private final Catalogue catalogue;
    private final Function<String, Optional<AssetRef>> assets;
    private final Authorizer authorizer;
    private final String scope;
    private final InstantSource clock;
    private final BiConsumer<String, String> hints;
    private final Map<String, FlowManifest> current = new LinkedHashMap<>();
    private final Map<String, Bound> bound = new LinkedHashMap<>();
    private final List<Subscription> subscriptions = new ArrayList<>();
    private volatile Conformance conformance = new Conformance(Map.of(), List.of(), Map.of());
    private volatile boolean closed;

    private record Bound(AgentBinder.Bound handle, String flow, long version, String digest) {
    }

    private FlowReconciler(Space flowControl, String self, Map<String, String> selfHints, AgentBinder binder, Catalogue catalogue,
                           Function<String, Optional<AssetRef>> assets, Authorizer authorizer, String scope, InstantSource clock,
                           BiConsumer<String, String> hints) {
        this.flowControl = flowControl;
        this.self = self;
        this.selfHints = selfHints;
        this.binder = binder;
        this.catalogue = catalogue;
        this.assets = assets;
        this.authorizer = authorizer;
        this.scope = scope;
        this.clock = clock;
        this.hints = hints;
    }

    /**
     * Attaches to the flow-control space and reconciles to the newest manifests now (FR-11).
     *
     * @param self   this peer's id
     * @param assets resolves an asset reference to a live card (the group's ad cache); null resolves nothing
     * @param hints  receives the hints {@code flowVersion} and {@code flowConformance}; may be null
     */
    public static FlowReconciler attach(Space flowControl, String self, Map<String, String> selfHints, AgentBinder binder,
                                        Catalogue catalogue, Function<String, Optional<AssetRef>> assets, Authorizer authorizer,
                                        String scope, InstantSource clock, BiConsumer<String, String> hints) {
        FlowReconciler r = new FlowReconciler(Objects.requireNonNull(flowControl, "flowControl"), Objects.requireNonNull(self, "self"),
                selfHints == null ? Map.of() : Map.copyOf(selfHints), Objects.requireNonNull(binder, "binder"),
                Objects.requireNonNull(catalogue, "catalogue"), assets == null ? n -> Optional.empty() : assets, authorizer,
                scope == null ? "flow-control" : scope, Objects.requireNonNull(clock, "clock"), hints == null ? (k, v) -> { } : hints);
        r.reconcile();
        try {
            r.subscriptions.add(flowControl.notify(Template.of(FlowManifest.class), ev -> r.reconcile(), Lease.of(SUBSCRIPTION)));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "flow-control does not notify: {0}", e.toString());
        }
        return r;
    }

    public void renew() {
        subscriptions.forEach(s -> {
            try {
                s.renew(SUBSCRIPTION);
            } catch (RuntimeException ignored) {
                // lapsed
            }
        });
    }

    /** Re-reads the manifests and rebinds what changed (FR-3); safe to call from any thread, serialised. */
    public synchronized Conformance reconcile() {
        if (closed) {
            return conformance;
        }
        Map<String, FlowManifest> newest = new LinkedHashMap<>();
        try {
            for (Space.Entry<FlowManifest> e : flowControl.readAllEntries(Template.of(FlowManifest.class), 500)) {
                if (!permitted(e.issuer())) {
                    continue;
                }
                FlowManifest m = e.value();
                FlowManifest c = newest.get(m.flow());
                if (c == null || m.version() > c.version()) {
                    newest.put(m.flow(), m);
                }
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "cannot read flow-control: {0}", e.toString());
            return conformance;
        }
        current.clear();
        current.putAll(newest);
        Map<String, Long> versions = new LinkedHashMap<>();
        List<String> boundNow = new ArrayList<>();
        Map<String, String> failures = new LinkedHashMap<>();
        for (FlowManifest m : newest.values()) {
            versions.put(m.flow(), m.version());
            for (FlowManifest.Binding b : m.bindings()) {
                if (!b.names(self, selfHints)) {
                    continue;
                }
                String name = b.instance();
                String digest = m.hash() + "|" + b;
                Bound existing = bound.get(name);
                if (existing != null && existing.digest().equals(digest)) {
                    boundNow.add(name);
                    continue;
                }
                Optional<Object> agent = catalogue.lookup(b.agent());
                if (agent.isEmpty()) {
                    failures.put(name, "no class for agent " + b.agent() + " on this peer");
                    continue;
                }
                Map<String, String> overrides = new LinkedHashMap<>(b.attributes());
                if (!b.lane().isBlank()) {
                    overrides.putIfAbsent("space", b.lane());
                }
                boolean assetsOk = true;
                for (Map.Entry<String, String> a : b.assets().entrySet()) {
                    Optional<AssetRef> ref = assets.apply(a.getValue());
                    if (ref.isEmpty()) {
                        failures.put(name, "no card serves asset " + AssetRefs.nameOf(a.getValue()));
                        assetsOk = false;
                        break;
                    }
                    overrides.put(a.getKey(), ref.get().uri());
                    overrides.put(a.getKey() + ".shape", ref.get().shape() == null ? "" : ref.get().shape());
                }
                if (!assetsOk) {
                    continue;
                }
                if (existing != null) {
                    drain(existing, m.policy());
                    bound.remove(name);
                }
                try {
                    AgentBinder.Bound handle = binder.bind(agent.get(), name, overrides, m.tags());
                    bound.put(name, new Bound(handle, m.flow(), m.version(), digest));
                    boundNow.add(name);
                    LOG.log(System.Logger.Level.INFO, "flow {0} v{1}: bound {2} as {3}", m.flow(), m.version(), b.agent(), name);
                } catch (RuntimeException e) {
                    failures.put(name, "bind refused: " + e.getMessage());
                }
            }
        }
        // bindings a newer version dropped are drained
        for (String name : new ArrayList<>(bound.keySet())) {
            if (!boundNow.contains(name)) {
                Bound old = bound.remove(name);
                FlowManifest m = current.get(old.flow());
                drain(old, m == null ? FlowManifest.FLUSH : m.policy());
            }
        }
        conformance = new Conformance(versions, boundNow, failures);
        versions.forEach((flow, v) -> hints.accept(FlowManifest.VERSION_TAG, Long.toString(v)));
        hints.accept("flowConformance", Integer.toString(failures.size()));
        return conformance;
    }

    /** Flush closes the loops and lets in-flight takes finish by their lease; abort closes now (FR-7). */
    private void drain(Bound old, String policy) {
        try {
            old.handle().close();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "drain of {0}: {1}", old.flow(), e.toString());
        }
        if (FlowManifest.FLUSH.equals(policy)) {
            // the closed loops finish their current invocation; nothing new is taken
            LOG.log(System.Logger.Level.INFO, "flow {0}: stage drained (flush)", old.flow());
        }
    }

    private boolean permitted(AgentId issuer) {
        if (authorizer == null || issuer == null) {
            return true;
        }
        try {
            return authorizer.permits(issuer, Authorizer.Operation.DIRECTIVE_ISSUER, scope);
        } catch (RuntimeException e) {
            return false;
        }
    }

    public Optional<FlowManifest> current(String flow) {
        return Optional.ofNullable(current.get(flow));
    }

    public Map<String, FlowManifest> current() {
        return Map.copyOf(current);
    }

    public Conformance conformance() {
        return conformance;
    }

    @Override
    public synchronized void close() {
        closed = true;
        subscriptions.forEach(s -> {
            try {
                s.close();
            } catch (RuntimeException ignored) {
                // closing
            }
        });
        for (Bound b : bound.values()) {
            try {
                b.handle().close();
            } catch (RuntimeException ignored) {
                // closing
            }
        }
        bound.clear();
    }
}
