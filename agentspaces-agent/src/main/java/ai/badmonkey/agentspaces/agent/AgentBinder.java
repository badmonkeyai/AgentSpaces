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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.BidFunction;
import ai.badmonkey.agentspaces.agent.annotation.Durations;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.Ballot;
import ai.badmonkey.agentspaces.agent.annotation.CapabilityRef;
import ai.badmonkey.agentspaces.agent.annotation.OnDecision;
import ai.badmonkey.agentspaces.agent.annotation.OnEstimate;
import ai.badmonkey.agentspaces.agent.annotation.OrderedTake;
import ai.badmonkey.agentspaces.agent.annotation.Part;
import ai.badmonkey.agentspaces.agent.annotation.PdtRef;
import ai.badmonkey.agentspaces.agent.annotation.Propose;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.capability.Contribution;
import ai.badmonkey.agentspaces.agent.capability.Motion;
import ai.badmonkey.agentspaces.agent.join.JoinBinding;
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.agent.pdt.RefResolver;
import ai.badmonkey.agentspaces.agent.reduce.ReduceBinding;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Binds annotated plain objects to spaces (plan §10.3, framework-neutral form):
 * {@link SpaceTake} methods become virtual-thread worker loops with
 * crash-equals-lease-lapse semantics, {@link SpaceNotify} methods become leased
 * subscriptions, {@link BidFunction} methods become AUCTION cost functions, and
 * an {@link AgentCard} is generated from the {@link AgentSpec} and the bound
 * method signatures, then published through discovery so other peers can ask
 * "who can produce a Finding from a ResearchTask" with no extra code.
 *
 * <p>The Embabel adapter maps {@code @Agent}/{@code @Action} metadata onto this
 * binder; Spring applications register beans with it from a post-processor. The
 * binder itself depends on neither.
 */
public final class AgentBinder implements AutoCloseable {

    /** The AgentCard's time to live (srad/agentspaces-event-loop.md §3.11): the starter refreshes it every 5 minutes. */
    public static final java.time.Duration CARD_TTL = java.time.Duration.ofMinutes(15);

    private static final System.Logger LOG = System.getLogger(AgentBinder.class.getName());
    /** Remembered entry ids per notify subscription, for at-least-once dedup. */
    private static final int NOTIFY_DEDUP_CAPACITY = 4096;

    private final PeerIdentity identity;
    private final GroupId group;
    private final String groupName;
    private final InstantSource clock;
    private final DiscoveryService discovery;
    /**
     * The registry every card name comes from (ISSUE-WorkflowShape §9.1). Share
     * the spaces' registry so the card advertises what the space writes; the
     * default names {@code <fqcn>#v1}, as the spaces do by default.
     */
    private volatile SchemaRegistry schemas = new SimpleSchemaRegistry();
    /** Set by the first bind; the registry is fixed from then on. */
    private volatile boolean everBound;
    private final AdvertisementSigner signer = new AdvertisementSigner();
    /** The binder's listener on the view's AppCard level, or null without discovery. */
    private final AutoCloseable gateRegistration;
    private final Map<String, Space> spaces = new ConcurrentHashMap<>();
    /** Space name to the agent whose @BidFunction prices it (QA4 A4-8): one per space per peer. */
    private final Map<String, String> bidders = new ConcurrentHashMap<>();
    private final List<Bound> bound = new CopyOnWriteArrayList<>();

    // ------------------------------------------------------------------ declaration readers (review §7.2)

    /** The registered readers, by class: loaded once from ServiceLoader, replaced by explicit registration. */
    private static final Map<Class<?>, ai.badmonkey.agentspaces.agent.spi.AgentDeclarationReader> READERS =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static volatile boolean readersLoaded;
    /** Declarations per class, cleared whenever a reader is registered. */
    private static final Map<Class<?>, Optional<ai.badmonkey.agentspaces.agent.spi.AgentDeclaration>> DECLARATIONS =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Registers a reader for every binder in this JVM, replacing a loaded or registered one of the same class
     * (review §7.2). Call before any bean binds.
     *
     * @param reader the reader
     */
    public static void registerDeclarationReader(ai.badmonkey.agentspaces.agent.spi.AgentDeclarationReader reader) {
        Objects.requireNonNull(reader, "reader");
        loadReaders();
        READERS.put(reader.getClass(), reader);
        DECLARATIONS.clear();
    }

    /** Removes a registered reader, for tests. */
    public static void unregisterDeclarationReader(Class<?> readerClass) {
        READERS.remove(readerClass);
        DECLARATIONS.clear();
    }

    // ------------------------------------------------------------------ take invokers (review §7.8)

    private static final List<ai.badmonkey.agentspaces.agent.spi.TakeInvoker> INVOKERS = new CopyOnWriteArrayList<>();

    /**
     * Registers a take invoker for every binder in this JVM (review §7.8), replacing one of the same class.
     *
     * @param invoker the invoker
     */
    public static void registerTakeInvoker(ai.badmonkey.agentspaces.agent.spi.TakeInvoker invoker) {
        Objects.requireNonNull(invoker, "invoker");
        INVOKERS.removeIf(existing -> existing.getClass() == invoker.getClass());
        INVOKERS.add(invoker);
    }

    /** Removes a registered invoker, for tests. */
    public static void unregisterTakeInvoker(Class<?> invokerClass) {
        INVOKERS.removeIf(existing -> existing.getClass() == invokerClass);
    }

    /** The invoker that claims a method, if any. */
    private static Optional<ai.badmonkey.agentspaces.agent.spi.TakeInvoker> invokerFor(Object bean, Method method) {
        for (ai.badmonkey.agentspaces.agent.spi.TakeInvoker invoker : INVOKERS) {
            if (invoker.claims(bean, method)) {
                return Optional.of(invoker);
            }
        }
        return Optional.empty();
    }

    private static void loadReaders() {
        if (readersLoaded) {
            return;
        }
        synchronized (READERS) {
            if (readersLoaded) {
                return;
            }
            for (ClassLoader loader : readerLoaders()) {
                try {
                    for (ai.badmonkey.agentspaces.agent.spi.AgentDeclarationReader reader
                            : java.util.ServiceLoader.load(ai.badmonkey.agentspaces.agent.spi.AgentDeclarationReader.class, loader)) {
                        READERS.putIfAbsent(reader.getClass(), reader);
                    }
                } catch (java.util.ServiceConfigurationError e) {
                    LOG.log(System.Logger.Level.WARNING, "a declaration reader failed to load", e);
                }
            }
            readersLoaded = true;
        }
    }

    private static List<ClassLoader> readerLoaders() {
        List<ClassLoader> loaders = new ArrayList<>();
        loaders.add(AgentBinder.class.getClassLoader());
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        if (context != null && !loaders.contains(context)) {
            loaders.add(context);
        }
        return loaders;
    }

    /**
     * The declaration a reader finds on a class (review §7.2): the first reader, in order, that answers.
     *
     * @param type the class
     * @return the declaration, or empty when no reader recognises the class
     */
    public static Optional<ai.badmonkey.agentspaces.agent.spi.AgentDeclaration> declarationOf(Class<?> type) {
        loadReaders();
        if (READERS.isEmpty()) {
            return Optional.empty();
        }
        return DECLARATIONS.computeIfAbsent(type, t -> READERS.values().stream()
                .sorted(java.util.Comparator.comparingInt(ai.badmonkey.agentspaces.agent.spi.AgentDeclarationReader::order))
                .map(reader -> reader.read(userClass(t)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst());
    }

    /** A proxy's user class: a CGLIB or ByteBuddy {@code $$} subclass reads as the class it proxies. */
    static Class<?> userClass(Class<?> type) {
        Class<?> at = type;
        while (at.getName().contains("$$") && at.getSuperclass() != null && at.getSuperclass() != Object.class) {
            at = at.getSuperclass();
        }
        return at;
    }
    /** Vote capabilities by vote-space name, for {@code @Ballot} and {@code @OnDecision}. */
    private final Map<String, VoteCapability> votes = new ConcurrentHashMap<>();
    /** Ordered-take coordinators by space name, for {@code @OrderedTake}. */
    private final Map<String, OrderedTakes> coordinators = new ConcurrentHashMap<>();
    /** Typed capability clients by type, for {@code @CapabilityRef}. */
    private final Map<Class<?>, Object> clients = new ConcurrentHashMap<>();
    /** A fallback resolver for {@code @CapabilityRef} types (the group's typed-client lookup), or null. */
    private volatile java.util.function.Function<Class<?>, Object> clientResolver;
    /** Builds the handles of {@code @PdtRef} sites from the group's persistent trees, or null (PDT 2 phase 7, P2-X-03). */
    private volatile RefResolver refResolver;
    /** The {@code @PdtRef} handles a bound method's trailing parameters receive, resolved once at bind (P2-X-03). */
    private final Map<Method, Object[]> refArguments = new ConcurrentHashMap<>();
    /** The peer's push-sum aggregate, the target of a {@link Contribution} return, or null. */
    private volatile PushSumAggregate aggregate;
    /**
     * How an agent's signing identity is made from its name (QA4 A4-7 phase 3).
     * The default is the peer-signed identity every fleet has always used;
     * {@code identity::subordinate} gives each bound agent a certified key of
     * its own, and then the spaces it is handed are per-agent views.
     */
    private volatile java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> identities;

    /**
     * Creates a binder.
     *
     * @param identity  the local peer identity
     * @param group     the group agents are bound into
     * @param discovery the group's discovery service for AgentCard publication;
     *                  {@code null} skips card publication
     * @param clock     the time source for card freshness
     */
    public AgentBinder(PeerIdentity identity, GroupId group,
                       DiscoveryService discovery, InstantSource clock) {
        this(identity, group, discovery, clock, null);
    }

    /**
     * Creates a binder that also knows its group's application-facing name, so
     * a binding annotation's {@code group} attribute can address it by that
     * name as well as by GroupId value (spec §10.3).
     *
     * @param identity  the local peer identity
     * @param group     the group agents are bound into
     * @param discovery the group's discovery service; {@code null} skips cards
     * @param clock     the time source for card freshness
     * @param groupName the group's application-facing name; {@code null} when
     *                  the group is addressable by id only
     */
    public AgentBinder(PeerIdentity identity, GroupId group,
                       DiscoveryService discovery, InstantSource clock, String groupName) {
        this(identity, group, discovery, clock, groupName, null);
    }

    /**
     * Creates a binder whose agents' signing identities come from {@code identities}
     * (QA4 A4-7 phase 3): {@code identity::agentIdentity} (the default when null)
     * binds agents under the peer key as always; {@code identity::subordinate}
     * gives every bound agent a peer-certified key of its own, so its records are
     * {@code AGENT_ATTESTED} and its card carries the key.
     *
     * @param identity   the peer identity
     * @param group      the group
     * @param discovery  the discovery service for card publication, or null
     * @param clock      the time source
     * @param groupName  the application-facing group name, or null
     * @param identities the agent identity factory, or null for the default
     */
    public AgentBinder(PeerIdentity identity, GroupId group,
                       DiscoveryService discovery, InstantSource clock, String groupName,
                       java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> identities) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.group = Objects.requireNonNull(group, "group");
        this.discovery = discovery;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.groupName = groupName;
        this.identities = identities != null ? identities : identity::agentIdentity;
        // SPEC §6.1a.5 (v0.1.32; AppCard origin and formats, 2026-11-09): a card above the spill threshold travels as a
        // stub only while the view reads it, so a change of the view's AppCard level reissues every such card
        this.gateRegistration = discovery == null ? null : discovery.onAppCardGateChange(open -> reissueOversized());
    }

    /**
     * Replaces the schema registry card names come from (ISSUE-WorkflowShape
     * §9.1): give the binder the registry its spaces were built with, and the
     * {@code consumes}, {@code produces}, {@code spaceBindings}, and action
     * schemas of every card name exactly what the spaces write. Refused once an
     * agent has been bound, since that card already carries the earlier names.
     *
     * @param schemas the registry
     * @return this binder
     * @throws IllegalStateException after the first bind
     */
    public AgentBinder schemas(SchemaRegistry schemas) {
        Objects.requireNonNull(schemas, "schemas");
        if (everBound) {
            throw new IllegalStateException(
                    "the schema registry must be set before the first bind");
        }
        this.schemas = schemas;
        return this;
    }

    /** Returns the schema registry card names come from. */
    public SchemaRegistry schemas() {
        return schemas;
    }

    /**
     * Installs the constraint gate (ISSUE-Constraints FR-8, FR-10): consulted before a take claim of each verb
     * binding and before the binder writes a bound method's result. Null (the default) gates nothing.
     */
    public AgentBinder constraintGate(ai.badmonkey.agentspaces.constraints.ConstraintGate gate) {
        this.constraintGate = gate;
        AutoCloseable previous = gateChanges;
        gateChanges = null;
        if (previous != null) {
            closeQuietly(previous);
        }
        if (gate != null) {
            gate.onReload(this::rouseUngatedTakes);
            gateChanges = gate.onChange(this::gateChanged); // EL-W-07: a gated take wakes on what moves the judgement
        }
        gateChanged(); // a gated take waiting on the old gate looks at the new one now
        rouseUngatedTakes(); // a take waiting long without a gate looks at the new one now
        return this;
    }

    // ---- ungated takes wait on changes (idle-loop item 2, reviews/IDLE-LOOP-2026-11-18.md §6)

    /**
     * How long an ungated take waits for a matching entry before its loop looks again: the one take-wait rule's
     * backstop (srad/agentspaces-event-loop.md EL-W-07, {@link ai.badmonkey.agentspaces.common.time.Waits#BACKSTOP}).
     * It wakes at once on a change to the space, and a gate installed or reloaded interrupts it, so the length only
     * bounds what neither covers. A gated take keeps its {@code pollTimeout}.
     */
    static final Duration UNGATED_TAKE_WAIT = ai.badmonkey.agentspaces.common.time.Waits.BACKSTOP;

    /** One take loop's wait, so a gate change interrupts it only while it waits, never in its action. */
    private static final class TakeWait {
        final Thread thread;
        /** In the ungated take, interruptible by a gate change. Under this object's monitor. */
        boolean waiting;
        /** Inside the ungated take call at all, interrupted or not. Under this object's monitor. */
        boolean inTake;

        TakeWait(Thread thread) {
            this.thread = thread;
        }
    }

    /** The longest a gate change waits for an interrupted take to leave its wait. */
    private static final long ROUSE_WAIT_MILLIS = 2_000;

    private final Set<TakeWait> ungatedWaits = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Interrupts every ungated take while it waits, and returns once each has left its take, so its loop looks at the
     * gate before any entry written after this returns can be claimed. An interrupted take can still claim what was
     * there before, as a take that started before the gate always could; it cannot wake for a later write and claim
     * it past the gate, which a bare interrupt raced with. Bounded: a take that does not leave in
     * {@link #ROUSE_WAIT_MILLIS} is left to finish.
     */
    private void rouseUngatedTakes() {
        for (TakeWait wait : ungatedWaits) {
            synchronized (wait) {
                if (!wait.waiting) {
                    continue;
                }
                wait.waiting = false;
                wait.thread.interrupt();
                // A real-time bound on a thread leaving its take, on the system's time source (EL-C-08).
                long deadline = ai.badmonkey.agentspaces.common.time.TimeSource.system().monoNanos() + ROUSE_WAIT_MILLIS * 1_000_000L;
                boolean interrupted = false;
                while (wait.inTake) {
                    long left = (deadline - ai.badmonkey.agentspaces.common.time.TimeSource.system().monoNanos()) / 1_000_000L;
                    if (left <= 0) {
                        break;
                    }
                    try {
                        wait.wait(left);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private boolean ungated() {
        ai.badmonkey.agentspaces.constraints.ConstraintGate gate = constraintGate;
        return gate == null || gate.loaded().isEmpty();
    }

    public ai.badmonkey.agentspaces.constraints.ConstraintGate constraintGate() {
        return constraintGate;
    }

    private volatile ai.badmonkey.agentspaces.constraints.ConstraintGate constraintGate;

    /** The installed gate's {@code onChange} registration, or null. */
    private volatile AutoCloseable gateChanges;

    /** Each take loop's change signal, run on a change of the gate (EL-W-07); a loop's space runs it too. */
    private final Set<ai.badmonkey.agentspaces.common.time.ChangeSignal> gatedWaiters =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** The gate changed (an event of a lane, a version, a jury decision): every gated take looks again. */
    private void gateChanged() {
        for (ai.badmonkey.agentspaces.common.time.ChangeSignal waiter : gatedWaiters) {
            waiter.run();
        }
    }

    /**
     * A take loop's change signal: run by its space's changes and the gate's, so its gated look waits for either
     * (EL-W-07). Close the handle when the loop ends.
     */
    private AutoCloseable gatedWaiter(Space space, ai.badmonkey.agentspaces.common.time.ChangeSignal signal) {
        gatedWaiters.add(signal);
        AutoCloseable spaceChanges = space.onChangeSignal(signal); // only a wake (Space.onChangeSignal)
        return () -> {
            gatedWaiters.remove(signal);
            spaceChanges.close();
        };
    }

    /**
     * The one take-wait rule for a gated take (srad/agentspaces-event-loop.md EL-W-07): when the gate admits nothing,
     * the take waits for a change of its space or of the gate ({@link
     * ai.badmonkey.agentspaces.constraints.ConstraintGate#onChange}: a lane's event, a version, a jury decision), or for
     * the gate's own next due ({@link ai.badmonkey.agentspaces.constraints.ConstraintGate#nextClaimDue}), under the one
     * backstop, {@link ai.badmonkey.agentspaces.common.time.Waits#BACKSTOP}. It was a 1 s look (§10.17).
     */
    static final Duration GATED_TAKE_WAIT = ai.badmonkey.agentspaces.common.time.Waits.BACKSTOP;

    /**
     * The gate's look before a claim (§9.4): the entries the template matches are read, the first the gate admits
     * narrows the take to its key, and when every candidate is refused the loop waits as {@link #GATED_TAKE_WAIT}
     * says. Returns the template to take with, or empty when nothing may be taken now.
     */
    private Optional<Template<?>> gated(Space space, Template<?> template, Duration pollTimeout,
                                        ai.badmonkey.agentspaces.common.time.ChangeSignal changed) {
        ai.badmonkey.agentspaces.constraints.ConstraintGate gate = constraintGate;
        if (gate == null || gate.loaded().isEmpty()) {
            return Optional.of(template);
        }
        long seen = changed.seen(); // before the look: a change of the space or the gate meanwhile is not missed
        boolean signals = space.changeSignals() >= 0;
        List<? extends Space.Entry<?>> candidates;
        try {
            candidates = space.readAllEntries(template, 16);
        } catch (RuntimeException e) {
            return Optional.of(template);
        }
        for (Space.Entry<?> candidate : candidates) {
            if (gate.beforeClaim(candidate).admitted()) {
                return Optional.of(gate.narrow(template, candidate));
            }
        }
        // nothing admitted now (nothing there, or every candidate refused): wait and look again, rather than a
        // blocking take that would claim an entry the gate never saw. The wait ends at the next change of the space
        // (a new entry is looked at at once) or of the gate (a lane's event, a version, a jury decision), at the
        // gate's own next due, or at the backstop (EL-W-07; was a 1 s look, and before that a 250 ms sleep).
        try {
            if (!signals) {
                // a space that keeps no change counter signals nothing: a short look, as before
                waits().sleep(Duration.ofMillis(Math.max(20, Math.min(pollTimeout.toMillis(), 250))));
            } else {
                ai.badmonkey.agentspaces.common.time.TimeSource time = changed.time();
                long deadline = ai.badmonkey.agentspaces.common.time.Waits.deadline(time,
                        pollTimeout.compareTo(GATED_TAKE_WAIT) > 0 ? pollTimeout : GATED_TAKE_WAIT);
                Optional<java.time.Instant> due = gate.nextClaimDue();
                if (due.isPresent()) {
                    long ahead = Math.max(0, java.time.Duration.between(time.instant(), due.get()).toMillis());
                    deadline = Math.min(deadline, ai.badmonkey.agentspaces.common.time.Waits.deadline(time,
                            Duration.ofMillis(ahead)));
                }
                changed.await(seen, deadline);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return Optional.empty();
    }

    /** A short poll when the gate is on: the look already found an admitted entry, so the take should not wait long. */
    private Duration gatedPoll(Duration pollTimeout) {
        ai.badmonkey.agentspaces.constraints.ConstraintGate gate = constraintGate;
        return gate == null || gate.loaded().isEmpty() ? pollTimeout : Duration.ofMillis(Math.min(200, pollTimeout.toMillis()));
    }

    /** The gate's look before a result write (§9.6): a refusal throws to the bound method's caller. */
    private void gateResult(Object entry, Map<String, String> tags) {
        ai.badmonkey.agentspaces.constraints.ConstraintGate gate = constraintGate;
        if (gate == null || entry == null) {
            return;
        }
        String wire;
        try {
            wire = schemas.schemaNameOf(entry.getClass());
        } catch (RuntimeException e) {
            wire = entry.getClass().getName();
        }
        ai.badmonkey.agentspaces.constraints.ConstraintGate.Decision d = gate.beforeResult(entry, wire, tags);
        if (!d.admitted()) {
            throw new ai.badmonkey.agentspaces.constraints.ConstraintRefusedException(d.unit(), d.key(), d.reason());
        }
    }

    /**
     * Replaces the agent identity factory for agents bound from now on.
     *
     * @param identities the factory
     * @return this binder
     */
    public AgentBinder identities(java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> identities) {
        this.identities = Objects.requireNonNull(identities, "identities");
        return this;
    }

    /**
     * Tests whether a binding annotation's {@code group} attribute addresses
     * this binder: empty addresses every group, otherwise the value must be
     * this group's name or GroupId value.
     *
     * @param annotatedGroup the annotation's {@code group} attribute
     * @return whether the binding is this binder's to make
     */
    public boolean owns(String annotatedGroup) {
        return annotatedGroup == null || annotatedGroup.isEmpty()
                || annotatedGroup.equals(groupName) || annotatedGroup.equals(group.value());
    }

    /**
     * Tests whether a class declares any space binding at all: a
     * {@link SpaceTake}, {@link SpaceNotify}, or {@link BidFunction} method,
     * or a {@link SpaceRef} or {@link PdtRef} field.
     *
     * @param type the candidate class
     * @return whether the class has bindings
     */
    public static boolean hasBindings(Class<?> type) {
        return !declaredBindings(type).isEmpty();
    }

    /**
     * Tests whether a class takes {@link PdtRef} handles, in a field or a verb's parameter, which only a group with
     * persistent trees can give (PDT 2 phase 7, P2-X-03).
     *
     * @param type the candidate class
     * @return whether any binding of the class takes a ref
     */
    public static boolean takesRefs(Class<?> type) {
        return declaredBindings(type).stream().anyMatch(Declared::refs);
    }

    /**
     * Tests whether this binder's group satisfies a class (spec §10.3, §10.4):
     * at least one of its bindings addresses this group, and every binding
     * that does names a space registered here (or, when it names none, this
     * binder has exactly one space to infer), and a binding that takes
     * {@link PdtRef} handles finds a resolver here. A class with no bindings is
     * trivially satisfied, since it needs no space.
     *
     * @param type the candidate class
     * @return whether the class can be bound here
     */
    public boolean satisfies(Class<?> type) {
        // Review §7.3 (X46): an implied binding never decides group membership; a class whose only
        // bindings are implied binds into every group, as a class with none does.
        List<Declared> declared = declaredBindings(type).stream().filter(d -> !d.implied()).toList();
        if (declared.isEmpty()) {
            return true;
        }
        boolean any = false;
        for (Declared binding : declared) {
            if (!owns(binding.group())) {
                continue;
            }
            any = true;
            if (binding.refs() && refResolver == null) {
                return false;
            }
            for (String spaceName : binding.spaceNames()) {
                if (spaceName.isEmpty() ? spaces.size() != 1 : !spaces.containsKey(spaceName)) {
                    return false;
                }
            }
        }
        return any;
    }

    /**
     * Registers a named space agents can bind to.
     *
     * @param name  the name used in annotations
     * @param space the space
     * @return this binder
     */
    public AgentBinder space(String name, Space space) {
        spaces.put(Objects.requireNonNull(name), Objects.requireNonNull(space));
        return this;
    }

    /**
     * Registers the vote capability over a registered vote space, so
     * {@code @Ballot} and {@code @OnDecision} methods naming that space can be bound.
     *
     * @param spaceName the vote space's registered name
     * @param vote      the capability voting in it
     * @return this binder
     */
    public AgentBinder vote(String spaceName, VoteCapability vote) {
        votes.put(Objects.requireNonNull(spaceName, "spaceName"), Objects.requireNonNull(vote, "vote"));
        return this;
    }

    /** The TTL floor's check of the verb annotations' leases at bind time (EL-E-03, decision 18); lenient by default. */
    private volatile ai.badmonkey.agentspaces.common.time.TtlFloor ttlFloor =
            ai.badmonkey.agentspaces.common.time.TtlFloor.lenient();

    /**
     * Sets how a lease or TTL under 60 s on a bound method's verb annotation is judged (srad/agentspaces-event-loop.md
     * EL-E-03, EL-E-04): lenient by default (a WARN), refused under a strict check. A {@code @TtlOverride(reason)} beside
     * the annotation, or an expert override keyed {@code binder.<AgentClass>.<method>.<member>}, passes it.
     *
     * @param floor the check
     * @return this binder
     */
    public AgentBinder ttlFloor(ai.badmonkey.agentspaces.common.time.TtlFloor floor) {
        this.ttlFloor = Objects.requireNonNull(floor, "floor");
        return this;
    }

    /** The verb annotations whose lease and TTL members the floor checks. */
    private static final List<Class<? extends java.lang.annotation.Annotation>> LEASED = List.of(OrderedTake.class,
            SpaceReduce.class, SpaceJoin.class, SpaceTake.class, SpaceNotify.class, OnDecision.class, OnEstimate.class,
            Ballot.class, Propose.class);

    /**
     * Checks every lease and TTL member of the type's verb annotations against the floor, after the bind's attribute
     * overrides (EL-E-03): a value equal to the annotation's own default passes (the TTL register carries its reason).
     */
    private void checkTtls(Class<?> type) {
        ai.badmonkey.agentspaces.common.time.TtlFloor floor = ttlFloor;
        for (Method method : type.getMethods()) {
            ai.badmonkey.agentspaces.api.ttl.TtlOverride override =
                    method.getAnnotation(ai.badmonkey.agentspaces.api.ttl.TtlOverride.class);
            for (Class<? extends java.lang.annotation.Annotation> kind : LEASED) {
                java.lang.annotation.Annotation found = annotationOf(method, kind);
                if (found == null) {
                    continue;
                }
                for (Method member : kind.getDeclaredMethods()) {
                    String name = member.getName().toLowerCase(java.util.Locale.ROOT);
                    if (!(name.contains("lease") || name.contains("ttl")) || member.getReturnType() != String.class) {
                        continue;
                    }
                    String value;
                    try {
                        value = (String) member.invoke(found);
                    } catch (ReflectiveOperationException e) {
                        throw new IllegalStateException("reading " + kind.getSimpleName() + "." + member.getName(), e);
                    }
                    if (value == null || value.isBlank()) {
                        continue;
                    }
                    Duration parsed;
                    try {
                        parsed = Durations.parse(value);
                    } catch (IllegalArgumentException malformed) {
                        continue; // the binding that reads it refuses it, naming the method
                    }
                    Object fallback = member.getDefaultValue();
                    Duration registered = fallback instanceof String s && !s.isBlank() ? Durations.parse(s) : null;
                    floor.check("binder." + type.getSimpleName() + "." + method.getName() + "." + member.getName(),
                            parsed, registered, override == null ? null : override.reason());
                }
            }
        }
    }

    /**
     * Registers the ordered-log coordinator over a registered space, so
     * {@code @OrderedTake} methods naming that space can be bound.
     *
     * @param spaceName   the coordinated space's registered name
     * @param coordinator the coordinator
     * @return this binder
     */
    public AgentBinder ordered(String spaceName, OrderedTakes coordinator) {
        coordinators.put(Objects.requireNonNull(spaceName, "spaceName"),
                Objects.requireNonNull(coordinator, "coordinator"));
        return this;
    }

    /**
     * Registers the peer's push-sum aggregate, the target of a {@link Contribution}
     * returned from a {@code @SpaceNotify} or {@code @SpaceTake} method.
     *
     * @param aggregate the aggregate
     * @return this binder
     */
    public AgentBinder aggregate(PushSumAggregate aggregate) {
        this.aggregate = Objects.requireNonNull(aggregate, "aggregate");
        return this;
    }

    /**
     * Registers a typed capability client for {@code @CapabilityRef} fields of its type.
     *
     * @param type   the field type
     * @param client the instance to inject
     * @param <C>    the client type
     * @return this binder
     */
    public <C> AgentBinder client(Class<C> type, C client) {
        clients.put(Objects.requireNonNull(type, "type"), Objects.requireNonNull(client, "client"));
        return this;
    }

    /**
     * Sets the fallback for {@code @CapabilityRef} types not registered here: the
     * group's typed-client lookup under the facade. The function returns
     * {@code null} for a type it cannot resolve.
     *
     * @param resolver the resolver
     * @return this binder
     */
    public AgentBinder clients(java.util.function.Function<Class<?>, Object> resolver) {
        this.clientResolver = Objects.requireNonNull(resolver, "resolver");
        return this;
    }

    /**
     * Sets what builds the handles of {@link PdtRef} fields and parameters (PDT 2 phase 7,
     * P2-X-03, PD-R-11): the group's persistent-tree facade, which the hosts register when
     * {@code agentspaces.pdt.enabled} is true. Without one, a class with a {@code @PdtRef}
     * binding does not satisfy this binder and binding it fails, naming the site.
     *
     * @param resolver the resolver
     * @return this binder
     */
    public AgentBinder refs(RefResolver resolver) {
        this.refResolver = Objects.requireNonNull(resolver, "resolver");
        return this;
    }

    /**
     * Binds an annotated object: wires bid functions, starts take loops and
     * notify subscriptions, and publishes the agent's card.
     *
     * @param agent the annotated object
     * @return a handle that unbinds on close
     */
    public Bound bind(Object agent) {
        return bind(agent, null);
    }

    /**
     * Binds an agent under an explicit name, overriding the {@code @AgentSpec}
     * (or class) name: for several instances of one agent class that must be
     * several agents — council seats, per-tenant workers — each bound as
     * {@code <peer>/<name>} with, under a subordinate identity factory, a
     * certified key of its own.
     *
     * @param agent the agent instance
     * @param name  the agent's local name, or null for the declared name
     * @return the binding
     */
    /**
     * Binds with attribute overrides and record tags (ISSUE-FlowManifest FR-4, FR-8): an override is keyed
     * {@code <method>.<attribute>} or {@code <attribute>} for every method and may name any attribute the bound
     * methods' verb annotations declare; an override of an attribute no method declares is refused by name. The
     * tags (the manifest's {@code flow} and {@code flowVersion}) are added to every record the binding writes.
     */
    public Bound bind(Object agent, String name, Map<String, String> overrides, Map<String, String> recordTags) {
        Objects.requireNonNull(agent, "agent");
        Map<String, String> effective = overrides == null ? Map.of() : Map.copyOf(overrides);
        if (!effective.isEmpty()) {
            Overrides.check(agent.getClass(), effective);
        }
        OVERRIDES.set(effective);
        try {
            Bound handle = bind(agent, name);
            if (recordTags != null && !recordTags.isEmpty()) {
                handle.recordTags.putAll(recordTags);
            }
            return handle;
        } finally {
            OVERRIDES.remove();
        }
    }

    public Bound bind(Object agent, String name) {
        Objects.requireNonNull(agent, "agent");
        everBound = true;
        Class<?> type = agent.getClass();
        Spec spec = findSpec(type);
        if (name == null || name.isEmpty()) {
            name = spec != null && !spec.name().isEmpty()
                    ? spec.name()
                    : decapitalize(type.getSimpleName());
        }
        ai.badmonkey.agentspaces.api.spi.AgentIdentity agentIdentity = identities.apply(name);
        if (!agentIdentity.id().equals(identity.agent(name))) {
            throw new IllegalStateException("identity factory returned " + agentIdentity.id().encoded()
                    + " for agent '" + name + "' of peer " + identity.peerId().display());
        }
        AgentId agentId = agentIdentity.id();
        // Review §7.2 (X20): two agents under one name on one peer would publish one card id twice, and
        // the ad-cache keeps the newest, so the card would flap between them. Refuse here, by name.
        for (Bound prior : bound) {
            if (prior.running && prior.agentId.localName().equals(name) && prior.boundClass != null
                    && prior.boundClass != type) {
                throw new IllegalStateException("an agent named '" + name + "' is already bound on this peer by "
                        + prior.boundClass.getName() + " (" + prior.agentId.encoded() + "); bind "
                        + type.getName() + " under another name");
            }
        }
        checkTtls(type); // EL-E-03: a lease under 60 s warns, or refuses under a strict floor, before anything runs
        Optional<ai.badmonkey.agentspaces.agent.spi.AgentDeclaration> declaration = declarationOf(type);
        // Review §7.3: the implied takes a declaration reader asks for, as @SpaceTake annotations scoped by tag.
        Map<Method, SpaceTake> impliedTakes = new java.util.HashMap<>();
        Map<Method, ai.badmonkey.agentspaces.agent.spi.ImpliedBinding> impliedBindings = new java.util.HashMap<>();
        final String agentName = name;
        declaration.ifPresent(d -> d.implied().forEach(implied -> {
            impliedTakes.put(implied.method(), impliedTake(implied, agentName));
            impliedBindings.put(implied.method(), implied);
        }));

        injectSpaceRefs(agent, type, agentIdentity);
        injectCapabilityRefs(agent, type);
        injectPdtRefs(agent, type);

        Bound handle = new Bound(agentIdentity);
        handle.boundClass = type;
        Set<String> consumes = new LinkedHashSet<>();
        Set<String> produces = new LinkedHashSet<>();
        Map<String, String> spaceBindings = new LinkedHashMap<>();
        // Review §7.4: where a binding writes its results when that is not its take space, keyed by the
        // produced schema; merged after the consumed bindings, which win on a shared schema.
        Map<String, String> producedBindings = new LinkedHashMap<>();
        List<CardAction> actions = new ArrayList<>();

        // Bid functions first, so AUCTION spaces are priced before any take runs.
        for (Method method : type.getMethods()) {
            BidFunction bid = annotationOf(method, BidFunction.class);
            if (bid == null || !owns(bid.group())) {
                continue;
            }
            requireSignature(method, 1, "a @BidFunction method takes the entry parameter");
            if (method.getReturnType() != double.class && method.getReturnType() != Double.class) {
                throw new IllegalArgumentException(
                        "@BidFunction method must return double: " + method);
            }
            Space space = resolveSpace(bid.space(), method);
            if (!(space instanceof ReplicatedSpace replicated)) {
                throw new IllegalArgumentException(
                        "@BidFunction requires a ReplicatedSpace: " + bid.space());
            }
            // QA4 A4-8: a space consults one cost function, and the setter used to
            // let the last bidder bound silently replace the first. Refuse here,
            // where both agents' names are known, so the message says who collided.
            String spaceName = resolveSpaceName(bid.space(), method);
            String prior = bidders.putIfAbsent(spaceName, agentId.localName());
            if (prior != null && !prior.equals(agentId.localName())) {
                throw new IllegalStateException(prior + " and " + agentId.localName()
                        + " both declare @BidFunction on '" + spaceName + "'; one bid function"
                        + " per space per peer — seat the second bidder on its own peer");
            }
            replicated.bidFunction(entry -> (double) invoke(agent, method, entry));
        }

        for (Method method : type.getMethods()) {
            SpaceTake take = annotationOf(method, SpaceTake.class);
            boolean implied = false;
            if (take == null && impliedTakes.containsKey(method)) {
                take = impliedTakes.get(method);
                implied = true;
                if (!impliedTakeBindsHere(take)) {
                    // Review §7.3 (X46): nowhere to bind in this group; advertised, not served here.
                    ai.badmonkey.agentspaces.agent.spi.ImpliedBinding binding = impliedBindings.get(method);
                    LOG.log(System.Logger.Level.INFO, agentName + "." + method.getName() + ": no space for the implied"
                            + " take in group " + groupName + " (spaces " + spaces.keySet()
                            + "); advertised as local, not served here");
                    CardAction local = localAction(method, binding);
                    actions.add(local);
                    consumes.addAll(local.consumes());
                    produces.addAll(local.produces());
                    take = null;
                }
            }
            if (take != null && owns(take.group())) {
                Optional<ai.badmonkey.agentspaces.agent.spi.TakeInvoker> invoker = invokerFor(agent, method);
                if (invoker.isPresent()) {
                    // Review §7.8: the invoker owns every parameter after the entry.
                    if (method.getParameterCount() < 1) {
                        throw new IllegalArgumentException("a @SpaceTake method takes the entry parameter: " + method);
                    }
                    invoker.get().validate(agent, method);
                    LOG.log(System.Logger.Level.INFO, agentName + "." + method.getName() + ": served through "
                            + invoker.get().getClass().getSimpleName());
                } else {
                    requireSignature(method, 1, "a @SpaceTake method takes the entry parameter");
                }
                Class<?> entryType = method.getParameterTypes()[0];
                String schema = schemas.register(entryType);
                consumes.add(schema);
                String takeSpace = resolveSpaceName(take.space(), method);
                spaceBindings.put(schema, takeSpace);
                String resultSpace = take.resultSpace().isEmpty() ? takeSpace : resolveSpaceName(take.resultSpace(), method);
                for (Class<?> produced : producedTypes(method, take.produces())) {
                    String producedSchema = schemas.register(produced);
                    produces.add(producedSchema);
                    if (!resultSpace.equals(takeSpace)) {
                        producedBindings.putIfAbsent(producedSchema, resultSpace);
                    }
                }
                actions.add(action(method, take.produces(), take.description(), List.of(schema),
                        takeSpace, implied ? CardAction.EMBABEL_ACTION : CardAction.TAKE,
                        CardAction.Contract.of(List.of(take.pre()), List.of(take.post()), take.cost())));
                int loops = Math.max(1, take.concurrency());
                for (int i = 0; i < loops; i++) {
                    handle.threads.add(startTakeLoop(agent, method, take, handle));
                }
            }
            SpaceNotify notify = annotationOf(method, SpaceNotify.class);
            if (notify != null && owns(notify.group())) {
                requireSignature(method, 1, "a @SpaceNotify method takes the entry parameter");
                Class<?> entryType = method.getParameterTypes()[0];
                String schema = schemas.register(entryType);
                consumes.add(schema);
                String notifySpace = resolveSpaceName(notify.space(), method);
                spaceBindings.putIfAbsent(schema, notifySpace);
                String notifyResult = notify.resultSpace().isEmpty() ? notifySpace
                        : resolveSpaceName(notify.resultSpace(), method);
                for (Class<?> produced : producedTypes(method, notify.produces())) {
                    String producedSchema = schemas.register(produced);
                    produces.add(producedSchema);
                    if (!notifyResult.equals(notifySpace)) {
                        producedBindings.putIfAbsent(producedSchema, notifyResult);
                    }
                }
                actions.add(action(method, notify.produces(), notify.description(), List.of(schema),
                        notifySpace, CardAction.NOTIFY));
                handle.subscriptions.add(subscribe(agent, method, notify, entryType, handle));
            }
            Ballot ballot = annotationOf(method, Ballot.class);
            if (ballot != null && owns(ballot.group())) {
                requireSignature(method, 1, "a @Ballot method takes the VoteCapability.Proposal parameter");
                if (method.getParameterTypes()[0] != VoteCapability.Proposal.class
                        || method.getReturnType() != String.class) {
                    throw new IllegalArgumentException("a @Ballot method takes a"
                            + " VoteCapability.Proposal and returns the String option (null abstains): "
                            + method);
                }
                consumes.add(schemas.register(VoteCapability.Proposal.class));
                produces.add(schemas.register(VoteCapability.Ballot.class));
                spaceBindings.putIfAbsent(schemas.register(VoteCapability.Proposal.class),
                        resolveVoteSpaceName(ballot.space(), method));
                actions.add(new CardAction(method.getName(), "",
                        List.of(schemas.register(VoteCapability.Proposal.class)),
                        List.of(schemas.register(VoteCapability.Ballot.class)),
                        resolveVoteSpaceName(ballot.space(), method), CardAction.BALLOT));
                handle.subscriptions.add(castBallots(agent, method, ballot, handle));
            }
            OnDecision onDecision = annotationOf(method, OnDecision.class);
            if (onDecision != null && owns(onDecision.group())) {
                requireSignature(method, 1, "an @OnDecision method takes the VoteCapability.Decision parameter");
                if (method.getParameterTypes()[0] != VoteCapability.Decision.class) {
                    throw new IllegalArgumentException(
                            "an @OnDecision method takes a VoteCapability.Decision: " + method);
                }
                for (Class<?> produced : producedTypes(method, new Class<?>[0])) {
                    produces.add(schemas.register(produced));
                }
                actions.add(action(method, new Class<?>[0], "",
                        List.of(schemas.register(VoteCapability.Decision.class)),
                        resolveVoteSpaceName(onDecision.space(), method), CardAction.ON_DECISION));
                handle.subscriptions.add(watchDecisions(agent, method, onDecision, handle));
            }
            OrderedTake orderedTake = annotationOf(method, OrderedTake.class);
            if (orderedTake != null && owns(orderedTake.group())) {
                requireSignature(method, 1, "an @OrderedTake method takes the entry parameter");
                Class<?> entryType = method.getParameterTypes()[0];
                String schema = schemas.register(entryType);
                consumes.add(schema);
                String orderedSpace = resolveCoordinatedSpaceName(orderedTake.space(), method);
                spaceBindings.put(schema, orderedSpace);
                String orderedResult = orderedTake.resultSpace().isEmpty() ? orderedSpace
                        : resolveSpaceName(orderedTake.resultSpace(), method);
                for (Class<?> produced : producedTypes(method, orderedTake.produces())) {
                    String producedSchema = schemas.register(produced);
                    produces.add(producedSchema);
                    if (!orderedResult.equals(orderedSpace)) {
                        producedBindings.putIfAbsent(producedSchema, orderedResult);
                    }
                }
                actions.add(action(method, orderedTake.produces(), orderedTake.description(), List.of(schema),
                        resolveCoordinatedSpaceName(orderedTake.space(), method),
                        CardAction.ORDERED_TAKE,
                        CardAction.Contract.of(List.of(orderedTake.pre()), List.of(orderedTake.post()), orderedTake.cost())));
                handle.threads.add(startOrderedTakeLoop(agent, method, orderedTake, handle));
            }
            SpaceJoin join = annotationOf(method, SpaceJoin.class);
            if (join != null && owns(join.group())) {
                requireSignature(method, 1, "a @SpaceJoin method takes the Joined parameter");
                if (method.getParameterTypes()[0] != Joined.class) {
                    throw new IllegalArgumentException("a @SpaceJoin method takes a Joined: " + method);
                }
                String spaceName = resolveSpaceName(join.space(), method);
                List<String> consumed = new ArrayList<>();
                for (Part part : join.parts()) {
                    String schema = schemas.register(part.value());
                    consumed.add(schema);
                    consumes.add(schema);
                    spaceBindings.putIfAbsent(schema, part.space().isEmpty()
                            ? spaceName : resolveSpaceName(part.space(), method));
                }
                for (Class<?> produced : producedTypes(method, join.produces())) {
                    produces.add(schemas.register(produced));
                }
                actions.add(action(method, join.produces(), join.description(), consumed, spaceName,
                        CardAction.JOIN));
                handle.closeables.add(startJoin(agent, method, join, handle, spaceName));
            }
            SpaceReduce reduce = annotationOf(method, SpaceReduce.class);
            if (reduce != null && owns(reduce.group())) {
                requireSignature(method, 2, "a @SpaceReduce method takes the accumulator and the element");
                Class<?> accumulatorType = method.getParameterTypes()[0];
                Class<?> elementType = method.getParameterTypes()[1];
                if (accumulatorType.isPrimitive() || method.getReturnType() == void.class
                        || !(method.getReturnType() == accumulatorType || method.getReturnType() == Tagged.class
                                || method.getReturnType() == Entries.class)) {
                    throw new IllegalArgumentException("a @SpaceReduce method takes (A accumulator, E element) and"
                            + " returns A, Tagged<A>, or Entries: " + method);
                }
                String spaceName = resolveSpaceName(reduce.space(), method);
                String schema = schemas.register(elementType);
                consumes.add(schema);
                spaceBindings.putIfAbsent(schema, spaceName);
                produces.add(schemas.register(accumulatorType));
                for (Class<?> produced : producedTypes(method, reduce.produces())) {
                    produces.add(schemas.register(produced));
                }
                CardAction base = action(method, reduce.produces(), reduce.description(), List.of(schema),
                        spaceName, CardAction.REDUCE);
                List<String> produced = new ArrayList<>(base.produces());
                if (!produced.contains(schemas.register(accumulatorType))) {
                    produced.add(0, schemas.register(accumulatorType));
                }
                actions.add(new CardAction(base.name(), base.description(), base.consumes(), produced,
                        base.space(), base.kind()));
                handle.closeables.add(startReduce(agent, method, reduce, handle, spaceName, accumulatorType,
                        elementType));
            }
            OnEstimate onEstimate = annotationOf(method, OnEstimate.class);
            if (onEstimate != null && owns(onEstimate.group())) {
                requireSignature(method, 1, "an @OnEstimate method takes the PushSumAggregate.Estimate parameter");
                if (method.getParameterTypes()[0] != PushSumAggregate.Estimate.class) {
                    throw new IllegalArgumentException(
                            "an @OnEstimate method takes a PushSumAggregate.Estimate: " + method);
                }
                settleOf(onEstimate, method); // a start-up refusal for both forms or a bad span (EL-T-06)
                if (aggregate == null) {
                    throw new IllegalArgumentException("@OnEstimate at " + method + " but no aggregate is"
                            + " registered; register it with binder.aggregate(aggregate) or"
                            + " group.provide(aggregate)");
                }
                for (Class<?> produced : producedTypes(method, new Class<?>[0])) {
                    produces.add(schemas.register(produced));
                }
                String estimateResultSpace = writesAnEntry(method)
                        ? resolveSpaceName(onEstimate.resultSpace(), method) : null;
                actions.add(action(method, new Class<?>[0], onEstimate.description(), List.of(),
                        estimateResultSpace, CardAction.ON_ESTIMATE));
                handle.closeables.add(startEstimateWatch(agent, method, onEstimate, handle, estimateResultSpace));
            }
            Propose propose = annotationOf(method, Propose.class);
            if (propose != null && owns(propose.group())) {
                requireSignature(method, 1, "a @Propose method takes the cue parameter");
                Class<?> cueType = method.getParameterTypes()[0];
                String cueSchema = schemas.register(cueType);
                String proposalSchema = schemas.register(VoteCapability.Proposal.class);
                String cueSpaceName = resolveSpaceName(propose.space(), method);
                consumes.add(cueSchema);
                produces.add(proposalSchema);
                spaceBindings.putIfAbsent(cueSchema, cueSpaceName);
                spaceBindings.putIfAbsent(proposalSchema, resolveVoteSpaceName(propose.vote(), method));
                actions.add(new CardAction(method.getName(), propose.description(), List.of(cueSchema),
                        List.of(proposalSchema), cueSpaceName, CardAction.PROPOSE));
                handle.subscriptions.add(startPropose(agent, method, propose, handle, cueSpaceName));
            }
        }
        // ISSUE-Motion FR-7: a declared Motion return produces a Proposal into the
        // vote space, which bind time knows only when there is exactly one.
        if (votes.size() == 1) {
            for (Method method : type.getMethods()) {
                if (method.getReturnType() == Motion.class && hasBindingAnnotation(method)) {
                    spaceBindings.putIfAbsent(schemas.register(VoteCapability.Proposal.class),
                            votes.keySet().iterator().next());
                }
            }
        }

        // Review §7.4: a produced schema's space, after the consumed bindings, which win on a shared schema.
        producedBindings.forEach(spaceBindings::putIfAbsent);
        // Review §7.2: the actions a declaration advertises without binding, unless the binder bound the method.
        declaration.ifPresent(d -> {
            Set<String> boundNames = new java.util.HashSet<>();
            actions.forEach(a -> boundNames.add(a.name()));
            for (ai.badmonkey.agentspaces.agent.spi.ExtraAction extra : d.extraActions()) {
                if (boundNames.contains(extra.method().getName())) {
                    continue;
                }
                List<String> consumed = new ArrayList<>();
                for (Class<?> parameter : extra.method().getParameterTypes()) {
                    if (isEntryParameter(parameter)) {
                        consumed.add(schemas.register(parameter));
                    }
                }
                List<String> produced = new ArrayList<>();
                for (Class<?> producedType : producedTypes(extra.method(), new Class<?>[0])) {
                    produced.add(schemas.register(producedType));
                }
                actions.add(new CardAction(extra.method().getName(), extra.description(), consumed, produced, null,
                        extra.kind(), extra.contract()));
                consumes.addAll(consumed);
                produces.addAll(produced);
            }
        });
        handle.card = buildCard(agentIdentity, spec, consumes, produces, spaceBindings,
                declaration.map(ai.badmonkey.agentspaces.agent.spi.AgentDeclaration::meta).orElse(Map.of()))
                .withActions(declared(actions))
                .withUi(uiDeclaration(type));   // v0.1.26 (ISSUE-UiComponents): what the agent can show, or null
        publishCard(handle);
        bound.add(handle);
        return handle;
    }

    /**
     * Adopts a card built elsewhere (the Embabel binder's, spec §10.4) so it is
     * published now and re-published by {@link #refreshCards()} like every
     * bound agent's card; without this, an externally published card would age
     * out of every ad-cache at its TTL.
     *
     * @param card the card to publish and keep fresh
     * @return a handle; closing it stops refreshing the card
     */
    public Bound adopt(AgentCard card) {
        Objects.requireNonNull(card, "card");
        Bound handle = new Bound(identity.agentIdentity(card.agent().localName()));
        handle.card = card;
        cardIssues.issued(card.id(), card.issued()); // a refresh is later than the adopted card
        publishCard(handle);
        bound.add(handle);
        return handle;
    }

    /** Re-publishes every bound agent's card with a fresh issue time (leased, P2). */
    public void refreshCards() {
        for (Bound handle : bound) {
            refresh(handle, false);
        }
    }

    /** Re-publishes the cards whose form follows the view's AppCard level: those above the spill threshold. */
    private void reissueOversized() {
        for (Bound handle : bound) {
            AgentCard current = handle.card;
            if (current != null && discovery.spiller().oversized(current)) {
                refresh(handle, true);
            }
        }
    }

    /**
     * Re-publishes one card with a fresh issue time; a reissue for the AppCard level is strictly newer than the card it
     * replaces even on a clock that has not moved, so every cache takes it.
     */
    private void refresh(Bound handle, boolean strictlyNewer) {
        AgentCard current = handle.card;
        if (current == null) {
            return;
        }
        // SPEC §4.2 v0.1.13: an extra renewal driver; renewing identities
        // also renew themselves lazily whenever they sign.
        handle.identity.renewIfDue(clock.instant());
        // A refresh is a millisecond past the last card issued under the id, even on a clock that has not moved or
        // within the millisecond of the last issue, so every cache takes it (§10.16); a reissue for the AppCard level
        // (strictlyNewer) needs exactly that.
        java.time.Instant issuedAt = cardIssues.next(current.id(), clock.instant());
        handle.card = new AgentCard(current.id(), identity.peerId(), group,
                issuedAt, current.ttl(), current.agent(), current.description(),
                current.goals(), current.consumes(), current.produces(),
                current.costHints(), current.spaceBindings(), current.agentPublicKey(),
                current.actions(),
                // the identity's current certificate, renewed above when due
                current.agentPublicKey() == null ? null
                        : handle.identity.certificateCovering(issuedAt).orElse(null),
                current.ui());
        publishCard(handle);
    }

    /**
     * Unbinds every binding of one agent (SPEC §5.6, v0.1.13): its loops and
     * subscriptions stop and its card is no longer refreshed, so it lapses by
     * TTL. Called when the agent is revoked; every receiver already refuses
     * what it would sign.
     *
     * @param agent the agent
     * @return how many bindings were closed
     */
    public int unbind(AgentId agent) {
        Objects.requireNonNull(agent, "agent");
        int closed = 0;
        for (Bound handle : bound) {
            if (handle.agentId().equals(agent) && bound.remove(handle)) {
                handle.close();
                closed++;
            }
        }
        return closed;
    }

    @Override
    public void close() {
        bound.forEach(Bound::close);
        bound.clear();
        AutoCloseable changes = gateChanges;
        gateChanges = null;
        if (changes != null) {
            closeQuietly(changes);
        }
        if (gateRegistration != null) {
            try {
                gateRegistration.close();
            } catch (Exception e) {
                throw new IllegalStateException("failed leaving the AppCard gate", e);
            }
        }
    }

    /** A bound agent: closing stops its loops and subscriptions. */
    public final class Bound implements AutoCloseable {
        private final AgentId agentId;
        private final ai.badmonkey.agentspaces.api.spi.AgentIdentity identity;
        private final List<Thread> threads = new ArrayList<>();
        private final List<Subscription> subscriptions = new ArrayList<>();
        /** Bindings that own their own threads and subscriptions (joins). */
        private final List<AutoCloseable> closeables = new ArrayList<>();
        private volatile boolean running = true;
        private volatile AgentCard card;
        /** The class bound, for the same-name check (review §7.2, X20); null for an adopted card. */
        private volatile Class<?> boundClass;
        /** Tags every record this binding writes carries (a manifest's flow and version). */
        final Map<String, String> recordTags = new java.util.concurrent.ConcurrentHashMap<>();

        public Map<String, String> recordTags() {
            return Map.copyOf(recordTags);
        }

        private Bound(ai.badmonkey.agentspaces.api.spi.AgentIdentity identity) {
            this.identity = identity;
            this.agentId = identity.id();
        }

        /** The identity this agent signs with: peer-signed, or its own certified key. */
        public ai.badmonkey.agentspaces.api.spi.AgentIdentity identity() {
            return identity;
        }

        /** Returns the bound agent's id. */
        public AgentId agentId() {
            return agentId;
        }

        /** Returns the agent's current card. */
        public AgentCard card() {
            return card;
        }

        /** Whether the binding still runs: false once closed, or unbound by a revocation. */
        public boolean isRunning() {
            return running;
        }

        @Override
        public void close() {
            running = false;
            threads.forEach(Thread::interrupt);
            subscriptions.forEach(Subscription::close);
            for (AutoCloseable closeable : closeables) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    LOG.log(System.Logger.Level.DEBUG, agentId.localName() + ": close failed", e);
                }
            }
        }
    }

    // ---------------------------------------------------------------- internals

    /**
     * How a take loop answers a failed poll, so a fault can neither spin the
     * loop nor flood the log. A space closed under a still-bound agent ends
     * the loop, because a closed space does not reopen. Any other fault pauses
     * before the next poll (10 ms, doubling to 1 s) and warns on the first
     * fault of a run, then at most once a minute with the count so far. A good
     * poll resets the pause and starts a new run.
     */
    static final class TakeFailures {
        static final long FIRST_PAUSE_MILLIS = 10;
        static final long MAX_PAUSE_MILLIS = 1_000;
        private static final long WARN_EVERY_NANOS = Duration.ofMinutes(1).toNanos();

        private final String label;
        private final boolean quietFirst;
        private final ai.badmonkey.agentspaces.common.time.TimeSource time;
        private long pauseMillis;
        private long run;
        private long lastWarnNanos;

        /**
         * @param label      the loop's agent and method, for the log
         * @param quietFirst log the first fault of a run at DEBUG (an expected
         *                   refusal, such as a leader election in flight)
         */
        TakeFailures(String label, boolean quietFirst) {
            this(label, quietFirst, ai.badmonkey.agentspaces.common.time.TimeSource.system());
        }

        /**
         * @param time the source the pause sleeps on (the binder's, EL-W-01) and the warning throttle reads
         */
        TakeFailures(String label, boolean quietFirst, ai.badmonkey.agentspaces.common.time.TimeSource time) {
            this.label = label;
            this.quietFirst = quietFirst;
            this.time = time;
        }

        /** Records a good poll: the next fault starts a new run with the shortest pause. */
        void succeeded() {
            if (run > 0) {
                LOG.log(System.Logger.Level.DEBUG, label + ": take recovered after " + run + " failed polls");
            }
            run = 0;
            pauseMillis = 0;
        }

        /**
         * Records a failed poll and pauses before the next one.
         *
         * @return false when the loop should end: the space closed, or the
         *         pause was interrupted
         */
        boolean failed(RuntimeException e) {
            if (closedSpace(e)) {
                LOG.log(System.Logger.Level.INFO, label + ": space closed; the take loop ends");
                return false;
            }
            run++;
            long now = time.monoNanos(); // a log throttle (EL-C-08)
            if (run == 1) {
                LOG.log(quietFirst ? System.Logger.Level.DEBUG : System.Logger.Level.WARNING,
                        label + ": take failed; retrying with backoff", e);
                lastWarnNanos = now;
            } else if (now - lastWarnNanos >= WARN_EVERY_NANOS) {
                LOG.log(System.Logger.Level.WARNING,
                        label + ": take still failing (" + run + " failed polls in a row)", e);
                lastWarnNanos = now;
            }
            pauseMillis = pauseMillis == 0 ? FIRST_PAUSE_MILLIS : Math.min(MAX_PAUSE_MILLIS, pauseMillis * 2);
            try {
                time.sleep(Duration.ofMillis(pauseMillis));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        }

        /** Whether the fault, or any cause under it, says the space is closed. */
        static boolean closedSpace(Throwable e) {
            for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
                if (t instanceof ai.badmonkey.agentspaces.api.error.SpaceClosedException) {
                    return true;
                }
            }
            return false;
        }
    }

    /** Runs a worker method with its take bound to this thread (see {@link TakeContext}). */
    private Object invokeWithin(TakeContext context, Object agent, Method method, Object entry) {
        TakeContext.bind(context);
        try {
            return invoke(agent, method, entry);
        } finally {
            TakeContext.unbind();
        }
    }

    /** Runs a taken entry through its invoker (review §7.8), the take context bound on this thread meanwhile. */
    private static Object invokeThrough(ai.badmonkey.agentspaces.agent.spi.TakeInvoker invoker, TakeContext context,
                                        Object agent, Method method, Object entry) {
        TakeContext.bind(context);
        try {
            return invoker.invoke(agent, method, entry, context);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(method.getName() + " failed through " + invoker.getClass().getSimpleName(), e);
        } finally {
            TakeContext.unbind();
        }
    }

    private Thread startTakeLoop(Object agent, Method method, SpaceTake take, Bound handle) {
        Space space = spaceFor(handle.identity, take.space(), method);
        Space resultSpace = take.resultSpace().isEmpty() ? space
                : spaceFor(handle.identity, take.resultSpace(), method);
        Class<?> entryType = method.getParameterTypes()[0];
        Lease takeLease = Lease.of(Durations.parse(take.lease()));
        Duration pollTimeout = Durations.parse(take.pollTimeout());
        Lease resultLease = Lease.of(Durations.parse(take.resultLease()));
        String label = handle.agentId.localName() + "." + method.getName();
        Template<?> template = templateFor(entryType, take.tags(), take.where(), method);

        return Thread.ofVirtual().name("space-take-" + label).start(() -> {
            TakeFailures failures = new TakeFailures(label, false, waits());
            TakeWait wait = new TakeWait(Thread.currentThread());
            ungatedWaits.add(wait);
            ai.badmonkey.agentspaces.common.time.ChangeSignal changed = new ai.badmonkey.agentspaces.common.time.ChangeSignal(waits());
            AutoCloseable waiter = gatedWaiter(space, changed);
            try {
                while (handle.running) {
                    Optional<? extends TakenEntry<?>> taken;
                    try {
                        Optional<Template<?>> admitted = gated(space, template, pollTimeout, changed);
                        if (admitted.isEmpty()) {
                            continue;
                        }
                        if (ungated()) {
                            // idle-loop item 2: wait on the space's changes, not a one-second poll; a gate change interrupts
                            synchronized (wait) {
                                wait.waiting = true;
                                wait.inTake = true;
                            }
                            if (!ungated()) { // a gate loaded between the look and the wait: look again
                                synchronized (wait) {
                                    wait.waiting = false;
                                    wait.inTake = false;
                                    wait.notifyAll();
                                }
                                if (handle.running) {
                                    Thread.interrupted();
                                }
                                continue;
                            }
                            try {
                                taken = space.take(admitted.get(), takeLease,
                                        pollTimeout.compareTo(UNGATED_TAKE_WAIT) > 0 ? pollTimeout : UNGATED_TAKE_WAIT);
                            } finally {
                                synchronized (wait) {
                                    wait.waiting = false;
                                    wait.inTake = false;
                                    wait.notifyAll(); // a gate change waiting for this take to leave
                                    if (handle.running) {
                                        Thread.interrupted(); // a gate change's interrupt, now spent; a close keeps its own
                                    }
                                }
                            }
                        } else {
                            taken = space.take(admitted.get(), takeLease, gatedPoll(pollTimeout));
                        }
                    } catch (RuntimeException e) {
                        if (!handle.running || !failures.failed(e)) {
                            return;
                        }
                        continue;
                    }
                    failures.succeeded();
                    if (taken.isEmpty()) {
                        continue;
                    }
                    try {
                        // §10.33: a renewal from inside the method ends with the binding, as the ordered path's does
                        TakeContext context = new TakeContext(taken.get(), takeLease.duration(), handle.agentId,
                                space.name(), () -> handle.running);
                        Optional<ai.badmonkey.agentspaces.agent.spi.TakeInvoker> invoker = invokerFor(agent, method);
                        Object result = invoker.isPresent()
                                ? invokeThrough(invoker.get(), context, agent, method, taken.get().entry())
                                : invokeWithin(context, agent, method, taken.get().entry());
                        dispatch(result, space, resultSpace, resultLease, taken.get(), handle, method, label);
                    } catch (RuntimeException | Error e) {
                        // The crash idiom: no complete, the TAKE lease lapses, and the
                        // entry reappears for another worker (spec §7.2). Errors are
                        // caught too, so an AssertionError or StackOverflowError in
                        // the action cannot silently kill the worker loop.
                        LOG.log(System.Logger.Level.WARNING,
                                label + ": action failed; lease will lapse", e);
                    }
                }
            } finally {
                ungatedWaits.remove(wait);
                closeQuietly(waiter);
            }
        });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Subscription subscribe(Object agent, Method method, SpaceNotify notify,
                                   Class<?> entryType, Bound handle) {
        Space space = spaceFor(handle.identity, notify.space(), method);
        Space resultSpace = notify.resultSpace().isEmpty() ? space
                : spaceFor(handle.identity, notify.resultSpace(), method);
        Lease lease = Lease.of(Durations.parse(notify.lease()));
        Lease resultLease = Lease.of(Durations.parse(notify.resultLease()));
        // Delivery is at-least-once; the binder carries the choreography
        // discipline so the method body never has to: dedupe per entry id (watch),
        // then run the reaction on its own virtual thread so slow work never
        // holds the fabric's delivery thread, and write a non-null result back
        // as the next entry in the flow.
        String label = agent.getClass().getSimpleName() + "." + method.getName();
        Template<?> template = templateFor(entryType, notify.tags(), notify.where(), method);
        if (notify.on().length == 0) {
            throw new IllegalArgumentException("@SpaceNotify at " + method + " reacts to no event kind");
        }
        return watch(space, template, lease, handle, "space-notify", label, Set.of(notify.on()), spaceEvent ->
                Thread.ofVirtual().name("space-notify-" + label).start(() -> {
                    try {
                        Object result = invoke(agent, method, spaceEvent.entry());
                        dispatch(result, null, resultSpace, resultLease, null, handle, method, label);
                    } catch (RuntimeException e) {
                        LOG.log(System.Logger.Level.WARNING, label + ": notify failed", e);
                    }
                }));
    }

    /**
     * The leased, deduplicated subscription every reacting binding shares
     * ({@code @SpaceNotify}, {@code @Propose}): WRITTEN events only, each entry
     * id delivered once per binding against a bounded set, and the subscription
     * renewed at half-lease on a virtual thread while the agent stays bound, so
     * a long-lived reactor never goes deaf when its lease would otherwise lapse
     * (spec §7.2). The reaction runs on the delivery thread; callers hand off to
     * their own virtual thread.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Subscription watch(Space space, Template<?> template, Lease lease, Bound handle,
                               String threadPrefix, String label,
                               java.util.function.Consumer<SpaceEvent<?>> onWritten) {
        return watch(space, template, lease, handle, threadPrefix, label,
                Set.of(SpaceEvent.Kind.WRITTEN), onWritten);
    }

    private Subscription watch(Space space, Template<?> template, Lease lease, Bound handle,
                               String threadPrefix, String label, Set<SpaceEvent.Kind> kinds,
                               java.util.function.Consumer<SpaceEvent<?>> onEvent) {
        Set<Object> seen = java.util.Collections.newSetFromMap(
                new java.util.LinkedHashMap<>() {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Object, Boolean> eldest) {
                        return size() > NOTIFY_DEDUP_CAPACITY;
                    }
                });
        Subscription subscription = space.notify((Template) template, event -> {
            SpaceEvent<?> spaceEvent = (SpaceEvent<?>) event;
            if (!kinds.contains(spaceEvent.kind())) {
                return;
            }
            // Once per entry per kind: a WRITTEN and a later EXPIRED of one entry are
            // two deliveries; a redelivered WRITTEN is not.
            Object key = kinds.size() == 1 ? spaceEvent.entryId()
                    : spaceEvent.entryId().value() + ":" + spaceEvent.kind();
            synchronized (seen) {
                if (!seen.add(key)) {
                    return; // redelivery of an event this agent already handled
                }
            }
            onEvent.accept(spaceEvent);
        }, lease);
        renewWhileBound(subscription, lease.duration(), handle, threadPrefix + "-renew-" + label);
        return subscription;
    }

    /**
     * The {@code @Propose} runtime (ISSUE-Propose §9.3): the cue watched as a
     * notify is, the proposal id derived from the cue's key, the method invoked
     * once per proposal id per bound agent, the vote opened once per replica,
     * as the bound agent.
     */
    private Subscription startPropose(Object agent, Method method, Propose propose, Bound handle,
                                      String cueSpaceName) {
        Class<?> returnType = method.getReturnType();
        if (returnType != String.class && returnType != Motion.class && returnType != void.class) {
            throw new IllegalArgumentException("a @Propose method returns the question as a String,"
                    + " a Motion, or void: " + method);
        }
        if (propose.options().length < 2) {
            throw new IllegalArgumentException("@Propose needs at least two options at " + method);
        }
        if (propose.quorum() <= 0) {
            throw new IllegalArgumentException("@Propose quorum must be positive at " + method
                    + ": " + propose.quorum());
        }
        boolean byTag = !propose.keyTag().isEmpty();
        if (byTag && propose.key().length > 0) {
            throw new IllegalArgumentException("@Propose at " + method
                    + " names both key and keyTag; choose one");
        }
        boolean keyed = byTag || propose.key().length > 0;
        if (!keyed && returnType != Motion.class) {
            throw new IllegalArgumentException("@Propose at " + method + " names no key or keyTag;"
                    + " the proposal id needs one, or the method returns a Motion carrying its own");
        }
        Class<?> cueType = method.getParameterTypes()[0];
        List<Method> accessors = byTag ? List.of() : Keys.accessors(cueType, propose.key());
        String voteSpaceName = resolveVoteSpaceName(propose.vote(), method);
        VoteCapability vote = voteFor(handle.identity, voteSpaceName);
        Space cueSpace = spaceFor(handle.identity, cueSpaceName, method);
        Template<?> template = templateFor(cueType, propose.tags(), propose.where(), method);
        Lease proposalLease = Lease.of(Durations.parse(propose.lease()));
        Lease subscriptionLease = Lease.of(Durations.parse(propose.subscriptionLease()));
        List<String> options = List.of(propose.options());
        String label = handle.agentId.localName() + "." + method.getName();
        Set<String> opened = java.util.Collections.newSetFromMap(
                new java.util.LinkedHashMap<>() {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                        return size() > NOTIFY_DEDUP_CAPACITY;
                    }
                });
        return watch(cueSpace, template, subscriptionLease, handle, "space-propose", label, event -> {
            String key = !keyed ? null
                    : byTag ? event.tags().get(propose.keyTag()) : Keys.keyOf(event.entry(), accessors);
            if (keyed && key == null) {
                LOG.log(System.Logger.Level.DEBUG, label + ": the cue carries no key; no proposal");
                return;
            }
            String proposalId = keyed ? propose.prefix() + key : null;
            if (proposalId != null) {
                synchronized (opened) {
                    if (!opened.add(proposalId)) {
                        return; // another cue of the same proposal: the lead has already asked
                    }
                }
            }
            Thread.ofVirtual().name("space-propose-" + label).start(() -> {
                try {
                    if (proposalId != null && vote.proposal(proposalId).isPresent()) {
                        LOG.log(System.Logger.Level.DEBUG, label + ": proposal '" + proposalId
                                + "' already open; nothing proposed");
                        return;
                    }
                    Object result = invoke(agent, method, event.entry());
                    if (result == null) {
                        if (proposalId != null) {
                            synchronized (opened) {
                                opened.remove(proposalId); // asked nothing: the next cue may ask
                            }
                        }
                        return;
                    }
                    if (result instanceof Motion motion) {
                        move(motion, handle, method, label, voteSpaceName);
                        return;
                    }
                    vote.propose(proposalId, (String) result, options, propose.quorum(), proposalLease);
                } catch (RuntimeException e) {
                    if (proposalId != null) {
                        synchronized (opened) {
                            opened.remove(proposalId); // a refused open may be retried by the next cue
                        }
                    }
                    LOG.log(System.Logger.Level.WARNING, label + ": propose failed", e);
                }
            });
        });
    }

    /** Whether a method carries any binding annotation whose return goes through {@link #dispatch}. */
    private static boolean hasBindingAnnotation(Method method) {
        return annotationOf(method, SpaceTake.class) != null || annotationOf(method, SpaceNotify.class) != null
                || annotationOf(method, OrderedTake.class) != null || annotationOf(method, OnDecision.class) != null
                || annotationOf(method, SpaceJoin.class) != null || annotationOf(method, Propose.class) != null
                || annotationOf(method, OnEstimate.class) != null || annotationOf(method, SpaceReduce.class) != null;
    }

    /** Whether a method's declared return is something the binder writes as an entry. */
    private static boolean writesAnEntry(Method method) {
        Class<?> returned = method.getReturnType();
        return returned != void.class && returned != Contribution.class && returned != Motion.class;
    }

    /**
     * The {@code @OnEstimate} runtime (ISSUE-OnEstimate §9.4): the aggregate
     * evaluates the settle rule on its own tick and calls back once per epoch
     * on a virtual thread; the binder dispatches the return as for any binding.
     */
    /** The default settle span of {@code @OnEstimate}: eight rounds, 8 s. */
    static final java.time.Duration DEFAULT_SETTLE = java.time.Duration.ofSeconds(8);

    /**
     * An {@code @OnEstimate}'s settle rule (srad/agentspaces-event-loop.md EL-T-06): the {@code settle} duration, or
     * the deprecated {@code settleTicks} read as rounds of {@code ROUND_PERIOD} (owner answer 1, 2026-11-22), or the
     * 8 s default. Both given is refused, naming the method.
     */
    @SuppressWarnings("deprecation")
    static PushSumAggregate.Settle settleOf(OnEstimate on, Method method) {
        boolean span = !on.settle().isBlank();
        if (span && on.settleTicks() >= 0) {
            throw new IllegalArgumentException("@OnEstimate at " + method + " gives both settle and settleTicks;"
                    + " give settle alone");
        }
        if (span) {
            java.time.Duration parsed;
            try {
                parsed = Durations.parse(on.settle());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("@OnEstimate at " + method + ": settle '" + on.settle()
                        + "' is not a duration", e);
            }
            return PushSumAggregate.Settle.after(parsed, on.tolerance());
        }
        if (on.settleTicks() >= 0) {
            return PushSumAggregate.Settle.after(on.settleTicks(), on.tolerance());
        }
        return PushSumAggregate.Settle.after(DEFAULT_SETTLE, on.tolerance());
    }

    private AutoCloseable startEstimateWatch(Object agent, Method method, OnEstimate on, Bound handle,
                                             String resultSpaceName) {
        Space resultSpace = resultSpaceName == null ? null
                : spaceFor(handle.identity, resultSpaceName, method);
        Lease resultLease = Lease.of(Durations.parse(on.resultLease()));
        String label = handle.agentId.localName() + "." + method.getName();
        return aggregate.onEstimate(id -> id.startsWith(on.prefix()),
                settleOf(on, method), estimate -> {
                    try {
                        Object result = invoke(agent, method, estimate);
                        dispatch(result, null, resultSpace, resultLease, null, handle, method, label);
                    } catch (RuntimeException e) {
                        LOG.log(System.Logger.Level.WARNING, label + ": estimate reaction failed", e);
                    }
                });
    }

    /**
     * One declared action for a bound method (SPEC §6.1, v0.1.13): what it
     * consumes, what its return type produces, where it is bound.
     */
    private CardAction action(Method method, Class<?>[] declaredProduces, String description,
                              List<String> consumed, String space, String kind) {
        return action(method, declaredProduces, description, consumed, space, kind, null);
    }

    /** The same, with the contract the annotation declares (ISSUE-CardContract); null when it declares none. */
    private CardAction action(Method method, Class<?>[] declaredProduces, String description,
                              List<String> consumed, String space, String kind, CardAction.Contract contract) {
        requireCapabilityForDeclaredReturn(method);
        List<String> produced = new ArrayList<>();
        for (Class<?> type : producedTypes(method, declaredProduces)) {
            produced.add(schemas.register(type));
        }
        return new CardAction(method.getName(), description, consumed, produced, space, kind, contract);
    }

    /**
     * The card's action list: sorted by name so the card's bytes do not depend
     * on reflection order, with overloads disambiguated by their consumed
     * schema; null when the agent binds no method (so its card is unchanged).
     */
    private static List<CardAction> declared(List<CardAction> actions) {
        if (actions.isEmpty()) {
            return null;
        }
        Map<String, Long> counts = new java.util.HashMap<>();
        actions.forEach(a -> counts.merge(a.name(), 1L, Long::sum));
        List<CardAction> named = new ArrayList<>();
        for (CardAction a : actions) {
            String name = counts.get(a.name()) > 1
                    ? a.name() + "(" + String.join(",", a.consumes()) + ")" : a.name();
            named.add(new CardAction(name, a.description(), a.consumes(), a.produces(),
                    a.space(), a.kind(), a.contract()));   // the contract rides along (ISSUE-CardContract)
        }
        named.sort(java.util.Comparator.comparing(CardAction::name));
        if (named.size() > AgentCard.MAX_ACTIONS) {
            throw new IllegalArgumentException("an agent binds at most " + AgentCard.MAX_ACTIONS
                    + " actions; this one binds " + named.size());
        }
        return named;
    }

    /**
     * The card's {@code ui} group from every {@code @UiComponent} on the type and its public methods
     * (SPEC §6.1a.3, v0.1.26; ISSUE-UiComponents FR-5): one component per annotation, the entries it reads and writes
     * from the annotations' entry types, the catalogs derived; null when the type declares none. The card refuses a
     * control that names no action of the card.
     */
    static ai.badmonkey.agentspaces.api.ad.UiDeclaration uiDeclaration(Class<?> type) {
        List<ai.badmonkey.agentspaces.agent.annotation.UiComponent> found = new ArrayList<>();
        // the declaration may sit on the class, a superclass, or an interface the class implements (a Clojure
        // defagent's generated interface, a Groovy trait): the same places the agent's spec is read from
        for (Class<?> t = type; t != null && t != Object.class; t = t.getSuperclass()) {
            found.addAll(List.of(t.getAnnotationsByType(ai.badmonkey.agentspaces.agent.annotation.UiComponent.class)));
            for (Class<?> i : t.getInterfaces()) {
                found.addAll(List.of(i.getAnnotationsByType(ai.badmonkey.agentspaces.agent.annotation.UiComponent.class)));
            }
        }
        for (Method method : type.getMethods()) {
            found.addAll(List.of(method.getAnnotationsByType(ai.badmonkey.agentspaces.agent.annotation.UiComponent.class)));
        }
        if (found.isEmpty()) {
            return null;
        }
        List<ai.badmonkey.agentspaces.api.ad.UiDeclaration.Component> components = new ArrayList<>();
        Set<String> reads = new LinkedHashSet<>();
        Set<String> writes = new LinkedHashSet<>();
        for (ai.badmonkey.agentspaces.agent.annotation.UiComponent u : found) {
            String entry = !u.entryIri().isBlank() ? u.entryIri() : (u.entry() == Void.class ? "" : u.entry().getName());
            String id = u.id().isBlank() ? u.component() : u.id();
            components.add(new ai.badmonkey.agentspaces.api.ad.UiDeclaration.Component(id, u.catalog(), u.component(), entry,
                    List.of(u.controls()), u.role()));
            if (!entry.isEmpty()) {
                if (u.reads()) {
                    reads.add(entry);
                }
                if (u.writes()) {
                    writes.add(entry);
                }
            }
        }
        return ai.badmonkey.agentspaces.api.ad.UiDeclaration.of(components,
                new ai.badmonkey.agentspaces.api.ad.UiDeclaration.Entries(List.copyOf(reads), List.copyOf(writes)));
    }

    /**
     * Builds the card. The space bindings (spec §6.1, v0.1.10) map each
     * consumed schema name to the space this agent takes or watches it in, so
     * a remote caller can route an input entry without configuring routes.
     */
    private AgentCard buildCard(ai.badmonkey.agentspaces.api.spi.AgentIdentity agentIdentity, Spec spec,
                                Set<String> consumes, Set<String> produces,
                                Map<String, String> spaceBindings) {
        return buildCard(agentIdentity, spec, consumes, produces, spaceBindings, Map.of());
    }

    /**
     * The issue millisecond of every card this binder signed, so a card issued again in the same millisecond (a bind
     * and a refresh, two reissues for the AppCard level) is a millisecond later and reaches every cache
     * (srad/agentspaces-event-loop.md §10.16, the same-millisecond audit).
     */
    private final ai.badmonkey.agentspaces.common.time.IssueClock cardIssues =
            new ai.badmonkey.agentspaces.common.time.IssueClock();

    private AgentCard buildCard(ai.badmonkey.agentspaces.api.spi.AgentIdentity agentIdentity, Spec spec,
                                Set<String> consumes, Set<String> produces,
                                Map<String, String> spaceBindings, Map<String, String> costHints) {
        AgentId agentId = agentIdentity.id();
        String cardId = "aspace://" + group.value() + "/agent/" + identity.peerId().value()
                + "/" + agentId.localName();
        java.time.Instant issuedAt = cardIssues.next(cardId, clock.instant());
        AgentCard card = new AgentCard(
                cardId,
                identity.peerId(), group, issuedAt, CARD_TTL,
                agentId,
                spec == null ? "" : spec.description(),
                spec == null ? List.of() : spec.goals(),
                List.copyOf(consumes), List.copyOf(produces), Map.copyOf(costHints),
                Map.copyOf(spaceBindings),
                // The card says which key the agent signs with only when that key is
                // the agent's own; a peer-signed agent's card is byte-identical to before.
                agentIdentity.isSubordinate() ? agentIdentity.publicKey() : null);
        // v0.1.13: and carries the certificate that proves it, for this issue time.
        return card.withAgentCertificate(agentIdentity.isSubordinate()
                ? agentIdentity.certificateCovering(issuedAt).orElse(null) : null);
    }

    /**
     * Signs and publishes a bound agent's card, or its stub when the card passes the spill threshold and the view reads
     * stubs (SPEC §6.1a.5, v0.1.32); the handle keeps the full card, so a refresh never needs the stub.
     */
    private void publishCard(Bound handle) {
        AgentCard card = handle.card;
        if (discovery != null && card != null) {
            discovery.spiller().prepare(card, discovery.appCardGateOpen())
                    .thenAccept(prepared -> discovery.publish(signer.sign(prepared, identity)));
        }
    }

    /** The agent identity an {@link AgentSpec} (or composed stereotype) declares. */
    private record Spec(String name, String description, List<String> goals) {
    }

    /**
     * Tests whether a class declares an agent identity: an {@link AgentSpec}
     * on the class itself, or a composed stereotype — any annotation on the
     * class that is itself annotated {@code @AgentSpec} — whose same-named
     * attributes ({@code name}, {@code description}, {@code goals}) supply the
     * values, Spring-style, so one custom annotation can mean "component +
     * agent" at once.
     *
     * @param type the candidate class
     * @return whether the class declares an agent identity
     */
    /** The card name a class binds under by default: its spec's name, else the decapitalised simple name. */
    public static String cardNameOf(Class<?> type) {
        Spec spec = findSpec(type);
        return spec != null && !spec.name().isEmpty() ? spec.name() : decapitalize(type.getSimpleName());
    }

    public static boolean isAgentType(Class<?> type) {
        return findSpec(type) != null;
    }

    /**
     * A {@code @SpaceTake} for an implied binding (review §7.3): the binding's space, lease and poll, and the
     * one tag that addresses entries to this action, so the loop never claims another stage's entries.
     */
    private static SpaceTake impliedTake(ai.badmonkey.agentspaces.agent.spi.ImpliedBinding binding, String agentName) {
        Map<String, Object> values = new java.util.HashMap<>();
        values.put("space", binding.space());
        values.put("lease", binding.lease());
        values.put("pollTimeout", binding.pollTimeout());
        values.put("description", binding.description());
        values.put("tags", new String[]{ai.badmonkey.agentspaces.agent.spi.ImpliedBinding.tagFor(agentName,
                binding.method().getName())});
        values.put("pre", binding.pre().toArray(String[]::new));
        values.put("post", binding.post().toArray(String[]::new));
        values.put("cost", binding.cost());
        return (SpaceTake) java.lang.reflect.Proxy.newProxyInstance(SpaceTake.class.getClassLoader(),
                new Class<?>[]{SpaceTake.class}, (proxy, m, args) -> {
                    switch (m.getName()) {
                        case "annotationType": return SpaceTake.class;
                        case "toString": return "@SpaceTake(implied " + values + ")";
                        case "hashCode": return System.identityHashCode(proxy);
                        case "equals": return proxy == args[0];
                        default:
                            Object value = values.get(m.getName());
                            return value != null ? value : m.getDefaultValue();
                    }
                });
    }

    /** An entry parameter of an advertised action: not primitive, not a string, not a framework's own type. */
    private static boolean isEntryParameter(Class<?> type) {
        String name = type.getName();
        return !type.isPrimitive() && type != String.class && !name.startsWith("java.")
                && !name.startsWith("com.embabel.") && !name.startsWith("ai.badmonkey.agentspaces.agent.");
    }

    /** Whether an implied take's space resolves in this group: the named space, or the sole space. */
    private boolean impliedTakeBindsHere(SpaceTake take) {
        return take.space().isEmpty() ? spaces.size() == 1 : spaces.containsKey(take.space());
    }

    /** The card action for an implied take that cannot bind in this group: advertised, never invocable. */
    private CardAction localAction(Method method, ai.badmonkey.agentspaces.agent.spi.ImpliedBinding binding) {
        List<String> consumed = List.of(schemas.register(method.getParameterTypes()[0]));
        List<String> produced = new ArrayList<>();
        for (Class<?> type : producedTypes(method, new Class<?>[0])) {
            produced.add(schemas.register(type));
        }
        return new CardAction(method.getName(), binding.description(), consumed, produced, null, CardAction.LOCAL,
                CardAction.Contract.of(binding.pre(), binding.post(), binding.cost()));
    }

    private static Spec findSpec(Class<?> type) {
        for (Class<?> candidate : supertypes(type)) {
            Spec spec = declaredSpec(candidate);
            if (spec != null) {
                declarationOf(type).filter(d -> !d.name().isEmpty() && !d.name().equals(spec.name()))
                        .ifPresent(d -> LOG.log(System.Logger.Level.WARNING, type.getName()
                                + " declares both an @AgentSpec name '" + spec.name() + "' and a framework name '"
                                + d.name() + "'; the @AgentSpec name wins"));
                return spec;
            }
        }
        // Review §7.2: a framework's own declaration, read by a registered reader, names the one card.
        return declarationOf(type)
                .map(d -> new Spec(d.name(), d.description(), d.goals()))
                .orElse(null);
    }

    private static Spec declaredSpec(Class<?> type) {
        AgentSpec direct = type.getAnnotation(AgentSpec.class);
        if (direct != null) {
            return new Spec(direct.name(), direct.description(), List.of(direct.goals()));
        }
        for (java.lang.annotation.Annotation composed : type.getAnnotations()) {
            if (!composed.annotationType().isAnnotationPresent(AgentSpec.class)) {
                continue;
            }
            return new Spec(
                    stringAttribute(composed, "name"),
                    stringAttribute(composed, "description"),
                    goalsAttribute(composed));
        }
        return null;
    }

    /**
     * A binding annotation on a method, or on the same-signature method of a
     * supertype (SPEC §10.3): the class itself first, then its superclasses,
     * then its interfaces, so an object that implements an annotated
     * interface binds as if it carried the annotations itself. That is how a
     * {@code java.lang.reflect.Proxy}, a LangChain4j {@code AiServices}
     * proxy, a Clojure {@code reify}, or a Kotlin object joins the fleet:
     * Java inherits no method annotation on its own, and the interface is the
     * natural place to declare what the fleet sees. The first declaration in
     * that order wins.
     *
     * @param method     the method on the bound object's class
     * @param annotation the annotation type
     * @param <A>        the annotation type
     * @return the annotation, or null
     */
    /** The attribute overrides of the bind in progress on this thread (ISSUE-FlowManifest FR-4), or null. */
    private static final ThreadLocal<Map<String, String>> OVERRIDES = new ThreadLocal<>();
    /** The verb annotations whose attributes a manifest may override. */
    private static final List<Class<? extends java.lang.annotation.Annotation>> OVERRIDABLE = List.of(SpaceTake.class,
            SpaceNotify.class, OrderedTake.class, SpaceJoin.class);

    static <A extends java.lang.annotation.Annotation> A annotationOf(Method method, Class<A> annotation) {
        A found = declaredAnnotationOf(method, annotation);
        Map<String, String> overrides = OVERRIDES.get();
        if (found == null || overrides == null || overrides.isEmpty() || !OVERRIDABLE.contains(annotation)) {
            return found;
        }
        return Overrides.apply(found, annotation, method, overrides);
    }

    static <A extends java.lang.annotation.Annotation> A declaredAnnotationOf(Method method, Class<A> annotation) {
        A own = method.getAnnotation(annotation);
        if (own != null) {
            return own;
        }
        for (Class<?> candidate : supertypes(method.getDeclaringClass())) {
            if (candidate == method.getDeclaringClass()) {
                continue;
            }
            try {
                A inherited = candidate.getMethod(method.getName(), method.getParameterTypes())
                        .getAnnotation(annotation);
                if (inherited != null) {
                    return inherited;
                }
            } catch (NoSuchMethodException e) {
                // the supertype declares no such method; keep looking
            }
        }
        return null;
    }

    /** The class, its superclasses (nearest first), then every interface, each once. */
    static List<Class<?>> supertypes(Class<?> type) {
        List<Class<?>> ordered = new ArrayList<>();
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            ordered.add(c);
        }
        java.util.ArrayDeque<Class<?>> queue = new java.util.ArrayDeque<>(ordered);
        while (!queue.isEmpty()) {
            for (Class<?> iface : queue.poll().getInterfaces()) {
                if (!ordered.contains(iface)) {
                    ordered.add(iface);
                    queue.add(iface);
                }
            }
        }
        return ordered;
    }

    private static String stringAttribute(java.lang.annotation.Annotation composed,
                                          String attribute) {
        try {
            Method accessor = composed.annotationType().getMethod(attribute);
            return accessor.getReturnType() == String.class
                    ? (String) accessor.invoke(composed) : "";
        } catch (ReflectiveOperationException e) {
            return "";
        }
    }

    private static List<String> goalsAttribute(java.lang.annotation.Annotation composed) {
        try {
            Method accessor = composed.annotationType().getMethod("goals");
            return accessor.getReturnType() == String[].class
                    ? List.of((String[]) accessor.invoke(composed)) : List.of();
        } catch (ReflectiveOperationException e) {
            return List.of();
        }
    }

    /** Wires every {@link SpaceRef} field, walking up the class hierarchy. */
    private void injectSpaceRefs(Object agent, Class<?> type, ai.badmonkey.agentspaces.api.spi.AgentIdentity agentIdentity) {
        for (Class<?> at = type; at != null && at != Object.class; at = at.getSuperclass()) {
            for (java.lang.reflect.Field field : at.getDeclaredFields()) {
                SpaceRef ref = field.getAnnotation(SpaceRef.class);
                if (ref == null || !owns(ref.group())) {
                    continue;
                }
                if (!field.getType().isAssignableFrom(Space.class)) {
                    throw new IllegalArgumentException(
                            "a @SpaceRef field must have type Space: " + field);
                }
                try {
                    field.setAccessible(true);
                    field.set(agent, spaceFor(agentIdentity, ref.value(), field));
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("cannot inject @SpaceRef " + field, e);
                }
            }
        }
    }

    private Space resolveSpace(String name, Object site) {
        return spaces.get(resolveSpaceName(name, site));
    }

    // ------------------------------------------------------------ Layer 4 bindings

    /** Wires every {@link CapabilityRef} field: a registered client, else the group's lookup. */
    private void injectCapabilityRefs(Object agent, Class<?> type) {
        for (Class<?> at = type; at != null && at != Object.class; at = at.getSuperclass()) {
            for (java.lang.reflect.Field field : at.getDeclaredFields()) {
                CapabilityRef ref = field.getAnnotation(CapabilityRef.class);
                if (ref == null || !owns(ref.group())) {
                    continue;
                }
                Object client = clients.get(field.getType());
                if (client == null && clientResolver != null) {
                    client = clientResolver.apply(field.getType());
                }
                if (client == null) {
                    throw new IllegalArgumentException("no capability client of type "
                            + field.getType().getName() + " for @CapabilityRef " + field
                            + "; register one with binder.client(type, instance), provide the"
                            + " capability on the group (group.provide(...)), or add a"
                            + " CapabilityClientFactory for it");
                }
                try {
                    field.setAccessible(true);
                    field.set(agent, client);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("cannot inject @CapabilityRef " + field, e);
                }
            }
        }
    }

    /** Wires every {@link PdtRef} field this group owns with the handle its resolver builds (PDT 2 phase 7, P2-X-03). */
    private void injectPdtRefs(Object agent, Class<?> type) {
        for (Class<?> at = type; at != null && at != Object.class; at = at.getSuperclass()) {
            for (java.lang.reflect.Field field : at.getDeclaredFields()) {
                PdtRef ref = field.getAnnotation(PdtRef.class);
                if (ref == null || !owns(ref.group())) {
                    continue;
                }
                Object handle = resolveRef(ref.value(), field.getType(), field.getGenericType(),
                        at.getSimpleName() + "." + field.getName());
                try {
                    field.setAccessible(true);
                    field.set(agent, handle);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("cannot inject @PdtRef " + field, e);
                }
            }
        }
    }

    /** The handle of one {@code @PdtRef} site, checked against the site's raw type; failures name the site. */
    private Object resolveRef(String name, Class<?> raw, Type generic, String site) {
        RefResolver resolver = refResolver;
        if (resolver == null) {
            throw new IllegalArgumentException("@PdtRef(\"" + name + "\") at " + site + " needs the group's persistent"
                    + " trees: set agentspaces.pdt.enabled=true, or register a resolver with binder.refs(pdt::refFor)");
        }
        Object handle;
        try {
            handle = resolver.resolve(name, generic);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new IllegalArgumentException("@PdtRef(\"" + name + "\") at " + site + ": " + e.getMessage(), e);
        }
        if (!raw.isInstance(handle)) {
            throw new IllegalArgumentException("@PdtRef(\"" + name + "\") at " + site + " is a " + raw.getName()
                    + ", and the resolver built a " + (handle == null ? "null" : handle.getClass().getName()));
        }
        return handle;
    }

    /** Resolves a vote-space name: empty means the sole registered vote; the space must have one. */
    private String resolveVoteSpaceName(String name, Object site) {
        if (name.isEmpty()) {
            if (votes.size() == 1) {
                return votes.keySet().iterator().next();
            }
            throw new IllegalArgumentException("no vote space named at " + site + " and "
                    + votes.size() + " vote capabilities are registered " + votes.keySet()
                    + "; name one, e.g. space = \"votes\"");
        }
        if (!votes.containsKey(name)) {
            throw new IllegalArgumentException("space '" + name + "' at " + site
                    + " has no vote capability; register it with binder.vote(\"" + name
                    + "\", vote) or group.provide(vote); registered vote spaces: " + votes.keySet());
        }
        return name;
    }

    /** Resolves a coordinated-space name: empty means the sole coordinated space. */
    private String resolveCoordinatedSpaceName(String name, Object site) {
        if (name.isEmpty()) {
            if (coordinators.size() == 1) {
                return coordinators.keySet().iterator().next();
            }
            throw new IllegalArgumentException("no space named at " + site + " and "
                    + coordinators.size() + " ordered coordinators are registered "
                    + coordinators.keySet() + "; name one, e.g. space = \"payments\"");
        }
        if (!coordinators.containsKey(name)) {
            throw new IllegalArgumentException("space '" + name + "' at " + site
                    + " has no ordered-log coordinator; register it with binder.ordered(\"" + name
                    + "\", OrderedTakes.over(...)) or group.ordered(...); coordinated spaces: "
                    + coordinators.keySet());
        }
        return name;
    }

    /**
     * The vote capability an agent casts with: the registered one, or — for an
     * agent with a key of its own — the same capability over the agent's view of
     * the vote space, so the ballot is the agent's attested record (QA4 A4-7).
     */
    private VoteCapability voteFor(ai.badmonkey.agentspaces.api.spi.AgentIdentity agentIdentity,
                                   String spaceName) {
        VoteCapability registered = votes.get(spaceName);
        Space space = spaces.get(spaceName);
        if (agentIdentity.isSubordinate() && space instanceof ReplicatedSpace replicated) {
            return registered.as(replicated.as(agentIdentity), agentIdentity.id());
        }
        return registered;
    }

    /**
     * The {@code @Ballot} binding: one ballot per proposal, held and future (owner decision 23, SPEC §10), cast as the
     * agent, off the delivery thread.
     */
    private Subscription castBallots(Object agent, Method method, Ballot ballot, Bound handle) {
        String spaceName = resolveVoteSpaceName(ballot.space(), method);
        VoteCapability voter = voteFor(handle.identity, spaceName);
        Space voteSpace = spaces.get(spaceName);
        Lease ballotLease = Lease.of(Durations.parse(ballot.lease()));
        String label = handle.agentId.localName() + "." + method.getName();
        Set<String> voted = java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());
        java.util.function.Consumer<VoteCapability.Proposal> consider = proposal -> {
            if (!proposal.proposalId().startsWith(ballot.prefix()) || !voted.add(proposal.proposalId())) {
                return;
            }
            Thread.ofVirtual().name("space-ballot-" + label).start(() -> {
                try {
                    // A binding made again after a restart finds the ballot its agent cast before: judged once.
                    if (voter.ballotHeld(proposal.proposalId())) {
                        return;
                    }
                    Object option = invoke(agent, method, proposal);
                    if (option != null) {
                        voter.castBallot(proposal.proposalId(), (String) option, ballotLease);
                    }
                } catch (RuntimeException e) {
                    voted.remove(proposal.proposalId()); // a refused ballot may be retried by redelivery
                    LOG.log(System.Logger.Level.WARNING, label + ": ballot failed", e);
                }
            });
        };
        Subscription subscription = voteSpace.notify(Template.of(VoteCapability.Proposal.class), event -> {
            if (event.kind() == SpaceEvent.Kind.WRITTEN) {
                consider.accept(event.entry());
            }
        }, Lease.of(Duration.ofHours(1)));
        // A proposal that folded before this binding existed raises no WRITTEN for it, and a pull with this replica's
        // own digest never sends it again: a member whose proposal came by rumor a moment before its agent bound never
        // voted (R2's council). So the binding reads the open proposals it already holds. The watch is in place first,
        // so a proposal folding meanwhile is seen by one path or the other and the voted set lets it through once.
        for (VoteCapability.Proposal held : voteSpace.readAll(Template.of(VoteCapability.Proposal.class),
                VoteCapability.BIND_SWEEP_LIMIT)) {
            consider.accept(held);
        }
        renewWhileBound(subscription, Duration.ofHours(1), handle, "space-ballot-renew-" + label);
        return subscription;
    }

    /** The {@code @OnDecision} binding: once per proposal when the quorum closes here, held and future. */
    private Subscription watchDecisions(Object agent, Method method, OnDecision onDecision, Bound handle) {
        String spaceName = resolveVoteSpaceName(onDecision.space(), method);
        VoteCapability vote = votes.get(spaceName);
        Space resultSpace = onDecision.resultSpace().isEmpty()
                ? spaceFor(handle.identity, spaceName, method)
                : spaceFor(handle.identity, onDecision.resultSpace(), method);
        Lease lease = Lease.of(Durations.parse(onDecision.lease()));
        Lease resultLease = Lease.of(Durations.parse(onDecision.resultLease()));
        String label = handle.agentId.localName() + "." + method.getName();
        Subscription subscription = vote.onDecision(id -> id.startsWith(onDecision.prefix()), decision ->
                Thread.ofVirtual().name("space-decision-" + label).start(() -> {
                    try {
                        Object result = invoke(agent, method, decision);
                        dispatch(result, null, resultSpace, resultLease, null, handle, method, label);
                    } catch (RuntimeException e) {
                        LOG.log(System.Logger.Level.WARNING, label + ": decision reaction failed", e);
                    }
                }), lease);
        renewWhileBound(subscription, lease.duration(), handle, "space-decision-renew-" + label);
        return subscription;
    }

    /** The {@code @OrderedTake} binding: {@code @SpaceTake}'s loop over the space's coordinator. */
    private Thread startOrderedTakeLoop(Object agent, Method method, OrderedTake take, Bound handle) {
        String spaceName = resolveCoordinatedSpaceName(take.space(), method);
        OrderedTakes coordinator = coordinators.get(spaceName);
        Space registered = spaces.get(spaceName);
        if (registered == null) {
            throw new IllegalArgumentException("coordinated space '" + spaceName
                    + "' is not a registered space; register it with binder.space(...)");
        }
        // An attested agent takes and completes as itself (rule A6): the coordinator
        // must claim as that agent, and completions go through the agent's own handle.
        if (handle.identity.isSubordinate()
                && (!coordinator.holder().equals(handle.agentId) || !coordinator.signsAsHolder())) {
            throw new IllegalArgumentException("@OrderedTake on attested agent "
                    + handle.agentId.encoded() + " but the ordered coordinator for '" + spaceName
                    + "' takes as " + coordinator.holder().encoded()
                    + (coordinator.signsAsHolder() ? "" : " under the peer key") + "; build the coordinator"
                    + " with OrderedTakes.over(..., agentIdentity, ...) for this agent");
        }
        Space space = spaceFor(handle.identity, spaceName, method);
        // The coordinator's claims name its holder; only a handle writing as that
        // holder can complete them, so the two must agree — say so at bind time.
        if (space.writer().isPresent() && !space.writer().get().equals(coordinator.holder())) {
            throw new IllegalArgumentException("the ordered coordinator for '" + spaceName
                    + "' takes as " + coordinator.holder().encoded() + " but the registered space"
                    + " writes as " + space.writer().get().encoded() + "; build both with the same"
                    + " identity and agent name, or register the coordinator's own space");
        }
        Space resultSpace = take.resultSpace().isEmpty() ? space
                : spaceFor(handle.identity, take.resultSpace(), method);
        Class<?> entryType = method.getParameterTypes()[0];
        Lease takeLease = Lease.of(Durations.parse(take.lease()));
        Duration pollTimeout = Durations.parse(take.pollTimeout());
        Lease resultLease = Lease.of(Durations.parse(take.resultLease()));
        String label = handle.agentId.localName() + "." + method.getName();
        Template<?> template = templateFor(entryType, take.tags(), take.where(), method);
        return Thread.ofVirtual().name("space-ordered-take-" + label).start(() -> {
            // a leader election in flight refuses a take or two, so the first
            // fault of a run is quiet here: the next poll resubmits
            TakeFailures failures = new TakeFailures(label, true, waits());
            ai.badmonkey.agentspaces.common.time.ChangeSignal changed = new ai.badmonkey.agentspaces.common.time.ChangeSignal(waits());
            AutoCloseable waiter = gatedWaiter(space, changed);
            try {
            while (handle.running) {
                Optional<? extends TakenEntry<?>> taken;
                try {
                    Optional<Template<?>> admitted = gated(space, template, pollTimeout, changed);
                    if (admitted.isEmpty()) {
                        continue;
                    }
                    taken = coordinator.take(admitted.get(), takeLease, gatedPoll(pollTimeout));
                } catch (RuntimeException e) {
                    if (!handle.running || !failures.failed(e)) {
                        return;
                    }
                    continue;
                }
                failures.succeeded();
                if (taken.isEmpty()) {
                    continue;
                }
                try {
                    // srad/agentspaces-event-loop.md EL-E-08, owner answer 12 (2026-11-22): the take lease is renewed
                    // at half the lease while the handler runs, as ConnectorRuntime renews an invocation's claim, so a
                    // handler that outlasts the 30 s lease keeps its entry; a hung handler keeps it while it hangs and
                    // its agent stays bound, never past the binding's end (§10.14)
                    // §10.16, owner decision 2 (2026-11-22): the watchdog names a handler that holds its entry long,
                    // and an optional hold limit (off by default) stops the renewal of a hung one
                    ai.badmonkey.agentspaces.common.time.TimeSource time = heldTime();
                    long heldSince = time.monoNanos();
                    ai.badmonkey.agentspaces.common.time.Watchdog.Registration heldWatch =
                            time.watchdog().registerHeld(new ai.badmonkey.agentspaces.common.time.Watchdog.Held(
                                    handle.agentId.toString(), method.getName(),
                                    String.valueOf(taken.get().entryId()), heldSince));
                    Duration limit = holdLimit;
                    HeldTake held = new HeldTake(taken.get(), takeLease.duration(), label, handle, time, heldSince,
                            limit);
                    AutoCloseable renewer = renewTakeWhileRunning(held);
                    // C05 (§10.37, owner decision 5 of 2026-11-23): a wake from a sleep, or any wall step, re-checks
                    // the held take at once against its wall-time lease; the hold limit stays on the monotonic clock
                    AutoCloseable woke = time.onWallStep(step -> recheckHeld(held));
                    // SPEC v0.1.37 §9.4: a timer wake more than 30 s late with no wall step (a frozen process) is a
                    // wake too, and re-checks the take the same way
                    AutoCloseable late = time.onLateWake(lateness -> recheckHeld(held));
                    // §10.33 (I-05): a renewal from inside the method (an LLM advisor or listener during a model
                    // stream) obeys the same end as the binder's own renewer: the agent unbound, the hold limit, or
                    // a take that lapsed (C05)
                    java.util.function.BooleanSupplier renewable = held::renewingFromInside;
                    Object result;
                    try {
                        result = invokeWithin(new TakeContext(taken.get(),
                                takeLease.duration(), handle.agentId, spaceName, renewable),
                                agent, method, taken.get().entry());
                    } finally {
                        closeQuietly(woke);
                        closeQuietly(late);
                        closeQuietly(renewer);
                        heldWatch.close();
                    }
                    if (held.lapsed()) {
                        // C05: the take lapsed while the handler ran (its claim is no longer this taker's, or a sleep
                        // outlasted its lease); another member may hold the entry now, so this result is late:
                        // nothing completed, nothing written
                        LOG.log(System.Logger.Level.WARNING, label + ": held entry " + taken.get().entryId()
                                + " lapsed while its handler ran (" + held.lapseCause + "); the result is late and"
                                + " is dropped: nothing completed, nothing written");
                        continue;
                    }
                    dispatch(result, space, resultSpace, resultLease, taken.get(), handle, method, label);
                } catch (RuntimeException | Error e) {
                    // The crash idiom, exactly once: no complete, the TAKE lease
                    // lapses, and the log's next committed claim reassigns the entry.
                    LOG.log(System.Logger.Level.WARNING,
                            label + ": action failed; lease will lapse", e);
                }
            }
            } finally {
                closeQuietly(waiter);
            }
        });
    }

    /** Starts (or joins) the aggregate epoch a {@link Contribution} names. */
    private void contribute(Contribution contribution, String label) {
        PushSumAggregate target = aggregate;
        if (target == null) {
            throw new IllegalStateException(label + " returned a Contribution but no aggregate is"
                    + " registered; call binder.aggregate(aggregate) or group.provide(aggregate)");
        }
        target.start(contribution.epochId(), contribution.value());
    }

    /** The hold limit of a running {@code @OrderedTake} handler; null: off (the default). */
    private volatile Duration holdLimit;
    private final java.util.concurrent.atomic.AtomicLong holdLimitStops = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Sets the hold limit of an {@code @OrderedTake} handler (srad/agentspaces-event-loop.md §10.16, owner decision 2
     * of 2026-11-22): once a handler has held its entry this long, the binder stops renewing the take lease, so the
     * entry lapses to another member's clerk at the lease's end, as if the handler had crashed; the handler itself is
     * left running. Off by default, so a long handler keeps its entry while it runs (owner answer 12). Applies to
     * handlers that take an entry after the call.
     *
     * @param limit the limit, or null or zero for none
     * @return this binder
     */
    public AgentBinder holdLimit(Duration limit) {
        if (limit != null && limit.isNegative()) {
            throw new IllegalArgumentException("the hold limit must not be negative: " + limit);
        }
        this.holdLimit = limit == null || limit.isZero() ? null : limit;
        return this;
    }

    /**
     * The hold limit in force, or null when off (the default); hosts that configure it read it back.
     *
     * @return the limit, or null
     */
    public Duration holdLimit() {
        return holdLimit;
    }

    /**
     * How many renewals the hold limit stopped.
     *
     * @return the count
     */
    public long holdLimitStops() {
        return holdLimitStops.get();
    }

    /** The time a held handler is judged on: the binder's own time source, else the system's. */
    private ai.badmonkey.agentspaces.common.time.TimeSource heldTime() {
        return waits();
    }

    /**
     * The source the binder's waits and renewers use (srad/agentspaces-event-loop.md EL-W-01, EL-W-02): its clock when
     * that is a time source of its own, else the system's.
     */
    private ai.badmonkey.agentspaces.common.time.TimeSource waits() {
        return ai.badmonkey.agentspaces.common.time.Waits.base(clock);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.DEBUG, "closing a renewer failed", e);
        }
    }

    /** How many times a running {@code @OrderedTake} handler's take lease was renewed by the binder. */
    private final java.util.concurrent.atomic.AtomicLong takeRenewals = new java.util.concurrent.atomic.AtomicLong();

    /**
     * How many times the binder renewed a running {@code @OrderedTake} handler's take lease (EL-E-08, owner answer
     * 12); for tests and status.
     *
     * @return the count
     */
    public long takeRenewals() {
        return takeRenewals.get();
    }

    /** How many held {@code @OrderedTake} takes were found lapsed (C05); their results were dropped. */
    private final java.util.concurrent.atomic.AtomicLong lateTakes = new java.util.concurrent.atomic.AtomicLong();
    /** How many held takes a wake or wall step renewed at once (C05). */
    private final java.util.concurrent.atomic.AtomicLong wakeRenewals = new java.util.concurrent.atomic.AtomicLong();

    /**
     * How many held {@code @OrderedTake} takes lapsed while their handler ran (SPEC v0.1.37 §7.4 and §9.4, C05): the
     * take's claim was no longer this taker's ({@link TakenEntry#held()}: another claim displaced it, or the ordered
     * log decided another), or the wall time passed the take's own lease end (the wall instant of its take or last
     * renewal plus the lease), most often across a sleep that the monotonic renewal did not count. Each one stopped
     * renewing for good and its result was dropped: nothing completed, nothing written ({@code late_takes}).
     *
     * @return the count
     */
    public long lateTakes() {
        return lateTakes.get();
    }

    /**
     * How many held takes a wake or a wall step renewed at once because their lease still held (C05).
     *
     * @return the count
     */
    public long wakeRenewals() {
        return wakeRenewals.get();
    }

    /** The binder's wall reading, in ms: what a take's lease end is judged against (the space judges on wall time). */
    private long wallMillis() {
        return clock.millis();
    }

    /**
     * One held {@code @OrderedTake} take while its handler runs: its own lease end on the wall clock, renewed by the
     * binder's renewer, by a renewal from inside the handler, and by a wake's re-check (C05). Once lapsed it stays
     * lapsed: no path renews it again, and its result is late. Guarded by itself.
     */
    private final class HeldTake {
        final TakenEntry<?> taken;
        final Duration lease;
        final String label;
        final Bound handle;
        final ai.badmonkey.agentspaces.common.time.TimeSource time;
        final long heldSince;
        final Duration limit;
        volatile long expiresAtMillis;
        private boolean lapsed;
        /** Why the take lapsed, for the log; null while it has not. */
        volatile String lapseCause;
        /** Set when the renewal ended for good: the agent unbound, the hold limit, or a failed renewal. */
        private boolean stopped;

        HeldTake(TakenEntry<?> taken, Duration lease, String label, Bound handle,
                 ai.badmonkey.agentspaces.common.time.TimeSource time, long heldSince, Duration limit) {
            this.taken = taken;
            this.lease = lease;
            this.label = label;
            this.handle = handle;
            this.time = time;
            this.heldSince = heldSince;
            this.limit = limit;
            this.expiresAtMillis = wallMillis() + lease.toMillis();
        }

        /** Whether the hold limit has passed, on the monotonic clock: a sleep does not use it up. */
        boolean pastHoldLimit() {
            return limit != null && time.monoNanos() - heldSince >= limit.toNanos();
        }

        /**
         * Marks the take lapsed (SPEC v0.1.37 §7.4) once its claim is no longer this taker's or the wall clock has
         * passed its lease end; counts it once. Callers hold this.
         */
        private boolean judgeLapse(long nowMillis) {
            if (lapsed) {
                return true;
            }
            if (nowMillis >= expiresAtMillis) {
                lapse("its lease ended at " + java.time.Instant.ofEpochMilli(expiresAtMillis) + "; a sleep outlasted it");
            } else if (!taken.held()) {
                lapse("its claim is no longer this taker's: another claim displaced it");
            }
            return lapsed;
        }

        private void lapse(String cause) {
            lapsed = true;
            lapseCause = cause;
            lateTakes.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, label + ": held entry " + taken.entryId() + " lapsed while its handler"
                    + " ran (" + cause + "); it is no longer renewed and its result will be dropped (C05)");
        }

        /** Whether the take lapsed; judged now as well, so a result is never dispatched past the lease's end. */
        synchronized boolean lapsed() {
            return judgeLapse(wallMillis());
        }

        /** Renews the take for its lease, if it may still be renewed; returns whether the caller should go on. */
        synchronized boolean renew(boolean fromWake) {
            long now = wallMillis();
            if (judgeLapse(now) || stopped) {
                return false;
            }
            if (!handle.running) {
                stopped = true; // its agent was unbound while the handler ran
                return false;
            }
            if (pastHoldLimit()) {
                stopped = true;
                holdLimitStops.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING, label + ": held entry " + taken.entryId() + " for "
                        + Duration.ofNanos(time.monoNanos() - heldSince).toMillis() + " ms, past the hold limit of "
                        + limit.toMillis() + " ms; the take lease is no longer renewed and lapses to another taker");
                return false;
            }
            try {
                taken.renew(lease);
                expiresAtMillis = now + lease.toMillis();
                takeRenewals.incrementAndGet();
                if (fromWake) {
                    wakeRenewals.incrementAndGet();
                }
                return true;
            } catch (ai.badmonkey.agentspaces.api.error.LeaseExpiredException e) {
                lapse("its renewal was refused: " + e.getMessage()); // the claim went between the check and here
                return false;
            } catch (RuntimeException e) {
                stopped = true;
                LOG.log(System.Logger.Level.WARNING, label + ": renewing the take lease failed", e);
                return false;
            }
        }

        /**
         * The I-05 guard of a renewal from inside the handler ({@link TakeContext#renew()}): the agent bound, the hold
         * limit not passed, the take not lapsed. A true answer is followed by the renewal, so the lease end moves.
         */
        synchronized boolean renewingFromInside() {
            long now = wallMillis();
            if (judgeLapse(now) || !handle.running || pastHoldLimit()) {
                return false;
            }
            expiresAtMillis = now + lease.toMillis();
            return true;
        }
    }

    /**
     * C05 (SPEC v0.1.37 §7.4 and §9.4): a wake from a sleep, any wall step, or a timer wake more than 30 s late with no
     * wall step (a frozen process) re-checks a held take at once. Past its lease end on the wall clock, or with its
     * claim no longer this taker's, it has lapsed: it is never renewed again and its result will be late. Otherwise it
     * is renewed at once, unless the agent was unbound or the hold limit (monotonic) has passed. Without this the next
     * renewal came on the monotonic period, which does not count a sleep, so it could come after the wall-time lease
     * had ended and another member had taken the entry.
     */
    private void recheckHeld(HeldTake held) {
        held.renew(true);
    }

    /**
     * Renews a take at half its lease until interrupted (EL-E-08, owner answer 12): what keeps an {@code @OrderedTake}
     * handler's entry while it runs, so the 30 s default exceeds the 15 s cycle that renews it. The renewal also ends
     * with the binding (closed, or unbound by a revocation): the unbind interrupts the handler's thread, but a handler
     * that ignores the interrupt would otherwise keep its entry for as long as it hung, and no other member could ever
     * take it (srad/agentspaces-event-loop.md §10.14, the phase 2 wire adversary). It ends, too, at the hold limit
     * (owner decision 2, 2026-11-22; off by default), and once the take has lapsed (C05).
     */
    private AutoCloseable renewTakeWhileRunning(HeldTake held) {
        Duration every = held.lease.dividedBy(2).isZero() ? held.lease : held.lease.dividedBy(2);
        // A task on the binder's time source (EL-W-02): renewal and expiry judged on one source; closed when the
        // handler returns.
        return ai.badmonkey.agentspaces.common.time.Periodic.every(held.time, every,
                "space-ordered-renew-" + held.label, () -> held.renew(false));
    }

    /**
     * Renews a leased subscription at half-lease for as long as the agent stays bound: a task on the binder's time
     * source (srad/agentspaces-event-loop.md EL-W-02), so the renewal's cadence and the lease it renews are judged on
     * one source and a virtual source releases it on its advance; the binding's close cancels it.
     */
    private void renewWhileBound(Subscription subscription, Duration lease, Bound handle, String name) {
        Duration half = lease.dividedBy(2);
        handle.closeables.add(ai.badmonkey.agentspaces.common.time.Periodic.every(waits(), half, name, () -> {
            if (!handle.running) {
                return false;
            }
            try {
                subscription.renew(lease);
                return true;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, name + ": renewal failed; reactions stop", e);
                return false;
            }
        }));
    }

    /**
     * The space handle an agent acts through (QA4 A4-7 phase 3): the registered
     * handle itself for a peer-signed agent, exactly as before; for an agent
     * with a key of its own, a per-agent view of the replica, so its writes,
     * takes, and completions are issued, signed, and attributed as that agent.
     */
    private Space spaceFor(ai.badmonkey.agentspaces.api.spi.AgentIdentity agentIdentity, String name, Object site) {
        Space space = resolveSpace(name, site);
        if (agentIdentity.isSubordinate() && space instanceof ReplicatedSpace replicated) {
            return replicated.as(agentIdentity);
        }
        return space;
    }

    /** Resolves an annotation's space name: empty means the sole registered space. */
    private String resolveSpaceName(String name, Object site) {
        if (name.isEmpty()) {
            if (spaces.size() == 1) {
                return spaces.keySet().iterator().next();
            }
            throw new IllegalArgumentException("no space named at " + site + " and "
                    + spaces.size() + " spaces are registered " + spaces.keySet()
                    + "; name one, e.g. space = \"tasks\"");
        }
        if (!spaces.containsKey(name)) {
            throw new IllegalArgumentException("no registered space named '" + name
                    + "' at " + site + "; registered: " + spaces.keySet());
        }
        return name;
    }

    /** One binding annotation's group and the space names it references. */
    /** One binding: its group, the spaces it names, and whether it takes {@code @PdtRef} handles (P2-X-03). */
    private record Declared(String group, List<String> spaceNames, boolean refs, boolean implied) {
        Declared(String group, List<String> spaceNames) {
            this(group, spaceNames, false, false);
        }

        Declared(String group, List<String> spaceNames, boolean refs) {
            this(group, spaceNames, refs, false);
        }
    }

    /** Every binding annotation on a class, with its group and referenced spaces. */
    private static List<Declared> declaredBindings(Class<?> type) {
        List<Declared> declared = new ArrayList<>();
        for (Method method : type.getMethods()) {
            int first = declared.size();
            // An empty resultSpace means "the take space", never an inferred
            // name, so only a named result space is a requirement of its own.
            SpaceTake take = annotationOf(method, SpaceTake.class);
            if (take != null) {
                declared.add(new Declared(take.group(), take.resultSpace().isEmpty()
                        ? List.of(take.space()) : List.of(take.space(), take.resultSpace())));
            }
            SpaceNotify notify = annotationOf(method, SpaceNotify.class);
            if (notify != null) {
                declared.add(new Declared(notify.group(), notify.resultSpace().isEmpty()
                        ? List.of(notify.space())
                        : List.of(notify.space(), notify.resultSpace())));
            }
            BidFunction bid = annotationOf(method, BidFunction.class);
            if (bid != null) {
                declared.add(new Declared(bid.group(), List.of(bid.space())));
            }
            Ballot ballot = annotationOf(method, Ballot.class);
            if (ballot != null) {
                declared.add(new Declared(ballot.group(), List.of(ballot.space())));
            }
            OnDecision decision = annotationOf(method, OnDecision.class);
            if (decision != null) {
                declared.add(new Declared(decision.group(), decision.resultSpace().isEmpty()
                        ? List.of(decision.space()) : List.of(decision.space(), decision.resultSpace())));
            }
            OrderedTake ordered = annotationOf(method, OrderedTake.class);
            if (ordered != null) {
                declared.add(new Declared(ordered.group(), ordered.resultSpace().isEmpty()
                        ? List.of(ordered.space()) : List.of(ordered.space(), ordered.resultSpace())));
            }
            SpaceJoin join = annotationOf(method, SpaceJoin.class);
            if (join != null) {
                List<String> names = new ArrayList<>();
                names.add(join.space());
                if (!join.resultSpace().isEmpty()) {
                    names.add(join.resultSpace());
                }
                for (Part part : join.parts()) {
                    if (!part.space().isEmpty()) {
                        names.add(part.space());
                    }
                }
                declared.add(new Declared(join.group(), names));
            }
            Propose propose = annotationOf(method, Propose.class);
            if (propose != null) {
                declared.add(new Declared(propose.group(), propose.vote().isEmpty()
                        ? List.of(propose.space()) : List.of(propose.space(), propose.vote())));
            }
            OnEstimate onEstimate = annotationOf(method, OnEstimate.class);
            if (onEstimate != null) {
                declared.add(new Declared(onEstimate.group(),
                        writesAnEntry(method) ? List.of(onEstimate.resultSpace()) : List.of()));
            }
            SpaceReduce reduce = annotationOf(method, SpaceReduce.class);
            if (reduce != null) {
                // The accumulator lives in the reduce's own space; a class whose only
                // binding is a reduce has bindings too (found by the Micronaut enroller).
                declared.add(new Declared(reduce.group(), List.of(reduce.space())));
            }
            // PDT 2 phase 7, P2-X-03: a verb whose trailing parameters take @PdtRef handles needs the group's trees
            if (method.getParameterCount() > 0 && pdtRefOf(method, method.getParameterCount() - 1) != null) {
                for (int i = first; i < declared.size(); i++) {
                    Declared verb = declared.get(i);
                    declared.set(i, new Declared(verb.group(), verb.spaceNames(), true));
                }
            }
        }
        for (Class<?> at = type; at != null && at != Object.class; at = at.getSuperclass()) {
            for (java.lang.reflect.Field field : at.getDeclaredFields()) {
                SpaceRef ref = field.getAnnotation(SpaceRef.class);
                if (ref != null) {
                    declared.add(new Declared(ref.group(), List.of(ref.value())));
                }
                PdtRef pdtRef = field.getAnnotation(PdtRef.class);
                if (pdtRef != null) {
                    declared.add(new Declared(pdtRef.group(), List.of(), true));
                }
            }
        }
        // Review §7.3: the bindings a declaration reader implies for methods with no space annotation.
        declarationOf(type).ifPresent(d -> d.implied().forEach(implied ->
                declared.add(new Declared("", List.of(implied.space()), false, true))));
        return declared;
    }

    // ------------------------------------------------ tags, Tagged returns, joins (issue #16)

    /**
     * The method's template: its parameter type plus the tag filters the
     * annotation declares, each {@code "key=value"} or {@code "key"}, judged by
     * the space before the payload is decoded (SPEC §7.2).
     */
    private static Template<?> templateFor(Class<?> type, String[] tags, String[] where, Object site) {
        Template<?> template = Template.of(type);
        for (String filter : tags) {
            int at = filter.indexOf('=');
            String key = at < 0 ? filter : filter.substring(0, at);
            if (key.isBlank()) {
                throw new IllegalArgumentException("malformed tag filter '" + filter + "' at " + site
                        + "; use \"key=value\" or \"key\"");
            }
            template = at < 0 ? template.hasTag(key)
                    : template.whereTag(key, Matchers.eq(filter.substring(at + 1)));
        }
        // Field filters (ISSUE-WorkflowVerbs; the portable grammar of SPEC §7.2, JS-V-03):
        // the field's value against the literal, numerically when both are numbers, so
        // numbers and enums read as written and the three clients judge alike.
        for (String filter : where) {
            ai.badmonkey.agentspaces.api.space.FieldFilter parsed;
            try {
                parsed = ai.badmonkey.agentspaces.api.space.FieldFilter.parse(filter);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(e.getMessage() + " at " + site, e);
            }
            template = template.where(parsed.field(), parsed.matcher());
        }
        return template;
    }

    /**
     * What a method's return produces, for the card: the {@code Proposal} a
     * {@link Motion} becomes; the entry type inside a {@link Tagged}; the
     * permitted subclasses of a sealed return; the {@code produces} attribute's
     * types for an {@link Entries} fork or an {@code Object} return (nothing when
     * it is not given); else the return type itself.
     */
    private static List<Class<?>> producedTypes(Method method, Class<?>[] declaredProduces) {
        Class<?> returned = method.getReturnType();
        if (returned == void.class || returned == Contribution.class) {
            return List.of();
        }
        if (returned == Motion.class) {
            return List.of(VoteCapability.Proposal.class);  // a motion becomes a Proposal entry
        }
        if (returned == Entries.class || returned == Object.class) {
            return List.of(declaredProduces);
        }
        if (returned == Tagged.class) {
            Type generic = method.getGenericReturnType();
            if (generic instanceof ParameterizedType parameterized
                    && parameterized.getActualTypeArguments()[0] instanceof Class<?> wrapped) {
                return List.of(wrapped);
            }
            throw new IllegalArgumentException("a method returning Tagged must declare the entry type,"
                    + " e.g. Tagged<Finding>: " + method);
        }
        if (returned.isSealed()) {
            return List.of(returned.getPermittedSubclasses());
        }
        return List.of(returned);
    }

    /** The one produced type a card-level binding needs, for the bindings that produce exactly one. */
    private static Class<?> producedType(Method method) {
        List<Class<?>> produced = producedTypes(method, new Class<?>[0]);
        return produced.size() == 1 ? produced.get(0) : method.getReturnType();
    }

    /**
     * The return handling every binding shares (ISSUE-Motion §10.3, issue #16).
     * Null, and a void method, complete a take and write nothing; a
     * {@link Contribution} goes to the aggregate; a {@link Tagged} entry is
     * unwrapped and written with its tags; any other value is written as the
     * next entry, atomically with the completion when the result space is the
     * take space.
     *
     * @param space the take space, or null when nothing was taken
     * @param taken the take to complete, or null for a reaction
     */
    private void dispatch(Object result, Space space, Space resultSpace, Lease resultLease,
                          TakenEntry<?> taken, Bound handle, Method site, String label) {
        if (result == null) {
            if (taken != null) {
                space.complete(taken);
            }
            return;
        }
        if (result instanceof Entries entries) {
            // The fork (ISSUE-WorkflowVerbs): the take is completed once, then each
            // element is dispatched as if returned alone. Not atomic across the
            // elements; forked entries carry the input's id for the stage downstream.
            if (taken != null) {
                space.complete(taken);
            }
            for (Object element : entries.elements()) {
                if (element instanceof Entries) {
                    throw new IllegalArgumentException(label + " returned a fork inside a fork");
                }
                dispatch(element, null, resultSpace, resultLease, null, handle, site, label);
            }
            return;
        }
        if (result instanceof Contribution contribution) {
            if (taken != null) {
                space.complete(taken);
            }
            contribute(contribution, label);
            return;
        }
        if (result instanceof Motion motion) {
            // ISSUE-Motion FR-4: the take is finished before the vote is opened, so
            // a failed open cannot make a completed task reappear.
            if (taken != null) {
                space.complete(taken);
            }
            move(motion, handle, site, label, null);
            return;
        }
        Object entry = result;
        Map<String, String> tags = Map.of();
        if (result instanceof Tagged<?> tagged) {
            entry = tagged.entry();
            tags = tagged.tags();
            if (entry instanceof Contribution || entry instanceof Motion || entry instanceof Entries) {
                throw new IllegalArgumentException(label + " returned a Tagged "
                        + entry.getClass().getSimpleName() + "; it is not an entry and carries no tags");
            }
        }
        if (handle != null && !handle.recordTags.isEmpty()) {
            Map<String, String> merged = new LinkedHashMap<>(handle.recordTags);
            merged.putAll(tags);
            tags = merged;
        }
        gateResult(entry, tags);
        if (taken != null && resultSpace == space) {
            if (tags.isEmpty()) {
                space.complete(taken, entry, resultLease);
            } else {
                space.complete(taken, entry, resultLease, tags);
            }
            return;
        }
        if (taken != null) {
            space.complete(taken);
        }
        if (tags.isEmpty()) {
            resultSpace.write(entry, resultLease);
        } else {
            resultSpace.write(entry, resultLease, tags);
        }
    }

    /**
     * Opens the vote a {@link Motion} names, as the bound agent, once per
     * proposal id per replica (ISSUE-Motion §9.2).
     *
     * @param defaultVoteSpace the vote space to use when the motion names none,
     *                         or null to infer the sole registered one
     */
    private void move(Motion motion, Bound handle, Method site, String label, String defaultVoteSpace) {
        String spaceName = !motion.space().isEmpty() ? resolveVoteSpaceName(motion.space(), site)
                : defaultVoteSpace != null ? defaultVoteSpace : resolveVoteSpaceName("", site);
        VoteCapability vote = voteFor(handle.identity, spaceName);
        if (vote.proposal(motion.proposalId()).isPresent()) {
            LOG.log(System.Logger.Level.DEBUG, label + ": proposal '" + motion.proposalId()
                    + "' already open; motion skipped");
            return;
        }
        vote.propose(motion.proposalId(), motion.question(), motion.options(), motion.quorum(),
                motion.lease());
    }

    /**
     * A declared {@link Motion} or {@link Contribution} return buys the fail-fast
     * (ISSUE-Motion FR-5, FR-6): the capability it needs must be registered when
     * the method is bound, not when its first value comes back.
     */
    private void requireCapabilityForDeclaredReturn(Method method) {
        if (method.getReturnType() == Motion.class && votes.isEmpty()) {
            throw new IllegalArgumentException(method + " declares a Motion return but no vote"
                    + " capability is registered; register it with binder.vote(\"votes\", vote) or"
                    + " group.provide(vote)");
        }
        if (method.getReturnType() == Contribution.class && aggregate == null) {
            throw new IllegalArgumentException(method + " declares a Contribution return but no"
                    + " aggregate is registered; register it with binder.aggregate(aggregate) or"
                    + " group.provide(aggregate)");
        }
    }

    private JoinBinding startJoin(Object agent, Method method, SpaceJoin join, Bound handle,
                                  String spaceName) {
        boolean gathers = !join.settle().isEmpty() || java.util.Arrays.stream(join.parts())
                .anyMatch(p -> p.atLeast() > 1 || !p.countedBy().isEmpty());
        if (join.parts().length < 2 && !gathers) {
            throw new IllegalArgumentException("a @SpaceJoin needs at least two parts, or one part that"
                    + " gathers (atLeast, countedBy, or settle): " + method);
        }
        Space space = spaceFor(handle.identity, spaceName, method);
        Space resultSpace = join.resultSpace().isEmpty() ? space
                : spaceFor(handle.identity, join.resultSpace(), method);
        List<JoinBinding.PartSpec> parts = new ArrayList<>();
        for (Part part : join.parts()) {
            if (!part.key().isEmpty() && !part.keyTag().isEmpty()) {
                throw new IllegalArgumentException("part " + part.value().getSimpleName() + " of "
                        + method + " names both key and keyTag; choose one");
            }
            boolean byField = part.keyTag().isEmpty();
            String keyField = byField ? (part.key().isEmpty() ? join.key() : part.key()) : null;
            if (byField) {
                JoinBinding.PartSpec.accessor(part.value(), keyField); // fails fast, naming the field
            }
            Space partSpace = part.space().isEmpty() ? space
                    : spaceFor(handle.identity, part.space(), method);
            String countedByType = null;
            String countedByField = null;
            if (!part.countedBy().isEmpty()) {
                int dot = part.countedBy().lastIndexOf('.');
                if (dot <= 0 || dot == part.countedBy().length() - 1) {
                    throw new IllegalArgumentException("part " + part.value().getSimpleName() + " of "
                            + method + " has countedBy '" + part.countedBy()
                            + "'; use \"SimpleTypeName.field\"");
                }
                countedByType = part.countedBy().substring(0, dot);
                countedByField = part.countedBy().substring(dot + 1);
            }
            parts.add(new JoinBinding.PartSpec(part.value(),
                    templateFor(part.value(), part.tags(), part.where(), method), partSpace, keyField,
                    byField ? null : part.keyTag(), part.optional(), part.atLeast(), countedByType,
                    countedByField));
        }
        OrderedTakes coordinator = null;
        if (join.mode() == SpaceJoin.Mode.ORDERED) {
            coordinator = coordinators.get(spaceName);
            if (coordinator == null) {
                throw new IllegalArgumentException("@SpaceJoin at " + method + " is ORDERED but space '"
                        + spaceName + "' has no ordered-log coordinator; register it with"
                        + " binder.ordered(\"" + spaceName + "\", OrderedTakes.over(...)) or"
                        + " group.ordered(...); coordinated spaces: " + coordinators.keySet());
            }
            requireCoordinatorAgrees(coordinator, space, spaceName, handle, "@SpaceJoin");
        }
        String name = join.name().isEmpty()
                ? handle.agentId.localName() + "." + method.getName() : join.name();
        String label = handle.agentId.localName() + "." + method.getName();
        Lease resultLease = Lease.of(Durations.parse(join.resultLease()));
        JoinBinding.Config config = new JoinBinding.Config(name, join.mode(), space, parts,
                Durations.parse(join.within()), join.maxOpen(),
                Lease.of(Durations.parse(join.lease())), Lease.of(Durations.parse(join.ticketLease())),
                Lease.of(Durations.parse(join.takeLease())), Durations.parse(join.pollTimeout()),
                coordinator, join.settle().isEmpty() ? Duration.ZERO : Durations.parse(join.settle()));
        JoinBinding binding = new JoinBinding(config, joined -> invoke(agent, method, joined),
                result -> dispatch(result, null, resultSpace, resultLease, null, handle, method, label), clock);
        binding.start();
        return binding;
    }

    /** Resolves a {@code @SpaceReduce} and starts its binding (ISSUE-SpaceReduce §9.3). */
    private ReduceBinding startReduce(Object agent, Method method, SpaceReduce reduce, Bound handle,
                                      String spaceName, Class<?> accumulatorType, Class<?> elementType) {
        if (!reduce.key().isEmpty() && !reduce.keyTag().isEmpty()) {
            throw new IllegalArgumentException("@SpaceReduce at " + method + " names both key and keyTag; choose one");
        }
        if (!reduce.key().isEmpty()) {
            try {
                Keys.accessor(elementType, reduce.key());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(e.getMessage() + " as the reduce key at " + method, e);
            }
        }
        Space space = spaceFor(handle.identity, spaceName, method);
        String name = reduce.name().isEmpty()
                ? handle.agentId.localName() + "." + method.getName() : reduce.name();
        for (AutoCloseable other : handle.closeables) {
            if (other instanceof JoinBinding join && join.name().equals(name)) {
                throw new IllegalArgumentException("@SpaceReduce at " + method + " is named '" + name
                        + "' like a @SpaceJoin on this agent; tickets would collide, name one of them");
            }
        }
        OrderedTakes coordinator = null;
        if (reduce.mode() == SpaceReduce.Mode.ORDERED) {
            coordinator = coordinators.get(spaceName);
            if (coordinator == null) {
                throw new IllegalArgumentException("@SpaceReduce at " + method + " is ORDERED but space '"
                        + spaceName + "' has no ordered-log coordinator; register it with"
                        + " binder.ordered(\"" + spaceName + "\", OrderedTakes.over(...)) or"
                        + " group.ordered(...); coordinated spaces: " + coordinators.keySet());
            }
            requireCoordinatorAgrees(coordinator, space, spaceName, handle, "@SpaceReduce");
        }
        String label = handle.agentId.localName() + "." + method.getName();
        Lease accumulatorLease = Lease.of(Durations.parse(reduce.accumulatorLease()));
        ReduceBinding.Config config = new ReduceBinding.Config(name, reduce.mode(), space, elementType,
                templateFor(elementType, reduce.tags(), reduce.where(), method), accumulatorType,
                reduce.key().isEmpty() ? null : reduce.key(), reduce.keyTag().isEmpty() ? null : reduce.keyTag(),
                Lease.of(Durations.parse(reduce.lease())), Durations.parse(reduce.pollTimeout()), accumulatorLease,
                Lease.of(Durations.parse(reduce.subscriptionLease())), reduce.maxOpen(), coordinator);
        ReduceBinding binding = new ReduceBinding(config,
                (accumulator, element) -> invoke(agent, method, accumulator, element),
                other -> dispatch(other, null, space, accumulatorLease, null, handle, method, label), clock);
        binding.start();
        return binding;
    }

    /**
     * The coordinator's claims name its holder and only a handle writing as that
     * holder can complete them; an attested agent must take as itself (rule A6).
     */
    private void requireCoordinatorAgrees(OrderedTakes coordinator, Space space, String spaceName,
                                          Bound handle, String what) {
        if (handle.identity.isSubordinate()
                && (!coordinator.holder().equals(handle.agentId) || !coordinator.signsAsHolder())) {
            throw new IllegalArgumentException(what + " on attested agent "
                    + handle.agentId.encoded() + " but the ordered coordinator for '" + spaceName
                    + "' takes as " + coordinator.holder().encoded()
                    + (coordinator.signsAsHolder() ? "" : " under the peer key") + "; build the coordinator"
                    + " with OrderedTakes.over(..., agentIdentity, ...) for this agent");
        }
        if (space.writer().isPresent() && !space.writer().get().equals(coordinator.holder())) {
            throw new IllegalArgumentException("the ordered coordinator for '" + spaceName
                    + "' takes as " + coordinator.holder().encoded() + " but the registered space"
                    + " writes as " + space.writer().get().encoded() + "; build both with the same"
                    + " identity and agent name, or register the coordinator's own space");
        }
    }

    /** Calls a bound method with its own arguments, then the {@code @PdtRef} handles its trailing parameters take. */
    private Object invoke(Object agent, Method method, Object... arguments) {
        Object[] refs = refArguments.get(method);
        Object[] all = arguments;
        if (refs != null) {
            all = java.util.Arrays.copyOf(arguments, arguments.length + refs.length);
            System.arraycopy(refs, 0, all, arguments.length, refs.length);
        }
        try {
            return method.invoke(agent, all);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("inaccessible agent method: " + method, e);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(method + " threw", e.getCause());
        }
    }

    /**
     * Checks a verb method's own parameters and resolves the {@code @PdtRef} parameters that follow
     * them, once, for every call (PDT 2 phase 7, P2-X-03).
     */
    private void requireSignature(Method method, int parameters, String message) {
        java.lang.reflect.Parameter[] all = method.getParameters();
        int own = 0;
        while (own < all.length && pdtRefOf(method, own) == null) {
            own++;
        }
        for (int i = own; i < all.length; i++) {
            if (pdtRefOf(method, i) == null) {
                throw new IllegalArgumentException("a @PdtRef parameter follows the method's own parameters,"
                        + " and parameter " + i + " has none: " + method);
            }
        }
        if (own != parameters) {
            throw new IllegalArgumentException(message + ": " + method);
        }
        if (own == all.length) {
            return;
        }
        Object[] handles = new Object[all.length - own];
        for (int i = own; i < all.length; i++) {
            PdtRef ref = pdtRefOf(method, i);
            if (!ref.group().isEmpty()) {
                throw new IllegalArgumentException("a @PdtRef parameter takes its group from the method's binding;"
                        + " remove group = \"" + ref.group() + "\" from parameter " + i + " of " + method);
            }
            handles[i - own] = resolveRef(ref.value(), all[i].getType(), all[i].getParameterizedType(),
                    method.getDeclaringClass().getSimpleName() + "." + method.getName() + " parameter " + i);
        }
        refArguments.put(method, handles);
    }

    /**
     * A parameter's {@link PdtRef}, on the method or, as for the verb annotations, on the same
     * method of a supertype, since Java does not inherit parameter annotations.
     */
    private static PdtRef pdtRefOf(Method method, int index) {
        for (Class<?> candidate : supertypes(method.getDeclaringClass())) {
            try {
                Method declared = candidate == method.getDeclaringClass() ? method
                        : candidate.getMethod(method.getName(), method.getParameterTypes());
                PdtRef ref = declared.getParameters()[index].getAnnotation(PdtRef.class);
                if (ref != null) {
                    return ref;
                }
            } catch (NoSuchMethodException e) {
                // the supertype declares no such method; keep looking
            }
        }
        return null;
    }

    private static String decapitalize(String name) {
        if (name.isEmpty()) {
            return "agent"; // an anonymous class has no simple name; its binding still names a group to check
        }
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }

    /** Attribute overrides as proxied annotations (ISSUE-FlowManifest §9.3). */
    static final class Overrides {

        private Overrides() {
        }

        /** Every override key must name an attribute some verb annotation of the type declares. */
        static void check(Class<?> type, Map<String, String> overrides) {
            Set<String> attributes = new LinkedHashSet<>();
            Set<String> methods = new LinkedHashSet<>();
            for (Method m : type.getMethods()) {
                for (Class<? extends java.lang.annotation.Annotation> a : OVERRIDABLE) {
                    if (declaredAnnotationOf(m, a) != null) {
                        methods.add(m.getName());
                        for (Method attr : a.getDeclaredMethods()) {
                            attributes.add(attr.getName());
                        }
                    }
                }
            }
            for (String key : overrides.keySet()) {
                String attribute = key.contains(".") ? key.substring(key.lastIndexOf('.') + 1) : key;
                String method = key.contains(".") ? key.substring(0, key.lastIndexOf('.')) : null;
                if (attribute.equals("shape") || attribute.equals("group") || attribute.equals("description")) {
                    continue; // asset shapes travel beside the attribute; group and description are not overridable
                }
                if (!attributes.contains(attribute)) {
                    throw new IllegalArgumentException("override '" + key + "' names no attribute a verb annotation of "
                            + type.getSimpleName() + " declares; known: " + attributes);
                }
                if (method != null && !methods.contains(method)) {
                    throw new IllegalArgumentException("override '" + key + "' names no bound method of " + type.getSimpleName()
                            + "; known: " + methods);
                }
            }
        }

        @SuppressWarnings("unchecked")
        static <A extends java.lang.annotation.Annotation> A apply(A found, Class<A> type, Method method, Map<String, String> overrides) {
            Map<String, String> mine = new LinkedHashMap<>();
            for (Method attr : type.getDeclaredMethods()) {
                String specific = overrides.get(method.getName() + "." + attr.getName());
                String general = overrides.get(attr.getName());
                String value = specific != null ? specific : general;
                if (value != null && attr.getName().equals("group") == false && attr.getName().equals("produces") == false) {
                    mine.put(attr.getName(), value);
                }
            }
            if (mine.isEmpty()) {
                return found;
            }
            return (A) java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, m, args) -> {
                String name = m.getName();
                if (name.equals("annotationType")) {
                    return type;
                }
                if (name.equals("toString")) {
                    return found.toString() + " with overrides " + mine;
                }
                if (name.equals("hashCode")) {
                    return found.hashCode();
                }
                if (name.equals("equals")) {
                    return proxy == args[0];
                }
                String value = mine.get(name);
                if (value == null) {
                    return m.invoke(found, args);
                }
                return convert(value, m.getReturnType(), name, type);
            });
        }

        static Object convert(String value, Class<?> returnType, String attribute, Class<?> annotation) {
            if (returnType == String.class) {
                return value;
            }
            if (returnType == String[].class) {
                return value.isBlank() ? new String[0] : java.util.Arrays.stream(value.split(",")).map(String::trim)
                        .filter(s -> !s.isEmpty()).toArray(String[]::new);
            }
            if (returnType == int.class) {
                return Integer.parseInt(value.trim());
            }
            if (returnType == long.class) {
                return Long.parseLong(value.trim());
            }
            if (returnType == boolean.class) {
                return Boolean.parseBoolean(value.trim());
            }
            if (returnType.isEnum()) {
                for (Object constant : returnType.getEnumConstants()) {
                    if (constant.toString().equalsIgnoreCase(value.trim())) {
                        return constant;
                    }
                }
                throw new IllegalArgumentException("override " + attribute + "=" + value + " is not one of "
                        + java.util.Arrays.toString(returnType.getEnumConstants()));
            }
            throw new IllegalArgumentException("attribute " + attribute + " of @" + annotation.getSimpleName()
                    + " cannot be overridden from text");
        }
    }
}
