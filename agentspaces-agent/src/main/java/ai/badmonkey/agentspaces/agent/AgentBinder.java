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
import ai.badmonkey.agentspaces.agent.annotation.OrderedTake;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.capability.Contribution;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.space.Lease;
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

    private static final System.Logger LOG = System.getLogger(AgentBinder.class.getName());
    /** Remembered entry ids per notify subscription, for at-least-once dedup. */
    private static final int NOTIFY_DEDUP_CAPACITY = 4096;

    private final PeerIdentity identity;
    private final GroupId group;
    private final String groupName;
    private final InstantSource clock;
    private final DiscoveryService discovery;
    private final SchemaRegistry schemas;
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final Map<String, Space> spaces = new ConcurrentHashMap<>();
    /** Space name to the agent whose @BidFunction prices it (QA4 A4-8): one per space per peer. */
    private final Map<String, String> bidders = new ConcurrentHashMap<>();
    private final List<Bound> bound = new CopyOnWriteArrayList<>();
    /** Vote capabilities by vote-space name, for {@code @Ballot} and {@code @OnDecision}. */
    private final Map<String, VoteCapability> votes = new ConcurrentHashMap<>();
    /** Ordered-take coordinators by space name, for {@code @OrderedTake}. */
    private final Map<String, OrderedTakes> coordinators = new ConcurrentHashMap<>();
    /** Typed capability clients by type, for {@code @CapabilityRef}. */
    private final Map<Class<?>, Object> clients = new ConcurrentHashMap<>();
    /** A fallback resolver for {@code @CapabilityRef} types (the group's typed-client lookup), or null. */
    private volatile java.util.function.Function<Class<?>, Object> clientResolver;
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
        this.schemas = new SimpleSchemaRegistry();
        this.identities = identities != null ? identities : identity::agentIdentity;
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
     * or a {@link SpaceRef} field.
     *
     * @param type the candidate class
     * @return whether the class has bindings
     */
    public static boolean hasBindings(Class<?> type) {
        return !declaredBindings(type).isEmpty();
    }

    /**
     * Tests whether this binder's group satisfies a class (spec §10.3, §10.4):
     * at least one of its bindings addresses this group, and every binding
     * that does names a space registered here (or, when it names none, this
     * binder has exactly one space to infer). A class with no bindings is
     * trivially satisfied, since it needs no space.
     *
     * @param type the candidate class
     * @return whether the class can be bound here
     */
    public boolean satisfies(Class<?> type) {
        List<Declared> declared = declaredBindings(type);
        if (declared.isEmpty()) {
            return true;
        }
        boolean any = false;
        for (Declared binding : declared) {
            if (!owns(binding.group())) {
                continue;
            }
            any = true;
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
    public Bound bind(Object agent, String name) {
        Objects.requireNonNull(agent, "agent");
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

        injectSpaceRefs(agent, type, agentIdentity);
        injectCapabilityRefs(agent, type);

        Bound handle = new Bound(agentIdentity);
        Set<String> consumes = new LinkedHashSet<>();
        Set<String> produces = new LinkedHashSet<>();
        Map<String, String> spaceBindings = new LinkedHashMap<>();
        List<CardAction> actions = new ArrayList<>();

        // Bid functions first, so AUCTION spaces are priced before any take runs.
        for (Method method : type.getMethods()) {
            BidFunction bid = method.getAnnotation(BidFunction.class);
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
            SpaceTake take = method.getAnnotation(SpaceTake.class);
            if (take != null && owns(take.group())) {
                requireSignature(method, 1, "a @SpaceTake method takes the entry parameter");
                Class<?> entryType = method.getParameterTypes()[0];
                String schema = schemas.register(entryType);
                consumes.add(schema);
                spaceBindings.put(schema, resolveSpaceName(take.space(), method));
                if (method.getReturnType() != void.class) {
                    produces.add(schemas.register(method.getReturnType()));
                }
                actions.add(action(method, take.description(), List.of(schema),
                        resolveSpaceName(take.space(), method), CardAction.TAKE));
                handle.threads.add(startTakeLoop(agent, method, take, handle));
            }
            SpaceNotify notify = method.getAnnotation(SpaceNotify.class);
            if (notify != null && owns(notify.group())) {
                requireSignature(method, 1, "a @SpaceNotify method takes the entry parameter");
                Class<?> entryType = method.getParameterTypes()[0];
                String schema = schemas.register(entryType);
                consumes.add(schema);
                spaceBindings.putIfAbsent(schema, resolveSpaceName(notify.space(), method));
                if (method.getReturnType() != void.class) {
                    produces.add(schemas.register(method.getReturnType()));
                }
                actions.add(action(method, notify.description(), List.of(schema),
                        resolveSpaceName(notify.space(), method), CardAction.NOTIFY));
                handle.subscriptions.add(subscribe(agent, method, notify, entryType, handle));
            }
            Ballot ballot = method.getAnnotation(Ballot.class);
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
            OnDecision onDecision = method.getAnnotation(OnDecision.class);
            if (onDecision != null && owns(onDecision.group())) {
                requireSignature(method, 1, "an @OnDecision method takes the VoteCapability.Decision parameter");
                if (method.getParameterTypes()[0] != VoteCapability.Decision.class) {
                    throw new IllegalArgumentException(
                            "an @OnDecision method takes a VoteCapability.Decision: " + method);
                }
                if (method.getReturnType() != void.class) {
                    produces.add(schemas.register(method.getReturnType()));
                }
                actions.add(action(method, "",
                        List.of(schemas.register(VoteCapability.Decision.class)),
                        resolveVoteSpaceName(onDecision.space(), method), CardAction.ON_DECISION));
                handle.subscriptions.add(watchDecisions(agent, method, onDecision, handle));
            }
            OrderedTake orderedTake = method.getAnnotation(OrderedTake.class);
            if (orderedTake != null && owns(orderedTake.group())) {
                requireSignature(method, 1, "an @OrderedTake method takes the entry parameter");
                Class<?> entryType = method.getParameterTypes()[0];
                String schema = schemas.register(entryType);
                consumes.add(schema);
                spaceBindings.put(schema, resolveCoordinatedSpaceName(orderedTake.space(), method));
                if (method.getReturnType() != void.class) {
                    produces.add(schemas.register(method.getReturnType()));
                }
                actions.add(action(method, orderedTake.description(), List.of(schema),
                        resolveCoordinatedSpaceName(orderedTake.space(), method),
                        CardAction.ORDERED_TAKE));
                handle.threads.add(startOrderedTakeLoop(agent, method, orderedTake, handle));
            }
        }

        handle.card = buildCard(agentIdentity, spec, consumes, produces, spaceBindings)
                .withActions(declared(actions));
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
        publishCard(handle);
        bound.add(handle);
        return handle;
    }

    /** Re-publishes every bound agent's card with a fresh issue time (leased, P2). */
    public void refreshCards() {
        for (Bound handle : bound) {
            AgentCard current = handle.card;
            if (current == null) {
                continue;
            }
            // SPEC §4.2 v0.1.13: an extra renewal driver; renewing identities
            // also renew themselves lazily whenever they sign.
            handle.identity.renewIfDue(clock.instant());
            java.time.Instant issuedAt = clock.instant();
            handle.card = new AgentCard(current.id(), identity.peerId(), group,
                    issuedAt, current.ttl(), current.agent(), current.description(),
                    current.goals(), current.consumes(), current.produces(),
                    current.costHints(), current.spaceBindings(), current.agentPublicKey(),
                    current.actions(),
                    // the identity's current certificate, renewed above when due
                    current.agentPublicKey() == null ? null
                            : handle.identity.certificateCovering(issuedAt).orElse(null));
            publishCard(handle);
        }
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
    }

    /** A bound agent: closing stops its loops and subscriptions. */
    public final class Bound implements AutoCloseable {
        private final AgentId agentId;
        private final ai.badmonkey.agentspaces.api.spi.AgentIdentity identity;
        private final List<Thread> threads = new ArrayList<>();
        private final List<Subscription> subscriptions = new ArrayList<>();
        private volatile boolean running = true;
        private volatile AgentCard card;

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
        }
    }

    // ---------------------------------------------------------------- internals

    /** Runs a worker method with its take bound to this thread (see {@link TakeContext}). */
    private Object invokeWithin(TakeContext context, Object agent, Method method, Object entry) {
        TakeContext.bind(context);
        try {
            return invoke(agent, method, entry);
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
        boolean hasResult = method.getReturnType() != void.class;
        String label = handle.agentId.localName() + "." + method.getName();

        return Thread.ofVirtual().name("space-take-" + label).start(() -> {
            Template<?> template = Template.of(entryType);
            while (handle.running) {
                Optional<? extends TakenEntry<?>> taken;
                try {
                    taken = space.take(template, takeLease, pollTimeout);
                } catch (RuntimeException e) {
                    if (!handle.running) {
                        return;
                    }
                    LOG.log(System.Logger.Level.WARNING, label + ": take failed", e);
                    continue;
                }
                if (taken.isEmpty()) {
                    continue;
                }
                try {
                    Object result = invokeWithin(new TakeContext(taken.get(),
                            takeLease.duration(), handle.agentId, space.name()),
                            agent, method, taken.get().entry());
                    if (result instanceof Contribution contribution) {
                        space.complete(taken.get());
                        contribute(contribution, label);
                    } else if (hasResult && result != null && resultSpace == space) {
                        space.complete(taken.get(), result, resultLease);
                    } else {
                        space.complete(taken.get());
                        if (hasResult && result != null) {
                            resultSpace.write(result, resultLease);
                        }
                    }
                } catch (RuntimeException | Error e) {
                    // The crash idiom: no complete, the TAKE lease lapses, and the
                    // entry reappears for another worker (spec §7.2). Errors are
                    // caught too, so an AssertionError or StackOverflowError in
                    // the action cannot silently kill the worker loop.
                    LOG.log(System.Logger.Level.WARNING,
                            label + ": action failed; lease will lapse", e);
                }
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
        boolean hasResult = method.getReturnType() != void.class;
        // Delivery is at-least-once; the binder carries the choreography
        // discipline so the method body never has to: dedupe per entry id,
        // then run the reaction on its own virtual thread so slow work never
        // holds the fabric's delivery thread, and write a non-null result back
        // as the next entry in the flow.
        Set<Object> seen = java.util.Collections.newSetFromMap(
                new java.util.LinkedHashMap<>() {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Object, Boolean> eldest) {
                        return size() > NOTIFY_DEDUP_CAPACITY;
                    }
                });
        String label = agent.getClass().getSimpleName() + "." + method.getName();
        Subscription subscription = space.notify((Template) Template.of(entryType), event -> {
            SpaceEvent<?> spaceEvent = (SpaceEvent<?>) event;
            if (spaceEvent.kind() != SpaceEvent.Kind.WRITTEN) {
                return;
            }
            synchronized (seen) {
                if (!seen.add(spaceEvent.entryId())) {
                    return; // redelivery of an entry this agent already handled
                }
            }
            Thread.ofVirtual().name("space-notify-" + label).start(() -> {
                try {
                    Object result = invoke(agent, method, spaceEvent.entry());
                    if (result instanceof Contribution contribution) {
                        contribute(contribution, label);
                    } else if (hasResult && result != null) {
                        resultSpace.write(result, resultLease);
                    }
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, label + ": notify failed", e);
                }
            });
        }, lease);
        // The subscription is leased (spec §7.2): renew it at half-lease while the
        // agent stays bound, so a long-lived reactor never goes deaf when its
        // lease would otherwise lapse. Closing the handle interrupts this thread.
        long halfLeaseMillis = Math.max(1L, lease.duration().toMillis() / 2);
        handle.threads.add(Thread.ofVirtual().name("space-notify-renew-" + label).start(() -> {
            while (handle.running) {
                try {
                    Thread.sleep(halfLeaseMillis);
                } catch (InterruptedException e) {
                    return;
                }
                if (!handle.running) {
                    return;
                }
                try {
                    subscription.renew(lease.duration());
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING,
                            label + ": subscription renewal failed; reactions stop", e);
                    return;
                }
            }
        }));
        return subscription;
    }

    /**
     * One declared action for a bound method (SPEC §6.1, v0.1.13): what it
     * consumes, what its return type produces, where it is bound.
     */
    private CardAction action(Method method, String description, List<String> consumed,
                              String space, String kind) {
        List<String> produced = method.getReturnType() == void.class
                ? List.of() : List.of(schemas.register(method.getReturnType()));
        return new CardAction(method.getName(), description, consumed, produced, space, kind);
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
                    a.space(), a.kind()));
        }
        named.sort(java.util.Comparator.comparing(CardAction::name));
        if (named.size() > AgentCard.MAX_ACTIONS) {
            throw new IllegalArgumentException("an agent binds at most " + AgentCard.MAX_ACTIONS
                    + " actions; this one binds " + named.size());
        }
        return named;
    }

    /**
     * Builds the card. The space bindings (spec §6.1, v0.1.10) map each
     * consumed schema name to the space this agent takes or watches it in, so
     * a remote caller can route an input entry without configuring routes.
     */
    private AgentCard buildCard(ai.badmonkey.agentspaces.api.spi.AgentIdentity agentIdentity, Spec spec,
                                Set<String> consumes, Set<String> produces,
                                Map<String, String> spaceBindings) {
        AgentId agentId = agentIdentity.id();
        java.time.Instant issuedAt = clock.instant();
        AgentCard card = new AgentCard(
                "aspace://" + group.value() + "/agent/" + identity.peerId().value()
                        + "/" + agentId.localName(),
                identity.peerId(), group, issuedAt, Duration.ofMinutes(15),
                agentId,
                spec == null ? "" : spec.description(),
                spec == null ? List.of() : spec.goals(),
                List.copyOf(consumes), List.copyOf(produces), Map.of(),
                Map.copyOf(spaceBindings),
                // The card says which key the agent signs with only when that key is
                // the agent's own; a peer-signed agent's card is byte-identical to before.
                agentIdentity.isSubordinate() ? agentIdentity.publicKey() : null);
        // v0.1.13: and carries the certificate that proves it, for this issue time.
        return card.withAgentCertificate(agentIdentity.isSubordinate()
                ? agentIdentity.certificateCovering(issuedAt).orElse(null) : null);
    }

    private void publishCard(Bound handle) {
        if (discovery != null && handle.card != null) {
            discovery.publish(signer.sign(handle.card, identity));
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
    public static boolean isAgentType(Class<?> type) {
        return findSpec(type) != null;
    }

    private static Spec findSpec(Class<?> type) {
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

    /** The {@code @Ballot} binding: one ballot per proposal, cast as the agent, off the delivery thread. */
    private Subscription castBallots(Object agent, Method method, Ballot ballot, Bound handle) {
        String spaceName = resolveVoteSpaceName(ballot.space(), method);
        VoteCapability voter = voteFor(handle.identity, spaceName);
        Space voteSpace = spaces.get(spaceName);
        Lease ballotLease = Lease.of(Durations.parse(ballot.lease()));
        String label = handle.agentId.localName() + "." + method.getName();
        Set<String> voted = java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());
        Subscription subscription = voteSpace.notify(Template.of(VoteCapability.Proposal.class), event -> {
            if (event.kind() != SpaceEvent.Kind.WRITTEN) {
                return;
            }
            VoteCapability.Proposal proposal = event.entry();
            if (!proposal.proposalId().startsWith(ballot.prefix()) || !voted.add(proposal.proposalId())) {
                return;
            }
            Thread.ofVirtual().name("space-ballot-" + label).start(() -> {
                try {
                    Object option = invoke(agent, method, proposal);
                    if (option != null) {
                        voter.castBallot(proposal.proposalId(), (String) option, ballotLease);
                    }
                } catch (RuntimeException e) {
                    voted.remove(proposal.proposalId()); // a refused ballot may be retried by redelivery
                    LOG.log(System.Logger.Level.WARNING, label + ": ballot failed", e);
                }
            });
        }, Lease.of(Duration.ofHours(1)));
        renewWhileBound(subscription, Duration.ofHours(1), handle, "space-ballot-renew-" + label);
        return subscription;
    }

    /** The {@code @OnDecision} binding: once per proposal when the quorum closes here. */
    private Subscription watchDecisions(Object agent, Method method, OnDecision onDecision, Bound handle) {
        String spaceName = resolveVoteSpaceName(onDecision.space(), method);
        VoteCapability vote = votes.get(spaceName);
        Space resultSpace = onDecision.resultSpace().isEmpty()
                ? spaceFor(handle.identity, spaceName, method)
                : spaceFor(handle.identity, onDecision.resultSpace(), method);
        Lease lease = Lease.of(Durations.parse(onDecision.lease()));
        Lease resultLease = Lease.of(Durations.parse(onDecision.resultLease()));
        boolean hasResult = method.getReturnType() != void.class;
        String label = handle.agentId.localName() + "." + method.getName();
        Subscription subscription = vote.onDecision(id -> id.startsWith(onDecision.prefix()), decision ->
                Thread.ofVirtual().name("space-decision-" + label).start(() -> {
                    try {
                        Object result = invoke(agent, method, decision);
                        if (result instanceof Contribution contribution) {
                            contribute(contribution, label);
                        } else if (hasResult && result != null) {
                            resultSpace.write(result, resultLease);
                        }
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
        boolean hasResult = method.getReturnType() != void.class;
        String label = handle.agentId.localName() + "." + method.getName();
        return Thread.ofVirtual().name("space-ordered-take-" + label).start(() -> {
            Template<?> template = Template.of(entryType);
            while (handle.running) {
                Optional<? extends TakenEntry<?>> taken;
                try {
                    taken = coordinator.take(template, takeLease, pollTimeout);
                } catch (RuntimeException e) {
                    if (!handle.running) {
                        return;
                    }
                    continue; // a leader election in flight: the next poll resubmits
                }
                if (taken.isEmpty()) {
                    continue;
                }
                try {
                    Object result = invokeWithin(new TakeContext(taken.get(),
                            takeLease.duration(), handle.agentId, spaceName),
                            agent, method, taken.get().entry());
                    if (result instanceof Contribution contribution) {
                        space.complete(taken.get());
                        contribute(contribution, label);
                    } else if (hasResult && result != null && resultSpace == space) {
                        space.complete(taken.get(), result, resultLease);
                    } else {
                        space.complete(taken.get());
                        if (hasResult && result != null) {
                            resultSpace.write(result, resultLease);
                        }
                    }
                } catch (RuntimeException | Error e) {
                    // The crash idiom, exactly once: no complete, the TAKE lease
                    // lapses, and the log's next committed claim reassigns the entry.
                    LOG.log(System.Logger.Level.WARNING,
                            label + ": action failed; lease will lapse", e);
                }
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

    /** Renews a leased subscription at half-lease for as long as the agent stays bound. */
    private void renewWhileBound(Subscription subscription, Duration lease, Bound handle, String name) {
        long halfLeaseMillis = Math.max(1L, lease.toMillis() / 2);
        handle.threads.add(Thread.ofVirtual().name(name).start(() -> {
            while (handle.running) {
                try {
                    Thread.sleep(halfLeaseMillis);
                } catch (InterruptedException e) {
                    return;
                }
                if (!handle.running) {
                    return;
                }
                try {
                    subscription.renew(lease);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, name + ": renewal failed; reactions stop", e);
                    return;
                }
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
    private record Declared(String group, List<String> spaceNames) {
    }

    /** Every binding annotation on a class, with its group and referenced spaces. */
    private static List<Declared> declaredBindings(Class<?> type) {
        List<Declared> declared = new ArrayList<>();
        for (Method method : type.getMethods()) {
            // An empty resultSpace means "the take space", never an inferred
            // name, so only a named result space is a requirement of its own.
            SpaceTake take = method.getAnnotation(SpaceTake.class);
            if (take != null) {
                declared.add(new Declared(take.group(), take.resultSpace().isEmpty()
                        ? List.of(take.space()) : List.of(take.space(), take.resultSpace())));
            }
            SpaceNotify notify = method.getAnnotation(SpaceNotify.class);
            if (notify != null) {
                declared.add(new Declared(notify.group(), notify.resultSpace().isEmpty()
                        ? List.of(notify.space())
                        : List.of(notify.space(), notify.resultSpace())));
            }
            BidFunction bid = method.getAnnotation(BidFunction.class);
            if (bid != null) {
                declared.add(new Declared(bid.group(), List.of(bid.space())));
            }
            Ballot ballot = method.getAnnotation(Ballot.class);
            if (ballot != null) {
                declared.add(new Declared(ballot.group(), List.of(ballot.space())));
            }
            OnDecision decision = method.getAnnotation(OnDecision.class);
            if (decision != null) {
                declared.add(new Declared(decision.group(), decision.resultSpace().isEmpty()
                        ? List.of(decision.space()) : List.of(decision.space(), decision.resultSpace())));
            }
            OrderedTake ordered = method.getAnnotation(OrderedTake.class);
            if (ordered != null) {
                declared.add(new Declared(ordered.group(), ordered.resultSpace().isEmpty()
                        ? List.of(ordered.space()) : List.of(ordered.space(), ordered.resultSpace())));
            }
        }
        for (Class<?> at = type; at != null && at != Object.class; at = at.getSuperclass()) {
            for (java.lang.reflect.Field field : at.getDeclaredFields()) {
                SpaceRef ref = field.getAnnotation(SpaceRef.class);
                if (ref != null) {
                    declared.add(new Declared(ref.group(), List.of(ref.value())));
                }
            }
        }
        return declared;
    }

    private static Object invoke(Object agent, Method method, Object argument) {
        try {
            return method.invoke(agent, argument);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("inaccessible agent method: " + method, e);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(method + " threw", e.getCause());
        }
    }

    private static void requireSignature(Method method, int parameters, String message) {
        if (method.getParameterCount() != parameters) {
            throw new IllegalArgumentException(message + ": " + method);
        }
    }

    private static String decapitalize(String name) {
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }
}
