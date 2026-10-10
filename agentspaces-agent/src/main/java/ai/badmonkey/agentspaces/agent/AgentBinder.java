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
import ai.badmonkey.agentspaces.agent.annotation.Propose;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.capability.Contribution;
import ai.badmonkey.agentspaces.agent.capability.Motion;
import ai.badmonkey.agentspaces.agent.join.JoinBinding;
import ai.badmonkey.agentspaces.agent.join.Joined;
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
        this.identities = identities != null ? identities : identity::agentIdentity;
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

        injectSpaceRefs(agent, type, agentIdentity);
        injectCapabilityRefs(agent, type);

        Bound handle = new Bound(agentIdentity);
        Set<String> consumes = new LinkedHashSet<>();
        Set<String> produces = new LinkedHashSet<>();
        Map<String, String> spaceBindings = new LinkedHashMap<>();
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
            if (take != null && owns(take.group())) {
                requireSignature(method, 1, "a @SpaceTake method takes the entry parameter");
                Class<?> entryType = method.getParameterTypes()[0];
                String schema = schemas.register(entryType);
                consumes.add(schema);
                spaceBindings.put(schema, resolveSpaceName(take.space(), method));
                for (Class<?> produced : producedTypes(method, take.produces())) {
                    produces.add(schemas.register(produced));
                }
                actions.add(action(method, take.produces(), take.description(), List.of(schema),
                        resolveSpaceName(take.space(), method), CardAction.TAKE));
                handle.threads.add(startTakeLoop(agent, method, take, handle));
            }
            SpaceNotify notify = annotationOf(method, SpaceNotify.class);
            if (notify != null && owns(notify.group())) {
                requireSignature(method, 1, "a @SpaceNotify method takes the entry parameter");
                Class<?> entryType = method.getParameterTypes()[0];
                String schema = schemas.register(entryType);
                consumes.add(schema);
                spaceBindings.putIfAbsent(schema, resolveSpaceName(notify.space(), method));
                for (Class<?> produced : producedTypes(method, notify.produces())) {
                    produces.add(schemas.register(produced));
                }
                actions.add(action(method, notify.produces(), notify.description(), List.of(schema),
                        resolveSpaceName(notify.space(), method), CardAction.NOTIFY));
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
                spaceBindings.put(schema, resolveCoordinatedSpaceName(orderedTake.space(), method));
                for (Class<?> produced : producedTypes(method, orderedTake.produces())) {
                    produces.add(schemas.register(produced));
                }
                actions.add(action(method, orderedTake.produces(), orderedTake.description(), List.of(schema),
                        resolveCoordinatedSpaceName(orderedTake.space(), method),
                        CardAction.ORDERED_TAKE));
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
        /** Bindings that own their own threads and subscriptions (joins). */
        private final List<AutoCloseable> closeables = new ArrayList<>();
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
        String label = handle.agentId.localName() + "." + method.getName();
        Template<?> template = templateFor(entryType, take.tags(), take.where(), method);

        return Thread.ofVirtual().name("space-take-" + label).start(() -> {
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
        long halfLeaseMillis = Math.max(1L, lease.duration().toMillis() / 2);
        handle.threads.add(Thread.ofVirtual().name(threadPrefix + "-renew-" + label).start(() -> {
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
    private AutoCloseable startEstimateWatch(Object agent, Method method, OnEstimate on, Bound handle,
                                             String resultSpaceName) {
        Space resultSpace = resultSpaceName == null ? null
                : spaceFor(handle.identity, resultSpaceName, method);
        Lease resultLease = Lease.of(Durations.parse(on.resultLease()));
        String label = handle.agentId.localName() + "." + method.getName();
        return aggregate.onEstimate(id -> id.startsWith(on.prefix()),
                PushSumAggregate.Settle.after(on.settleTicks(), on.tolerance()), estimate -> {
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
        requireCapabilityForDeclaredReturn(method);
        List<String> produced = new ArrayList<>();
        for (Class<?> type : producedTypes(method, declaredProduces)) {
            produced.add(schemas.register(type));
        }
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
        for (Class<?> candidate : supertypes(type)) {
            Spec spec = declaredSpec(candidate);
            if (spec != null) {
                return spec;
            }
        }
        return null;
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
    static <A extends java.lang.annotation.Annotation> A annotationOf(Method method, Class<A> annotation) {
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
                    dispatch(result, space, resultSpace, resultLease, taken.get(), handle, method, label);
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
        // Field filters (ISSUE-WorkflowVerbs): the field's string form against the
        // literal, the rule a join key uses, so numbers and enums read as written.
        for (String filter : where) {
            int at = filter.indexOf("!=");
            boolean negated = at >= 0;
            if (!negated) {
                at = filter.indexOf('=');
            }
            String field = at < 0 ? "" : filter.substring(0, at);
            if (at < 0 || field.isBlank()) {
                throw new IllegalArgumentException("malformed field filter '" + filter + "' at " + site
                        + "; use \"field=value\" or \"field!=value\"");
            }
            String value = filter.substring(at + (negated ? 2 : 1));
            template = template.where(field, Matchers.predicate(v ->
                    negated != value.equals(String.valueOf(v))));
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

    private static Object invoke(Object agent, Method method, Object... arguments) {
        try {
            return method.invoke(agent, arguments);
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
