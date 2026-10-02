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

import ai.badmonkey.agentspaces.agent.annotation.BidFunction;
import ai.badmonkey.agentspaces.agent.annotation.ProvidesCapability;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.capability.CapabilityClientFactory;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;

import java.lang.reflect.Method;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The application-facing entry point (spec §10.2): a registry of joined groups
 * and their spaces behind one fluent chain, deliberately in Embabel's
 * context-narrowing style, where each step returns a narrower typed context and
 * the terminal operations are the typed {@link Space} verbs:
 *
 * <pre>{@code
 * spaces.group("research-fleet").space("tasks")
 *       .write(new ResearchTask("agentic memory", 3), Lease.of(Duration.ofMinutes(30)));
 * }</pre>
 *
 * <p>Wiring code (the Spring Boot autoconfiguration, an example's main method,
 * a test) registers groups and spaces once at startup; application code only
 * navigates. Each group carries an {@link AgentBinder} sharing the group's
 * spaces, so {@code spaces.group("g").bind(agentBean)} is the whole worker
 * enrollment, and {@link #bind(Object)} on the facade itself routes a bean to
 * every group its annotations name or whose spaces satisfy it (spec §10.3,
 * §10.4), publishing a card per group.
 *
 * <p>Capability services (spec §10.5) hang off the same chain: a group's
 * {@link GroupContext#capabilities() CapabilityRuntime} advertises the
 * providers registered through {@link GroupContext#provide(CapabilityProvider)}
 * or bound as {@link ProvidesCapability} beans, and
 * {@link GroupContext#capability(Class)} resolves a typed client such as
 * {@code VoteClient} through the {@link CapabilityClientFactory} SPI.
 */
public final class AgentSpaces implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(AgentSpaces.class.getName());

    private final PeerIdentity identity;
    private final InstantSource clock;
    private final java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> identities;
    private final Map<String, GroupContext> groups = new ConcurrentHashMap<>();
    private final Map<Class<?>, CapabilityClientFactory<?>> clientFactories =
            new ConcurrentHashMap<>();

    /**
     * Creates the facade. Typed-client factories on the classpath are picked
     * up through {@link ServiceLoader}; {@link #clientFactory} adds more.
     *
     * @param identity the local peer identity agents bind under
     * @param clock    the time source for card freshness
     */
    public AgentSpaces(PeerIdentity identity, InstantSource clock) {
        this(identity, clock, null);
    }

    /**
     * Creates the facade with an agent identity factory (QA4 A4-7 phase 3) that
     * every group's binder uses: {@code identity::agentIdentity} (the default when
     * null) binds agents under the peer key as always; {@code identity::subordinate}
     * gives each bound agent a peer-certified key of its own.
     *
     * @param identity   the peer identity
     * @param clock      the time source
     * @param identities the agent identity factory, or null for the default
     */
    public AgentSpaces(PeerIdentity identity, InstantSource clock,
                       java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> identities) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.identities = identities != null ? identities : identity::agentIdentity;
        loadClientFactories();
    }

    /**
     * Registers a joined group. Wiring code calls this once per group.
     *
     * @param name      the application-facing group name
     * @param groupId   the group id
     * @param runtime   the group runtime; null for single-JVM local wiring
     * @param discovery the group's discovery service; null skips card publication
     * @return the group's context, for space registration
     */
    public GroupContext register(String name, GroupId groupId,
                                 GroupRuntime runtime, DiscoveryService discovery) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(groupId, "groupId");
        GroupContext context = new GroupContext(name, groupId, runtime, discovery);
        GroupContext raced = groups.putIfAbsent(name, context);
        if (raced != null) {
            throw new IllegalStateException("group already registered: '" + name + "'");
        }
        return context;
    }

    /**
     * Attaches a capability runtime to an already registered group (spec §8,
     * §10.5), for wiring code that builds the runtime itself. Without this, a
     * networked group creates its own on first use.
     *
     * @param groupName    the group name
     * @param capabilities the group's capability runtime
     * @return the group context
     */
    public GroupContext register(String groupName, CapabilityRuntime capabilities) {
        return group(groupName).capabilities(capabilities);
    }

    /**
     * Registers a typed-client factory explicitly, alongside those the
     * {@link ServiceLoader} found. A factory for an already known client type
     * replaces the earlier one.
     *
     * @param factory the factory
     * @return this facade
     */
    public AgentSpaces clientFactory(CapabilityClientFactory<?> factory) {
        Objects.requireNonNull(factory, "factory");
        clientFactories.put(Objects.requireNonNull(factory.clientType(), "clientType"), factory);
        return this;
    }

    /** Returns the client types a {@link GroupContext#capability(Class)} can resolve. */
    public Set<Class<?>> clientTypes() {
        return Set.copyOf(clientFactories.keySet());
    }

    /**
     * Narrows to one joined group.
     *
     * @param name the group name
     * @return the group context
     * @throws IllegalArgumentException when no such group was registered
     */
    public GroupContext group(String name) {
        GroupContext context = groups.get(Objects.requireNonNull(name, "name"));
        if (context == null) {
            throw new IllegalArgumentException("no joined group named '" + name
                    + "'; joined groups: " + groups.keySet());
        }
        return context;
    }

    /** Returns the names of the joined groups. */
    public Set<String> groupNames() {
        return Set.copyOf(groups.keySet());
    }

    /**
     * Binds a bean into every group it belongs to (spec §10.3, §10.4):
     *
     * <ul>
     * <li>A {@link ProvidesCapability} bean implementing
     * {@link CapabilityProvider} is registered on the capability runtime of
     * the group its annotation names, or of every networked group when it
     * names none, which starts it and advertises it.</li>
     * <li>An agent bean (an {@code @AgentSpec} type or one with space-bound
     * methods) binds into every registered group whose spaces satisfy it:
     * bindings naming a group belong to that group only, bindings naming none
     * belong to every group that registers their spaces. One card is published
     * per group bound.</li>
     * </ul>
     *
     * @param bean the annotated object
     * @return one bound handle per group the bean was bound into; empty for a
     *         plain bean or a pure capability provider
     * @throws IllegalArgumentException when the bean names a group that is not
     *                                  registered, or no group satisfies it
     */
    public List<AgentBinder.Bound> bind(Object bean) {
        Objects.requireNonNull(bean, "bean");
        Class<?> type = bean.getClass();
        ProvidesCapability provides = type.getAnnotation(ProvidesCapability.class);
        if (provides != null) {
            // QA4 A4-1: the annotation promises a provider. Without the interface
            // there is nothing to register, and silently skipping it boots an
            // application whose capability is never advertised, with nothing
            // logged to say so. Every other annotation fails fast here; so does
            // this one.
            if (!(bean instanceof CapabilityProvider provider)) {
                throw new IllegalArgumentException(type.getName()
                        + " is annotated @ProvidesCapability(\"" + provides.value()
                        + "\") but does not implement " + CapabilityProvider.class.getName()
                        + ", so there is no capability to register or advertise");
            }
            provideInto(provides.group(), provider);
        }
        if (!AgentBinder.isAgentType(type) && !AgentBinder.hasBindings(type)) {
            return List.of();
        }
        for (String named : namedGroups(type)) {
            if (!groups.containsKey(named) && groups.values().stream()
                    .noneMatch(context -> context.id().value().equals(named))) {
                throw new IllegalArgumentException(type.getName() + " binds into group '"
                        + named + "', which is not registered; joined groups: "
                        + groups.keySet());
            }
        }
        List<AgentBinder.Bound> bound = new ArrayList<>();
        for (GroupContext context : groups.values()) {
            if (context.binder().satisfies(type)) {
                bound.add(context.bind(bean));
            }
        }
        if (bound.isEmpty()) {
            throw new IllegalArgumentException("no joined group registers the spaces "
                    + type.getName() + " binds to; joined groups: " + groups.keySet());
        }
        return bound;
    }

    /**
     * Re-publishes every bound agent's card in every group and every
     * registered capability runtime's advertisements (leased, P2). Hosts drive
     * this on a cadence well inside the 15-minute TTL.
     */
    public void refreshCards() {
        for (GroupContext context : groups.values()) {
            context.binder().refreshCards();
            CapabilityRuntime capabilities = context.capabilityRuntime;
            if (capabilities != null) {
                capabilities.refreshTick();
            }
        }
    }

    @Override
    public void close() {
        groups.values().forEach(GroupContext::close);
        groups.clear();
    }

    // ---------------------------------------------------------------- internals

    private void loadClientFactories() {
        try {
            for (CapabilityClientFactory<?> factory : ServiceLoader.load(
                    CapabilityClientFactory.class, AgentSpaces.class.getClassLoader())) {
                clientFactories.putIfAbsent(factory.clientType(), factory);
            }
        } catch (ServiceConfigurationError e) {
            LOG.log(System.Logger.Level.WARNING, "capability client factory failed to load", e);
        }
    }

    private void provideInto(String groupName, CapabilityProvider provider) {
        if (!groupName.isEmpty()) {
            group(groupName).provide(provider);
            return;
        }
        List<GroupContext> networked = groups.values().stream()
                .filter(context -> context.runtime != null && context.discoveryService != null)
                .toList();
        if (networked.isEmpty()) {
            throw new IllegalStateException("no networked group can advertise "
                    + provider.capabilityType() + "; joined groups: " + groups.keySet());
        }
        networked.forEach(context -> context.provide(provider));
    }

    /** The non-empty group names a class's binding annotations reference. */
    private static Set<String> namedGroups(Class<?> type) {
        Set<String> named = new LinkedHashSet<>();
        for (Method method : type.getMethods()) {
            SpaceTake take = method.getAnnotation(SpaceTake.class);
            if (take != null) {
                named.add(take.group());
            }
            SpaceNotify notify = method.getAnnotation(SpaceNotify.class);
            if (notify != null) {
                named.add(notify.group());
            }
            BidFunction bid = method.getAnnotation(BidFunction.class);
            if (bid != null) {
                named.add(bid.group());
            }
        }
        for (Class<?> at = type; at != null && at != Object.class; at = at.getSuperclass()) {
            for (java.lang.reflect.Field field : at.getDeclaredFields()) {
                SpaceRef ref = field.getAnnotation(SpaceRef.class);
                if (ref != null) {
                    named.add(ref.group());
                }
            }
        }
        named.remove("");
        return named;
    }

    /** One joined group: its spaces, discovery, agent binder, and capabilities. */
    public final class GroupContext implements AutoCloseable {

        private final String name;
        private final GroupId groupId;
        private final GroupRuntime runtime;
        private final DiscoveryService discoveryService;
        private final AgentBinder binder;
        private final Map<String, Space> spaces = new ConcurrentHashMap<>();
        private final Map<String, AgentId> writers = new ConcurrentHashMap<>();
        private final Map<String, CapabilityProvider> providers = new ConcurrentHashMap<>();
        private final Map<Class<?>, Object> clients = new ConcurrentHashMap<>();
        private volatile CapabilityRuntime capabilityRuntime;
        private volatile boolean ownsCapabilities;
        private volatile CapabilityPipes pipes;

        private GroupContext(String name, GroupId groupId,
                             GroupRuntime runtime, DiscoveryService discovery) {
            this.name = name;
            this.groupId = groupId;
            this.runtime = runtime;
            this.discoveryService = discovery;
            this.binder = new AgentBinder(identity, groupId, discovery, clock, name, identities);
            // @CapabilityRef fields resolve through this group's typed clients.
            this.binder.clients(type -> clientFactories.containsKey(type) ? capability(type) : null);
            if (runtime != null) {
                // SPEC §6.1 v0.1.13 (C4): a revoked local agent stops acting here
                // too; every receiver already refuses what it would sign.
                runtime.credentialRevocations().addListener(revocation -> {
                    AgentId revoked = revocation.target().agent();
                    if (revoked != null && revoked.peer().equals(identity.peerId())
                            && binder.unbind(revoked) > 0) {
                        LOG.log(System.Logger.Level.WARNING, "agent " + revoked.encoded()
                                + " revoked (" + revocation.reason() + "); unbound in group '"
                                + name + "'");
                    }
                });
            }
        }

        /** Returns the group's application-facing name. */
        public String name() {
            return name;
        }

        /** Returns the group id. */
        public GroupId id() {
            return groupId;
        }

        /** Returns the local peer identity agents in this group bind under. */
        public PeerIdentity identity() {
            return identity;
        }

        /** Returns the time source cards and advertisements are issued on. */
        public InstantSource clock() {
            return clock;
        }

        /** Returns the group runtime, when this group is networked. */
        public GroupRuntime runtime() {
            return runtime;
        }

        /**
         * Returns the group's discovery service.
         *
         * @throws IllegalStateException when the group was wired without one
         */
        public DiscoveryService discovery() {
            if (discoveryService == null) {
                throw new IllegalStateException(
                        "group '" + name + "' was wired without discovery");
            }
            return discoveryService;
        }

        /**
         * Registers a named space in this group. Wiring code calls this once
         * per space; the space also becomes bindable by name in annotations.
         *
         * @param spaceName the space name
         * @param space     the space
         * @return this context
         */
        public GroupContext space(String spaceName, Space space) {
            Objects.requireNonNull(spaceName, "spaceName");
            Objects.requireNonNull(space, "space");
            if (spaces.putIfAbsent(spaceName, space) != null) {
                throw new IllegalStateException("space already registered in '"
                        + name + "': '" + spaceName + "'");
            }
            binder.space(spaceName, space);
            return this;
        }

        /**
         * Registers a named space together with the agent id its local
         * writes are attributed to (the {@code agentName} a
         * {@code ReplicatedSpace} was built with). Capabilities that stamp
         * entries with their writer, such as the vote capability's ballots,
         * need it to build a client over a discovered space binding.
         *
         * @param spaceName the space name
         * @param space     the space
         * @param writer    the agent id the space's local writes carry
         * @return this context
         */
        public GroupContext space(String spaceName, Space space, AgentId writer) {
            space(spaceName, space);
            writers.put(spaceName, Objects.requireNonNull(writer, "writer"));
            return this;
        }

        /**
         * Narrows to one space; the returned {@link Space} carries the terminal
         * typed operations.
         *
         * @param spaceName the space name
         * @return the space
         * @throws IllegalArgumentException when no such space was registered
         */
        public Space space(String spaceName) {
            Space space = spaces.get(Objects.requireNonNull(spaceName, "spaceName"));
            if (space == null) {
                throw new IllegalArgumentException("no space named '" + spaceName
                        + "' in group '" + name + "'; spaces: " + spaces.keySet());
            }
            return space;
        }

        /** Returns the names of this group's registered spaces. */
        public Set<String> spaceNames() {
            return Set.copyOf(spaces.keySet());
        }

        /**
         * Returns the writer agent id a space was registered with, if any.
         *
         * @param spaceName the space name
         * @return the declared writer
         */
        public Optional<AgentId> writer(String spaceName) {
            return Optional.ofNullable(writers.get(Objects.requireNonNull(spaceName)));
        }

        /** Returns the group's agent binder. */
        public AgentBinder binder() {
            return binder;
        }

        /**
         * Binds an annotated agent object into this group's spaces.
         *
         * @param agent the annotated object
         * @return the bound handle
         */
        public AgentBinder.Bound bind(Object agent) {
            return binder.bind(agent);
        }

        /**
         * Binds an agent under an explicit name (several instances of one class as
         * several agents); see {@link AgentBinder#bind(Object, String)}.
         *
         * @param agent the agent instance
         * @param name  the agent's local name
         * @return the binding
         */
        public AgentBinder.Bound bind(Object agent, String name) {
            return binder.bind(agent, name);
        }

        // -------------------------------------------------- capabilities (§10.5)

        /**
         * Attaches a capability runtime built by wiring code. At most one per
         * group; a networked group without one creates its own on first use.
         *
         * @param capabilities the runtime
         * @return this context
         */
        public GroupContext capabilities(CapabilityRuntime capabilities) {
            Objects.requireNonNull(capabilities, "capabilities");
            synchronized (this) {
                if (capabilityRuntime != null) {
                    throw new IllegalStateException(
                            "group '" + name + "' already has a capability runtime");
                }
                capabilityRuntime = capabilities;
                ownsCapabilities = false;
            }
            return this;
        }

        /**
         * Returns the group's capability runtime, creating one over the
         * group's runtime and discovery when none was attached.
         *
         * @return the capability runtime
         * @throws IllegalStateException when the group is not networked
         */
        public CapabilityRuntime capabilities() {
            CapabilityRuntime current = capabilityRuntime;
            if (current != null) {
                return current;
            }
            synchronized (this) {
                if (capabilityRuntime == null) {
                    if (runtime == null || discoveryService == null) {
                        throw new IllegalStateException("group '" + name
                                + "' has no capability runtime and is not networked");
                    }
                    capabilityRuntime = new CapabilityRuntime(runtime, discoveryService, identity);
                    ownsCapabilities = true;
                }
                return capabilityRuntime;
            }
        }

        /**
         * Registers, starts, and advertises a capability provider in this
         * group, and remembers it so typed clients can wrap the local
         * instance (a {@code VoteCapability} registered here backs this
         * group's {@code VoteClient}).
         *
         * @param provider the provider
         * @return this context
         */
        public GroupContext provide(CapabilityProvider provider) {
            Objects.requireNonNull(provider, "provider");
            capabilities().register(provider);
            providers.put(provider.capabilityType(), provider);
            // The binder learns what the annotations need: a vote by its space, the
            // peer's aggregate for Contribution returns.
            if (provider instanceof ai.badmonkey.agentspaces.capabilities.vote.VoteCapability vote) {
                binder.vote(vote.spaceName(), vote);
            }
            if (provider instanceof ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate aggregate) {
                binder.aggregate(aggregate);
            }
            return this;
        }

        /**
         * Registers a vote capability for {@code @Ballot} and {@code @OnDecision}
         * methods over a registered vote space, without advertising it (use
         * {@link #provide} when the capability should be advertised too).
         *
         * @param spaceName the vote space's registered name
         * @param vote      the capability
         * @return this context
         */
        public GroupContext vote(String spaceName,
                                 ai.badmonkey.agentspaces.capabilities.vote.VoteCapability vote) {
            binder.vote(spaceName, vote);
            return this;
        }

        /**
         * Registers the ordered-log coordinator over a registered space, so
         * {@code @OrderedTake} methods naming it can be bound; the coordinator's
         * log is registered as a capability so the peer tick drives it.
         *
         * @param spaceName   the coordinated space's registered name
         * @param coordinator the coordinator ({@code OrderedTakes.over(...)})
         * @return this context
         */
        public GroupContext ordered(String spaceName,
                                    ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes coordinator) {
            Objects.requireNonNull(coordinator, "coordinator");
            binder.ordered(spaceName, coordinator);
            if (runtime != null && discoveryService != null) {
                provide(coordinator.raft());
            }
            return this;
        }

        /**
         * Returns the provider of a capability type registered locally in this
         * group, if any.
         *
         * @param capabilityType the capability type URI
         * @return the local provider
         */
        public Optional<CapabilityProvider> provider(String capabilityType) {
            return providerOf(capabilityType);
        }

        /**
         * Revokes an agent in this group (SPEC §6.1, v0.1.13): every key it holds,
         * honoured when this peer is the founder or the agent's own peer. A local
         * agent is unbound as the revocation lands.
         *
         * @param agent  the agent
         * @param reason one of {@code CredentialRevocation.REASONS}
         * @return the revocation, when this peer had the authority to issue it
         */
        public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revokeAgent(
                AgentId agent, String reason) {
            return networked().revokeAgent(agent, reason);
        }

        /**
         * Revokes one key of an agent in this group (SPEC §6.1, v0.1.13).
         *
         * @param agent  the agent
         * @param key    its raw Ed25519 public key
         * @param reason one of {@code CredentialRevocation.REASONS}
         * @return the revocation, when this peer had the authority to issue it
         */
        public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revokeAgentKey(
                AgentId agent, byte[] key, String reason) {
            return networked().revokeAgentKey(agent, key, reason);
        }

        /**
         * Revokes a join credential in this group (SPEC §6.1, v0.1.13); the
         * founder's call. The member it admitted is evicted.
         *
         * @param credential the credential string
         * @param reason     one of {@code CredentialRevocation.REASONS}
         * @return the revocation, when this peer had the authority to issue it
         */
        public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revokeJoinCredential(
                String credential, String reason) {
            return networked().revokeJoinCredential(credential, reason);
        }

        /**
         * Revokes any certified target in this group (SPEC §6.1, v0.1.13): an
         * agent, an agent key, a channel leaf, or a join credential, honoured
         * fleet-wide when the group's validator ranks this peer's authority for it.
         *
         * @param target        what is revoked
         * @param reason        one of {@code CredentialRevocation.REASONS}
         * @param effectiveFrom from when signatures stop verifying under a
         *                      non-compromise reason (null: now); no later than now
         * @return the revocation, when this peer had the authority to issue it
         */
        public Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revoke(
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target target, String reason,
                java.time.Instant effectiveFrom) {
            return networked().revoke(target, reason, effectiveFrom);
        }

        /**
         * Revokes a peer's identity in this group (SPEC §6.1): honoured only when
         * this peer is the group's trust root. The revoked peer is evicted and its
         * links cut wherever the revocation lands.
         *
         * @param peer   the peer whose identity is withdrawn
         * @param reason operator-facing reason
         * @return the revocation, when this peer had the authority to issue it
         */
        public Optional<ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement> revokePeer(
                ai.badmonkey.agentspaces.common.id.PeerId peer, String reason) {
            return networked().revoke(peer, reason);
        }

        private GroupRuntime networked() {
            if (runtime == null) {
                throw new IllegalStateException("group '" + name + "' is not networked; nothing to revoke in");
            }
            return runtime;
        }

        private Optional<CapabilityProvider> providerOf(String capabilityType) {
            return Optional.ofNullable(providers.get(Objects.requireNonNull(capabilityType)));
        }

        /**
         * Rotates the group's content key now (SPEC §11a.3, v0.1.13): mints the
         * next epoch through the group's {@code key-wrap} capability, cutting
         * over after {@code cutoverDelay} so members can fetch it first. This
         * peer must be an authorized rotator (the founder, or a
         * {@code KEY_ROTATE} grant).
         *
         * @param cutoverDelay how long members have to fetch the epoch before writers switch
         * @return the new epoch
         * @throws IllegalStateException when the group runs no key-wrap holder, or this peer may not rotate
         */
        public long rotateContentKey(java.time.Duration cutoverDelay) {
            Objects.requireNonNull(cutoverDelay, "cutoverDelay");
            CapabilityProvider provider = provider(
                    ai.badmonkey.agentspaces.capabilities.keywrap.GroupKeyDistributor.TYPE)
                    .orElseThrow(() -> new IllegalStateException("group '" + name
                            + "' runs no key-wrap capability to rotate through"));
            return ((ai.badmonkey.agentspaces.capabilities.keywrap.GroupKeyDistributor) provider)
                    .rotate(identity(), clock().instant().plus(cutoverDelay));
        }

        /**
         * Returns the group's capability pipes, the direct-frame multiplexer
         * pipe-bound capabilities such as push-sum share. Created once per
         * group, since a group runtime carries a single PIPE_DATA handler:
         * applications that build their own pipes should register the
         * capabilities built over them through {@link #provide} rather than
         * letting a client create a second aggregator here.
         *
         * @return the pipes
         * @throws IllegalStateException when the group is not networked
         */
        public CapabilityPipes pipes() {
            CapabilityPipes current = pipes;
            if (current != null) {
                return current;
            }
            synchronized (this) {
                if (pipes == null) {
                    if (runtime == null) {
                        throw new IllegalStateException(
                                "group '" + name + "' is not networked; no pipes");
                    }
                    pipes = new CapabilityPipes(runtime, CborCodec.defaultCodec());
                }
                return pipes;
            }
        }

        /**
         * Resolves a typed capability client for this group (spec §10.5), for
         * example {@code capability(VoteClient.class).propose(...)}. One client
         * per type is created per group and reused.
         *
         * @param clientType the client type a registered
         *                   {@link CapabilityClientFactory} produces
         * @param <C>        the client type
         * @return the client
         * @throws IllegalArgumentException when no factory produces the type
         * @throws IllegalStateException    when the factory cannot resolve the
         *                                  capability in this group
         */
        public <C> C capability(Class<C> clientType) {
            Objects.requireNonNull(clientType, "clientType");
            CapabilityClientFactory<?> factory = clientFactories.get(clientType);
            if (factory == null) {
                throw new IllegalArgumentException("no capability client factory for "
                        + clientType.getName() + "; known clients: " + clientFactories.keySet());
            }
            Object client = clients.computeIfAbsent(clientType, type -> {
                Object created = factory.create(this);
                if (!clientType.isInstance(created)) {
                    throw new IllegalStateException(factory + " produced a "
                            + (created == null ? "null" : created.getClass().getName())
                            + ", not a " + clientType.getName());
                }
                return created;
            });
            return clientType.cast(client);
        }

        @Override
        public void close() {
            binder.close();
            clients.clear();
            CapabilityRuntime capabilities = capabilityRuntime;
            if (capabilities != null && ownsCapabilities) {
                capabilities.close();
            }
        }
    }
}
