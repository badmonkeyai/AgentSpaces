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
package ai.badmonkey.agentspaces.agent.remote;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The fleet's discovered capabilities as available actions (spec §14, the
 * "planner sees the fleet" question, resolved): every foreign {@link AgentCard}
 * in the group's ad-cache whose consumed and produced schemas resolve to local
 * classes becomes a {@link RemoteAction} — name, description, goals, typed
 * input and output, and an invocation that is a space round-trip. A planner
 * (Embabel's GOAP through {@code embabel-agentspaces}, or any other) can
 * therefore treat the whole fleet's advertised skills as its own action space,
 * and cross-peer plans become first-class.
 *
 * <p>The registry is a live view: cards are TTL-leased, so {@link #available()}
 * reflects the fleet as it is now — an agent that stops refreshing its card
 * ages out of the action space by itself, exactly as it ages out of discovery.
 *
 * <p>Routing (spec §10.6): an invocation writes its input entry into the space
 * the remote worker takes from. An explicit {@link #route(Class, String)} wins;
 * otherwise the card's own {@linkplain AgentCard#spaceFor(String) space
 * binding} for the consumed type is used when it names a registered space;
 * otherwise a group with exactly one space needs no routes at all. Results are
 * awaited in the input's space unless {@link #resultsIn(Class, String)} says
 * otherwise, matching {@code @SpaceTake}'s own result-space default.
 */
public final class RemoteActions {

    private final DiscoveryService discovery;
    private final PeerId self;
    private final Map<String, Space> spaces;
    /** The group's registry, consulted before the class-name rule; null for the rule alone. */
    private final SchemaRegistry schemas;
    private final Map<Class<?>, String> taskRoutes = new ConcurrentHashMap<>();
    private final Map<Class<?>, String> resultRoutes = new ConcurrentHashMap<>();
    private volatile Correlation correlation = Correlation.sharedFields();
    private volatile Lease writeLease = Lease.of(Duration.ofMinutes(10));

    /**
     * Creates a registry over explicit pieces.
     *
     * @param discovery the group's discovery service (the card source)
     * @param self      the local peer id; its own cards are never actions
     * @param spaces    the group's spaces by name
     */
    public RemoteActions(DiscoveryService discovery, PeerId self, Map<String, Space> spaces) {
        this(discovery, self, spaces, null);
    }

    /**
     * Creates a registry over explicit pieces that resolves card schema names
     * through a schema registry first (ISSUE-WorkflowShape §9.1): the registry
     * the group's spaces and binder share, so a card naming an IRI such as
     * {@code https://example.org/claims#Claim} resolves to the local class.
     * Names the registry does not know fall back to the class-name rule when
     * they look like class names.
     *
     * @param discovery the group's discovery service (the card source)
     * @param self      the local peer id; its own cards are never actions
     * @param spaces    the group's spaces by name
     * @param schemas   the schema registry, or null for the class-name rule alone
     */
    public RemoteActions(DiscoveryService discovery, PeerId self, Map<String, Space> spaces,
                         SchemaRegistry schemas) {
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.self = Objects.requireNonNull(self, "self");
        this.spaces = Map.copyOf(Objects.requireNonNull(spaces, "spaces"));
        this.schemas = schemas;
        if (this.spaces.isEmpty()) {
            throw new IllegalArgumentException("a RemoteActions registry needs at least one space");
        }
    }

    /**
     * Creates a registry over a joined group from the {@link AgentSpaces}
     * facade.
     *
     * @param group the group context
     * @param self  the local peer id
     * @return the registry over the group's discovery, spaces, and schema registry
     */
    public static RemoteActions over(AgentSpaces.GroupContext group, PeerId self) {
        Objects.requireNonNull(group, "group");
        Map<String, Space> byName = new ConcurrentHashMap<>();
        for (String name : group.spaceNames()) {
            byName.put(name, group.space(name));
        }
        return new RemoteActions(group.discovery(), self, byName, group.schemaRegistry());
    }

    /**
     * Routes invocations whose input is the given type into a named space (the
     * space the remote workers take that type from).
     *
     * @param inputType the input entry type
     * @param spaceName the space invocations write into
     * @return this registry
     */
    public RemoteActions route(Class<?> inputType, String spaceName) {
        taskRoutes.put(Objects.requireNonNull(inputType), requireSpaceName(spaceName));
        return this;
    }

    /**
     * Awaits results of the given type in a named space, when workers complete
     * into a different space than they take from.
     *
     * @param outputType the result entry type
     * @param spaceName  the space results appear in
     * @return this registry
     */
    public RemoteActions resultsIn(Class<?> outputType, String spaceName) {
        resultRoutes.put(Objects.requireNonNull(outputType), requireSpaceName(spaceName));
        return this;
    }

    /**
     * Replaces the default {@linkplain Correlation#sharedFields() shared-field}
     * correlation for actions created after this call.
     *
     * @param correlation the correlation
     * @return this registry
     */
    public RemoteActions correlation(Correlation correlation) {
        this.correlation = Objects.requireNonNull(correlation, "correlation");
        return this;
    }

    /**
     * Replaces the default ten-minute write lease on invocation entries.
     *
     * @param lease the lease for written input entries
     * @return this registry
     */
    public RemoteActions writeLease(Lease lease) {
        this.writeLease = Objects.requireNonNull(lease, "lease");
        return this;
    }

    /**
     * Returns the fleet's currently advertised, locally invocable actions: one
     * per foreign card's declared invocable action and (consumed, produced) type
     * pair that resolves here, so two actions over the same types (a take and a
     * notify, say) stay two. A card that declares no actions yields the full
     * pairing of its flat consumed and produced lists; an unserved pairing
     * simply times out when invoked.
     *
     * @return the available remote actions, newest cards first
     */
    public List<RemoteAction> available() {
        List<RemoteAction> actions = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        List<AgentCard> cards = discovery.find(AgentCard.class,
                card -> !card.issuer().equals(self));
        cards.sort((a, b) -> b.issued().compareTo(a.issued()));
        for (AgentCard card : cards) {
            for (Pair pair : pairs(card)) {
                String consumed = pair.consumed();
                String produced = pair.produced();
                Optional<Class<?>> input = resolve(consumed);
                Optional<Class<?>> output = resolve(produced);
                if (input.isEmpty() || output.isEmpty()) {
                    continue;
                }
                String key = card.agent().encoded() + "|"
                        + (pair.action() == null ? "" : pair.action().name()) + "|"
                        + consumed + "|" + produced;
                if (!seen.add(key)) {
                    continue; // an older refresh of the same capability
                }
                taskSpaceFor(input.get(), card, pair.action(), consumed).ifPresent(taskSpace -> {
                    // No result route: await in the take space, as @SpaceTake
                    // itself completes there by default (spec §10.6).
                    Space resultSpace = routedSpace(output.get(), resultRoutes)
                            .orElse(taskSpace);
                    actions.add(new RemoteAction(card, input.get(), output.get(),
                            taskSpace, resultSpace, correlation, writeLease, pair.action()));
                });
            }
        }
        return actions;
    }

    /**
     * The (consumed, produced) pairs a card offers (spec §10.6, v0.1.13): its
     * declared invocable actions when the card declares any, so a two-action
     * agent yields exactly two remote actions; otherwise the cross product of
     * the card's flat lists, as an older card needs.
     */
    private record Pair(String consumed, String produced,
                        ai.badmonkey.agentspaces.api.ad.CardAction action) {
    }

    private static List<Pair> pairs(AgentCard card) {
        List<Pair> pairs = new ArrayList<>();
        if (card.actions() != null) {
            for (ai.badmonkey.agentspaces.api.ad.CardAction action : card.actions()) {
                if (!action.invocable()) {
                    continue; // ballots and decision reactions are not invoked by writing an entry
                }
                for (String consumed : action.consumes()) {
                    for (String produced : action.produces()) {
                        pairs.add(new Pair(consumed, produced, action));
                    }
                }
            }
            return pairs;
        }
        for (String consumed : card.consumes()) {
            for (String produced : card.produces()) {
                pairs.add(new Pair(consumed, produced, null));
            }
        }
        return pairs;
    }

    /**
     * Returns the available actions producing the given type.
     *
     * @param outputType the wanted result type
     * @return the matching actions
     */
    public List<RemoteAction> producing(Class<?> outputType) {
        Objects.requireNonNull(outputType, "outputType");
        return available().stream()
                .filter(action -> outputType.isAssignableFrom(action.outputType()))
                .toList();
    }

    /**
     * Returns the available actions consuming the given type.
     *
     * @param inputType the input type
     * @return the matching actions
     */
    public List<RemoteAction> consuming(Class<?> inputType) {
        Objects.requireNonNull(inputType, "inputType");
        return available().stream()
                .filter(action -> action.inputType().isAssignableFrom(inputType))
                .toList();
    }

    // ---------------------------------------------------------------- internals

    /**
     * Resolves a card schema name to a local class: through the group's schema
     * registry first (an IRI or any name the registry knows), then by the
     * class-name rule ({@code <fqn>#v1}) for names that look like class names.
     * A name containing {@code :} or {@code /} is an IRI, which never resolves
     * as a class.
     */
    private Optional<Class<?>> resolve(String schemaName) {
        if (schemas != null) {
            Optional<Class<?>> known = schemas.classFor(schemaName);
            if (known.isPresent()) {
                // ASF-029 holds for the registry's answer too: a platform class
                // is never an entry schema, whatever named it.
                return isPlatform(known.get().getName()) ? Optional.empty() : known;
            }
        }
        if (schemaName.indexOf(':') >= 0 || schemaName.indexOf('/') >= 0) {
            return Optional.empty();
        }
        int versionAt = schemaName.lastIndexOf('#');
        String className = versionAt < 0 ? schemaName : schemaName.substring(0, versionAt);
        // ASF-029: schema names come from foreign cards any admitted peer can
        // publish. The class is looked up WITHOUT initialization, so a hostile
        // name cannot run static initializers, and platform packages are never
        // entry schemas, so they are refused outright.
        if (isPlatform(className)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Class.forName(className, false,
                    RemoteActions.class.getClassLoader()));
        } catch (ClassNotFoundException | LinkageError e) {
            return Optional.empty();
        }
    }

    /** ASF-029: the platform packages that are never entry schemas. */
    private static boolean isPlatform(String className) {
        return className.startsWith("java.") || className.startsWith("javax.")
                || className.startsWith("jdk.") || className.startsWith("sun.")
                || className.startsWith("com.sun.");
    }

    /** Explicit route, then the action's space, then the card's binding, then the sole space. */
    private Optional<Space> taskSpaceFor(Class<?> inputType, AgentCard card,
                                         ai.badmonkey.agentspaces.api.ad.CardAction action,
                                         String consumed) {
        Optional<Space> routed = routedSpace(inputType, taskRoutes);
        if (routed.isPresent()) {
            return routed;
        }
        // The action's space and the card's binding are foreign data: only a
        // name that resolves to a registered space here is followed.
        if (action != null && action.space() != null && spaces.containsKey(action.space())) {
            return Optional.of(spaces.get(action.space()));
        }
        Optional<Space> bound = card.spaceFor(consumed).map(spaces::get);
        if (bound.isPresent()) {
            return bound;
        }
        if (spaces.size() == 1) {
            return Optional.of(spaces.values().iterator().next());
        }
        return Optional.empty(); // several spaces, no route, no binding: not invocable
    }

    private Optional<Space> routedSpace(Class<?> type, Map<Class<?>, String> routes) {
        String routed = routes.get(type);
        return routed == null ? Optional.empty() : Optional.of(spaces.get(routed));
    }

    private String requireSpaceName(String spaceName) {
        Objects.requireNonNull(spaceName, "spaceName");
        if (!spaces.containsKey(spaceName)) {
            throw new IllegalArgumentException("no space named '" + spaceName
                    + "'; spaces: " + spaces.keySet());
        }
        return spaceName;
    }
}
