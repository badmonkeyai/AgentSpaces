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
import ai.badmonkey.agentspaces.agent.remote.RemoteActions;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The fleet's discovered capabilities as Embabel planner actions (spec §10.6): every {@link RemoteAction}
 * the {@link RemoteActions} registries expose becomes one typed {@code @Action} method on a generated
 * {@code @Agent} class, so Embabel's GOAP planner type-matches remote capabilities exactly as it matches
 * local actions, and the remote step is an ordinary leased space round-trip under the covers.
 *
 * <p>The second generation of the bridge (reviews/embabel-agentspaces-review-2026-11-12.md §7): it spans
 * the groups it is built over (§7.7), generates one action per <em>capability</em> (group, task space,
 * result space, input and output types) however many peers offer it, names actions stably, carries the
 * card's description, a normalised cost and the foreign preconditions onto the generated action (§7.6),
 * fires each action once and withdraws a timed-out input (§7.5), and can generate a vacant generation so
 * dead actions leave the platform (§7.9).
 *
 * <p>Embabel's annotations are referenced by name only, so this class loads with no Embabel artifact
 * present; {@link #agentInstance()} returns empty then.
 */
public final class EmbabelRemoteActions {

    /** The generated agent's fully qualified class-name prefix. */
    public static final String GENERATED_PREFIX =
            "ai.badmonkey.agentspaces.embabel.generated.RemoteFleetAgent";

    private static final String AGENT_ANNOTATION = "com.embabel.agent.api.annotation.Agent";
    private static final String ACTION_ANNOTATION = "com.embabel.agent.api.annotation.Action";
    private static final String GOAL_ANNOTATION = "com.embabel.agent.api.annotation.AchievesGoal";
    private static final String METADATA_READER = "com.embabel.agent.api.annotation.support.AgentMetadataReader";
    private static final String OPERATION_CONTEXT = "com.embabel.agent.api.common.OperationContext";
    private static final String RETRY_POLICY = "com.embabel.agent.core.ActionRetryPolicy";
    private static final int MAX_DESCRIPTION = 500;

    /** Which contract terms the generated actions carry (review §7.6). */
    public enum CarryConditions { NONE, PRE, PRE_POST }

    /**
     * How the generated actions behave on the platform (review §7.5, §7.6).
     *
     * @param fireOnce    whether the generated actions carry Embabel's fire-once retry policy
     * @param onTimeout   what happens to the input entry on a timeout
     * @param defaultCost the cost an action carries when its card names none, 0 to 1
     * @param conditions  which contract terms the generated actions carry
     */
    public record Attributes(boolean fireOnce, RemoteAction.OnTimeout onTimeout, double defaultCost,
                             CarryConditions conditions) {
        public Attributes {
            Objects.requireNonNull(onTimeout, "onTimeout");
            Objects.requireNonNull(conditions, "conditions");
            if (defaultCost < 0 || defaultCost > 1) {
                throw new IllegalArgumentException("defaultCost is 0 to 1: " + defaultCost);
            }
        }

        /** Fire once, withdraw on timeout, cost 0.5, carry the preconditions. */
        public static Attributes defaults() {
            return new Attributes(true, RemoteAction.OnTimeout.WITHDRAW, 0.5, CarryConditions.PRE);
        }
    }

    private final List<RemoteActions> registries;
    private final Duration invokeTimeout;
    private final String agentName;
    private final String description;
    private final Attributes attributes;

    /**
     * Creates the bridge over one registry.
     *
     * @param actions       the remote-action registry (the card view)
     * @param invokeTimeout how long a generated action waits for its result
     * @param agentName     the generated agent's name in the platform
     * @param description   the generated agent's description
     */
    public EmbabelRemoteActions(RemoteActions actions, Duration invokeTimeout, String agentName, String description) {
        this(List.of(Objects.requireNonNull(actions, "actions")), invokeTimeout, agentName, description,
                Attributes.defaults());
    }

    /**
     * Creates the bridge over several registries, one per group (review §7.7).
     *
     * @param registries    the registries, one per bridged group; at least one
     * @param invokeTimeout how long a generated action waits for its result
     * @param agentName     the generated agent's name in the platform
     * @param description   the generated agent's description
     * @param attributes    how the generated actions behave
     */
    public EmbabelRemoteActions(List<RemoteActions> registries, Duration invokeTimeout, String agentName,
                                String description, Attributes attributes) {
        this.registries = List.copyOf(Objects.requireNonNull(registries, "registries"));
        if (this.registries.isEmpty()) {
            throw new IllegalArgumentException("a bridge spans at least one registry");
        }
        this.invokeTimeout = Objects.requireNonNull(invokeTimeout, "invokeTimeout");
        this.agentName = Objects.requireNonNull(agentName, "agentName");
        this.description = Objects.requireNonNull(description, "description");
        this.attributes = Objects.requireNonNull(attributes, "attributes");
    }

    /** Returns the first registry this bridge generates from (the one of a one-group bridge). */
    public RemoteActions actions() {
        return registries.get(0);
    }

    /** Returns every registry this bridge generates from, one per bridged group. */
    public List<RemoteActions> registries() {
        return registries;
    }

    /** Returns the generated agent's name in the platform. */
    public String agentName() {
        return agentName;
    }

    public Attributes attributes() {
        return attributes;
    }

    /**
     * The fleet's currently advertised capabilities across every bridged group, one per capability
     * (review §7.7): peers offering the same key (group, task space, result space, input and output types)
     * are one action, represented by the newest card.
     *
     * @return the deduplicated actions, in a stable order
     */
    public List<RemoteAction> available() {
        Map<String, RemoteAction> byKey = new LinkedHashMap<>();
        for (RemoteActions registry : registries) {
            for (RemoteAction action : registry.available()) {
                String key = capabilityKey(action);
                RemoteAction held = byKey.get(key);
                if (held == null || action.card().issued().isAfter(held.card().issued())) {
                    byKey.put(key, action);
                }
            }
        }
        return new ArrayList<>(byKey.values());
    }

    /** The capability key of an action (review §7.7): what is offered and where, never who offers it. */
    public static String capabilityKey(RemoteAction action) {
        return action.card().group().value() + "|" + action.taskSpace().name() + "|" + action.resultSpace().name()
                + "|" + action.inputType().getName() + "|" + action.outputType().getName();
    }

    /** The ad-caches' change counters summed (review §7.9), so a deployer can poll without recomputing. */
    public long generation() {
        long sum = 0;
        for (RemoteActions registry : registries) {
            sum += registry.generation();
        }
        return sum;
    }

    /** When a bridged cache will next change by itself, if any holds a card. */
    public Optional<java.time.Instant> nextExpiry() {
        java.time.Instant next = null;
        for (RemoteActions registry : registries) {
            java.time.Instant candidate = registry.nextExpiry().orElse(null);
            if (candidate != null && (next == null || candidate.isBefore(next))) {
                next = candidate;
            }
        }
        return Optional.ofNullable(next);
    }

    /**
     * Generates the {@code @Agent} instance whose {@code @Action} methods are the fleet's currently
     * advertised remote actions.
     *
     * @return the annotated instance, or empty when Embabel is not on the classpath or no remote action is
     *         currently advertised
     */
    public Optional<Object> agentInstance() {
        return agentInstance(available());
    }

    /**
     * Generates the {@code @Agent} instance for one snapshot of actions (review §7.9: the deployer compares
     * and deploys the same snapshot).
     *
     * @param snapshot the actions
     * @return the annotated instance, or empty when Embabel is absent or the snapshot is empty
     */
    public Optional<Object> agentInstance(List<RemoteAction> snapshot) {
        Class<? extends Annotation> agentAnnotation = annotation(AGENT_ANNOTATION);
        Class<? extends Annotation> actionAnnotation = annotation(ACTION_ANNOTATION);
        Class<? extends Annotation> goalAnnotation = annotation(GOAL_ANNOTATION);
        if (agentAnnotation == null || actionAnnotation == null || snapshot.isEmpty()) {
            return Optional.empty();
        }
        Map<String, RemoteAction> byMethod = methodNames(snapshot);
        RemoteActionInterceptor interceptor = new RemoteActionInterceptor(byMethod, invokeTimeout,
                attributes.onTimeout());
        Class<?> context = classOrNull(OPERATION_CONTEXT);

        DynamicType.Builder<?> builder = new ByteBuddy()
                .subclass(Object.class)
                .name(GENERATED_PREFIX + "$" + Integer.toHexString(System.identityHashCode(this))
                        + "$" + Long.toHexString(System.nanoTime()))
                .annotateType(AnnotationDescription.Builder.ofType(agentAnnotation)
                        .define("name", agentName)
                        .define("description", description)
                        .build());
        for (Map.Entry<String, RemoteAction> entry : byMethod.entrySet()) {
            RemoteAction action = entry.getValue();
            var method = builder.defineMethod(entry.getKey(), action.outputType(), Visibility.PUBLIC)
                    .withParameters(context == null ? new Class<?>[]{action.inputType()}
                            : new Class<?>[]{action.inputType(), context})
                    .intercept(MethodDelegation.to(interceptor))
                    .annotateMethod(actionAnnotation(actionAnnotation, action));
            if (goalAnnotation != null && !action.goals().isEmpty()) {
                method = method.annotateMethod(AnnotationDescription.Builder.ofType(goalAnnotation)
                        .define("description", String.join("; ", action.goals()))
                        .build());
            }
            builder = method;
        }
        return Optional.of(instantiate(builder, agentAnnotation));
    }

    /**
     * The vacant generation (review §7.9): an {@code @Agent} under the same name whose one action maps
     * {@link FleetVacant} to itself and can never be chosen, deployed when every fleet action has lapsed so
     * the previous generation's dead actions leave the platform.
     *
     * @return the annotated instance, or empty when Embabel is absent
     */
    public Optional<Object> vacantInstance() {
        Class<? extends Annotation> agentAnnotation = annotation(AGENT_ANNOTATION);
        Class<? extends Annotation> actionAnnotation = annotation(ACTION_ANNOTATION);
        if (agentAnnotation == null || actionAnnotation == null) {
            return Optional.empty();
        }
        DynamicType.Builder<?> builder = new ByteBuddy()
                .subclass(Object.class)
                .name(GENERATED_PREFIX + "$vacant$" + Long.toHexString(System.nanoTime()))
                .annotateType(AnnotationDescription.Builder.ofType(agentAnnotation)
                        .define("name", agentName)
                        .define("description", "No fleet actions are available now.")
                        .build())
                .defineMethod("fleetVacant", FleetVacant.class, Visibility.PUBLIC)
                .withParameters(FleetVacant.class)
                .intercept(net.bytebuddy.implementation.FixedValue.nullValue())
                .annotateMethod(AnnotationDescription.Builder.ofType(actionAnnotation)
                        .define("description", "No fleet actions are available now.")
                        .build());
        return Optional.of(instantiate(builder, agentAnnotation));
    }

    private static Object instantiate(DynamicType.Builder<?> builder, Class<?> agentAnnotation) {
        try {
            Class<?> generated = builder.make()
                    .load(agentAnnotation.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded();
            return generated.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot instantiate the generated fleet agent", e);
        }
    }

    /**
     * The generated {@code @Action}'s attributes (review §7.5, §7.6), each defined only when the application's
     * Embabel version has it: the card's description, a normalised cost, the foreign preconditions (and
     * postconditions when configured), and the fire-once retry policy.
     */
    private AnnotationDescription actionAnnotation(Class<? extends Annotation> actionAnnotation, RemoteAction action) {
        Set<String> attributes = new java.util.HashSet<>();
        for (Method attribute : actionAnnotation.getDeclaredMethods()) {
            attributes.add(attribute.getName());
        }
        AnnotationDescription.Builder annotation = AnnotationDescription.Builder.ofType(actionAnnotation);
        if (attributes.contains("description")) {
            String text = action.description();
            annotation = annotation.define("description",
                    text.length() > MAX_DESCRIPTION ? text.substring(0, MAX_DESCRIPTION) : text);
        }
        if (attributes.contains("cost")) {
            annotation = annotation.define("cost", normalisedCost(action));
        }
        CardAction.Contract contract = action.declared().map(CardAction::contract).orElse(null);
        if (contract != null && this.attributes.conditions() != CarryConditions.NONE) {
            if (attributes.contains("pre") && !contract.pre().isEmpty()) {
                annotation = annotation.defineArray("pre", contract.pre().toArray(String[]::new));
            }
            if (attributes.contains("post") && !contract.post().isEmpty()
                    && this.attributes.conditions() == CarryConditions.PRE_POST) {
                annotation = annotation.defineArray("post", contract.post().toArray(String[]::new));
            }
        }
        if (this.attributes.fireOnce() && attributes.contains("actionRetryPolicy")) {
            Class<?> policy = classOrNull(RETRY_POLICY);
            if (policy != null && policy.isEnum()) {
                for (Object constant : policy.getEnumConstants()) {
                    if ("FIRE_ONCE".equals(((Enum<?>) constant).name())) {
                        annotation = annotation.define("actionRetryPolicy", (Enum<?>) constant);
                    }
                }
            }
        }
        return annotation.build();
    }

    /**
     * The card's cost hint as Embabel's 0-to-1 cost (review §7.6): a number from 0 to 1 as is; x above 1 as
     * x/(1+x); an ISO duration of s seconds as s/(60+s); {@code low}, {@code medium}, {@code high} as 0.2,
     * 0.5, 0.8; anything else as the configured default, so a remote action is never free next to a local one.
     */
    double normalisedCost(RemoteAction action) {
        String hint = action.declared().map(CardAction::contract).map(CardAction.Contract::cost).orElse("");
        return normaliseCost(hint, attributes.defaultCost());
    }

    static double normaliseCost(String hint, double fallback) {
        if (hint == null || hint.isBlank()) {
            return fallback;
        }
        String text = hint.trim().toLowerCase(java.util.Locale.ROOT);
        switch (text) {
            case "low": return 0.2;
            case "medium": return 0.5;
            case "high": return 0.8;
            default: break;
        }
        try {
            double number = Double.parseDouble(text);
            if (number < 0) {
                return fallback;
            }
            return number <= 1 ? number : number / (1 + number);
        } catch (NumberFormatException e) {
            // not a number
        }
        try {
            double seconds = Duration.parse(hint.trim()).toMillis() / 1000.0;
            return seconds < 0 ? fallback : seconds / (60 + seconds);
        } catch (java.time.format.DateTimeParseException e) {
            return fallback;
        }
    }

    /**
     * Generates the agent and deploys it onto an Embabel platform, reflectively: first through Embabel's
     * annotation metadata reader when present ({@code createAgentMetadata(instance)} then
     * {@code platform.deploy(metadata)}), else through a direct single-argument {@code deploy} method.
     *
     * @param platform the Embabel {@code AgentPlatform} (typed as Object so this module needs no Embabel artifact)
     * @return {@code true} when an agent was generated and accepted
     */
    public boolean deployTo(Object platform) {
        return deployTo(platform, available());
    }

    /**
     * Deploys one snapshot of actions (review §7.9).
     *
     * @param platform the platform
     * @param snapshot the actions to deploy
     * @return {@code true} when an agent was generated and accepted
     */
    public boolean deployTo(Object platform, List<RemoteAction> snapshot) {
        Objects.requireNonNull(platform, "platform");
        return agentInstance(snapshot).map(instance -> deployInstance(platform, instance)).orElse(false);
    }

    /**
     * Deploys the vacant generation (review §7.9), replacing the previous one under the same name.
     *
     * @param platform the platform
     * @return {@code true} when accepted
     */
    public boolean deployVacant(Object platform) {
        Objects.requireNonNull(platform, "platform");
        return vacantInstance().map(instance -> deployInstance(platform, instance)).orElse(false);
    }

    private static boolean deployInstance(Object platform, Object deployable) {
        Class<?> readerType = classOrNull(METADATA_READER);
        if (readerType != null) {
            try {
                Object reader = readerType.getDeclaredConstructor().newInstance();
                Method create = readerType.getMethod("createAgentMetadata", Object.class);
                Object metadata = create.invoke(reader, deployable);
                if (metadata != null && deploy(platform, metadata)) {
                    return true;
                }
            } catch (ReflectiveOperationException e) {
                // Fall through to the direct path. The reader path runs against
                // real Embabel 1.5 in integration-tests/embabel-it.
            }
        }
        return deploy(platform, deployable);
    }

    private static boolean deploy(Object platform, Object agent) {
        for (Method method : platform.getClass().getMethods()) {
            if (method.getName().equals("deploy") && method.getParameterCount() == 1
                    && method.getParameterTypes()[0].isAssignableFrom(agent.getClass())) {
                try {
                    method.invoke(platform, agent);
                    return true;
                } catch (ReflectiveOperationException e) {
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * The names of the agents a platform holds, read through its {@code agents()} method when it has one
     * (review §7.9: the name-ownership check and the read-back after a deploy); empty when it has none.
     *
     * @param platform the platform
     * @return the agent names, in the platform's order
     */
    public static List<String> deployedNames(Object platform) {
        List<String> names = new ArrayList<>();
        try {
            Method agents = platform.getClass().getMethod("agents");
            Object listed = agents.invoke(platform);
            if (listed instanceof Iterable<?> iterable) {
                for (Object agent : iterable) {
                    Object name = agent.getClass().getMethod("getName").invoke(agent);
                    names.add(String.valueOf(name));
                }
            }
        } catch (ReflectiveOperationException | RuntimeException e) {
            // a platform without agents(): nothing to read back
        }
        return names;
    }

    /**
     * Stable, collision-free method names for a snapshot (review §7.7): {@code <agent>_<Input>} when unique
     * in the snapshot, then {@code _<Output>}, then {@code _<group>}, then six hex digits of the capability
     * key's hash; never a counter that depends on order.
     */
    static Map<String, RemoteAction> methodNames(List<RemoteAction> snapshot) {
        List<RemoteAction> ordered = new ArrayList<>(snapshot);
        ordered.sort(java.util.Comparator.comparing(EmbabelRemoteActions::capabilityKey));
        Map<String, List<RemoteAction>> byBase = new LinkedHashMap<>();
        for (RemoteAction action : ordered) {
            byBase.computeIfAbsent(identifier(action.name()), k -> new ArrayList<>()).add(action);
        }
        Map<String, RemoteAction> names = new LinkedHashMap<>();
        for (Map.Entry<String, List<RemoteAction>> group : byBase.entrySet()) {
            List<RemoteAction> actions = group.getValue();
            if (actions.size() == 1) {
                names.put(group.getKey(), actions.get(0));
                continue;
            }
            Map<String, List<RemoteAction>> byOutput = new LinkedHashMap<>();
            for (RemoteAction action : actions) {
                byOutput.computeIfAbsent(group.getKey() + "_" + action.outputType().getSimpleName(),
                        k -> new ArrayList<>()).add(action);
            }
            for (Map.Entry<String, List<RemoteAction>> level : byOutput.entrySet()) {
                if (level.getValue().size() == 1) {
                    names.put(level.getKey(), level.getValue().get(0));
                    continue;
                }
                Map<String, List<RemoteAction>> byGroup = new LinkedHashMap<>();
                for (RemoteAction action : level.getValue()) {
                    byGroup.computeIfAbsent(level.getKey() + "_" + identifier(action.card().group().value()),
                            k -> new ArrayList<>()).add(action);
                }
                for (Map.Entry<String, List<RemoteAction>> last : byGroup.entrySet()) {
                    if (last.getValue().size() == 1) {
                        names.put(last.getKey(), last.getValue().get(0));
                        continue;
                    }
                    for (RemoteAction action : last.getValue()) {
                        String hash = Integer.toHexString(capabilityKey(action).hashCode() & 0xffffff);
                        names.put(last.getKey() + "_" + ("000000" + hash).substring(hash.length()), action);
                    }
                }
            }
        }
        return names;
    }

    /** A Java identifier for a name: every other character becomes an underscore. */
    private static String identifier(String text) {
        StringBuilder name = new StringBuilder();
        for (char c : text.toCharArray()) {
            name.append(Character.isJavaIdentifierPart(c) ? c : '_');
        }
        if (name.length() == 0 || !Character.isJavaIdentifierStart(name.charAt(0))) {
            name.insert(0, '_');
        }
        return name.toString();
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Annotation> annotation(String name) {
        Class<?> type = classOrNull(name);
        return type != null && type.isAnnotation() ? (Class<? extends Annotation>) type : null;
    }

    private static Class<?> classOrNull(String name) {
        try {
            return Class.forName(name, false, EmbabelRemoteActions.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            try {
                return Class.forName(name, false, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException | NullPointerException inner) {
                return null;
            }
        }
    }
}
