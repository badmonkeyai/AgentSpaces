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
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The fleet's discovered capabilities as Embabel planner actions (spec §14,
 * resolved): every {@link RemoteAction} the {@link RemoteActions} registry
 * exposes becomes one typed {@code @Action} method on a generated
 * {@code @Agent} class, so Embabel's GOAP planner type-matches remote
 * capabilities exactly as it matches local actions — a plan can chain a local
 * action into a remote agent's skill and back, and the remote step is an
 * ordinary leased space round-trip under the covers.
 *
 * <p>The class is generated because the planner plans over <em>signatures</em>:
 * an action consuming {@code ResearchTask} and producing {@code Finding} must
 * be a real method with those types for condition matching to see it. Each
 * generated method delegates to its card's {@link RemoteAction#invoke}; a
 * result that never arrives inside the timeout throws, which the planner
 * treats as the action failing — and the take-lease model means a crashed
 * remote worker's task reappears for another, so the failure is genuine
 * fleet-wide absence, not one peer's crash.
 *
 * <p>Embabel's annotations are referenced by name only, so this class loads
 * with no Embabel artifact present; {@link #agentInstance()} simply returns
 * empty then. Deployment onto a running Embabel platform goes through
 * {@link #deployTo(Object)}, which reaches the platform reflectively — the
 * annotated-instance path Embabel's own metadata reader consumes.
 */
public final class EmbabelRemoteActions {

    /** The generated agent's fully qualified class-name prefix. */
    public static final String GENERATED_PREFIX =
            "ai.badmonkey.agentspaces.embabel.generated.RemoteFleetAgent";

    private static final String AGENT_ANNOTATION = "com.embabel.agent.api.annotation.Agent";
    private static final String ACTION_ANNOTATION = "com.embabel.agent.api.annotation.Action";
    private static final String GOAL_ANNOTATION =
            "com.embabel.agent.api.annotation.AchievesGoal";
    private static final String METADATA_READER =
            "com.embabel.agent.api.annotation.support.AgentMetadataReader";

    private final RemoteActions actions;
    private final Duration invokeTimeout;
    private final String agentName;
    private final String description;

    /**
     * Creates the bridge.
     *
     * @param actions       the remote-action registry (the card view)
     * @param invokeTimeout how long a generated action waits for its result
     * @param agentName     the generated agent's name in the platform
     * @param description   the generated agent's description
     */
    public EmbabelRemoteActions(RemoteActions actions, Duration invokeTimeout,
                                String agentName, String description) {
        this.actions = Objects.requireNonNull(actions, "actions");
        this.invokeTimeout = Objects.requireNonNull(invokeTimeout, "invokeTimeout");
        this.agentName = Objects.requireNonNull(agentName, "agentName");
        this.description = Objects.requireNonNull(description, "description");
    }

    /** Returns the remote-action registry this bridge generates from. */
    public RemoteActions actions() {
        return actions;
    }

    /** Returns the generated agent's name in the platform. */
    public String agentName() {
        return agentName;
    }

    /**
     * Generates the {@code @Agent} instance whose {@code @Action} methods are
     * the fleet's currently advertised remote actions.
     *
     * @return the annotated instance, or empty when Embabel is not on the
     *         classpath or no remote action is currently advertised
     */
    public Optional<Object> agentInstance() {
        Class<? extends Annotation> agentAnnotation = annotation(AGENT_ANNOTATION);
        Class<? extends Annotation> actionAnnotation = annotation(ACTION_ANNOTATION);
        Class<? extends Annotation> goalAnnotation = annotation(GOAL_ANNOTATION);
        if (agentAnnotation == null || actionAnnotation == null) {
            return Optional.empty();
        }
        List<RemoteAction> available = actions.available();
        if (available.isEmpty()) {
            return Optional.empty();
        }

        Map<String, RemoteAction> byMethod = new LinkedHashMap<>();
        for (RemoteAction action : available) {
            String method = methodName(action, byMethod.keySet());
            byMethod.put(method, action);
        }
        RemoteActionInterceptor interceptor =
                new RemoteActionInterceptor(byMethod, invokeTimeout);

        DynamicType.Builder<?> builder = new ByteBuddy()
                .subclass(Object.class)
                .name(GENERATED_PREFIX + "$" + Integer.toHexString(System.identityHashCode(this)))
                .annotateType(AnnotationDescription.Builder.ofType(agentAnnotation)
                        .define("name", agentName)
                        .define("description", description)
                        .build());
        for (Map.Entry<String, RemoteAction> entry : byMethod.entrySet()) {
            RemoteAction action = entry.getValue();
            var method = builder.defineMethod(entry.getKey(), action.outputType(),
                            Visibility.PUBLIC)
                    .withParameters(action.inputType())
                    .intercept(MethodDelegation.to(interceptor))
                    .annotateMethod(AnnotationDescription.Builder
                            .ofType(actionAnnotation).build());
            if (goalAnnotation != null && !action.goals().isEmpty()) {
                method = method.annotateMethod(AnnotationDescription.Builder
                        .ofType(goalAnnotation)
                        .define("description", String.join("; ", action.goals()))
                        .build());
            }
            builder = method;
        }
        try {
            Class<?> generated = builder.make()
                    .load(agentAnnotation.getClassLoader(),
                            ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded();
            return Optional.of(generated.getDeclaredConstructor().newInstance());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot instantiate the generated fleet agent", e);
        }
    }

    /**
     * Generates the agent and deploys it onto an Embabel platform,
     * reflectively: first through Embabel's annotation metadata reader when
     * present ({@code createAgentMetadata(instance)} then
     * {@code platform.deploy(metadata)}), else through a direct single-argument
     * {@code deploy} method on the platform.
     *
     * @param platform the Embabel {@code AgentPlatform} (typed as Object so
     *                 this module needs no Embabel artifact)
     * @return {@code true} when an agent was generated and accepted
     */
    public boolean deployTo(Object platform) {
        Objects.requireNonNull(platform, "platform");
        Optional<Object> instance = agentInstance();
        if (instance.isEmpty()) {
            return false;
        }
        Object deployable = instance.get();
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

    /** A planner-legible, collision-free Java identifier for the action. */
    private static String methodName(RemoteAction action, java.util.Set<String> taken) {
        StringBuilder name = new StringBuilder();
        for (char c : action.name().toCharArray()) {
            name.append(Character.isJavaIdentifierPart(c) ? c : '_');
        }
        if (!Character.isJavaIdentifierStart(name.charAt(0))) {
            name.insert(0, '_');
        }
        String base = name.toString();
        String candidate = base;
        int suffix = 2;
        while (taken.contains(candidate)) {
            candidate = base + "_" + suffix++;
        }
        return candidate;
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Annotation> annotation(String name) {
        Class<?> type = classOrNull(name);
        return type != null && type.isAnnotation()
                ? (Class<? extends Annotation>) type : null;
    }

    private static Class<?> classOrNull(String name) {
        try {
            return Class.forName(name, false,
                    EmbabelRemoteActions.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            try {
                return Class.forName(name, false,
                        Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException | NullPointerException inner) {
                return null;
            }
        }
    }
}
