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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Reads Embabel agent metadata reflectively by fully qualified annotation name
 * (spec §10.4), so this module compiles with no Embabel artifact and works
 * with whichever Embabel version the application ships. The names are the
 * published Embabel API annotations; attribute reads tolerate absent
 * attributes across versions, and an application can only reach this code
 * when the Embabel classes are present, because the autoconfiguration guards
 * on the {@code @Agent} class by name.
 */
public final class EmbabelIntrospector {

    /** The Embabel agent annotation, by name. */
    public static final String AGENT_ANNOTATION = "com.embabel.agent.api.annotation.Agent";
    /** The Embabel action annotation, by name. */
    public static final String ACTION_ANNOTATION = "com.embabel.agent.api.annotation.Action";
    /** The Embabel goal-achievement annotation, by name. */
    public static final String GOAL_ANNOTATION =
            "com.embabel.agent.api.annotation.AchievesGoal";

    /**
     * What an Embabel agent declares, in AgentSpaces terms.
     *
     * @param name        the agent name
     * @param description the agent's description
     * @param goals       goal descriptions from {@code @AchievesGoal} actions
     * @param actions     the {@code @Action} methods
     */
    public record EmbabelAgent(String name, String description, List<String> goals,
                               List<Method> actions) {
    }

    private EmbabelIntrospector() {
    }

    /**
     * Tests whether the Embabel API is on the classpath.
     *
     * @param classLoader the application class loader
     * @return {@code true} when Embabel's agent annotation loads
     */
    public static boolean embabelPresent(ClassLoader classLoader) {
        try {
            Class.forName(AGENT_ANNOTATION, false, classLoader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Introspects one bean.
     *
     * @param type the bean's class
     * @return the Embabel metadata, or empty when the class carries no
     *         {@code @Agent} annotation
     */
    public static Optional<EmbabelAgent> introspect(Class<?> type) {
        Annotation agent = annotationNamed(type.getAnnotations(), AGENT_ANNOTATION);
        if (agent == null) {
            return Optional.empty();
        }
        String name = stringAttribute(agent, "name")
                .filter(value -> !value.isBlank())
                .orElseGet(() -> decapitalize(type.getSimpleName()));
        String description = stringAttribute(agent, "description").orElse("");
        List<String> goals = new ArrayList<>();
        List<Method> actions = new ArrayList<>();
        for (Method method : type.getMethods()) {
            if (annotationNamed(method.getAnnotations(), ACTION_ANNOTATION) != null) {
                actions.add(method);
            }
            Annotation goal = annotationNamed(method.getAnnotations(), GOAL_ANNOTATION);
            if (goal != null) {
                stringAttribute(goal, "description")
                        .filter(value -> !value.isBlank())
                        .ifPresentOrElse(goals::add, () -> goals.add(method.getName()));
            }
        }
        return Optional.of(new EmbabelAgent(name, description, goals, actions));
    }

    /**
     * The description an Embabel {@code @Action} declares, empty when it has none.
     *
     * @param action an {@code @Action} method
     * @return the description
     */
    public static String actionDescription(Method action) {
        Annotation annotation = annotationNamed(action.getAnnotations(), ACTION_ANNOTATION);
        return annotation == null ? ""
                : stringAttribute(annotation, "description").orElse("");
    }

    private static Annotation annotationNamed(Annotation[] annotations, String name) {
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().getName().equals(name)) {
                return annotation;
            }
        }
        return null;
    }

    private static Optional<String> stringAttribute(Annotation annotation, String attribute) {
        try {
            Object value = annotation.annotationType()
                    .getMethod(attribute).invoke(annotation);
            return value instanceof String text ? Optional.of(text) : Optional.empty();
        } catch (ReflectiveOperationException e) {
            return Optional.empty(); // the attribute does not exist in this version
        }
    }

    private static String decapitalize(String name) {
        if (name.isEmpty()) {
            return name;
        }
        return name.substring(0, 1).toLowerCase(Locale.ROOT) + name.substring(1);
    }
}
