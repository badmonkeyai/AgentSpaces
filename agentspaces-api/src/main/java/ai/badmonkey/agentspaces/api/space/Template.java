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
package ai.badmonkey.agentspaces.api.space;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A typed template for associative matching against space entries (spec §7.2): an
 * entry type plus zero or more field conditions. Templates are immutable; each
 * {@link #where} call returns a new template.
 *
 * <pre>{@code
 * Template.of(TaskEntry.class)
 *         .where("kind", eq("summarize"))
 *         .where("priority", gte(3))
 * }</pre>
 *
 * <p>Fields resolve against record components first, then against conventional
 * accessor methods ({@code field()}, {@code getField()}, {@code isField()}).
 * Unknown fields fail fast at template construction, so a typo surfaces where the
 * template is written and never as a silently empty match. Accessors are cached
 * per entry type.
 *
 * @param <T> the entry type this template matches
 */
public final class Template<T> {

    private static final Map<Class<?>, Map<String, Method>> ACCESSOR_CACHE = new ConcurrentHashMap<>();

    private final Class<T> type;
    private final List<Condition> conditions;

    private record Condition(String field, Matcher matcher, Method accessor) {
    }

    private Template(Class<T> type, List<Condition> conditions) {
        this.type = type;
        this.conditions = conditions;
    }

    /**
     * Creates a template matching all entries of the given type.
     *
     * @param type the entry type
     * @param <T>  the entry type
     * @return the template
     */
    public static <T> Template<T> of(Class<T> type) {
        Objects.requireNonNull(type, "type");
        return new Template<>(type, List.of());
    }

    /**
     * Returns a new template with an additional field condition.
     *
     * @param field   the field name on the entry type
     * @param matcher the condition the field value must satisfy
     * @return the extended template
     * @throws IllegalArgumentException if the entry type has no such field
     */
    public Template<T> where(String field, Matcher matcher) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(matcher, "matcher");
        Method accessor = accessorFor(type, field);
        List<Condition> extended = new ArrayList<>(conditions);
        extended.add(new Condition(field, matcher, accessor));
        return new Template<>(type, List.copyOf(extended));
    }

    /** Returns the entry type this template matches. */
    public Class<T> type() {
        return type;
    }

    /**
     * Tests a candidate object against this template.
     *
     * @param candidate the candidate entry; may be {@code null}
     * @return {@code true} when the candidate is of the template type and every
     *         condition accepts its field value
     */
    public boolean matches(Object candidate) {
        if (!type.isInstance(candidate)) {
            return false;
        }
        for (Condition condition : conditions) {
            Object value;
            try {
                value = condition.accessor().invoke(candidate);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(
                        "failed reading field '" + condition.field() + "' of " + type.getName(), e);
            }
            if (!condition.matcher().matches(value)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "Template[" + type.getSimpleName() + ", conditions=" + conditions.size() + "]";
    }

    private static Method accessorFor(Class<?> type, String field) {
        Map<String, Method> accessors = ACCESSOR_CACHE.computeIfAbsent(type, Template::discoverAccessors);
        Method accessor = accessors.get(field);
        if (accessor == null) {
            throw new IllegalArgumentException(
                    "type " + type.getName() + " has no field '" + field + "'; known fields: "
                            + accessors.keySet().stream().sorted().toList());
        }
        return accessor;
    }

    private static Map<String, Method> discoverAccessors(Class<?> type) {
        Map<String, Method> accessors = new ConcurrentHashMap<>();
        if (type.isRecord()) {
            for (RecordComponent component : type.getRecordComponents()) {
                accessors.put(component.getName(), component.getAccessor());
            }
            return accessors;
        }
        for (Method method : type.getMethods()) {
            if (method.getParameterCount() != 0 || method.getReturnType() == void.class
                    || method.getDeclaringClass() == Object.class) {
                continue;
            }
            String name = method.getName();
            if (name.startsWith("get") && name.length() > 3) {
                accessors.putIfAbsent(decapitalize(name.substring(3)), method);
            } else if (name.startsWith("is") && name.length() > 2
                    && (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class)) {
                accessors.putIfAbsent(decapitalize(name.substring(2)), method);
            } else {
                accessors.putIfAbsent(name, method);
            }
        }
        return accessors;
    }

    private static String decapitalize(String name) {
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }
}
