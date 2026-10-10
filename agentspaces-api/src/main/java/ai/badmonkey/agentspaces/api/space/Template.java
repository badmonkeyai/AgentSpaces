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
 * entry type plus zero or more field conditions and zero or more tag conditions.
 * Templates are immutable; each {@link #where} or {@link #whereTag} call returns
 * a new template.
 *
 * <pre>{@code
 * Template.of(TaskEntry.class)
 *         .where("kind", eq("summarize"))
 *         .where("priority", gte(3))
 *         .whereTag("region", eq("eu"))
 * }</pre>
 *
 * <p>Fields resolve against record components first, then against conventional
 * accessor methods ({@code field()}, {@code getField()}, {@code isField()}).
 * Unknown fields fail fast at template construction, so a typo surfaces where the
 * template is written and never as a silently empty match. Accessors are cached
 * per entry type.
 *
 * <p>Tag conditions (issue #16 §9.2) are evaluated by the space against the entry
 * record's tags, after the type comparison and before the payload is decoded, so
 * a tagged template never pays for decoding an entry it would not select. A
 * template with only tag conditions still selects by type. {@link #matches}
 * covers type and fields only; {@link #matchesTags} covers the tags.
 *
 * @param <T> the entry type this template matches
 */
public final class Template<T> {

    private static final Map<Class<?>, Map<String, Method>> ACCESSOR_CACHE = new ConcurrentHashMap<>();

    private final Class<T> type;
    private final List<Condition> conditions;
    private final List<TagCondition> tagConditions;

    private record Condition(String field, Matcher matcher, Method accessor) {
    }

    /**
     * One condition on an entry record's tags: the value under {@code key} (or
     * {@code null} when the record has no such tag) must satisfy the matcher.
     *
     * @param key     the tag key
     * @param matcher the condition the tag value must satisfy
     */
    public record TagCondition(String key, Matcher matcher) {
        public TagCondition {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(matcher, "matcher");
        }
    }

    private Template(Class<T> type, List<Condition> conditions, List<TagCondition> tagConditions) {
        this.type = type;
        this.conditions = conditions;
        this.tagConditions = tagConditions;
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
        return new Template<>(type, List.of(), List.of());
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
        return new Template<>(type, List.copyOf(extended), tagConditions);
    }

    /**
     * Returns a new template with an additional condition on the entry record's
     * tags (issue #16 §9.2). The matcher is given the value stored under
     * {@code key}, or {@code null} when the record carries no such tag, so the
     * ordinary matchers ({@code eq}, {@code in}, {@code contains}) fail on an
     * absent key and {@code isNull()} selects untagged entries.
     *
     * @param key     the tag key
     * @param matcher the condition the tag value must satisfy
     * @return the extended template
     */
    public Template<T> whereTag(String key, Matcher matcher) {
        TagCondition condition = new TagCondition(key, matcher);
        List<TagCondition> extended = new ArrayList<>(tagConditions);
        extended.add(condition);
        return new Template<>(type, conditions, List.copyOf(extended));
    }

    /**
     * Returns a new template requiring the entry record to carry a tag under
     * {@code key}, whatever its value: {@code whereTag(key, notNull())}.
     *
     * @param key the tag key
     * @return the extended template
     */
    public Template<T> hasTag(String key) {
        return whereTag(key, Matchers.notNull());
    }

    /** Returns the entry type this template matches. */
    public Class<T> type() {
        return type;
    }

    /**
     * Returns the tag conditions, in the order they were added; empty for a
     * template that selects by type and fields alone.
     */
    public List<TagCondition> tagConditions() {
        return tagConditions;
    }

    /**
     * Tests an entry record's tags against this template's tag conditions. A
     * template with no tag conditions accepts every tag map, including the
     * empty one.
     *
     * @param tags the record's tags
     * @return {@code true} when every tag condition accepts the tags
     */
    public boolean matchesTags(Map<String, String> tags) {
        Objects.requireNonNull(tags, "tags");
        for (TagCondition condition : tagConditions) {
            if (!condition.matcher().matches(tags.get(condition.key()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Tests a candidate object against this template's type and field conditions.
     * Tag conditions are not consulted here: they apply to the entry record, not
     * the value, and the space evaluates them with {@link #matchesTags} before it
     * decodes the value.
     *
     * @param candidate the candidate entry; may be {@code null}
     * @return {@code true} when the candidate is of the template type and every
     *         field condition accepts its field value
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
        return "Template[" + type.getSimpleName() + ", conditions=" + conditions.size()
                + ", tagConditions=" + tagConditions.size() + "]";
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
