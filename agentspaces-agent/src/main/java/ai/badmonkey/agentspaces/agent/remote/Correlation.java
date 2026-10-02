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

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Decides whether a result entry answers a given request entry. A remote
 * invocation is a write followed by a wait, and the space may carry results for
 * other requests too; the correlation picks out ours.
 *
 * <p>The {@link #sharedFields() default} matches the convention every
 * AgentSpaces example already uses: a result relates to its request through the
 * domain fields they share (a {@code Finding} carries the {@code topic} of the
 * {@code ResearchTask} it answers). Two record types correlate when every
 * component name they have in common holds equal values.
 */
@FunctionalInterface
public interface Correlation {

    /**
     * Tests whether a candidate result answers the request.
     *
     * @param input     the request entry that was written
     * @param candidate a result entry observed after the write
     * @return {@code true} when the candidate answers the request
     */
    boolean matches(Object input, Object candidate);

    /** Accepts any result observed after the write; first one wins. */
    static Correlation any() {
        return (input, candidate) -> true;
    }

    /**
     * The default: record components with the same name in both the request and
     * the candidate must hold equal values. Types sharing no component names
     * (or that are not records) fall back to {@link #any()} semantics, since
     * nothing ties a result to a request more specifically.
     *
     * @return the shared-field correlation
     */
    static Correlation sharedFields() {
        return (input, candidate) -> {
            Map<String, Method> inputAccessors = recordAccessors(input.getClass());
            Map<String, Method> candidateAccessors = recordAccessors(candidate.getClass());
            for (Map.Entry<String, Method> accessor : inputAccessors.entrySet()) {
                Method other = candidateAccessors.get(accessor.getKey());
                if (other == null) {
                    continue;
                }
                if (!Objects.equals(read(accessor.getValue(), input), read(other, candidate))) {
                    return false;
                }
            }
            return true;
        };
    }

    private static Map<String, Method> recordAccessors(Class<?> type) {
        Map<String, Method> accessors = new HashMap<>();
        if (type.isRecord()) {
            for (RecordComponent component : type.getRecordComponents()) {
                accessors.put(component.getName(), component.getAccessor());
            }
        }
        return accessors;
    }

    private static Object read(Method accessor, Object target) {
        try {
            return accessor.invoke(target);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("unreadable record component: " + accessor, e);
        }
    }
}
