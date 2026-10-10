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

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * How the binder reads a key out of an entry (issue #16, ISSUE-Propose): the
 * rule {@code Template.where} uses, a record component or a {@code getX},
 * {@code isX}, or plain {@code x()} accessor, resolved once at bind time so a
 * missing field fails fast naming the type, and read per entry as a string.
 * A join keys its parts this way; a {@code @Propose} keys its proposal id,
 * joining several fields with {@code :}.
 */
public final class Keys {

    /** The separator between the fields of a composite key. */
    public static final String SEPARATOR = ":";

    private Keys() {
    }

    /**
     * Resolves the accessor a field is read through.
     *
     * @throws IllegalArgumentException when the type has no such field
     */
    public static Method accessor(Class<?> type, String field) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(field, "field");
        if (field.isEmpty()) {
            throw new IllegalArgumentException("a key field name is empty on " + type.getName());
        }
        if (type.isRecord()) {
            for (RecordComponent component : type.getRecordComponents()) {
                if (component.getName().equals(field)) {
                    return component.getAccessor();
                }
            }
        } else {
            String capitalised = Character.toUpperCase(field.charAt(0)) + field.substring(1);
            for (String candidate : List.of(field, "get" + capitalised, "is" + capitalised)) {
                try {
                    Method method = type.getMethod(candidate);
                    if (method.getReturnType() != void.class && method.getParameterCount() == 0) {
                        return method;
                    }
                } catch (NoSuchMethodException e) {
                    // try the next spelling
                }
            }
        }
        throw new IllegalArgumentException("type " + type.getName() + " has no field '" + field
                + "' to carry a key");
    }

    /** Resolves the accessors of several fields, in order. */
    public static List<Method> accessors(Class<?> type, String... fields) {
        List<Method> accessors = new ArrayList<>(fields.length);
        for (String field : fields) {
            accessors.add(accessor(type, field));
        }
        return accessors;
    }

    /**
     * Reads a key from a value: the fields' string forms joined by
     * {@link #SEPARATOR}, or null when any field is null.
     */
    public static String keyOf(Object value, List<Method> accessors) {
        Objects.requireNonNull(value, "value");
        StringBuilder key = new StringBuilder();
        for (Method accessor : accessors) {
            Object part;
            try {
                part = accessor.invoke(value);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("failed reading key field '" + accessor.getName()
                        + "' of " + value.getClass().getName(), e);
            }
            if (part == null) {
                return null;
            }
            if (!key.isEmpty()) {
                key.append(SEPARATOR);
            }
            key.append(part);
        }
        return key.toString();
    }
}
