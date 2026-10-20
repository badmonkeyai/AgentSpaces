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

package org.springframework.beans.factory;

/**
 * Build-time stub of Spring's ObjectProvider, declaring only the methods the
 * auto-configuration calls; the real interface (which inherits
 * {@code getObject()} from {@code ObjectFactory}) replaces it at runtime.
 *
 * @param <T> the object type
 */
public interface ObjectProvider<T> {

    T getObject();

    T getIfAvailable();

    /** Spring's default: the available instance, or the supplier's when none. */
    default T getIfAvailable(java.util.function.Supplier<T> defaultSupplier) {
        T available = getIfAvailable();
        return available != null ? available : defaultSupplier.get();
    }

    /** Spring's default: every matching bean in {@code @Order} order; the stub streams the one instance or none. */
    default java.util.stream.Stream<T> orderedStream() {
        T available = getIfAvailable();
        return available == null ? java.util.stream.Stream.empty() : java.util.stream.Stream.of(available);
    }
}
