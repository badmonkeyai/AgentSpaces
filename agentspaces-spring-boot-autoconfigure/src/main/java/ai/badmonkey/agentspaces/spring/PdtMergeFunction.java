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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.pdt.refs.MergeFunction;

import java.util.Objects;

/**
 * A merge function bean for one ref name (srad/agentspaces-pdt.md §7.2,
 * PD-R-05): the starter registers it on every group's facade through
 * {@code Pdt.mergeFor(name, kind, function)}, so each reader of that ref in
 * this application merges vectors and blobs the same way. Name the kind the
 * function is written for ({@code PRef.KIND_VECTOR}, {@code KIND_BLOB}, and so
 * on): a ref of the same name with another kind, in any group, then refuses it
 * rather than running it over a value it was not written for, and a function
 * that throws leaves the rule of §3.6.1 in charge with a counted
 * {@code MERGE_FUNCTION_FAILED} event. Declare one bean per ref name:
 *
 * <pre>{@code
 * @Bean
 * PdtMergeFunction logMerge() {
 *     return PdtMergeFunction.of("log", PRef.KIND_VECTOR, (base, earlier, later) -> append(earlier, later));
 * }
 * }</pre>
 *
 * @param name     the ref name
 * @param kind     the kind the function expects, or null for any
 * @param function the function readers apply when heads meet
 */
public record PdtMergeFunction(String name, String kind, MergeFunction<?> function) {

    public PdtMergeFunction {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(function, "function");
        if (name.isBlank()) {
            throw new IllegalArgumentException("a merge function names a ref");
        }
        if (kind != null && !PRef.KINDS.contains(kind)) {
            throw new IllegalArgumentException("unknown kind " + kind + "; one of " + PRef.KINDS);
        }
    }

    /** A merge function for a ref name and any kind. */
    public PdtMergeFunction(String name, MergeFunction<?> function) {
        this(name, null, function);
    }

    /**
     * A merge function for a ref name and the kind it expects.
     *
     * @param name     the ref name
     * @param kind     the kind name
     * @param function the function
     * @param <T>      the value type
     * @return the bean value
     */
    public static <T> PdtMergeFunction of(String name, String kind, MergeFunction<T> function) {
        return new PdtMergeFunction(name, kind, function);
    }

    /**
     * A merge function for a ref name.
     *
     * @param name     the ref name
     * @param function the function
     * @param <T>      the value type
     * @return the bean value
     */
    public static <T> PdtMergeFunction of(String name, MergeFunction<T> function) {
        return new PdtMergeFunction(name, function);
    }
}
