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
package ai.badmonkey.agentspaces.pdt.refs;

/**
 * A merge function registered for a ref name with {@code Pdt.mergeFor} (PD-R-05):
 * the reader calls it, instead of the rule of srad/agentspaces-pdt.md §3.6.1,
 * whenever two heads of that name meet. Every reader of the ref must register
 * the same function, since every reader computes the merge for itself and the
 * result must agree on every member. The function must be deterministic in its
 * three arguments.
 *
 * @param <T> the value type of the ref
 */
@FunctionalInterface
public interface MergeFunction<T> {

    /**
     * Merges two heads.
     *
     * @param base    the value at their lowest common base, or null when the base walk found none
     * @param earlier the value of the head with the earlier issued HLC
     * @param later   the value of the head with the later issued HLC
     * @return the merged value
     */
    T merge(T base, T earlier, T later);
}
