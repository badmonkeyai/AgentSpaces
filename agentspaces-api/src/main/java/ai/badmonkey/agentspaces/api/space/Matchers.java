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

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Standard field matchers for {@link Template} conditions, intended for static
 * import: {@code Template.of(Task.class).where("priority", gte(3))}.
 *
 * <p>Ordering matchers compare via {@link Comparable}; a value whose type does not
 * match the expected value's type simply fails the match, so a template never
 * throws on a heterogeneous space.
 */
public final class Matchers {

    private Matchers() {
    }

    /**
     * Matches values equal to the expected value.
     *
     * @param expected the expected value; may be {@code null} to match null
     * @return the matcher
     */
    public static Matcher eq(Object expected) {
        return actual -> Objects.equals(actual, expected);
    }

    /**
     * Matches values unequal to the given value.
     *
     * @param unexpected the value to exclude
     * @return the matcher
     */
    public static Matcher ne(Object unexpected) {
        return actual -> !Objects.equals(actual, unexpected);
    }

    /**
     * Matches values strictly greater than the bound.
     *
     * @param bound the exclusive lower bound
     * @return the matcher
     */
    public static Matcher gt(Comparable<?> bound) {
        return comparing(bound, c -> c > 0);
    }

    /**
     * Matches values greater than or equal to the bound.
     *
     * @param bound the inclusive lower bound
     * @return the matcher
     */
    public static Matcher gte(Comparable<?> bound) {
        return comparing(bound, c -> c >= 0);
    }

    /**
     * Matches values strictly less than the bound.
     *
     * @param bound the exclusive upper bound
     * @return the matcher
     */
    public static Matcher lt(Comparable<?> bound) {
        return comparing(bound, c -> c < 0);
    }

    /**
     * Matches values less than or equal to the bound.
     *
     * @param bound the inclusive upper bound
     * @return the matcher
     */
    public static Matcher lte(Comparable<?> bound) {
        return comparing(bound, c -> c <= 0);
    }

    /**
     * Matches values contained in the given set of alternatives.
     *
     * @param alternatives the accepted values
     * @return the matcher
     */
    public static Matcher in(Object... alternatives) {
        // A tolerant set: duplicate and null alternatives are accepted rather
        // than rejected (Set.of forbids both), so in("a", "a") and in(x, null)
        // work as a caller naturally expects.
        Set<Object> accepted = new java.util.HashSet<>(
                java.util.Arrays.asList(alternatives));
        return accepted::contains;
    }

    /**
     * Matches character sequences containing the given fragment.
     *
     * @param fragment the fragment to look for
     * @return the matcher
     */
    public static Matcher contains(CharSequence fragment) {
        Objects.requireNonNull(fragment, "fragment");
        String needle = fragment.toString();
        return actual -> actual instanceof CharSequence cs && cs.toString().contains(needle);
    }

    /** Matches {@code null} values. */
    public static Matcher isNull() {
        return Objects::isNull;
    }

    /** Matches non-{@code null} values. */
    public static Matcher notNull() {
        return Objects::nonNull;
    }

    /**
     * Adapts an arbitrary predicate as a matcher.
     *
     * @param predicate the predicate over the field value
     * @return the matcher
     */
    public static Matcher predicate(Predicate<Object> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        return predicate::test;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Matcher comparing(Comparable<?> bound, java.util.function.IntPredicate accept) {
        Objects.requireNonNull(bound, "bound");
        return actual -> {
            if (!(actual instanceof Comparable comparable)
                    || !bound.getClass().isInstance(actual)) {
                return false;
            }
            return accept.test(comparable.compareTo(bound));
        };
    }
}
