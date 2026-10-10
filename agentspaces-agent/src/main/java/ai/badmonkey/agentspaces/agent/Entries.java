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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Several returns from one bound method: the fork (SPEC §10.3, ISSUE-WorkflowVerbs).
 * The binder dispatches each element as if the method had returned it alone
 * (an entry, a {@link Tagged} entry, a {@code Contribution}, a {@code Motion}),
 * in order, after completing a taken entry once. A fork from a take is not
 * atomic: a crash between two elements leaves the first written and the task
 * reappearing, so forked entries carry the input's id and a stage downstream
 * dedups on it. {@code null} elements are refused.
 *
 * <pre>{@code
 * @SpaceNotify(space = "assessment", produces = {QuoteRequest.class, QuoteTask.class})
 * public Entries fork(ClaimAssessment a) {
 *     return Entries.of(new QuoteRequest(a.claimId(), 3),
 *             new QuoteTask(a.claimId(), "north"), new QuoteTask(a.claimId(), "east"),
 *             new QuoteTask(a.claimId(), "south"));
 * }
 * }</pre>
 */
public final class Entries {

    private final List<Object> elements;

    private Entries(List<Object> elements) {
        this.elements = List.copyOf(elements);
    }

    /** The elements, in the order they are dispatched. */
    public List<Object> elements() {
        return elements;
    }

    /** A fork of the given returns; an empty fork writes nothing. */
    public static Entries of(Object... elements) {
        return of(Arrays.asList(Objects.requireNonNull(elements, "elements")));
    }

    /** A fork of the given returns; an empty fork writes nothing. */
    public static Entries of(List<?> elements) {
        List<Object> copy = new ArrayList<>(Objects.requireNonNull(elements, "elements").size());
        for (Object element : elements) {
            copy.add(Objects.requireNonNull(element, "a fork carries no null element"));
        }
        return new Entries(copy);
    }

    @Override
    public String toString() {
        return "Entries" + elements.stream().map(e -> e.getClass().getSimpleName()).toList();
    }
}
