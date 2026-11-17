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
package ai.badmonkey.agentspaces.agent.spi;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

/**
 * A take the binder runs for a method that carries no space annotation: the "map of 1" of
 * reviews/embabel-agentspaces-review-2026-11-12.md §7.3. The binder runs it as it would a
 * {@code @SpaceTake} with these attributes, <em>scoped by a tag</em> so the loop claims only
 * entries addressed to this action and never the entries another stage in the same space writes.
 * The tag is {@value #ACTION_TAG}{@code =<agent>.<method>}, and the card advertises the action
 * under the kind the reader names (an {@code embabel-action} for Embabel), which tells a caller's
 * bridge to write the tag.
 *
 * @param method      the method; exactly one entry parameter, an entry result
 * @param space       the space to take from, or empty for the group's sole space
 * @param lease       the take lease, a duration string ({@code 1m}, {@code PT1M})
 * @param pollTimeout the take poll, a duration string
 * @param description the action's description for the card
 * @param kind        the card kind the action is advertised under; must be an invocable kind
 * @param pre         the action's precondition terms, for the card's contract
 * @param post        the action's postcondition terms
 * @param cost        the action's cost hint, free text, or empty
 */
public record ImpliedBinding(Method method, String space, String lease, String pollTimeout,
                             String description, String kind, List<String> pre, List<String> post,
                             String cost) {

    /** The tag that addresses an entry to one implied action; its value is {@code <agent>.<method>}. */
    public static final String ACTION_TAG = "aspace.action";

    public ImpliedBinding {
        Objects.requireNonNull(method, "method");
        space = space == null ? "" : space;
        lease = lease == null || lease.isBlank() ? "1m" : lease;
        pollTimeout = pollTimeout == null || pollTimeout.isBlank() ? "1s" : pollTimeout;
        description = description == null ? "" : description;
        Objects.requireNonNull(kind, "kind");
        pre = pre == null ? List.of() : List.copyOf(pre);
        post = post == null ? List.of() : List.copyOf(post);
        cost = cost == null ? "" : cost;
    }

    /** The tag term an implied action's take template matches and a caller writes: {@code aspace.action=<agent>.<method>}. */
    public static String tagFor(String agentName, String methodName) {
        return ACTION_TAG + "=" + agentName + "." + methodName;
    }
}
