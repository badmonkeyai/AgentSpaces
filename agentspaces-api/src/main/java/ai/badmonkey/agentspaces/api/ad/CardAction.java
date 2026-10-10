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
package ai.badmonkey.agentspaces.api.ad;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * One action an agent offers (SPEC §6.1, v0.1.13): the declared pairing of
 * what one bound method consumes with what it produces, so a planner or the
 * remote-actions bridge (§10.6) sees exactly the agent's actions instead of
 * the cross product of its card-level schema lists.
 *
 * @param name        the action's name, unique within its card (the method name)
 * @param description what the action does; empty when undeclared
 * @param consumes    the schema names the action consumes
 * @param produces    the schema names it produces; empty for a void action
 * @param space       the space it takes or watches its input in; null when unbound
 * @param kind        how the action is bound: one of {@link #KINDS}. A kind this
 *                    reader does not recognize (a newer peer's) is kept rather than
 *                    refused, so the card still decodes, and reads as not
 *                    {@linkplain #invocable() invocable}
 */
public record CardAction(
        String name,
        String description,
        List<String> consumes,
        List<String> produces,
        @JsonInclude(JsonInclude.Include.NON_NULL) String space,
        String kind) {

    /** A {@code @SpaceTake} worker. */
    public static final String TAKE = "take";
    /** A {@code @SpaceNotify} reaction. */
    public static final String NOTIFY = "notify";
    /** An {@code @OrderedTake} exactly-once worker. */
    public static final String ORDERED_TAKE = "ordered-take";
    /** A {@code @Ballot} voter. */
    public static final String BALLOT = "ballot";
    /** An {@code @OnDecision} reaction to a closed vote. */
    public static final String ON_DECISION = "on-decision";
    /** An Embabel {@code @Action}. */
    public static final String EMBABEL_ACTION = "embabel-action";
    /**
     * A {@code @SpaceJoin} fan-in (issue #16): consumes every part's schema, is
     * never invocable by writing one entry.
     */
    public static final String JOIN = "join";
    /** A {@code @Propose} lead (ISSUE-Propose): consumes the cue, produces the proposal, never invocable. */
    public static final String PROPOSE = "propose";
    /** An {@code @OnEstimate} reaction (ISSUE-OnEstimate): consumes no entry, never invocable. */
    public static final String ON_ESTIMATE = "on-estimate";
    /** A {@code @SpaceReduce} fold (ISSUE-SpaceReduce): consumes the element, produces the accumulator, never invocable. */
    public static final String REDUCE = "reduce";

    /** The recognized kinds. */
    public static final Set<String> KINDS =
            Set.of(TAKE, NOTIFY, ORDERED_TAKE, BALLOT, ON_DECISION, EMBABEL_ACTION);

    /** The kinds a remote caller can invoke by writing the consumed entry. */
    public static final Set<String> INVOCABLE = Set.of(TAKE, NOTIFY, ORDERED_TAKE, EMBABEL_ACTION);

    /** The longest description a card carries per action. */
    public static final int MAX_DESCRIPTION_LENGTH = 512;

    public CardAction {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("an action needs a name");
        }
        description = description == null ? "" : description;
        if (description.length() > MAX_DESCRIPTION_LENGTH) {
            throw new IllegalArgumentException("action description longer than "
                    + MAX_DESCRIPTION_LENGTH + " characters: " + name);
        }
        consumes = List.copyOf(Objects.requireNonNull(consumes, "consumes"));
        produces = List.copyOf(Objects.requireNonNull(produces, "produces"));
        Objects.requireNonNull(kind, "kind");
    }

    /** Whether a remote caller can invoke this action by writing its input. */
    public boolean invocable() {
        return INVOCABLE.contains(kind);
    }
}
