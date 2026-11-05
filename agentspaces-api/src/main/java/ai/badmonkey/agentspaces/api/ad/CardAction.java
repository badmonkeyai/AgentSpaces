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
        String kind,
        // Appended in v0.1.25 (ISSUE-CardContract) and omitted from the wire when absent, so an action
        // without a contract is byte-identical to the v0.1.13 action. When present it is what a planner
        // reads: the situations or conditions the action needs, what it establishes, a cost hint, and
        // the methods (HTN) it belongs to.
        @JsonInclude(JsonInclude.Include.NON_NULL) Contract contract) {

    /**
     * The planner's view of an action (SPEC §6.1b, v0.1.25): {@code pre}, the situation names or
     * conditions that must hold before the action runs (beside its consumed schemas); {@code post},
     * the schemas it produces and the situations it establishes (beside its produced schemas);
     * {@code cost}, a free hint a planner orders by ("1", "low", "PT2M"); {@code methods}, the HTN
     * method names the action belongs to (reserved for strategy B4). Each list holds at most
     * {@link #MAX_CONTRACT_TERMS} terms.
     */
    public record Contract(List<String> pre, List<String> post, String cost, List<String> methods) {
        public Contract {
            pre = pre == null ? List.of() : List.copyOf(pre);
            post = post == null ? List.of() : List.copyOf(post);
            cost = cost == null ? "" : cost;
            methods = methods == null ? List.of() : List.copyOf(methods);
            if (pre.size() > MAX_CONTRACT_TERMS || post.size() > MAX_CONTRACT_TERMS || methods.size() > MAX_CONTRACT_TERMS) {
                throw new IllegalArgumentException("a contract carries at most " + MAX_CONTRACT_TERMS + " terms per list");
            }
            if (cost.length() > MAX_COST_LENGTH) {
                throw new IllegalArgumentException("a contract's cost hint is at most " + MAX_COST_LENGTH + " characters");
            }
        }

        /** Null when every term is empty, so an empty contract never reaches the wire. */
        public static Contract of(List<String> pre, List<String> post, String cost) {
            boolean empty = (pre == null || pre.isEmpty()) && (post == null || post.isEmpty()) && (cost == null || cost.isBlank());
            return empty ? null : new Contract(pre, post, cost, List.of());
        }
    }

    /** The most terms a contract list carries. */
    public static final int MAX_CONTRACT_TERMS = 32;
    /** The longest cost hint. */
    public static final int MAX_COST_LENGTH = 64;

    /** The v0.1.13 shape: an action without a contract. */
    public CardAction(String name, String description, List<String> consumes, List<String> produces, String space, String kind) {
        this(name, description, consumes, produces, space, kind, null);
    }

    /** This action with a contract (null removes it). */
    public CardAction withContract(Contract contract) {
        return new CardAction(name, description, consumes, produces, space, kind, contract);
    }

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
