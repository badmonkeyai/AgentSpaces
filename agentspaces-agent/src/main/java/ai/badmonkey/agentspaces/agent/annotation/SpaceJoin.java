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
package ai.badmonkey.agentspaces.agent.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Fan-in as one annotation (SPEC §10.3, issue #16): the method takes a
 * {@link ai.badmonkey.agentspaces.agent.join.Joined} bag and is invoked once
 * per {@linkplain #key() key} when every required {@link Part} of that key is
 * present in the space, on a virtual thread, with the usual return dispatch
 * (an entry, a {@link ai.badmonkey.agentspaces.agent.Tagged} entry, a
 * {@link ai.badmonkey.agentspaces.agent.capability.Contribution}, or
 * {@code null}). The binder subscribes to each part, seeds its per-key state
 * from the space on bind so parts that landed earlier still count, re-reads
 * the parts by key when it fires, and forgets a key whose parts stop arriving
 * after {@link #within()}.
 *
 * <pre>{@code
 * @SpaceJoin(space = "intake", key = "claimId", resultSpace = "assessment",
 *         parts = {@Part(FraudFinding.class), @Part(CoverageFinding.class),
 *                  @Part(value = Instance.class, keyTag = "claimId")})
 * public ClaimAssessment assemble(Joined j) {
 *     return new ClaimAssessment(j.key(), j.get(FraudFinding.class).fraudScore(), ...);
 * }
 * }</pre>
 *
 * <p>How many times a key fires is the {@link Mode}. {@link Mode#LOCAL} fires
 * once per key per bound agent, from an in-memory set, so bind one joiner or
 * make the method idempotent against its downstream result. The fleet-wide
 * modes write a {@link ai.badmonkey.agentspaces.agent.join.JoinTicket} entry
 * when a key completes and fire from a take of that ticket: {@link Mode#LEASED}
 * through the space's own claim race, once per lease per ticket, and
 * {@link Mode#ORDERED} through the space's ordered-log coordinator, where the
 * log's commit order makes the first committed ticket per key the only one
 * that fires, fleet-wide.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SpaceJoin {

    /** How often one key may fire. */
    enum Mode {
        /** Once per key per bound agent, remembered in memory. */
        LOCAL,
        /** A ticket entry taken under a lease: once per lease race per ticket. */
        LEASED,
        /** A ticket entry taken through the ordered log: once fleet-wide per key. */
        ORDERED
    }

    /**
     * The join's space: where its parts live unless a {@link Part#space()} says
     * otherwise, and where its tickets are written in the fleet-wide modes;
     * empty means the binder's sole registered space.
     */
    String space() default "";

    /** The group this binding belongs to; empty means any group whose spaces satisfy it. */
    String group() default "";

    /**
     * The name of the field (record component or {@code getX}/{@code isX}
     * accessor) that carries a case's key on every part, unless a part names
     * its own {@link Part#key()} or {@link Part#keyTag()}.
     */
    String key();

    /** The parts a key needs; two or more. */
    Part[] parts();

    /** How often one key may fire. */
    Mode mode() default Mode.LOCAL;

    /**
     * The join's name as it appears on tickets and cards; empty means
     * {@code <agent name>.<method name>}, which is what makes joiners of one
     * class on several peers share tickets in the fleet-wide modes.
     */
    String name() default "";

    /** Forget a key whose parts have stopped arriving after this long. */
    String within() default "1h";

    /**
     * Fire a complete key only after no new part has arrived for this long,
     * so a gather of an unknown number has an end; empty fires as soon as the
     * key is complete (ISSUE-WorkflowVerbs).
     */
    String settle() default "";

    /** The most keys kept open at once; the least recently touched is evicted beyond it. */
    int maxOpen() default 10_000;

    /** The part subscriptions' lease, renewed at half-lease while the agent stays bound. */
    String lease() default "1h";

    /** {@link Mode#LEASED} and {@link Mode#ORDERED}: the ticket's write lease. */
    String ticketLease() default "10m";

    /** {@link Mode#LEASED} and {@link Mode#ORDERED}: the ticket's TAKE lease. */
    String takeLease() default "30s";

    /** {@link Mode#LEASED} and {@link Mode#ORDERED}: how long one poll for a ticket waits. */
    String pollTimeout() default "1s";

    /** The space a returned entry is written into; empty means the join's space. */
    String resultSpace() default "";

    /** The write lease for a returned entry. */
    String resultLease() default "1h";

    /**
     * What this method produces when its return type does not say: the entry
     * types inside an {@code Entries} fork or an {@code Object} return,
     * declared on the card. A sealed return type declares its permitted
     * subclasses without this.
     */
    Class<?>[] produces() default {};

    /**
     * What this action does, published as the action's description on the
     * agent's card (SPEC §6.1); empty means the card's agent description stands in.
     */
    String description() default "";
}
