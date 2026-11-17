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
package ai.badmonkey.agentspaces.spoilers;

import ai.badmonkey.agentspaces.test.SimNetwork;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * What a rule does when it fires (srad/spoiler-peers.md §3.4.1, SP-C-07): the link actions of the 0.7.0
 * cut. {@code count} only records the firing, so one rule can count a frame another rule holds.
 *
 * @param type        the action
 * @param into        the receiving nodes the action is limited to; empty for every link the trigger matches
 * @param delayMillis the fleet-clock delay of {@link Type#DELAY}
 * @param copies      the copies of {@link Type#DUPLICATE}
 * @param order       the release order of {@link Type#REORDER} (a hold released in this order)
 */
public record Action(Type type, Set<String> into, long delayMillis, int copies, SimNetwork.Order order) {

    /** The link actions. */
    public enum Type { DROP, HOLD, DELAY, DUPLICATE, REORDER, COUNT }

    public Action {
        Objects.requireNonNull(type, "type");
        into = Set.copyOf(into);
    }

    public static Action drop() {
        return new Action(Type.DROP, Set.of(), 0, 1, SimNetwork.Order.FIFO);
    }

    /** Holds matching frames until the rule's release trigger, released in send order. */
    public static Action hold() {
        return new Action(Type.HOLD, Set.of(), 0, 1, SimNetwork.Order.FIFO);
    }

    public static Action delay(Duration span) {
        return new Action(Type.DELAY, Set.of(), span.toMillis(), 1, SimNetwork.Order.FIFO);
    }

    public static Action duplicate(int copies) {
        if (copies < 2) {
            throw new IllegalArgumentException("a duplicate sends at least two copies");
        }
        return new Action(Type.DUPLICATE, Set.of(), 0, copies, SimNetwork.Order.FIFO);
    }

    /** Holds matching frames and releases them in the given order (the seed of the script shuffles). */
    public static Action reorder(SimNetwork.Order order) {
        return new Action(Type.REORDER, Set.of(), 0, 1, order);
    }

    public static Action count() {
        return new Action(Type.COUNT, Set.of(), 0, 1, SimNetwork.Order.FIFO);
    }

    /** This action on the links into the named nodes only. */
    public Action into(String... nodes) {
        return new Action(type, Set.of(nodes), delayMillis, copies, order);
    }

    boolean holds() {
        return type == Type.HOLD || type == Type.REORDER;
    }
}
