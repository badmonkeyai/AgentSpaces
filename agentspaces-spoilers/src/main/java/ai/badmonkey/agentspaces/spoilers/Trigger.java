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

import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.peering.wire.Envelope;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * What a rule waits for (srad/spoiler-peers.md §3.3, SP-C-05): a frame or a decoded item on a link, a
 * space event at a member, a counter crossing a value, a fleet-clock time, or another rule firing. Never
 * a wall-clock duration. Frames and events are named by the shipped enums ({@link Envelope.Kind},
 * {@link SpaceEvent.Kind}) and items by {@link ItemKind} (SP-Q-MNT-03).
 */
public sealed interface Trigger {

    /** Any frame of the given kinds (none: any kind); narrowed with the withers of {@link Frame}. */
    static Frame frame(Envelope.Kind... kinds) {
        Set<Envelope.Kind> set = kinds.length == 0 ? Set.of() : Set.copyOf(EnumSet.of(kinds[0], kinds));
        return new Frame(set, null, null, null, 0, null, null, null, null, Map.of());
    }

    /** A space event a member raised, fed to the runner by {@link ScriptRunner#watch}. */
    static Event raised(String member, SpaceEvent.Kind kind) {
        return new Event(member, kind, null);
    }

    /** A counter, read after each fleet tick, reaching a value. */
    static Counter counter(String name, LongSupplier value, long atLeast) {
        return new Counter(name, value, atLeast);
    }

    /** A fleet-clock time, in milliseconds since the clock's epoch. */
    static Time at(long fleetMillis) {
        return new Time(fleetMillis, -1);
    }

    /** A number of gossip periods of fleet clock after the script is armed. */
    static Time afterPeriods(int periods) {
        return new Time(-1, periods);
    }

    /** The first firing of another rule of the script. */
    static After after(String rule) {
        return new After(rule);
    }

    /** A value captured by an earlier match, by its name (srad/spoiler-peers.md §3.2, bindings). */
    static Bound $(String name) {
        return new Bound(name);
    }

    /** A reference to a bound value. */
    record Bound(String name) {
        public Bound {
            Objects.requireNonNull(name, "name");
        }
    }

    /**
     * A frame trigger: its kinds (matched on the frame or a frame it carries), the link's two nodes by
     * fabric address, a stream it carries, the n-th match only, and at level 2 an item of a kind, about an
     * entry or CID (a literal or a {@link Bound} value), of a PDT node kind, capturing its id under a name.
     */
    record Frame(Set<Envelope.Kind> kinds, String from, String to, String stream, int nth, ItemKind item,
                 Object id, String node, String bind, Map<String, Object> attrs) implements Trigger {

        public Frame {
            attrs = attrs == null ? Map.of() : Map.copyOf(attrs);
        }

        /**
         * Only an item whose decoded attribute has this value (a literal or a {@link Bound} value): a claim's
         * {@code holderPeer}, a state's {@code issuerPeer}, so a rule names whose item it means whichever link carries it.
         */
        public Frame attr(String key, Object value) {
            Map<String, Object> more = new java.util.HashMap<>(attrs);
            more.put(key, value);
            return new Frame(kinds, from, to, stream, nth, item, id, node, bind, more);
        }


        public Frame from(String node) {
            return new Frame(kinds, node, to, stream, nth, item, id, this.node, bind, attrs);
        }

        public Frame to(String node) {
            return new Frame(kinds, from, node, stream, nth, item, id, this.node, bind, attrs);
        }

        public Frame stream(String value) {
            return new Frame(kinds, from, to, value, nth, item, id, node, bind, attrs);
        }

        /** Only the n-th frame that matches everything else, counted from 1. */
        public Frame nth(int n) {
            return new Frame(kinds, from, to, stream, n, item, id, node, bind, attrs);
        }

        public Frame item(ItemKind kind) {
            return new Frame(kinds, from, to, stream, nth, kind, id, node, bind, attrs);
        }

        /**
         * The item's entry id or CID: a literal text, a {@link Bound} value, or a collection of ids read when a frame
         * is matched (so a scenario can name ids it learns after arming).
         */
        public Frame id(Object value) {
            return new Frame(kinds, from, to, stream, nth, item, value, node, bind, attrs);
        }

        /** A {@link ItemKind#NODE} item of this PDT node kind, such as {@code vg}. */
        public Frame node(String kind) {
            return new Frame(kinds, from, to, stream, nth, item == null ? ItemKind.NODE : item, id, kind, bind, attrs);
        }

        /** Captures the matching item's id under a name for later triggers. */
        public Frame bind(String name) {
            return new Frame(kinds, from, to, stream, nth, item, id, node, name, attrs);
        }

        @Override
        public String toString() {
            return "frame" + (kinds.isEmpty() ? "(*)" : kinds) + (from == null ? "" : " from " + from)
                    + (to == null ? "" : " to " + to) + (stream == null ? "" : " stream " + stream)
                    + (item == null ? "" : " item " + item) + (node == null ? "" : " node " + node)
                    + (id == null ? "" : " id " + id) + (attrs.isEmpty() ? "" : " " + new java.util.TreeMap<>(attrs))
                    + (nth == 0 ? "" : " nth " + nth);
        }
    }

    /** A space event a member raised, optionally about one entry (a literal or a {@link Bound}). */
    record Event(String member, SpaceEvent.Kind kind, Object entry) implements Trigger {
        public Event entry(Object value) {
            return new Event(member, kind, value);
        }
    }

    /** A counter crossing a value. */
    record Counter(String name, LongSupplier value, long atLeast) implements Trigger {
        /** The name and threshold alone: the record's own text names the supplier's object, which differs on a replay. */
        @Override
        public String toString() {
            return "counter '" + name + "' at least " + atLeast;
        }
    }

    /** A fleet-clock time: absolute, or a number of periods after arming. */
    record Time(long atMillis, int periods) implements Trigger {
    }

    /** Another rule's first firing. */
    record After(String rule) implements Trigger {
    }
}
