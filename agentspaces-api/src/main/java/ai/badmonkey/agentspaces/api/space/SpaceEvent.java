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

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.util.Objects;

/**
 * An event delivered to a {@link SpaceListener}.
 *
 * <p>The {@code issuer} is the entry's authenticated writer: in a replicated
 * space it is the identity whose signature the entry record carries, so a
 * listener that must act only on entries from a specific party (a directive
 * gate honoring its console, an audit reader) checks it here rather than
 * trusting self-declared payload fields.
 *
 * @param kind    what happened to the entry
 * @param entryId the entry's identifier
 * @param entry   the entry value at the time of the event
 * @param issuer  the entry's authenticated writer
 * @param <T>     the entry type
 */
public record SpaceEvent<T>(Kind kind, EntryId entryId, T entry, AgentId issuer, AgentId actor) {

    /**
     * An event whose actor is the entry's issuer: every kind but a completion,
     * which names the claim holder that completed the entry as its actor
     * (SPEC §11a.4, v0.1.13).
     */
    public SpaceEvent(Kind kind, EntryId entryId, T entry, AgentId issuer) {
        this(kind, entryId, entry, issuer, issuer);
    }

    /** The kinds of entry event a space emits. */
    public enum Kind {
        /** A matching entry was written. */
        WRITTEN,
        /** A matching entry was taken exclusively. */
        TAKEN,
        /** A matching entry was completed (consumed permanently). */
        COMPLETED,
        /** A matching entry's write lease lapsed and the entry vanished. */
        EXPIRED,
        /** A taken entry's TAKE lease lapsed and the entry reappeared. */
        REAPPEARED
    }

    public SpaceEvent {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(issuer, "issuer");
        actor = actor == null ? issuer : actor;
    }
}
