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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The live {@link SpaceCredential} entries a {@code CREDENTIAL}-admitted
 * replica holds (SPEC §7.5, TECH-SPEC §7.10), keyed by the credential entry's
 * id. The owning {@link ReplicatedSpace} maintains it from its own CRDT state
 * under the space lock: a credential is indexed when its record merges (or is
 * written locally by the issuer), re-indexed when its lease is renewed, and
 * removed when it is cancelled, when its lease lapses at the sweep, or when
 * its tombstone is collected. {@link #admits} additionally checks the lease
 * against the caller's clock, so a credential stops admitting the instant it
 * lapses, sweep or no sweep.
 *
 * <p>Instances are thread-safe; a rule may read one while the space updates it.
 */
public final class CredentialIndex {

    /** One indexed credential: who it admits, for what, until when. */
    private record Live(SpaceCredential credential, long expiresAtMillis) {
    }

    private final Map<EntryId, Live> live = new HashMap<>();

    /**
     * Indexes (or re-indexes) a credential entry.
     *
     * @param entryId         the credential entry
     * @param credential      the decoded credential
     * @param expiresAtMillis when its write lease lapses
     */
    public synchronized void put(EntryId entryId, SpaceCredential credential, long expiresAtMillis) {
        live.put(Objects.requireNonNull(entryId, "entryId"),
                new Live(Objects.requireNonNull(credential, "credential"), expiresAtMillis));
    }

    /**
     * Drops a credential entry (cancelled, lapsed, or collected).
     *
     * @param entryId the credential entry
     * @return whether an entry was indexed under that id
     */
    public synchronized boolean remove(EntryId entryId) {
        return live.remove(Objects.requireNonNull(entryId, "entryId")) != null;
    }

    /**
     * Drops every credential whose lease has lapsed.
     *
     * @param nowMillis the space clock
     * @return how many were dropped
     */
    public synchronized int expire(long nowMillis) {
        int before = live.size();
        live.values().removeIf(entry -> entry.expiresAtMillis() <= nowMillis);
        return before - live.size();
    }

    /**
     * Whether a live credential admits the agent for the scope.
     *
     * @param agent     the agent
     * @param scope     the scope asked for
     * @param nowMillis the space clock
     * @return whether some unexpired indexed credential names both
     */
    public synchronized boolean admits(AgentId agent, SpaceAdmission.Scope scope, long nowMillis) {
        for (Live entry : live.values()) {
            if (entry.expiresAtMillis() > nowMillis
                    && entry.credential().agent().equals(agent)
                    && entry.credential().grants(scope)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The ids of the live credential entries naming an agent, for revocation.
     *
     * @param agent     the agent
     * @param nowMillis the space clock
     * @return the entry ids, possibly empty
     */
    public synchronized List<EntryId> entriesFor(AgentId agent, long nowMillis) {
        List<EntryId> ids = new ArrayList<>();
        for (Map.Entry<EntryId, Live> e : live.entrySet()) {
            if (e.getValue().expiresAtMillis() > nowMillis
                    && e.getValue().credential().agent().equals(agent)) {
                ids.add(e.getKey());
            }
        }
        return ids;
    }

    /** How many credential entries are indexed, lapsed ones included until the next sweep. */
    public synchronized int size() {
        return live.size();
    }
}
