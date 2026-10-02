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

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * The AgentSpace: a typed, leased tuple space (spec §7). The API is the Linda
 * quartet plus events, all lease-aware. Implementations range from a single-JVM
 * {@code LocalSpace} to the CRDT-replicated space; the interface is identical, and
 * replication is orthogonal to application code.
 *
 * <p>Reads and takes match associatively via {@link Template}s and run against
 * local state, so they never block on the network (spec P7). Blocking variants wait
 * for a matching entry to arrive, up to a timeout, and suit virtual threads.
 *
 * <p>Thread-safety: implementations MUST be safe for concurrent use.
 */
public interface Space {

    /** Returns the space's name within its group. */
    String name();

    /** Returns the space's identifier. */
    SpaceId id();

    /**
     * The agent this handle attributes its writes to, when the implementation
     * knows it. Every entry written through this handle is signed and reported
     * by {@code readAllIssued} and {@code SpaceEvent} as coming from this agent,
     * whatever the entry itself declares.
     *
     * <p>Exposed so that a component holding both a handle and an identity can
     * check they agree before it writes something only the matching identity
     * could have written — a ballot names its voter, and a ballot whose declared
     * voter is not this agent is dropped as forged at every replica, including
     * the writer's own (QA4 A4-4). Empty when the implementation does not track
     * a single writer; a caller that cannot tell must not guess.
     *
     * @return the writing agent, or empty when unknown
     */
    default java.util.Optional<AgentId> writer() {
        return java.util.Optional.empty();
    }

    /**
     * Publishes an entry under a write lease. The lease is the entry's TTL: an
     * unrenewed entry vanishes when the lease lapses (spec P2).
     *
     * @param entry the entry to publish
     * @param lease the write lease
     * @param <T>   the entry type
     * @return a handle for renewing or cancelling the entry
     */
    <T> EntryHandle write(T entry, Lease lease);

    /**
     * Publishes an entry under a write lease, tagged for routing and sharding.
     * Tags are free-form key-value pairs carried on the entry record and covered
     * by the writer's signature; a replica configured with a tag shard stores
     * only entries whose tags match its shard (spec §7.5).
     *
     * @param entry the entry to publish
     * @param lease the write lease
     * @param tags  free-form tags, e.g. {@code region=eu}, {@code tier=gold}
     * @param <T>   the entry type
     * @return a handle for renewing or cancelling the entry
     */
    <T> EntryHandle write(T entry, Lease lease, java.util.Map<String, String> tags);

    /**
     * Non-destructive, non-blocking match against current local state.
     *
     * @param template the template to match
     * @param <T>      the entry type
     * @return a matching entry, if one exists now
     */
    <T> Optional<T> read(Template<T> template);

    /**
     * Non-destructive match, waiting up to the timeout for a matching entry.
     *
     * @param template the template to match
     * @param timeout  how long to wait for a match
     * @param <T>      the entry type
     * @return a matching entry, or empty when the timeout elapses first
     */
    <T> Optional<T> read(Template<T> template, Duration timeout);

    /**
     * Non-destructive match returning up to {@code limit} entries present now.
     *
     * @param template the template to match
     * @param limit    the maximum number of entries to return
     * @param <T>      the entry type
     * @return the matching entries, possibly empty
     */
    <T> List<T> readAll(Template<T> template, int limit);

    /**
     * One matched entry with its authenticated writer. In a replicated space the
     * issuer is authenticated by the entry record's signature, so a reader that
     * makes trust decisions per entry (a vote tally, an audit reader, a result
     * consumer) uses this rather than a self-declared field inside the entry.
     *
     * @param entry  the entry value
     * @param issuer the entry's authenticated writer
     * @param <T>    the entry type
     */
    record Issued<T>(T entry, AgentId issuer, Attestation attestation) {
        /** Peer-asserted attribution: the pre-subordinate-key shape, still the default. */
        public Issued(T entry, AgentId issuer) {
            this(entry, issuer, Attestation.PEER_ASSERTED);
        }
    }

    /**
     * How far an entry's issuer is proven (spec §4.2, QA4 A4-7). Every entry is
     * signed by a key that hashes to its issuer's peer, so the peer is always
     * authenticated; whether the <em>agent</em> name within that peer is proven
     * depends on how the record was signed.
     */
    enum Attestation {
        /** Signed by the peer key; the agent's local name is the peer's own assertion. */
        PEER_ASSERTED,
        /** Signed by the agent's own key under a certificate the peer issued for that name. */
        AGENT_ATTESTED
    }

    /**
     * Non-destructive match returning up to {@code limit} entries present now,
     * each paired with its authenticated writer.
     *
     * @param template the template to match
     * @param limit    the maximum number of entries to return
     * @param <T>      the entry type
     * @return the matching entries with their issuers, possibly empty
     */
    <T> List<Issued<T>> readAllIssued(Template<T> template, int limit);

    /**
     * Destructive, exclusive match under the space's conflict strategy (spec §7.4),
     * waiting up to the timeout for a matching entry. The returned take holds a
     * TAKE lease: if the taker dies or lets the lease lapse without
     * {@link #complete(TakenEntry) completing}, the entry reappears for other
     * takers. That reappearance is the crash-recovery idiom of the model.
     *
     * @param template  the template to match
     * @param takeLease the TAKE lease requested
     * @param timeout   how long to wait for a match
     * @param <T>       the entry type
     * @return the taken entry, or empty when the timeout elapses or the strategy
     *         awards the entry elsewhere
     */
    <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease, Duration timeout);

    /**
     * Consumes a taken entry permanently.
     *
     * @param taken the take to complete
     * @throws ai.badmonkey.agentspaces.api.error.LeaseExpiredException if the TAKE lease lapsed
     */
    void complete(TakenEntry<?> taken);

    /**
     * Consumes a taken entry permanently and atomically writes a result entry.
     *
     * @param taken       the take to complete
     * @param result      the result entry to write
     * @param resultLease the write lease for the result entry
     * @param <R>         the result entry type
     * @return a handle for the written result entry
     * @throws ai.badmonkey.agentspaces.api.error.LeaseExpiredException if the TAKE lease lapsed
     */
    <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease);

    /**
     * Registers a leased subscription for events on entries matching the template.
     * Delivery is at-least-once with entry-record deduplication; listeners receive
     * a foundation for complex event processing over the entry stream.
     *
     * @param template the template events must match
     * @param listener the listener to invoke
     * @param lease    the subscription's lease
     * @param <T>      the entry type
     * @return the subscription; closing or letting it lapse stops delivery
     */
    <T> Subscription notify(Template<T> template, SpaceListener<T> listener, Lease lease);
}
