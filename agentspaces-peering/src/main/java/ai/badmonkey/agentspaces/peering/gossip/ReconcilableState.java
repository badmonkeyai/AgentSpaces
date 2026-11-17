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
package ai.badmonkey.agentspaces.peering.gossip;

import java.util.Map;

/**
 * A piece of replicated state that participates in anti-entropy (spec §5.3). The
 * gossip bus periodically sends this state's {@link #digest()} to one random
 * partner; the partner answers with {@link #deltaFor(byte[])} computed against
 * that digest, and the asker applies it via {@link #applyDelta(byte[])}. Digest
 * and delta encodings are opaque to the bus and owned by the state.
 *
 * <p>Implementations must make {@code applyDelta} idempotent and commutative,
 * which CRDT-backed states are by construction.
 */
public interface ReconcilableState {

    /** Returns a compact summary of everything this replica holds. */
    byte[] digest();

    /**
     * The SHA-256 of {@link #digest()}: what a probe and the opening of a hashed anti-entropy
     * round carry per stream (SPEC §5.2, §5.3). The default hashes afresh; a state that caches its
     * digest should cache this beside it, so an unchanged state answers a probe without re-hashing
     * (reviews/IDLE-WORK-2026-11-13.md item 3). Callers must not modify the returned array.
     *
     * @return the 32-byte hash of the current digest
     */
    default byte[] digestHash() {
        return ai.badmonkey.agentspaces.common.crypto.Digests.sha256(digest());
    }

    /**
     * Computes the delta a remote replica is missing, given its digest.
     *
     * @param remoteDigest the remote replica's digest
     * @return delta bytes for {@link #applyDelta}; empty array when nothing is missing
     */
    byte[] deltaFor(byte[] remoteDigest);

    /**
     * Merges a delta produced by another replica's {@link #deltaFor}.
     *
     * @param delta the delta bytes
     */
    void applyDelta(byte[] delta);

    /**
     * Merges a delta, as {@link #applyDelta}, and says whether it changed anything here. The bus
     * counts a pulled delta as activity, which keeps anti-entropy at its base period, only when it
     * did: a delta whose every item was refused or already held is a partner re-offering what this
     * replica will not take, and treating it as news held a quiet group at full rate
     * (reviews/IDLE-WORK-2026-11-13.md item 8). The default applies the delta and answers
     * {@code true}, which is the behaviour before this method existed.
     *
     * @param delta the delta bytes
     * @return whether this replica's state changed
     */
    default boolean applyDeltaChanged(byte[] delta) {
        applyDelta(delta);
        return true;
    }

    /**
     * A remote replica opened a hashed anti-entropy round (SPEC §5.3, v0.1.17) whose hash
     * for this stream equals this replica's: the partner holds exactly what this replica
     * holds, which is the digest match a writer takes as evidence that its own entries
     * replicated (srad/agentspaces-pdt.md PD-L-01). The default ignores it.
     *
     * @param from the partner whose hash matched
     */
    default void matched(ai.badmonkey.agentspaces.common.id.PeerId from) {
    }

    // ---- the trie audit (SPEC annex I.6, v0.1.35; srad/replication-by-entry.md §3.2). Each default says the
    // state keeps the text digest alone, so a state that does not override them takes no part in an audit and the
    // bus treats its stream as it treats peers, ads and the revocation streams.

    /**
     * The stream hash of SPEC annex I.6 over this state's audited set, or null when the state keeps the text digest
     * alone. Toward a member whose card carries {@code wire.ae=2} it is the per-stream hash a probe and the opening of
     * a round carry for this stream. Callers must not modify the returned array.
     *
     * @return the 32-byte hash, or null
     */
    default byte[] streamHash() {
        return null;
    }

    /**
     * The audited node at a path (annex I.6, <em>A tree node</em>): its count and its preimage. Only asked of a state
     * whose {@link #streamHash()} is not null.
     *
     * @param path one nibble per byte, at most 64 bytes; the root's path is empty
     * @return the node
     */
    default AuditNode auditNode(byte[] path) {
        throw new UnsupportedOperationException("this state keeps the text digest alone");
    }

    /**
     * Every audited line under a path, in the order and the joining of a leaf's preimage, whatever the count there:
     * what an answer of {@code lines} carries when the asker's own count chose that form, and what the asker compares
     * an answer of lines with. Only asked of a state whose {@link #streamHash()} is not null.
     *
     * @param path one nibble per byte, at most 64 bytes
     * @return the joined lines as UTF-8, empty when there are none
     */
    default byte[] auditLines(byte[] path) {
        throw new UnsupportedOperationException("this state keeps the text digest alone");
    }

    /**
     * {@link #auditLines(byte[])} when the joined lines hold at most {@code maxBytes} bytes, and null otherwise: what
     * the partner reads when the asker's count chose {@code lines} and lines past the budget are answered by
     * {@code nodes} instead (annex I.6, step 4). A state that can stop gathering its lines once they pass the bound
     * overrides it, so that lines which are never sent are never all gathered and sorted; the default builds them
     * whole and measures them.
     *
     * @param path     one nibble per byte, at most 64 bytes
     * @param maxBytes the most bytes the joined lines may hold
     * @return the joined lines as UTF-8, or null when they hold more than {@code maxBytes}
     */
    default byte[] auditLines(byte[] path, long maxBytes) {
        byte[] lines = auditLines(path);
        return lines.length > maxBytes ? null : lines;
    }

    /**
     * Which of the given lines this state holds among its audited lines under a path: what an asker compares a
     * partner's answer of {@code lines} with (annex I.6, step 5). A state that keeps its lines by entry id overrides it
     * to read only the slot each line names, so the comparison costs what the answer holds and never this state's
     * whole set under the path: a member of many entries meets a partner of few every round, and building its own
     * lines to compare a root of a few cost it 0.34 s per round at 200,000 entries (TypeScript adversarial review of
     * 2026-11-15, ADV-TS-3). The default builds every line under the path. Only asked of a state whose
     * {@link #streamHash()} is not null.
     *
     * @param path  one nibble per byte, at most 64 bytes
     * @param lines the partner's lines
     * @return the lines held, a subset of {@code lines}
     */
    default java.util.Set<String> auditHeld(byte[] path, java.util.Collection<String> lines) {
        java.util.Set<String> mine = new java.util.HashSet<>(java.util.Arrays.asList(
                new String(auditLines(path), java.nio.charset.StandardCharsets.UTF_8).split("\n")));
        java.util.Set<String> held = new java.util.HashSet<>();
        for (String line : lines) {
            if (mine.contains(line)) {
                held.add(line);
            }
        }
        return held;
    }

    /**
     * The delta, in the format of {@link #deltaFor}, that carries the state and the claim this replica holds of each
     * named entry, for a fetch by {@code ids} (annex I.6, <em>The fetch</em>). An id not held is left out.
     *
     * @param ids the entry ids, distinct
     * @return the delta, empty when none is held
     */
    default byte[] statesFor(java.util.Collection<String> ids) {
        return new byte[0];
    }

    /**
     * The answer to a fetch by {@code ids} cut to a byte budget (annex I.6, <em>The fetch</em> and <em>The budget</em>;
     * srad/replication-by-entry.md RE-I-01, RE-I-02, RE-I-06): the states and claims this replica holds of the named
     * entries, in the order the annex gives, up to {@code budget} bytes of delta. The default answers
     * {@link #statesFor(java.util.Collection)} whole, which suits a state that keeps no trie and is never fetched from.
     *
     * @param ids    the entry ids, distinct
     * @param budget the bytes the delta may take; zero or less leaves room for nothing but a first item
     * @param first  whether the delta must carry its first item even when that item alone passes the budget, which is
     *               so when nothing else in the answer goes before it
     * @return the delta and the count of held states left out for the budget
     */
    default Bounded statesFor(java.util.Collection<String> ids, long budget, boolean first) {
        return new Bounded(statesFor(ids), 0);
    }

    /**
     * The answer to a text digest cut to a byte budget, as {@link #statesFor(java.util.Collection, long, boolean)} cuts
     * a fetch: what a budgeted get-space draws (annex I.6, <em>The budget</em>; RE-I-04). The default answers
     * {@link #deltaFor(byte[])} whole: the streams other than a space's are small, and a member that asked with a
     * budget reads an answer under it the same way.
     *
     * @param remoteDigest the remote replica's digest, empty for a whole space
     * @param budget       the bytes the delta may take
     * @param first        whether the delta must carry its first item even past the budget
     * @return the delta and the count of held states left out for the budget
     */
    default Bounded deltaFor(byte[] remoteDigest, long budget, boolean first) {
        return new Bounded(deltaFor(remoteDigest), 0);
    }

    /**
     * Merges a delta as {@link #applyDeltaChanged} does and names the entries it carried, states and claims alike,
     * whether this replica took them or refused them at its door: what an asker reads to tell a fetched id the partner
     * sent from one it left out (annex I.6, <em>The fetch</em>; RE-I-03). The default names nothing, which only a state
     * that is never fetched from may answer.
     *
     * @param delta the delta bytes
     * @return whether this replica changed, and the entry ids the delta carried
     */
    default Applied applyDeltaNamed(byte[] delta) {
        return new Applied(applyDeltaChanged(delta), java.util.Set.of());
    }

    /**
     * A delta under a budget.
     *
     * @param delta the delta bytes in the format of {@link #deltaFor}; empty when nothing went in
     * @param left  the states this replica held for the request and left out for the budget, what a
     *              {@code PULL_RESP}'s {@code remainder} counts
     */
    record Bounded(byte[] delta, long left) {
    }

    /**
     * A merged delta and the entries it carried.
     *
     * @param changed whether this replica's state changed
     * @param ids     the entry ids of every state and claim the delta carried
     */
    record Applied(boolean changed, java.util.Set<String> ids) {
    }

    /**
     * A partner's audit answered lines this replica compared with its own: each line names something the partner
     * holds, which is the evidence a text line gave before (srad/agentspaces-pdt.md PD-L-01). The default ignores it.
     *
     * @param from  the partner
     * @param lines the partner's lines under one path, joined as a leaf's preimage
     */
    default void linesSeen(ai.badmonkey.agentspaces.common.id.PeerId from, String lines) {
    }

    /**
     * A partner's audit showed a node equal to this replica's own: the partner holds exactly this replica's lines
     * under the path, the evidence of {@link #matched} for that part of the set. The default ignores it.
     *
     * @param from the partner
     * @param path the path, one nibble per byte
     */
    default void matchedUnder(ai.badmonkey.agentspaces.common.id.PeerId from, byte[] path) {
    }

    /**
     * The state's status figures (srad/replication-by-entry.md RE-Q-MNT-02): what a member's status page and a pacing
     * adjuster read per stream. Null, the default, for a state that keeps none; the bus then reports the stream's audit
     * counters alone. Meant for a status page: a state may take its lock and walk what it holds to answer.
     *
     * @return the figures, or null
     */
    default Figures figures() {
        return null;
    }

    /**
     * A stream's status figures.
     *
     * @param live                 entries a reader sees now
     * @param tombstones           entries known and not served: lapsed, withdrawn, or completed, until collected
     * @param tombstonesPastExpiry those tombstones by the time since their lease expired, in named buckets
     * @param trieDepth            how many nibbles deep the audit's trie runs, 0 for a stream kept as text alone
     */
    record Figures(long live, long tombstones, Map<String, Long> tombstonesPastExpiry, int trieDepth) {
        /** Copies the buckets. */
        public Figures {
            tombstonesPastExpiry = Map.copyOf(tombstonesPastExpiry);
        }
    }

    /**
     * One audited node as a state reports it (annex I.6): the count of audited slots under the path and its
     * preimage, the 384 bytes of an inner node or the joined lines of a leaf (empty for an empty node).
     *
     * @param count    the audited slots under the path
     * @param inner    whether the node is inner, answered by its 384-byte preimage
     * @param preimage the preimage; the caller must not modify it
     */
    record AuditNode(long count, boolean inner, byte[] preimage) {

        /** The 16-byte fingerprint a parent lists: the first 16 bytes of the preimage's SHA-256, zeros when empty. */
        public byte[] fingerprint() {
            if (count == 0) {
                return new byte[16];
            }
            return java.util.Arrays.copyOf(ai.badmonkey.agentspaces.common.crypto.Digests.sha256(preimage), 16);
        }
    }
}
