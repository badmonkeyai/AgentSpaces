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
}
