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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.BitSet;
import java.util.Optional;
import java.util.Set;

/**
 * One backend of the piece store (srad/large-networks-bulk-data.md §3.5). A peer
 * composes several through {@link CompositeStore}: reads consult them in order,
 * writes go to the first writable one, and holdings derive from what every
 * backend reports. A backend keeps the manifest of each root it holds, so a
 * restart republishes what it has without a transfer.
 */
public interface PieceStore {

    /** The backend kind: {@code memory}, {@code file}, {@code in-place}, or a host-provided name. */
    String kind();

    /** Whether {@link #write} is allowed. */
    boolean writable();

    /** The roots this backend holds any piece of. */
    Set<String> roots();

    /** The manifest of a root this backend holds, when it holds one. */
    Optional<Manifest> manifest(String root);

    /**
     * Prepares a root for writes or registers it as held in full elsewhere.
     *
     * @param manifest the manifest
     */
    void open(Manifest manifest);

    /** The verified pieces of a root; empty when unknown. */
    BitSet have(String root);

    /**
     * Reads a range of a verified piece.
     *
     * @param root   the root
     * @param index  the piece
     * @param offset the offset within the piece
     * @param length the bytes wanted
     * @return the bytes, or empty when the piece is not held here
     */
    Optional<byte[]> read(String root, int index, int offset, int length);

    /**
     * Stores a verified piece. Callers verify first; a store never hashes.
     *
     * @param root  the root
     * @param index the piece
     * @param piece the bytes
     * @throws IllegalStateException when the backend is not writable or the root is not open
     */
    void write(String root, int index, byte[] piece);

    /**
     * Opens the complete artifact for reading.
     *
     * @param root the root
     * @return a read-only channel over the artifact
     * @throws IOException when the artifact is incomplete or cannot be opened
     */
    SeekableByteChannel openArtifact(String root) throws IOException;

    /** Marks a root as exempt from eviction. */
    void pin(String root);

    /** Lifts a {@link #pin}. */
    void unpin(String root);

    /** Whether a root is pinned, external, or otherwise exempt from eviction. */
    boolean pinned(String root);

    /** Bytes held by this backend. */
    long bytesUsed();

    /** This backend's byte quota; zero or less means unbounded. */
    long quota();

    /** Evicts unpinned roots, least recently served first, until within quota. */
    void evict();

    /** Drops a root and its pieces. */
    void forget(String root);

    /** Forces unpersisted state to durable storage; a no-op for a memory backend. */
    default void flush() {
    }

    /**
     * Whether this backend serves bytes the swarm did not write and verify itself, such as files
     * in place or objects behind a remote service. The swarm hashes each piece of an external
     * backend whole at its first serve and forgets the root on a mismatch (LB-T-10); it skips
     * external roots in its sampled check on start, since reading them may cost a remote call.
     *
     * @return true for an external backend; false by default
     */
    default boolean external() {
        return false;
    }
}
