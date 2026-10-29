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
package ai.badmonkey.agentspaces.peering.blocks;

import java.util.Optional;
import java.util.Set;

/**
 * Where a {@link BlockExchange} keeps its blocks (srad/large-networks-bulk-data.md LB-B-05):
 * the memory store of 0.4.0, or a file-backed one that survives a restart. The exchange
 * owns the quota, the references, and the eviction order; a store keeps bytes by CID and
 * the last time each was served.
 */
public interface BlockStore {

    /**
     * Reads a block and marks it served.
     *
     * @param cid the CID
     * @param nowMillis the clock
     * @return the bytes, when held
     */
    Optional<byte[]> get(String cid, long nowMillis);

    /**
     * Stores a block unless it is already held, in which case it is marked served.
     *
     * @param cid       the CID
     * @param bytes     the bytes; the store keeps its own copy
     * @param nowMillis the clock
     * @return whether the block was new to the store
     */
    boolean put(String cid, byte[] bytes, long nowMillis);

    /**
     * Drops a block.
     *
     * @param cid the CID
     * @return the bytes freed, or -1 when the block was not held
     */
    long remove(String cid);

    /** The CIDs held. */
    Set<String> cids();

    /** When a block was last served or stored, or -1 when not held. */
    long lastAccessMillis(String cid);

    /** How many blocks are held. */
    int size();

    /** The bytes held. */
    long bytesUsed();

    /** The store kind: {@code memory} or {@code file}. */
    String kind();
}
