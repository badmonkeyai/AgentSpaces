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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.common.crypto.Digests;

/**
 * Hashes a key's canonical CBOR bytes to the 256-bit digest a CHAMP trie indexes
 * by (srad/agentspaces-pdt.md §3.2, PD-V-01): level {@code d} of the trie reads bits
 * {@code 5d} to {@code 5d+4} of the digest as a big-endian bit string. {@link #SHA256}
 * is the one hasher of the format; tests substitute one that forces collisions.
 */
@FunctionalInterface
public interface KeyHasher {

    /** The format's hasher: sha2-256 of the key bytes. */
    KeyHasher SHA256 = Digests::sha256;

    /**
     * The digest of a key.
     *
     * @param keyBytes the key's canonical CBOR
     * @return a 32-byte digest
     */
    byte[] hash(byte[] keyBytes);
}
