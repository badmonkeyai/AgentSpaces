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
package ai.badmonkey.agentspaces.common.crypto;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A bounded memory of the Ed25519 signatures one holder has verified (swap amplification, ROBUSTNESS.md §10.8). A
 * replica receives the same signed state or record many times: once by gossip, again by anti-entropy, again inside
 * a graft answer, and the record again with every later state of its entry. Each copy used to cost a full
 * verification. This memory answers the repeat from the first verification.
 *
 * <p>A remembered verification is keyed by the signer's raw key, the signature, and the SHA-256 of the signed bytes,
 * and only a signature that verified is remembered. A hit therefore means these exact bytes verified under this
 * exact key before. To pass bytes that were never verified, an attacker would need other bytes with the same SHA-256.
 * The memory holds at most its capacity, and the least recently used verification leaves first, so a flood of valid
 * signatures can only push honest ones out, which then cost one verification each again. The memory answers only
 * whether a signature is valid: revocation, admission, certificate windows and stamps stay with the caller, which
 * judges them on every copy.
 */
public final class VerifiedSignatures {

    /** The capacity a replica gives its memory: a few thousand states and records, about 200 bytes each. */
    public static final int DEFAULT_CAPACITY = 4096;

    private static final int KEY_LENGTH = Ed25519.RAW_PUBLIC_KEY_LENGTH;
    private static final int SIGNATURE_LENGTH = 64;
    private static final int DIGEST_LENGTH = 32;

    private final int capacity;
    private final Map<Remembered, Boolean> remembered;
    private long verified;
    private long hits;

    /**
     * Creates a memory.
     *
     * @param capacity how many verifications it keeps at most
     */
    public VerifiedSignatures(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.remembered = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Remembered, Boolean> eldest) {
                return size() > VerifiedSignatures.this.capacity;
            }
        };
    }

    /**
     * Verifies an Ed25519 signature against a raw 32-byte key, as {@link Ed25519#verifyRaw} does, answering from
     * memory when these exact bytes verified under this exact key and signature before.
     *
     * @param rawPublicKey the signer's raw 32-byte key
     * @param bytes        the signed bytes
     * @param signature    the signature
     * @return whether the signature is valid
     */
    public boolean verify(byte[] rawPublicKey, byte[] bytes, byte[] signature) {
        Objects.requireNonNull(rawPublicKey, "rawPublicKey");
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(signature, "signature");
        if (rawPublicKey.length != KEY_LENGTH || signature.length != SIGNATURE_LENGTH) {
            return Ed25519.verifyRaw(rawPublicKey, bytes, signature); // no such signature verifies; nothing to remember
        }
        Remembered key = new Remembered(rawPublicKey, signature, Digests.sha256(bytes));
        synchronized (remembered) {
            if (remembered.get(key) != null) {
                hits++;
                return true;
            }
        }
        boolean valid = Ed25519.verifyRaw(rawPublicKey, bytes, signature);
        synchronized (remembered) {
            verified++;
            if (valid) {
                remembered.put(key, Boolean.TRUE);
            }
        }
        return valid;
    }

    /** How many signatures this memory verified in full, valid or not. */
    public long verified() {
        synchronized (remembered) {
            return verified;
        }
    }

    /** How many verifications this memory answered from an earlier one. */
    public long hits() {
        synchronized (remembered) {
            return hits;
        }
    }

    /** How many verifications it holds now, at most its capacity. */
    public int size() {
        synchronized (remembered) {
            return remembered.size();
        }
    }

    /** The key, the signature and the digest of the signed bytes, end to end in one array. */
    private static final class Remembered {
        private final byte[] bytes = new byte[KEY_LENGTH + SIGNATURE_LENGTH + DIGEST_LENGTH];
        private final int hash;

        Remembered(byte[] key, byte[] signature, byte[] digest) {
            System.arraycopy(key, 0, bytes, 0, KEY_LENGTH);
            System.arraycopy(signature, 0, bytes, KEY_LENGTH, SIGNATURE_LENGTH);
            System.arraycopy(digest, 0, bytes, KEY_LENGTH + SIGNATURE_LENGTH, DIGEST_LENGTH);
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Remembered other && hash == other.hash && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
