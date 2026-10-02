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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.util.Objects;

/**
 * HKDF (RFC 5869) with HMAC-SHA-256: extract-then-expand key derivation. The
 * sealed group-key wrap derives its AES key-encryption key from the X25519
 * shared secret through this function, binding the derivation to both parties'
 * public keys through the salt.
 */
public final class Hkdf {

    private static final String HMAC = "HmacSHA256";
    private static final int HASH_LENGTH = 32;

    private Hkdf() {
    }

    /**
     * Derives key material.
     *
     * @param ikm    the input keying material (a shared secret)
     * @param salt   the salt; MAY be empty, SHOULD bind the context's identities
     * @param info   the application- and use-specific label
     * @param length the number of bytes to derive, at most 255 * 32
     * @return the derived bytes
     */
    public static byte[] derive(byte[] ikm, byte[] salt, byte[] info, int length) {
        Objects.requireNonNull(ikm, "ikm");
        Objects.requireNonNull(salt, "salt");
        Objects.requireNonNull(info, "info");
        if (length <= 0 || length > 255 * HASH_LENGTH) {
            throw new IllegalArgumentException("length out of range: " + length);
        }
        try {
            // Extract: PRK = HMAC(salt, IKM).
            Mac mac = Mac.getInstance(HMAC);
            mac.init(new SecretKeySpec(
                    salt.length == 0 ? new byte[HASH_LENGTH] : salt, HMAC));
            byte[] prk = mac.doFinal(ikm);

            // Expand: T(i) = HMAC(PRK, T(i-1) || info || i).
            byte[] output = new byte[length];
            byte[] block = new byte[0];
            int offset = 0;
            for (int i = 1; offset < length; i++) {
                mac.init(new SecretKeySpec(prk, HMAC));
                mac.update(block);
                mac.update(info);
                mac.update((byte) i);
                block = mac.doFinal();
                int take = Math.min(block.length, length - offset);
                System.arraycopy(block, 0, output, offset, take);
                offset += take;
            }
            return output;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA-256 unavailable", e);
        }
    }
}
