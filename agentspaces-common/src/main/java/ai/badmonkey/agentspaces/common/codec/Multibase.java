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
package ai.badmonkey.agentspaces.common.codec;

import java.util.Objects;

/**
 * Minimal multibase support (the IPFS/multiformats convention of prefixing an
 * encoded string with a character naming its base). AgentSpaces identifiers use
 * base58btc, prefix {@code 'z'}, per §4.1 of the spec.
 */
public final class Multibase {

    /** The multibase prefix for base58btc. */
    public static final char BASE58BTC_PREFIX = 'z';

    private Multibase() {
    }

    /**
     * Encodes bytes as a base58btc multibase string ({@code 'z'} prefix).
     *
     * @param bytes the bytes to encode
     * @return the multibase string
     */
    public static String base58btc(byte[] bytes) {
        return BASE58BTC_PREFIX + Base58.encode(bytes);
    }

    /**
     * Decodes a multibase string. Only base58btc is supported in v0.1.
     *
     * @param encoded a multibase string with a supported prefix
     * @return the decoded bytes
     * @throws IllegalArgumentException if the prefix is missing or unsupported
     */
    public static byte[] decode(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.isEmpty() || encoded.charAt(0) != BASE58BTC_PREFIX) {
            throw new IllegalArgumentException(
                    "unsupported multibase prefix in: " + (encoded.isEmpty() ? "<empty>" : encoded));
        }
        return Base58.decode(encoded.substring(1));
    }
}
