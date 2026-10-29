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

    /** The multibase prefix for lower-case unpadded base32 (RFC 4648), the CIDv1 default. */
    public static final char BASE32_PREFIX = 'b';

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
     * Encodes bytes as a base32 multibase string ({@code 'b'} prefix, lower case,
     * unpadded), the form a CIDv1 takes and the form a content root uses.
     *
     * @param bytes the bytes to encode
     * @return the multibase string
     */
    public static String base32(byte[] bytes) {
        return BASE32_PREFIX + Base32.encode(bytes);
    }

    /**
     * Decodes a multibase string. Base58btc ({@code z}) and base32 ({@code b})
     * are supported; every other prefix is refused.
     *
     * @param encoded a multibase string with a supported prefix
     * @return the decoded bytes
     * @throws IllegalArgumentException if the prefix is missing or unsupported
     */
    public static byte[] decode(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.isEmpty()) {
            throw new IllegalArgumentException("unsupported multibase prefix in: <empty>");
        }
        return switch (encoded.charAt(0)) {
            case BASE58BTC_PREFIX -> Base58.decode(encoded.substring(1));
            case BASE32_PREFIX -> Base32.decode(encoded.substring(1));
            default -> throw new IllegalArgumentException("unsupported multibase prefix in: " + encoded);
        };
    }
}
