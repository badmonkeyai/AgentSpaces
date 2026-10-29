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

import java.util.Arrays;
import java.util.Objects;

/**
 * RFC 4648 base32 in the lower-case, unpadded form the multibase {@code b}
 * prefix names, which is what a CIDv1 string uses. Content roots in the content
 * swarm (srad/large-networks-bulk-data.md §3.1) are encoded this way so that an
 * artifact published here is nameable to IPFS-aware tooling by the same string.
 */
public final class Base32 {

    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz234567".toCharArray();
    private static final int[] DECODE = new int[128];

    static {
        Arrays.fill(DECODE, -1);
        for (int i = 0; i < ALPHABET.length; i++) {
            DECODE[ALPHABET[i]] = i;
            DECODE[Character.toUpperCase(ALPHABET[i])] = i;
        }
    }

    private Base32() {
    }

    /**
     * Encodes bytes as unpadded lower-case base32.
     *
     * @param input the bytes
     * @return the encoding, empty for empty input
     */
    public static String encode(byte[] input) {
        Objects.requireNonNull(input, "input");
        StringBuilder out = new StringBuilder((input.length * 8 + 4) / 5);
        int buffer = 0;
        int bits = 0;
        for (byte b : input) {
            buffer = (buffer << 8) | (b & 0xff);
            bits += 8;
            while (bits >= 5) {
                out.append(ALPHABET[(buffer >> (bits - 5)) & 0x1f]);
                bits -= 5;
            }
        }
        if (bits > 0) {
            out.append(ALPHABET[(buffer << (5 - bits)) & 0x1f]);
        }
        return out.toString();
    }

    /**
     * Decodes unpadded base32 in either case; trailing {@code =} padding is accepted and ignored.
     *
     * @param input the text
     * @return the bytes
     * @throws IllegalArgumentException on a character outside the alphabet
     */
    public static byte[] decode(String input) {
        Objects.requireNonNull(input, "input");
        int end = input.length();
        while (end > 0 && input.charAt(end - 1) == '=') {
            end--;
        }
        byte[] out = new byte[end * 5 / 8];
        int buffer = 0;
        int bits = 0;
        int n = 0;
        for (int i = 0; i < end; i++) {
            char c = input.charAt(i);
            int value = c < 128 ? DECODE[c] : -1;
            if (value < 0) {
                throw new IllegalArgumentException("not base32: '" + c + "' at " + i);
            }
            buffer = (buffer << 5) | value;
            bits += 5;
            if (bits >= 8) {
                out[n++] = (byte) ((buffer >> (bits - 8)) & 0xff);
                bits -= 8;
            }
        }
        return n == out.length ? out : Arrays.copyOf(out, n);
    }
}
