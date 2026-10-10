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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ISSUE-CanonicalMaps: a map's entries are encoded in RFC 8949 §4.2.1 order
 * (shorter UTF-8 key first, then bytewise), whatever order the map was built in,
 * and a record's components keep their declared order.
 */
class CborCodecCanonicalMapsTest {

    record Holder(String zeta, Map<String, String> tags, String alpha) {
    }

    private final CborCodec codec = CborCodec.defaultCodec();

    private static Map<String, String> ordered(String... kv) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map;
    }

    @Test
    void aMapBuiltInTwoOrdersEncodesToTheSameBytes() {
        byte[] one = codec.toBytes(new Holder("z", ordered("region", "eu", "claimId", "c-1", "key", "k"), "a"));
        byte[] two = codec.toBytes(new Holder("z", ordered("key", "k", "claimId", "c-1", "region", "eu"), "a"));
        assertThat(one).isEqualTo(two);
    }

    @Test
    void keysSortShorterFirstThenBytewise() {
        byte[] bytes = codec.toBytes(ordered("bb", "1", "a", "2", "ab", "3", "ccc", "4", "b", "5"));
        assertThat(keysOf(bytes)).containsExactly("a", "b", "ab", "bb", "ccc");
    }

    @Test
    void aNonBmpKeySortsByItsUtf8BytesNotItsUtf16Units() {
        // U+FF5E (3 UTF-8 bytes, EF BD 9E) and U+1F600 (4 bytes, F0 9F 98 80): by
        // UTF-16 units the emoji's high surrogate D83D sorts first; by RFC 8949 the
        // shorter encoding sorts first. And between two 3-byte keys, bytes decide.
        String fullwidthTilde = "～";
        String grinning = new String(Character.toChars(0x1F600));
        byte[] bytes = codec.toBytes(ordered(grinning, "1", fullwidthTilde, "2", "éa", "3"));
        assertThat(keysOf(bytes)).containsExactly("éa", fullwidthTilde, grinning);
    }

    @Test
    void recordComponentsKeepDeclaredOrder() {
        byte[] bytes = codec.toBytes(new Holder("z", Map.of(), "a"));
        assertThat(keysOf(bytes)).containsExactly("zeta", "tags", "alpha");
    }

    @Test
    void decodingKeepsWireOrderAndRoundTrips() {
        Holder holder = new Holder("z", ordered("region", "eu", "b", "x"), "a");
        Holder decoded = codec.fromBytes(codec.toBytes(holder), Holder.class);
        assertThat(decoded.tags()).containsExactly(Map.entry("b", "x"), Map.entry("region", "eu"));
        assertThat(codec.toBytes(decoded)).isEqualTo(codec.toBytes(holder));
    }

    /** The top-level map's keys in encoded order; values are short text strings here. */
    private static List<String> keysOf(byte[] cbor) {
        List<String> keys = new ArrayList<>();
        int i = 0;
        int head = cbor[i++] & 0xFF;
        boolean indefinite = head == 0xBF;
        int remaining = indefinite ? Integer.MAX_VALUE : head & 0x1F;
        while (remaining-- > 0) {
            if ((cbor[i] & 0xFF) == 0xFF) {
                break;
            }
            int[] key = text(cbor, i);
            keys.add(new String(cbor, key[0], key[1], StandardCharsets.UTF_8));
            i = key[0] + key[1];
            i = skip(cbor, i);
        }
        return keys;
    }

    /** Returns {start, length} of the text string at {@code i}. */
    private static int[] text(byte[] cbor, int i) {
        int info = cbor[i] & 0x1F;
        if (info < 24) {
            return new int[]{i + 1, info};
        }
        if (info == 24) {
            return new int[]{i + 2, cbor[i + 1] & 0xFF};
        }
        throw new IllegalStateException("long key in a test map");
    }

    /** Skips one value: text, an empty map, or a short definite/indefinite map. */
    private static int skip(byte[] cbor, int i) {
        int b = cbor[i] & 0xFF;
        int major = b >> 5;
        if (major == 3) {
            int[] t = text(cbor, i);
            return t[0] + t[1];
        }
        if (b == 0xBF) {
            i++;
            while ((cbor[i] & 0xFF) != 0xFF) {
                i = skip(cbor, skip(cbor, i));
            }
            return i + 1;
        }
        if (major == 5) {
            int n = b & 0x1F;
            i++;
            for (int k = 0; k < n; k++) {
                i = skip(cbor, skip(cbor, i));
            }
            return i;
        }
        throw new IllegalStateException("unexpected CBOR head " + Integer.toHexString(b));
    }
}
