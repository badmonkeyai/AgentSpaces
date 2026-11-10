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
package ai.badmonkey.agentspaces.peering.wire;

/**
 * Crafted CBOR for the stamp-item tests of HLC round four, 2026-11-08 (decision D3): the
 * canonical item of a stamp and the hostile items that stand in its place.
 */
public final class HostileStamps {

    private HostileStamps() {
    }

    /** The canonical CBOR item of an ASCII stamp text shorter than 24 bytes. */
    public static byte[] item(String text) {
        byte[] ascii = text.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        if (ascii.length >= 24) {
            throw new IllegalArgumentException("stamp text too long for a one-byte header: " + text);
        }
        return cat(new byte[]{(byte) (0x60 | ascii.length)}, ascii);
    }

    /**
     * The hostile items of decision D3 for the stamp {@code prefix + last}, where {@code last}
     * is one ASCII letter: overlong UTF-8 for that letter, indefinite-length text, tag 0, tag 32,
     * a byte string, and a non-shortest length header.
     */
    public static java.util.Map<String, byte[]> items(String prefix, String last) {
        byte[] text = (prefix + last).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        byte[] item = item(prefix + last);
        byte[] head = prefix.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        if (text.length + 1 >= 24) {
            throw new IllegalArgumentException("stamp text too long for a one-byte header: " + prefix + last);
        }
        int letter = last.charAt(0);
        java.util.Map<String, byte[]> items = new java.util.LinkedHashMap<>();
        items.put("overlong UTF-8 C1 81 in the node", cat(new byte[]{(byte) (0x60 | (head.length + 2))}, head,
                new byte[]{(byte) (0xC0 | (letter >> 6)), (byte) (0x80 | (letter & 0x3F))}));
        items.put("indefinite-length text", cat(new byte[]{0x7F}, item, new byte[]{(byte) 0xFF}));
        items.put("tag 0", cat(new byte[]{(byte) 0xC0}, item));
        items.put("tag 32", cat(new byte[]{(byte) 0xD8, 0x20}, item));
        items.put("a byte string", cat(new byte[]{(byte) (0x40 | text.length)}, text));
        items.put("a non-shortest length header", cat(new byte[]{0x78, (byte) text.length}, text));
        return items;
    }

    public static byte[] cat(byte[]... parts) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /** Replaces the one occurrence of {@code from} in {@code bytes}. */
    public static byte[] splice(byte[] bytes, byte[] from, byte[] to) {
        for (int i = 0; i + from.length <= bytes.length; i++) {
            if (java.util.Arrays.equals(bytes, i, i + from.length, from, 0, from.length)) {
                return cat(java.util.Arrays.copyOfRange(bytes, 0, i), to,
                        java.util.Arrays.copyOfRange(bytes, i + from.length, bytes.length));
            }
        }
        throw new AssertionError("the stamp item was not found");
    }
}
