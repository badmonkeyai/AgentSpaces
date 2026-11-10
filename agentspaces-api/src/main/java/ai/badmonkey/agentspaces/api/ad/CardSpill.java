/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.codec.Base32;
import ai.badmonkey.agentspaces.common.codec.WireRecord;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * The reference a stub card carries to its parts document on the content swarm (SPEC §6.1a.5, v0.1.32;
 * ISSUE-AppCardOriginFormats FR-12 to FR-15; AppCard origin and formats, 2026-11-09). A card whose canonical encoding
 * is more than {@link #SPILL_THRESHOLD_BYTES} moves the fields of its kind's spill set into one content-swarm artifact
 * and travels as a stub that names the artifact here; a reader fetches the parts when it needs them.
 *
 * @param root  the artifact's content root: {@code b}, then lower-case unpadded base32 of a CIDv1 with codec raw and a
 *              sha2-256 multihash, {@link #ROOT_LENGTH} characters in all
 * @param size  the parts document's length in bytes, 1 to {@link #MAX_SIZE}
 * @param parts the spill-set fields whose full value is non-empty, distinct and in the card's field order
 */
@WireRecord
public record CardSpill(String root, long size, List<String> parts) {

    /** A card whose encoding is more than this many bytes spills: the inline limit of a payload (SPEC §7.1). */
    public static final int SPILL_THRESHOLD_BYTES = 65_536;
    /** A publisher warns once a card's encoding passes this many bytes (56 KiB), just before it spills. */
    public static final int WARN_THRESHOLD_BYTES = 57_344;
    /** The piece size of a parts document, so every implementation computes the same root for the same bytes. */
    public static final int PIECE_SIZE = 65_536;
    /** The default bound on a parts document a reader fetches. */
    public static final long DEFAULT_MAX_FETCH_BYTES = 16_777_216L;
    /** The largest {@code size}: 2^53 - 1, the largest integer every implementation holds exactly. */
    public static final long MAX_SIZE = (1L << 53) - 1;
    /** The length of {@code root}. */
    public static final int ROOT_LENGTH = 59;

    /** The CIDv1 prefix of a content root: version 1, codec raw (0x55), multihash sha2-256 (0x12), 32 bytes (0x20). */
    private static final byte[] ROOT_PREFIX = {0x01, 0x55, 0x12, 0x20};
    private static final int ROOT_BYTES = ROOT_PREFIX.length + 32;

    public CardSpill {
        Objects.requireNonNull(root, "spill.root");
        Objects.requireNonNull(parts, "spill.parts");
        if (!isRoot(root)) {
            throw new IllegalArgumentException("spill.root is not a content root of codec raw over sha2-256: " + root);
        }
        if (size < 1 || size > MAX_SIZE) {
            throw new IllegalArgumentException("spill.size lies outside 1 to 2^53 - 1: " + size);
        }
        parts = List.copyOf(parts);
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("spill.parts names at least one field");
        }
        if (new HashSet<>(parts).size() != parts.size()) {
            throw new IllegalArgumentException("spill.parts repeats a name: " + parts);
        }
    }

    /**
     * Whether a text is a content root as {@code root} holds it: {@code b}, then the lower-case base32 (RFC 4648, no
     * padding, zero pad bits) of a CIDv1 with codec raw (0x55) and a sha2-256 multihash.
     *
     * @param text the text
     * @return true when it is such a root
     */
    public static boolean isRoot(String text) {
        if (text == null || text.length() != ROOT_LENGTH || text.charAt(0) != 'b') {
            return false;
        }
        String body = text.substring(1);
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (!(c >= 'a' && c <= 'z') && !(c >= '2' && c <= '7')) {
                return false;
            }
        }
        byte[] cid = Base32.decode(body);
        // the re-encoding equals the text only when the pad bits are zero
        return cid.length == ROOT_BYTES && Base32.encode(cid).equals(body)
                && Arrays.equals(Arrays.copyOf(cid, ROOT_PREFIX.length), ROOT_PREFIX);
    }

    /**
     * Checks {@code parts} against a kind's spill set: every name in the set, in the set's order (FR-14).
     *
     * @param spillSet the kind's spill set, in its field order
     * @throws IllegalArgumentException when a name lies outside the set or out of order
     */
    void requireWithin(List<String> spillSet) {
        int last = -1;
        for (String part : parts) {
            int at = spillSet.indexOf(part);
            if (at < 0) {
                throw new IllegalArgumentException("spill.parts names " + part + ", outside the spill set " + spillSet);
            }
            if (at <= last) {
                throw new IllegalArgumentException("spill.parts breaks the field order of " + spillSet + ": " + parts);
            }
            last = at;
        }
    }
}
