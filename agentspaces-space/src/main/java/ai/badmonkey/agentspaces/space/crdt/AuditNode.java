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
package ai.badmonkey.agentspaces.space.crdt;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * One audited node at a path, as annex I.6 defines it: empty, a leaf with its lines, or an inner node with its
 * 384-byte preimage. The preimage is what an answer carries ({@code lines} as UTF-8 text, {@code nodes} as bytes),
 * and its first 16 SHA-256 bytes are the fingerprint the parent lists.
 */
public final class AuditNode {

    /** What kind of node the path holds. */
    public enum Kind {
        /** No audited slot lies under the path; the parent lists it as 16 zero bytes and the count 0. */
        EMPTY,
        /** At most the threshold of audited slots, or the deepest path: answered by its lines. */
        LEAF,
        /** More than the threshold: answered by its sixteen children's fingerprints and counts. */
        INNER
    }

    private final byte[] path;
    private final Kind kind;
    private final long count;
    private final byte[] preimage;

    private AuditNode(byte[] path, Kind kind, long count, byte[] preimage) {
        this.path = path;
        this.kind = kind;
        this.count = count;
        this.preimage = preimage;
    }

    static AuditNode empty(byte[] path) {
        return new AuditNode(path, Kind.EMPTY, 0, new byte[0]);
    }

    static AuditNode leaf(byte[] path, long count, byte[] lines) {
        return new AuditNode(path, Kind.LEAF, count, lines);
    }

    static AuditNode inner(byte[] path, long count, byte[] nodes) {
        return new AuditNode(path, Kind.INNER, count, nodes);
    }

    /** Returns a copy of the path, one nibble per byte. */
    public byte[] path() {
        return path.clone();
    }

    /** Returns the kind of node. */
    public Kind kind() {
        return kind;
    }

    /** Returns the number of audited slots under the path. */
    public long count() {
        return count;
    }

    /** Returns a copy of the preimage: the joined lines of a leaf, the 384 bytes of an inner node, none when empty. */
    public byte[] preimage() {
        return preimage.clone();
    }

    /** Returns a leaf's lines as text, the {@code lines} of an answer; the empty text for an empty node. */
    public String lines() {
        if (kind == Kind.INNER) {
            throw new IllegalStateException("an inner node is answered by its nodes, not lines");
        }
        return new String(preimage, StandardCharsets.UTF_8);
    }

    /** Returns the 16-byte fingerprint: 16 zero bytes for an empty node, as a parent lists it. */
    public byte[] fingerprint() {
        if (kind == Kind.EMPTY) {
            return new byte[HashTrie.FINGERPRINT_BYTES];
        }
        return Arrays.copyOf(Slot.sha256().digest(preimage), HashTrie.FINGERPRINT_BYTES);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (byte b : path) {
            sb.append(Character.forDigit(b, 16));
        }
        return "AuditNode[" + (sb.isEmpty() ? "(root)" : sb.toString()) + ", " + kind + ", count=" + count + "]";
    }
}
