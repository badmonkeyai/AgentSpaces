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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.common.codec.CborCodec;

import java.util.Optional;

/**
 * The pipe messages of {@code aspace:cap/content-swarm}
 * (srad/large-networks-bulk-data.md §3.3): CBOR maps with the fields in the
 * declared order and an integer tag {@code t} first, carried as the payload of
 * a {@code PIPE_DATA} frame. No frame kind is added.
 */
public final class Messages {

    public static final int PIECE_WANT = 1;
    public static final int PIECE = 2;
    public static final int HAVE = 3;
    public static final int CANCEL = 4;
    public static final int REJECT = 5;
    public static final int MANIFEST_WANT = 6;
    public static final int MANIFEST = 7;

    /** {@code REJECT} reasons. */
    public static final int REASON_NOT_HELD = 1;
    public static final int REASON_OVER_BUDGET = 2;
    public static final int REASON_POLICY = 3;
    public static final int REASON_UNKNOWN_ROOT = 4;

    /** A request for a range of a piece. */
    public record PieceWant(int t, String root, int index, int offset, int length, int req) {
        public PieceWant(String root, int index, int offset, int length, int req) {
            this(PIECE_WANT, root, index, offset, length, req);
        }
    }

    /** A range of a piece, answering a want. */
    public record Piece(int t, String root, int index, int offset, byte[] bytes, int req) {
        public Piece(String root, int index, int offset, byte[] bytes, int req) {
            this(PIECE, root, index, offset, bytes, req);
        }
    }

    /** An optional push of newly held pieces to current sources. */
    public record Have(int t, String root, byte[] have, boolean complete) {
        public Have(String root, byte[] have, boolean complete) {
            this(HAVE, root, have, complete);
        }
    }

    /** Withdraws a want. */
    public record Cancel(int t, String root, int index, int req) {
        public Cancel(String root, int index, int req) {
            this(CANCEL, root, index, req);
        }
    }

    /** Refuses a want, with a reason. */
    public record Reject(int t, String root, int index, int reason) {
        public Reject(String root, int index, int reason) {
            this(REJECT, root, index, reason);
        }
    }

    /** Asks a holder for the manifest of a root it advertises (§3.8). */
    public record ManifestWant(int t, String root) {
        public ManifestWant(String root) {
            this(MANIFEST_WANT, root);
        }
    }

    /** A manifest record, CBOR of the {@code Manifest} entry value. */
    public record ManifestReply(int t, String root, byte[] record) {
        public ManifestReply(String root, byte[] record) {
            this(MANIFEST, root, record);
        }
    }

    private record Tag(int t) {
    }

    private Messages() {
    }

    /**
     * Reads the tag of a message without decoding the rest.
     *
     * @param codec   the codec
     * @param payload the pipe payload
     * @return the tag, or empty when the payload is not a tagged map
     */
    public static Optional<Integer> tagOf(CborCodec codec, byte[] payload) {
        try {
            Tag tag = codec.fromBytes(payload, Tag.class);
            return tag == null ? Optional.empty() : Optional.of(tag.t());
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Decodes a message of a known tag.
     *
     * @param codec   the codec
     * @param payload the payload
     * @param type    the record type for the tag
     * @param <T>     the record type
     * @return the message, or empty when it does not decode
     */
    public static <T> Optional<T> decode(CborCodec codec, byte[] payload, Class<T> type) {
        try {
            return Optional.ofNullable(codec.fromBytes(payload, type));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }
}
