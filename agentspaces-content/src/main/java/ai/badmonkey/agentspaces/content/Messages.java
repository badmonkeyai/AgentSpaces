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
import ai.badmonkey.agentspaces.content.tree.Cids;

import java.util.List;
import java.util.Optional;

/**
 * The pipe messages of {@code aspace:cap/content-swarm}
 * (srad/large-networks-bulk-data.md §3.3): CBOR maps with the fields in the
 * declared order and an integer tag {@code t} first, carried as the payload of
 * a {@code PIPE_DATA} frame. No frame kind is added.
 *
 * <p>Tags 8 to 11 are the tree exchange of srad/agentspaces-pdt.md §3.5
 * (PD-X-01, PD-X-02): {@link NodeWant}, {@link Nodes}, {@link TreeWant}, and
 * {@link TreeEnd}. A peer sends them only to a member whose current
 * content-swarm advertisement carries {@code trees = "1"}, and every receiver
 * checks a message's {@code violation()} before acting on it and drops one
 * outside its bounds. {@link Reject} is reused for a tree request with
 * {@link #TREE_INDEX} as the index and the root (or the first CID of a
 * {@link NodeWant}) in {@code root}.
 */
public final class Messages {

    public static final int PIECE_WANT = 1;
    public static final int PIECE = 2;
    public static final int HAVE = 3;
    public static final int CANCEL = 4;
    public static final int REJECT = 5;
    public static final int MANIFEST_WANT = 6;
    public static final int MANIFEST = 7;
    /** Asks a holder for specific nodes by CID (PD-X-02). */
    public static final int NODE_WANT = 8;
    /** Answers a {@link #NODE_WANT} or a {@link #TREE_WANT} with node bytes (PD-X-02). */
    public static final int NODES = 9;
    /** Asks a holder for every node under a root that is absent under the haves roots (PD-X-02). */
    public static final int TREE_WANT = 10;
    /** Closes a {@link #TREE_WANT} with the frontier the holder did not send (PD-X-02). */
    public static final int TREE_END = 11;

    /** {@code REJECT} reasons. */
    public static final int REASON_NOT_HELD = 1;
    public static final int REASON_OVER_BUDGET = 2;
    public static final int REASON_POLICY = 3;
    public static final int REASON_UNKNOWN_ROOT = 4;

    /** The {@code index} a {@link Reject} carries when it refuses a tree request (PD-X-02). */
    public static final int TREE_INDEX = -1;

    /** {@code TREE_END} reasons (§3.5): the walk reached every node absent under the haves. */
    public static final int END_DONE = 1;
    /** The holder sent its budget of bytes and stopped. */
    public static final int END_BUDGET = 2;
    /** The holder holds none of the haves roots; it sent the top levels and the frontier. */
    public static final int END_HAVES_NOT_HELD = 3;
    /** The walk reached the holder's cap of {@link #MAX_WALK_NODES} nodes (PD-P-02). */
    public static final int END_WALK_CAP = 4;

    /** The most CIDs one {@link NodeWant} asks for. */
    public static final int MAX_WANT_CIDS = 256;
    /** The most bytes of nodes (or sealed nodes) one {@link Nodes} carries: the swarm's range bound. */
    public static final int MAX_NODES_BYTES = 4 * 1024 * 1024;
    /** The most haves roots one {@link TreeWant} names. */
    public static final int MAX_HAVES = 64;
    /** The largest {@link TreeWant} budget in bytes. */
    public static final long MAX_TREE_BUDGET = 64L * 1024 * 1024;
    /** The most frontier CIDs one {@link TreeEnd} carries. */
    public static final int MAX_FRONTIER = 1024;
    /** The most nodes one {@link TreeWant} makes a holder walk (PD-P-02). */
    public static final int MAX_WALK_NODES = 100_000;

    /**
     * The prefix of the AAD a holder seals a {@link Nodes} message under in
     * {@code group-key} mode (PD-P-03): {@code "aspace:pdt|" + recipient PeerId text + "|" + req}.
     */
    public static final String SEALING_AAD_PREFIX = "aspace:pdt|";

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

    /**
     * Asks a holder for specific nodes (srad/agentspaces-pdt.md §3.5, PD-X-02).
     *
     * @param t    {@link #NODE_WANT}
     * @param req  the request id the answer carries back
     * @param cids the CIDs wanted, at most {@link #MAX_WANT_CIDS}
     */
    public record NodeWant(int t, int req, List<String> cids) {
        public NodeWant(int req, List<String> cids) {
            this(NODE_WANT, req, cids);
        }

        /**
         * Checks the bounds a receiver enforces before serving.
         *
         * @return the first violated rule, or empty when the message is well formed
         */
        public Optional<String> violation() {
            if (t != NODE_WANT) {
                return Optional.of("tag " + t + " is not NODE_WANT");
            }
            if (cids == null || cids.isEmpty()) {
                return Optional.of("a NODE_WANT names at least one cid");
            }
            if (cids.size() > MAX_WANT_CIDS) {
                return Optional.of("a NODE_WANT names at most " + MAX_WANT_CIDS + " cids, got " + cids.size());
            }
            return badCid(cids, "cids");
        }
    }

    /**
     * Node bytes answering a {@link NodeWant} or a {@link TreeWant} (§3.5, PD-X-02,
     * PD-P-03). In a group without a content key ring the holder sends the nodes in
     * {@code nodes} and leaves {@code sealed} and {@code epoch} null. In a group with
     * a key ring the holder seals the canonical CBOR of the nodes list under the
     * current write epoch with AAD {@link #SEALING_AAD_PREFIX} + recipient PeerId
     * text + {@code "|"} + req, sends it in {@code sealed} with the epoch in
     * {@code epoch}, and leaves {@code nodes} null. Together the node bytes (or the
     * sealed bytes) stay within {@link #MAX_NODES_BYTES}.
     *
     * @param t       {@link #NODES}
     * @param req     the request this answers
     * @param nodes   the node bytes in plaintext form, or null when sealed
     * @param sealed  the sealed nodes list, or null in plaintext form
     * @param epoch   the content-key epoch of {@code sealed}, or null in plaintext form
     * @param missing CIDs of the request the holder does not hold, or null when it holds every one
     */
    public record Nodes(int t, int req, List<byte[]> nodes, byte[] sealed, Long epoch, List<String> missing) {
        public Nodes(int req, List<byte[]> nodes, byte[] sealed, Long epoch, List<String> missing) {
            this(NODES, req, nodes, sealed, epoch, missing);
        }

        /** A plaintext answer. */
        public static Nodes plain(int req, List<byte[]> nodes, List<String> missing) {
            return new Nodes(req, nodes, null, null, missing);
        }

        /** A sealed answer. */
        public static Nodes sealed(int req, byte[] sealed, long epoch, List<String> missing) {
            return new Nodes(req, null, sealed, epoch, missing);
        }

        /**
         * Whether this answer carries plaintext nodes; false means sealed. Named
         * without a {@code get}/{@code is} prefix so Jackson never treats it as a
         * property of the wire record.
         */
        public boolean plaintext() {
            return sealed == null;
        }

        /**
         * Checks the bounds a receiver enforces before verifying any node.
         *
         * @return the first violated rule, or empty when the message is well formed
         */
        public Optional<String> violation() {
            if (t != NODES) {
                return Optional.of("tag " + t + " is not NODES");
            }
            if ((nodes == null) == (sealed == null)) {
                return Optional.of("a NODES carries exactly one of nodes and sealed");
            }
            if (sealed != null && epoch == null) {
                return Optional.of("a sealed NODES names its epoch");
            }
            if (sealed == null && epoch != null) {
                return Optional.of("a plaintext NODES carries no epoch");
            }
            if (epoch != null && epoch < 0) {
                return Optional.of("negative epoch " + epoch);
            }
            long bytes = 0;
            if (nodes != null) {
                for (byte[] node : nodes) {
                    if (node == null) {
                        return Optional.of("a null node");
                    }
                    bytes += node.length;
                }
            } else {
                bytes = sealed.length;
            }
            if (bytes > MAX_NODES_BYTES) {
                return Optional.of("a NODES carries at most " + MAX_NODES_BYTES + " bytes of nodes, got " + bytes);
            }
            if (missing != null) {
                if (missing.size() > MAX_WANT_CIDS) {
                    return Optional.of("a NODES names at most " + MAX_WANT_CIDS + " missing cids, got " + missing.size());
                }
                return badCid(missing, "missing");
            }
            return Optional.empty();
        }
    }

    /**
     * Asks a holder for every node under {@code root} that is absent under any root
     * in {@code haves}, breadth first, up to {@code budget} bytes (§3.5, PD-X-02, PD-X-03).
     *
     * @param t      {@link #TREE_WANT}
     * @param req    the request id the answers carry back
     * @param root   the root to walk
     * @param haves  roots the asker holds complete, at most {@link #MAX_HAVES}; may be empty
     * @param budget the most bytes of nodes the holder sends, at most {@link #MAX_TREE_BUDGET}
     * @param sketch reserved for a set sketch of the asker's holdings (PD-F-03); null, and ignored when present
     */
    public record TreeWant(int t, int req, String root, List<String> haves, long budget, byte[] sketch) {
        public TreeWant(int req, String root, List<String> haves, long budget) {
            this(TREE_WANT, req, root, haves, budget, null);
        }

        /**
         * Checks the bounds a receiver enforces before walking.
         *
         * @return the first violated rule, or empty when the message is well formed
         */
        public Optional<String> violation() {
            if (t != TREE_WANT) {
                return Optional.of("tag " + t + " is not TREE_WANT");
            }
            if (!Cids.isCid(root)) {
                return Optional.of("root is not a CID: " + root);
            }
            if (haves != null) {
                if (haves.size() > MAX_HAVES) {
                    return Optional.of("a TREE_WANT names at most " + MAX_HAVES + " haves, got " + haves.size());
                }
                Optional<String> bad = badCid(haves, "haves");
                if (bad.isPresent()) {
                    return bad;
                }
            }
            if (budget <= 0 || budget > MAX_TREE_BUDGET) {
                return Optional.of("budget " + budget + " is not in (0, " + MAX_TREE_BUDGET + "]");
            }
            return Optional.empty();
        }
    }

    /**
     * Closes a {@link TreeWant} (§3.5, PD-X-02): the holder sent what it could, and
     * {@code frontier} lists the CIDs it reached and did not send.
     *
     * @param t        {@link #TREE_END}
     * @param req      the request this closes
     * @param frontier CIDs not sent, at most {@link #MAX_FRONTIER}; empty when done
     * @param reason   {@link #END_DONE}, {@link #END_BUDGET}, {@link #END_HAVES_NOT_HELD}, or {@link #END_WALK_CAP}
     */
    public record TreeEnd(int t, int req, List<String> frontier, int reason) {
        public TreeEnd(int req, List<String> frontier, int reason) {
            this(TREE_END, req, frontier, reason);
        }

        /**
         * Checks the bounds a receiver enforces before continuing from the frontier.
         *
         * @return the first violated rule, or empty when the message is well formed
         */
        public Optional<String> violation() {
            if (t != TREE_END) {
                return Optional.of("tag " + t + " is not TREE_END");
            }
            if (frontier == null) {
                return Optional.of("a TREE_END carries a frontier list");
            }
            if (frontier.size() > MAX_FRONTIER) {
                return Optional.of("a TREE_END names at most " + MAX_FRONTIER + " frontier cids, got " + frontier.size());
            }
            if (reason < END_DONE || reason > END_WALK_CAP) {
                return Optional.of("unknown TREE_END reason " + reason);
            }
            return badCid(frontier, "frontier");
        }
    }

    private static Optional<String> badCid(List<String> cids, String field) {
        for (String cid : cids) {
            if (!Cids.isCid(cid)) {
                return Optional.of(field + " holds a value that is not a CID: " + cid);
            }
        }
        return Optional.empty();
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
     * Reads the tag from the first bytes of a payload without decoding anything:
     * every message encodes as a CBOR map whose first key is {@code t} with an
     * unsigned value, so a dispatcher can route a frame by tag before handing it
     * to a thread. Falls back to {@link #tagOf} when the bytes take another shape.
     *
     * @param codec   the codec, for the fallback
     * @param payload the pipe payload
     * @return the tag, or empty when the payload is not a tagged map
     */
    public static Optional<Integer> peekTag(CborCodec codec, byte[] payload) {
        if (payload != null && payload.length >= 4) {
            int head = payload[0] & 0xff;
            boolean map = (head >= 0xa1 && head <= 0xb7) || head == 0xbf;
            if (map && payload[1] == 0x61 && payload[2] == 't') {
                int value = payload[3] & 0xff;
                if (value <= 0x17) {
                    return Optional.of(value);
                }
                if (value == 0x18 && payload.length >= 5) {
                    return Optional.of(payload[4] & 0xff);
                }
            }
        }
        return tagOf(codec, payload);
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
