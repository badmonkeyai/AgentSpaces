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
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;
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
 *
 * <p>A receiver reads every message through {@link #decode}, which applies the
 * wire encoding of SPEC v0.1.31 §9.1 (PDT 2 phase 1, P2-X-09): a message that
 * is not its record's own encoding is dropped, counted, and strikes nobody. The
 * {@code root} of every piece message (tags 1 to 7), the {@code bytes} of a
 * {@link Piece} and the {@code record} of a {@link ManifestReply} are never null
 * (annex G), so a message that carries a null there does not decode either.
 *
 * <p>The level-2 forms of srad/agentspaces-pdt2.md §6.10 (PDT 2 phase 3, P2-F-05, P2-F-07): tag 12
 * {@link Challenge}, the appended {@code deltas} of {@link Nodes} and {@code sketch} of {@link TreeEnd},
 * and the sketch {@link TreeWant} ({@code root} {@code ""}). A peer sends them only to a member whose
 * content-swarm advertisement carries {@code pdt} at level 2 or above, and a writer omits an appended
 * field when it is null, so a message without it keeps its level-1 bytes.
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
    /**
     * Asks a holder to prove it holds nodes of a root (PDT 2 phase 3, P2-S-07): a level-2 form, sent
     * only to a member at feature level 2, answered with {@link #NODES} for the picks held and
     * {@link #REJECT} reason {@link #REASON_NOT_HELD} when it holds none.
     */
    public static final int CHALLENGE = 12;

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
    /** A reconciliation round answering a sketch {@link TreeWant}: an empty frontier and the reply in {@code sketch} (rule 13). */
    public static final int END_SKETCH = 5;

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
    /** The most picks one {@link Challenge} names (§3.2.4). */
    public static final int MAX_PICKS = 16;

    /**
     * The prefix of the AAD a holder seals a {@link Nodes} message under in
     * {@code group-key} mode (PD-P-03): {@code "aspace:pdt|" + recipient PeerId text + "|" + req}.
     */
    public static final String SEALING_AAD_PREFIX = "aspace:pdt|";

    /** A request for a range of a piece. */
    public record PieceWant(int t, String root, int index, int offset, int length, int req) {
        public PieceWant {
            Objects.requireNonNull(root, "root");
        }

        public PieceWant(String root, int index, int offset, int length, int req) {
            this(PIECE_WANT, root, index, offset, length, req);
        }
    }

    /** A range of a piece, answering a want. */
    public record Piece(int t, String root, int index, int offset, byte[] bytes, int req) {
        public Piece {
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(bytes, "bytes");
        }

        public Piece(String root, int index, int offset, byte[] bytes, int req) {
            this(PIECE, root, index, offset, bytes, req);
        }
    }

    /** An optional push of newly held pieces to current sources. */
    public record Have(int t, String root, byte[] have, boolean complete) {
        public Have {
            Objects.requireNonNull(root, "root");
        }

        public Have(String root, byte[] have, boolean complete) {
            this(HAVE, root, have, complete);
        }
    }

    /** Withdraws a want. */
    public record Cancel(int t, String root, int index, int req) {
        public Cancel {
            Objects.requireNonNull(root, "root");
        }

        public Cancel(String root, int index, int req) {
            this(CANCEL, root, index, req);
        }
    }

    /** Refuses a want, with a reason. */
    public record Reject(int t, String root, int index, int reason) {
        public Reject {
            Objects.requireNonNull(root, "root");
        }

        public Reject(String root, int index, int reason) {
            this(REJECT, root, index, reason);
        }
    }

    /** Asks a holder for the manifest of a root it advertises (§3.8). */
    public record ManifestWant(int t, String root) {
        public ManifestWant {
            Objects.requireNonNull(root, "root");
        }

        public ManifestWant(String root) {
            this(MANIFEST_WANT, root);
        }
    }

    /** A manifest record, CBOR of the {@code Manifest} entry value. */
    public record ManifestReply(int t, String root, byte[] record) {
        public ManifestReply {
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(record, "record");
        }

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
     * @param deltas  level-2 intra-chunk deltas (P2-Z-01), appended and omitted when null, and only in a plaintext
     *                answer; a receiver that cannot apply one asks for its {@code cid} by {@link NodeWant}
     */
    public record Nodes(int t, int req, List<byte[]> nodes, byte[] sealed, Long epoch, List<String> missing,
                        @JsonInclude(JsonInclude.Include.NON_NULL) List<NodeDelta> deltas) {

        /** The six fields of level 1. */
        public Nodes(int t, int req, List<byte[]> nodes, byte[] sealed, Long epoch, List<String> missing) {
            this(t, req, nodes, sealed, epoch, missing, null);
        }

        public Nodes(int req, List<byte[]> nodes, byte[] sealed, Long epoch, List<String> missing) {
            this(NODES, req, nodes, sealed, epoch, missing, null);
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
            if (sealed != null && deltas != null) {
                // PDT 2 phase 4 measures (§10.14): frames travel in plaintext answers only, and every port refuses the pair
                return Optional.of("a sealed NODES carries no deltas");
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
            if (deltas != null) {
                if (deltas.size() > MAX_WANT_CIDS) {
                    return Optional.of("a NODES carries at most " + MAX_WANT_CIDS + " deltas, got " + deltas.size());
                }
                for (NodeDelta delta : deltas) {
                    if (delta == null || !Cids.isCid(delta.cid()) || !Cids.isCid(delta.base()) || delta.dcz() == null) {
                        return Optional.of("deltas holds an entry without a CID, a base CID, and a frame");
                    }
                    bytes += delta.dcz().length;
                }
                if (bytes > MAX_NODES_BYTES) {
                    return Optional.of("a NODES carries at most " + MAX_NODES_BYTES + " bytes of nodes and deltas, got " + bytes);
                }
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
     * One intra-chunk delta of a level-2 {@link Nodes} (srad/agentspaces-pdt2.md §6.10.4, P2-Z-01): the
     * chunk {@code cid} as a {@code dcz} frame over the chunk {@code base}, in the format of
     * {@link ai.badmonkey.agentspaces.content.tree.Dcz} (PDT 2 phase 4). A receiver rebuilds the chunk, verifies it
     * against {@code cid}, and stores the chunk; one that cannot asks for {@code cid} by {@link NodeWant}.
     *
     * @param cid  the chunk the frame rebuilds
     * @param base the chunk the frame is relative to
     * @param dcz  the frame bytes
     */
    public record NodeDelta(String cid, String base, byte[] dcz) {
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
     * @param sketch a set sketch of the asker's holdings (PD-F-03, P2-K-02): with {@code root} {@code ""} and
     *               no haves the want is a reconciliation round (rule 12), a level-2 form; ignored beside a root
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
            if ("".equals(root) && sketch != null) {
                if (haves != null && !haves.isEmpty()) {
                    return Optional.of("a sketch TREE_WANT names no haves");
                }
                return budget <= 0 || budget > MAX_TREE_BUDGET
                        ? Optional.of("budget " + budget + " is not in (0, " + MAX_TREE_BUDGET + "]") : Optional.empty();
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

        /** Whether this is a sketch want, a reconciliation round with {@code root} {@code ""} (rule 12). */
        public boolean reconciles() {
            return "".equals(root) && sketch != null;
        }
    }

    /**
     * Closes a {@link TreeWant} (§3.5, PD-X-02): the holder sent what it could, and
     * {@code frontier} lists the CIDs it reached and did not send.
     *
     * @param t        {@link #TREE_END}
     * @param req      the request this closes
     * @param frontier CIDs not sent, at most {@link #MAX_FRONTIER}; empty when done
     * @param reason   {@link #END_DONE}, {@link #END_BUDGET}, {@link #END_HAVES_NOT_HELD}, {@link #END_WALK_CAP}, or
     *                 {@link #END_SKETCH}
     * @param sketch   the reply of a reconciliation round with reason {@link #END_SKETCH} (rule 13), appended and
     *                 omitted when null
     */
    public record TreeEnd(int t, int req, List<String> frontier, int reason,
                          @JsonInclude(JsonInclude.Include.NON_NULL) byte[] sketch) {

        /** The four fields of level 1. */
        public TreeEnd(int t, int req, List<String> frontier, int reason) {
            this(t, req, frontier, reason, null);
        }

        public TreeEnd(int req, List<String> frontier, int reason) {
            this(TREE_END, req, frontier, reason, null);
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
            if (reason < END_DONE || reason > END_SKETCH) {
                return Optional.of("unknown TREE_END reason " + reason);
            }
            if ((reason == END_SKETCH) != (sketch != null)) {
                return Optional.of("a TREE_END carries a sketch exactly when its reason is " + END_SKETCH);
            }
            if (reason == END_SKETCH && !frontier.isEmpty()) {
                return Optional.of("a sketch TREE_END carries an empty frontier");
            }
            return badCid(frontier, "frontier");
        }
    }

    /**
     * One pick of a {@link Challenge} (§3.2.4, §6.10.4): without {@code offset} and {@code length} it
     * names a node of the root, or for an artifact root a piece by the raw CID of its table digest; with
     * both it names a chunk group of a BLAKE3 blob.
     *
     * @param cid    the node, the piece, or the blob identity
     * @param offset the chunk group's first byte, or null for a node
     * @param length the chunk group's length, or null for a node
     */
    public record Pick(String cid, @JsonInclude(JsonInclude.Include.NON_NULL) Long offset,
                       @JsonInclude(JsonInclude.Include.NON_NULL) Integer length) {

        /** What {@link #names()} answers for a node pick. */
        public static final String NODE = "node";
        /** What {@link #names()} answers for a chunk-group pick. */
        public static final String CHUNK_GROUP = "chunk-group";

        /** A pick of a node (or a piece) by CID. */
        public static Pick node(String cid) {
            return new Pick(cid, null, null);
        }

        /** A pick of a chunk group of a BLAKE3 blob. */
        public static Pick chunkGroup(String identity, long offset, int length) {
            return new Pick(identity, offset, length);
        }

        /** {@link #NODE} or {@link #CHUNK_GROUP}; named without a prefix so Jackson never reads it as a property. */
        public String names() {
            return offset == null ? NODE : CHUNK_GROUP;
        }
    }

    /**
     * A possession challenge (PDT 2 phase 3, P2-S-07; §3.2.4): the challenger names up to
     * {@link #MAX_PICKS} random picks of a root a store claims to hold, and the store answers within the
     * challenger's deadline with {@link Nodes} under the same {@code req}, the picks it does not hold in
     * {@code missing}, or with {@link Reject} reason {@link #REASON_NOT_HELD} for the root when it holds none.
     *
     * @param t     {@link #CHALLENGE}
     * @param req   the request id the answer carries back
     * @param root  the root the store claims to hold
     * @param picks the picks, 1 to {@link #MAX_PICKS}
     */
    public record Challenge(int t, int req, String root, List<Pick> picks) {
        public Challenge(int req, String root, List<Pick> picks) {
            this(CHALLENGE, req, root, picks);
        }

        /**
         * Checks the bounds a holder enforces before it reads its store.
         *
         * @return the first violated rule, or empty when the message is well formed
         */
        public Optional<String> violation() {
            if (t != CHALLENGE) {
                return Optional.of("tag " + t + " is not CHALLENGE");
            }
            if (!Cids.isCid(root)) {
                return Optional.of("root is not a CID: " + root);
            }
            if (picks == null || picks.isEmpty()) {
                return Optional.of("a CHALLENGE names at least one pick");
            }
            if (picks.size() > MAX_PICKS) {
                return Optional.of("a CHALLENGE names at most " + MAX_PICKS + " picks, got " + picks.size());
            }
            for (Pick pick : picks) {
                if (pick == null || pick.cid() == null) {
                    return Optional.of("picks holds a value that is not a CID");
                }
                if ((pick.offset() == null) != (pick.length() == null)) {
                    return Optional.of("a pick carries both or neither of offset and length");
                }
                if (pick.offset() == null ? !Cids.isCid(pick.cid()) : !isBlake3Identity(pick.cid())) {
                    return Optional.of("picks holds a value that is not a CID: a node pick names a sha2-256 CID and a"
                            + " chunk-group pick a BLAKE3 identity");
                }
                if (pick.offset() != null && pick.offset() < 0) {
                    return Optional.of("a pick's offset is negative");
                }
                if (pick.length() != null && (pick.length() <= 0 || pick.length() > MAX_NODES_BYTES)) {
                    return Optional.of("a pick's length is not in (0, " + MAX_NODES_BYTES + "]");
                }
            }
            return Optional.empty();
        }
    }

    /**
     * Whether text is a BLAKE3 identity in its canonical form (P2-B-02): base32 of a CIDv1 of codec {@code raw}
     * with multihash 0x1e and a 32-byte digest, the name a chunk-group pick gives a blob.
     */
    static boolean isBlake3Identity(String text) {
        return Cids.isIdentity(text); // PDT 2 phase 4, P2-B-01: Cids names the identity
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
     * Decodes a message of a known tag from the network, through {@link CborCodec#fromWire} under
     * its annex I schema (PDT 2 phase 1, P2-X-09; SPEC v0.1.31 §9.1, rulings F1 and F3): a field the
     * schema does not name, a field out of order or of another type, or bytes that are not the
     * message's own encoding make it undecodable, and the caller drops and counts it.
     *
     * @param codec   the codec
     * @param payload the payload as received
     * @param type    the record type for the tag
     * @param <T>     the record type
     * @return the message, or empty when the payload is not the wire encoding of a {@code type}
     */
    public static <T> Optional<T> decode(CborCodec codec, byte[] payload, Class<T> type) {
        try {
            return Optional.of(codec.fromWire(payload, type));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }
}
