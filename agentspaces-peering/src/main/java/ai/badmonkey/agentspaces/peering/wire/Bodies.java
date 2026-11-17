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

import ai.badmonkey.agentspaces.common.id.PeerId;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

/** The kind-specific body payloads carried inside {@link Envelope}s. */
public final class Bodies {

    private Bodies() {
    }

    /**
     * PING body. The three appended fields (v0.1.17) piggyback on the probe, memberlist-style,
     * and are null from older nodes: a hash of the sender's anti-entropy digest per stream when
     * it has few streams, else a root hash over them, so a mismatch starts a targeted round; and the
     * items it forwarded most recently, so a member cut off from the gossip tree learns of them
     * from the probes it receives anyway.
     *
     * @param nonce   correlation nonce echoed in the ACK
     * @param root   SHA-256 over the sender's per-stream digest hashes, or null
     * @param hashes SHA-256 of the sender's digest per stream, or null
     * @param have   the sender's recently forwarded items, or null
     */
    public record Ping(long nonce,
                       @JsonInclude(JsonInclude.Include.NON_NULL) byte[] root,
                       @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, byte[]> hashes,
                       @JsonInclude(JsonInclude.Include.NON_NULL) List<HaveItem> have) {

        /** A bare probe, as older nodes send it. */
        public Ping(long nonce) {
            this(nonce, null, null, null);
        }
    }

    /**
     * PING_REQ body: ask the receiver to probe {@code target} for the asker.
     *
     * @param target the peer to probe
     * @param nonce  the asker's correlation nonce, echoed in the eventual ACK
     */
    public record PingReq(PeerId target, long nonce) {
    }

    /**
     * ACK body, with the same piggyback as the PING (v0.1.17), null when relayed or from older nodes.
     *
     * @param nonce    the correlated PING or PING_REQ nonce
     * @param onBehalf the probed peer when this ACK answers a PING_REQ; else null
     * @param root     SHA-256 over the sender's per-stream digest hashes, or null
     * @param hashes   SHA-256 of the sender's digest per stream, or null
     * @param have     the sender's recently forwarded items, or null
     */
    public record Ack(long nonce, PeerId onBehalf,
                      @JsonInclude(JsonInclude.Include.NON_NULL) byte[] root,
                      @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, byte[]> hashes,
                      @JsonInclude(JsonInclude.Include.NON_NULL) List<HaveItem> have) {

        /** A bare acknowledgement, as older nodes and relays send it. */
        public Ack(long nonce, PeerId onBehalf) {
            this(nonce, onBehalf, null, null, null);
        }
    }

    /**
     * RUMOR body: one item on a named stream.
     *
     * @param streamId      the stream, e.g. {@code peers}, {@code ads},
     *                      {@code space:tasks}
     * @param itemId        unique item identifier for deduplication
     * @param hopsRemaining forward budget; decremented at each hop
     * @param payload       stream-specific CBOR payload
     */
    public record Rumor(String streamId, String itemId, int hopsRemaining, byte[] payload) {
    }

    /**
     * DIGEST body. The anti-entropy exchange (spec §5.3) has three forms in one body, of which
     * exactly one field is set. {@code digests}: the asker's full per-stream digests, answered
     * with a PULL_RESP of the deltas it lacks (the form of every version so far). {@code hashes}
     * (v0.1.17, sent only to a peer whose advertisement carries {@code wire.hashes=1}): a
     * SHA-256 of the asker's digest per stream; the partner answers with {@code want} naming the
     * streams both hold whose hashes differ, or with nothing when all match, and the asker then
     * sends its full {@code digests} for those streams alone. A full digest grows with the
     * state, so two members in sync exchange one small frame per round instead of two large ones.
     *
     * <p>Under {@code wire.ae=2} (SPEC annex I.6, v0.1.35) the body gains four fields, each omitted
     * when absent, so a body without them is byte for byte what v0.1.34 sent: {@code tree}, the
     * nodes of a trie audit per stream; {@code ids}, the entry ids an asker fetches per stream;
     * {@code budget}, the bytes each answer the body draws may hold; and {@code sketch}, reserved
     * for a later reconciliation form, never sent and ignored when received. A sender puts them
     * only in a body toward a member whose current card carries {@code wire.ae=2}, since a reader
     * without the hint refuses a field its schema does not name. The form rules of annex I.6 are
     * checked when the record is made, so a body that breaks one does not decode.
     *
     * @param digests the asker's full digests by stream, or null
     * @param hashes  SHA-256 of the asker's digest by stream, or null
     * @param want    the streams whose full digests the partner asks for, or null
     * @param tree    audit nodes by stream, at most {@value #TREE_NODES_MAX} per stream, or null
     * @param ids     entry ids to fetch by stream, at most {@value #IDS_MAX} distinct per stream, or null
     * @param budget  the bytes each answer this body draws may hold, positive, or null
     * @param sketch  reserved (annex H grammar), or null
     */
    public record Digest(Map<String, byte[]> digests,
                         @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, byte[]> hashes,
                         @JsonInclude(JsonInclude.Include.NON_NULL) List<String> want,
                         @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, List<TreeNode>> tree,
                         @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, List<String>> ids,
                         @JsonInclude(JsonInclude.Include.NON_NULL) Long budget,
                         @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, byte[]> sketch) {

        /** Nodes one stream's {@code tree} holds at most (annex I.6). */
        public static final int TREE_NODES_MAX = 256;
        /** Distinct ids one stream's {@code ids} holds at most (annex I.6). */
        public static final int IDS_MAX = 4096;

        /** Checks the form rules of annex I.6 that a body's own fields carry. */
        public Digest {
            if (tree != null) {
                for (Map.Entry<String, List<TreeNode>> e : tree.entrySet()) {
                    if (e.getValue().size() > TREE_NODES_MAX) {
                        throw new IllegalArgumentException("a tree carries at most " + TREE_NODES_MAX
                                + " nodes per stream, not " + e.getValue().size());
                    }
                }
            }
            if (ids != null) {
                for (Map.Entry<String, List<String>> e : ids.entrySet()) {
                    List<String> named = e.getValue();
                    if (named.size() > IDS_MAX) {
                        throw new IllegalArgumentException("a fetch names at most " + IDS_MAX
                                + " ids per stream, not " + named.size());
                    }
                    if (new java.util.HashSet<>(named).size() != named.size()) {
                        throw new IllegalArgumentException("a fetch names an id twice in one stream");
                    }
                    for (String id : named) {
                        if (id.isEmpty()) {
                            throw new IllegalArgumentException("an entry id is non-empty text");
                        }
                    }
                }
            }
            if (budget != null && budget <= 0) {
                throw new IllegalArgumentException("a budget is a positive number of bytes, not " + budget);
            }
        }

        /** A digest as older nodes send it: the full per-stream digests. */
        public Digest(Map<String, byte[]> digests) {
            this(digests, null, null);
        }

        /** A body of the three fields of v0.1.17, as every member reads it. */
        public Digest(Map<String, byte[]> digests, Map<String, byte[]> hashes, List<String> want) {
            this(digests, hashes, want, null, null, null, null);
        }

        /** Whether the body carries a field of annex I.6, which only a member of {@code wire.ae=2} reads. */
        public boolean carriesAudit() {
            return tree != null || ids != null || budget != null || sketch != null;
        }
    }

    /**
     * One node of a trie audit (SPEC annex I.6, v0.1.35): its path, one nibble per byte, and exactly
     * one of {@code count}, which marks a request and gives the asker's own count at the path,
     * {@code nodes}, the 384-byte preimage of an inner node, and {@code lines}, the joined lines
     * under the path. The form rules are checked here, so a node that breaks one makes its carrier
     * undecodable and a reader never parses a malformed answer: a path of at most 64 bytes each
     * from 0 to 15; nodes of exactly 384 bytes whose sixteen counts sum above 32 and whose empty
     * children carry 16 zero bytes; lines that are empty text or non-empty lines free of control
     * characters, joined by single LFs in strictly ascending unsigned byte order.
     *
     * @param path  the node's path, the root's empty
     * @param count the asker's count at the path, on a request; else null
     * @param nodes an inner node's preimage, on an answer; else null
     * @param lines the lines under the path, on an answer; else null
     */
    public record TreeNode(byte[] path,
                           @JsonInclude(JsonInclude.Include.NON_NULL) Long count,
                           @JsonInclude(JsonInclude.Include.NON_NULL) byte[] nodes,
                           @JsonInclude(JsonInclude.Include.NON_NULL) String lines) {

        /** Nibbles in a path at most. */
        public static final int PATH_MAX = 64;
        /** Bytes of an inner node's preimage: sixteen children of a 16-byte fingerprint and an 8-byte count. */
        public static final int NODES_BYTES = 384;
        /** The leaf threshold T: no inner node holds this many slots or fewer. */
        public static final int LEAF_MAX = 32;

        /** Checks the form rules of annex I.6. */
        public TreeNode {
            if (path == null) {
                throw new IllegalArgumentException("a tree node names its path");
            }
            if (path.length > PATH_MAX) {
                throw new IllegalArgumentException("a path holds at most 64 nibbles, not " + path.length);
            }
            for (byte b : path) {
                if (b < 0 || b > 0x0F) {
                    throw new IllegalArgumentException("a path byte is a nibble from 0 to 15, not " + (b & 0xFF));
                }
            }
            int set = (count != null ? 1 : 0) + (nodes != null ? 1 : 0) + (lines != null ? 1 : 0);
            if (set != 1) {
                throw new IllegalArgumentException("a tree node holds exactly one of count, nodes and lines, not " + set);
            }
            if (count != null && count < 0) {
                throw new IllegalArgumentException("a count is not negative: " + count);
            }
            if (nodes != null) {
                checkNodes(nodes);
            }
            if (lines != null) {
                checkLines(lines);
            }
        }

        /** A request for the node at a path, carrying the asker's own count there. */
        public static TreeNode request(byte[] path, long count) {
            return new TreeNode(path, count, null, null);
        }

        /** An answer of an inner node's preimage. */
        public static TreeNode inner(byte[] path, byte[] nodes) {
            return new TreeNode(path, null, nodes, null);
        }

        /** An answer of lines. */
        public static TreeNode lines(byte[] path, String lines) {
            return new TreeNode(path, null, null, lines);
        }

        private static void checkNodes(byte[] nodes) {
            if (nodes.length != NODES_BYTES) {
                throw new IllegalArgumentException("nodes are exactly 384 bytes, not " + nodes.length);
            }
            long sum = 0;
            for (int child = 0; child < 16; child++) {
                int at = child * 24;
                long count = 0;
                for (int i = 0; i < 8; i++) {
                    count = (count << 8) | (nodes[at + 16 + i] & 0xFF);
                }
                if (count < 0) {
                    throw new IllegalArgumentException("a child's count is past every set a member can hold");
                }
                if (count == 0) {
                    for (int i = 0; i < 16; i++) {
                        if (nodes[at + i] != 0) {
                            throw new IllegalArgumentException("an empty child carries 16 zero bytes");
                        }
                    }
                }
                sum += count;
                if (sum < 0) {
                    throw new IllegalArgumentException("the children's counts overflow");
                }
            }
            if (sum <= LEAF_MAX) {
                throw new IllegalArgumentException("an inner node holds more than 32 slots, not " + sum);
            }
        }

        private static void checkLines(String lines) {
            if (lines.isEmpty()) {
                return;
            }
            byte[] previous = null;
            int from = 0;
            while (true) {
                int lf = lines.indexOf('\n', from);
                String line = lf < 0 ? lines.substring(from) : lines.substring(from, lf);
                if (line.isEmpty()) {
                    throw new IllegalArgumentException("lines hold no empty line");
                }
                for (int i = 0; i < line.length(); i++) {
                    char ch = line.charAt(i);
                    if (ch < 0x20 || ch == 0x7F) {
                        throw new IllegalArgumentException("a line holds no control character");
                    }
                }
                byte[] bytes = line.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                if (previous != null && java.util.Arrays.compareUnsigned(previous, bytes) >= 0) {
                    throw new IllegalArgumentException("lines are in strictly ascending order");
                }
                previous = bytes;
                if (lf < 0) {
                    return;
                }
                from = lf + 1;
            }
        }
    }

    /**
     * PULL_RESP body: per-stream deltas the receiver was missing. Under {@code wire.ae=2} (SPEC
     * annex I.6) an answer to a body that carried {@code budget} also says, per stream, how many
     * states the partner held for the request and left out for the budget; the field is omitted
     * otherwise, so the body is what v0.1.34 sent.
     *
     * @param deltas    stream id to opaque delta bytes
     * @param remainder states left out for the budget by stream, none negative, or null
     */
    public record PullResp(Map<String, byte[]> deltas,
                           @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, Long> remainder) {

        /** Checks that no remainder is negative. */
        public PullResp {
            if (remainder != null) {
                for (Long left : remainder.values()) {
                    if (left < 0) {
                        throw new IllegalArgumentException("a remainder is not negative: " + left);
                    }
                }
            }
        }

        /** A delta answer as every member sends it, with no remainder. */
        public PullResp(Map<String, byte[]> deltas) {
            this(deltas, null);
        }
    }

    /**
     * RELAY_FRAME body (spec §5.4): a complete signed frame carried for a peer
     * that cannot be dialed directly. A RELAY-role peer forwards it to the
     * target over its own connection; the target unwraps the inner frame and
     * verifies the origin's signature as usual, so the relay can neither read
     * into nor tamper with what it carries beyond what the envelope exposes.
     *
     * <p>Since srad/large-networks-bulk-data.md §3.7 a frame may carry a
     * <em>path</em>: the RELAY-role members it traverses in order, with
     * {@code hop} the index of the relay the outer envelope is addressed to. A
     * relay that cannot deliver forwards once to {@code path[hop + 1]}; a frame
     * without a path behaves as before (one hop, never re-relayed). Both fields
     * are omitted when absent, so a 0.4.0 reader sees the old body.
     *
     * @param target the peer the inner frame is for
     * @param frame  the origin's complete signed wire frame, verbatim
     * @param path   the relays to traverse, origin's choice; null for the one-hop form
     * @param hop    the index in {@code path} of the relay this envelope is addressed to; null without a path
     */
    public record RelayFrame(PeerId target, byte[] frame,
                             @JsonInclude(JsonInclude.Include.NON_NULL) List<PeerId> path,
                             @JsonInclude(JsonInclude.Include.NON_NULL) Integer hop) {

        /** The one-hop form, as 0.4.0 sends it. */
        public RelayFrame(PeerId target, byte[] frame) {
            this(target, frame, null, null);
        }

        /** The same frame addressed to the next relay on its path. */
        public RelayFrame nextHop() {
            return new RelayFrame(target, frame, path, hop == null ? null : hop + 1);
        }
    }

    /**
     * The CHANNEL_HELLO body (spec §5.6): the sender's announcement that it
     * accepts channel-attested (unsigned) frames on the connection this frame
     * arrived on. {@code attestedRemote} echoes the PeerID the sender's
     * transport attested for the other end of the channel, so the receiver can
     * confirm the announcement binds to this very connection and to itself.
     *
     * @param acceptsBare    whether the sender accepts channel-attested frames
     * @param attestedRemote the PeerID the sender attested for the remote end
     */
    /** The body of a {@code BATCH} frame: encoded inner envelopes, in send order (PERF2 §3.3). */
    public record Batch(List<byte[]> frames) {
    }

    /**
     * HAVE body (lazy push): items the sender forwarded since its last announcement.
     *
     * @param items the announced items
     */
    public record Have(List<HaveItem> items) {
    }

    /**
     * One announced item: the stream and the SHA-256 of the payload, which is how receivers
     * deduplicate rumors (spec §5.3), so the announcement names exactly what the seen-cache knows.
     *
     * @param streamId the stream
     * @param hash     SHA-256 of the item payload
     */
    public record HaveItem(String streamId, byte[] hash) {
    }

    /**
     * GRAFT body (lazy push): a request for one announced item.
     *
     * @param streamId the stream
     * @param hash     SHA-256 of the item payload
     */
    public record Graft(String streamId, byte[] hash) {
    }

    /**
     * PRUNE body (lazy push): the duplicate that made the sender demote the receiver to lazy.
     *
     * @param streamId the stream of the duplicate
     * @param hash     SHA-256 of its payload
     */
    public record Prune(String streamId, byte[] hash) {
    }

    /**
     * SESSION_HELLO body (spec §5.6, v0.1.17).
     *
     * @param ephemeralKey the sender's raw X25519 public key for this connection
     * @param nonce        the sender's fresh nonce for this connection
     * @param remote       the peer the sender believes is at the other end, so the hello binds
     *                     to that pairing and cannot be redirected
     * @param echo         null on the opening hello; on the answering hello, the opener's nonce,
     *                     which proves the answerer holds both halves
     */
    public record SessionHello(byte[] ephemeralKey, byte[] nonce, PeerId remote, byte[] echo) {
    }

    public record ChannelHello(boolean acceptsBare, PeerId attestedRemote) {
    }
}
