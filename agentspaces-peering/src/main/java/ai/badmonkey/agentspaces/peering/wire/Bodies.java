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
     * @param digests the asker's full digests by stream, or null
     * @param hashes  SHA-256 of the asker's digest by stream, or null
     * @param want    the streams whose full digests the partner asks for, or null
     */
        public record Digest(Map<String, byte[]> digests,
                         @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, byte[]> hashes,
                         @JsonInclude(JsonInclude.Include.NON_NULL) List<String> want) {

        /** A digest as older nodes send it: the full per-stream digests. */
        public Digest(Map<String, byte[]> digests) {
            this(digests, null, null);
        }
    }

    /**
     * PULL_RESP body: per-stream deltas the receiver was missing.
     *
     * @param deltas stream id to opaque delta bytes
     */
    public record PullResp(Map<String, byte[]> deltas) {
    }

    /**
     * RELAY_FRAME body (spec §5.4): a complete signed frame carried for a peer
     * that cannot be dialed directly. A RELAY-role peer forwards it to the
     * target over its own connection; the target unwraps the inner frame and
     * verifies the origin's signature as usual, so the relay can neither read
     * into nor tamper with what it carries beyond what the envelope exposes.
     *
     * @param target the peer the inner frame is for
     * @param frame  the origin's complete signed wire frame, verbatim
     */
    public record RelayFrame(PeerId target, byte[] frame) {
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
