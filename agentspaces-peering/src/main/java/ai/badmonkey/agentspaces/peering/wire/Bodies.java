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

import java.util.Map;

/** The kind-specific body payloads carried inside {@link Envelope}s. */
public final class Bodies {

    private Bodies() {
    }

    /**
     * PING body.
     *
     * @param nonce correlation nonce echoed in the ACK
     */
    public record Ping(long nonce) {
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
     * ACK body.
     *
     * @param nonce   the correlated PING or PING_REQ nonce
     * @param onBehalf the probed peer when this ACK answers a PING_REQ; else null
     */
    public record Ack(long nonce, PeerId onBehalf) {
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
     * DIGEST body: the sender's per-stream anti-entropy digests.
     *
     * @param digests stream id to opaque digest bytes
     */
    public record Digest(Map<String, byte[]> digests) {
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
    public record ChannelHello(boolean acceptsBare, PeerId attestedRemote) {
    }
}
