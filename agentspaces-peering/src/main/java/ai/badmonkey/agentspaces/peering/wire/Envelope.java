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

import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.Objects;

/**
 * The wire envelope (spec §9): every frame between peers is a CBOR-encoded,
 * signed envelope. The body is an opaque CBOR payload whose shape depends on the
 * {@link Kind}.
 *
 * <p>The {@code to} field binds an addressed frame to its intended recipient (a
 * signed component, ASF-010): a receiver drops a frame whose {@code to} is set
 * and is not this peer, so a captured frame cannot be redirected to, or replayed
 * at, a peer it was never sent to. {@code to} is {@code null} on a genuinely
 * unaddressed frame — the bootstrap self-introduction to a seed whose PeerID is
 * not yet known — and such frames get no destination protection (they carry only
 * a signed, idempotent self-advertisement).
 *
 * @param ver   protocol version; {@link WireCodec#WIRE_VERSION} (2) is the only
 *              version receivers accept (spec §9)
 * @param group the group the frame belongs to; receivers ignore frames for
 *              groups they are not members of
 * @param kind  the frame kind
 * @param from  the sending peer
 * @param to    the intended recipient, or {@code null} when unaddressed
 * @param stamp the sender's HLC stamp; receivers merge it into their clocks
 * @param body  kind-specific CBOR payload
 */
public record Envelope(
        int ver,
        GroupId group,
        Kind kind,
        PeerId from,
        PeerId to,
        HlcTimestamp stamp,
        byte[] body) {

    /** Frame kinds (spec §9). */
    public enum Kind {
        /** Membership liveness probe. */
        PING,
        /** Ask a third peer to probe a target on the asker's behalf. */
        PING_REQ,
        /** Liveness acknowledgement, correlated by nonce. */
        ACK,
        /** Push channel: a rumor item on a named stream. */
        RUMOR,
        /** Anti-entropy: the sender's per-stream digests. */
        DIGEST,
        /** Anti-entropy: deltas the receiver was missing. */
        PULL_RESP,
        /** Scoped discovery query (hop-budgeted). */
        QUERY,
        /** Discovery answer routed back to the asker. */
        QUERY_HIT,
        /** Content-addressed block request (spec §9). */
        BLOCK_WANT,
        /** Content-addressed block delivery (spec §9). */
        BLOCK,
        /** Direct capability frame (spec §9 pipes, v0.1 datagram form). */
        PIPE_DATA,
        /**
         * A frame carried on behalf of a peer that cannot be dialed directly
         * (spec §5.4 relay): the body holds the target and the origin's complete
         * signed frame, forwarded verbatim by a RELAY-role peer.
         */
        RELAY_FRAME,
        /**
         * Channel-authentication announcement (spec §5.6): a signed,
         * per-connection statement that the sender accepts channel-attested
         * (unsigned) frames on this connection, echoing the PeerID it attested
         * for the remote end so the announcement binds to this channel.
         */
        CHANNEL_HELLO,
        /**
         * Join-by-GroupID bootstrap request (spec §10.1, v0.1.10): a peer that
         * knows only a GroupID and a seed endpoint asks the seed for the
         * group's founding advertisement. The envelope's {@code group} names
         * the wanted group; the body is empty. Served to non-members like a
         * PING, under the bootstrap rate budget.
         */
        GROUP_AD_WANT,
        /**
         * Join-by-GroupID bootstrap answer: the body is the CBOR of the
         * {@code SignedGroupAdvertisement} the answering member joined with.
         * Only self-certifying founding advertisements are ever served, and
         * the requester re-derives the GroupID and checks the founder's
         * signature before acting on one (spec §4.4, §5.1).
         */
        GROUP_AD
    }

    public Envelope {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(stamp, "stamp");
        Objects.requireNonNull(body, "body");
        if (ver <= 0) {
            throw new IllegalArgumentException("ver must be positive: " + ver);
        }
    }
}
