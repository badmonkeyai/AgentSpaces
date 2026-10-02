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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;

import java.time.InstantSource;

/**
 * Hand-built wire frames for adversarial node tests: a frame is signed by
 * whichever identity the test chooses, stamped from the test clock, and
 * delivered by dialing the target's SimNetwork address from a throwaway
 * injector transport, so the target sees exactly the bytes a hostile or
 * buggy peer would put on the wire.
 */
final class TestFrames {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final WireCodec WIRE = new WireCodec(CODEC);

    private TestFrames() {
    }

    /** A fresh HLC stamp from the given clock, attributed to {@code from}. */
    static HlcTimestamp stamp(InstantSource clock, PeerId from, int logical) {
        return new HlcTimestamp(clock.millis(), logical, from.value());
    }

    /** CBOR-encodes a body; byte arrays pass through verbatim. */
    static byte[] body(Object body) {
        return body instanceof byte[] raw ? raw : CODEC.toBytes(body);
    }

    /** A fully signed frame from {@code from}, at wire version 2. */
    static byte[] signed(PeerIdentity from, GroupId group, Envelope.Kind kind, PeerId to,
                         HlcTimestamp stamp, Object body) {
        return WIRE.encode(new Envelope(PeerNode.WIRE_VERSION, group, kind, from.peerId(), to,
                stamp, body(body)), from);
    }

    /** A fully signed frame from {@code from}, stamped now on the given clock. */
    static byte[] signed(PeerIdentity from, GroupId group, Envelope.Kind kind, PeerId to,
                         InstantSource clock, int logical, Object body) {
        return signed(from, group, kind, to, stamp(clock, from.peerId(), logical), body);
    }
}
