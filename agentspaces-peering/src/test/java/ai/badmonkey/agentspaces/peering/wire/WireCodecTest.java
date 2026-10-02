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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WireCodecTest {

    private final WireCodec wire = new WireCodec();
    private final PeerIdentity sender = PeerIdentity.generate();

    private Envelope envelope(PeerIdentity from) {
        return new Envelope(2, GroupId.of("zG"), Envelope.Kind.RUMOR, from.peerId(),
                from.peerId(), new HlcTimestamp(42L, 0, "n"), new byte[]{1, 2, 3});
    }

    @Test
    void frameRoundTripsWithVerification() {
        byte[] frame = wire.encode(envelope(sender), sender);

        Optional<Envelope> decoded = wire.decode(frame);

        assertThat(decoded).isPresent();
        assertThat(decoded.get().from()).isEqualTo(sender.peerId());
        assertThat(decoded.get().kind()).isEqualTo(Envelope.Kind.RUMOR);
        assertThat(decoded.get().body()).containsExactly(1, 2, 3);
    }

    @Test
    void tamperedFrameIsDropped() {
        byte[] frame = wire.encode(envelope(sender), sender);
        // Flip a byte near the end (inside signature or body).
        frame[frame.length - 3] ^= 0x01;

        assertThat(wire.decode(frame)).isEmpty();
    }

    @Test
    void forgedSenderIsDropped() {
        // Another identity signs an envelope claiming the victim's PeerId.
        PeerIdentity attacker = PeerIdentity.generate();
        Envelope forged = envelope(sender); // claims sender as from
        // Encoding must refuse a mismatched identity outright.
        assertThatThrownBy(() -> wire.encode(forged, attacker))
                .isInstanceOf(IllegalArgumentException.class);

        // Hand-built frame with attacker's key and signature, victim's from:
        CborCodec codec = CborCodec.defaultCodec();
        byte[] canonical = codec.toBytes(forged);
        WireCodec.Signed signed = new WireCodec.Signed(
                forged, attacker.rawPublicKey(), attacker.sign(canonical));
        assertThat(wire.decode(codec.toBytes(signed))).isEmpty();
    }

    @Test
    void garbageIsDroppedQuietly() {
        assertThat(wire.decode(new byte[]{9, 9, 9, 9})).isEmpty();
        assertThat(wire.decode(new byte[0])).isEmpty();
    }

    @Test
    void bareFrameIsAcceptedOnlyFromTheAttestedPeer() {
        byte[] bare = wire.encodeBare(envelope(sender));

        assertThat(wire.decode(bare, Optional.of(sender.peerId()))).isPresent();
        assertThat(wire.decode(bare)).isEmpty();
        assertThat(wire.decode(bare, Optional.empty())).isEmpty();
        assertThat(wire.decode(bare, Optional.of(PeerIdentity.generate().peerId()))).isEmpty();
    }

    @Test
    void signedFrameStaysAcceptableEverywhereIncludingAttestedChannels() {
        byte[] frame = wire.encode(envelope(sender), sender);

        assertThat(wire.decode(frame, Optional.of(sender.peerId()))).isPresent();
        assertThat(wire.decode(frame,
                Optional.of(PeerIdentity.generate().peerId()))).isPresent();
    }

    @Test
    void halfSignedFramesAreDroppedEvenWhenAttested() {
        // A frame carrying a key without a signature (or the reverse) is
        // neither signed nor channel-attested; both hybrids are rejected.
        CborCodec codec = CborCodec.defaultCodec();
        Envelope env = envelope(sender);
        byte[] keyOnly = codec.toBytes(
                new WireCodec.Signed(env, sender.rawPublicKey(), null));
        byte[] sigOnly = codec.toBytes(
                new WireCodec.Signed(env, null, sender.sign(codec.toBytes(env))));

        assertThat(wire.decode(keyOnly, Optional.of(sender.peerId()))).isEmpty();
        assertThat(wire.decode(sigOnly, Optional.of(sender.peerId()))).isEmpty();
    }

    /** SPEC §9 / TECH §3: the sender key must be exactly 32 raw Ed25519 bytes; a truncated or padded key drops the frame. */
    @Test
    void aSenderKeyThatIsNotThirtyTwoBytesIsDropped() {
        CborCodec codec = CborCodec.defaultCodec();
        Envelope env = envelope(sender);
        byte[] sig = sender.sign(codec.toBytes(env));

        byte[] truncated = codec.toBytes(new WireCodec.Signed(
                env, java.util.Arrays.copyOf(sender.rawPublicKey(), 31), sig));
        byte[] padded = codec.toBytes(new WireCodec.Signed(
                env, java.util.Arrays.copyOf(sender.rawPublicKey(), 33), sig));

        assertThat(wire.decode(truncated)).isEmpty();
        assertThat(wire.decode(padded)).isEmpty();
        // Attestation does not rescue a malformed signed frame either.
        assertThat(wire.decode(truncated, Optional.of(sender.peerId()))).isEmpty();
    }

    /** SPEC §5.6 frame relaxation: "neither field" means absent, not empty; zero-length key and signature are a malformed signed frame, never a bare one. */
    @Test
    void emptyButNonNullKeyAndSignatureAreNotABareFrame() {
        CborCodec codec = CborCodec.defaultCodec();
        Envelope env = envelope(sender);

        byte[] emptyBoth = codec.toBytes(new WireCodec.Signed(env, new byte[0], new byte[0]));
        byte[] emptyKeyRealSig = codec.toBytes(new WireCodec.Signed(
                env, new byte[0], sender.sign(codec.toBytes(env))));

        // Even on a connection attested for the very sender, these must not
        // take the channel-attested path.
        assertThat(wire.decode(emptyBoth, Optional.of(sender.peerId()))).isEmpty();
        assertThat(wire.decode(emptyKeyRealSig, Optional.of(sender.peerId()))).isEmpty();
        assertThat(wire.decode(emptyBoth)).isEmpty();
    }

    /** SPEC §9: receivers accept only wire version 2; a validly signed frame at any other version is dropped as malformed, signed or bare. */
    @Test
    void unknownWireVersionsAreDropped() {
        Envelope future = new Envelope(99, GroupId.of("zG"), Envelope.Kind.RUMOR,
                sender.peerId(), sender.peerId(), new HlcTimestamp(42L, 0, "n"),
                new byte[]{1, 2, 3});
        Envelope past = new Envelope(1, GroupId.of("zG"), Envelope.Kind.RUMOR,
                sender.peerId(), sender.peerId(), new HlcTimestamp(42L, 0, "n"),
                new byte[]{1, 2, 3});

        assertThat(WireCodec.WIRE_VERSION).isEqualTo(2);
        assertThat(wire.decode(wire.encode(future, sender))).isEmpty();
        assertThat(wire.decode(wire.encode(past, sender))).isEmpty();
        assertThat(wire.decode(wire.encodeBare(future), Optional.of(sender.peerId()))).isEmpty();
        // The current version, identically signed, decodes.
        assertThat(wire.decode(wire.encode(envelope(sender), sender))).isPresent();
        // ver is inside the signed canonical bytes, so it cannot be changed in
        // flight: re-labelling a v99 frame as v2 breaks the signature.
        assertThatThrownBy(() -> new Envelope(0, GroupId.of("zG"), Envelope.Kind.RUMOR,
                sender.peerId(), sender.peerId(), new HlcTimestamp(42L, 0, "n"), new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
