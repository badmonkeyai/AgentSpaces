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
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;

import java.util.Objects;
import java.util.Optional;

/**
 * Encodes, signs, decodes, and verifies wire frames. A frame is the CBOR of a
 * {@link Signed} wrapper: the envelope, the sender's raw public key, and the
 * signature over the envelope's canonical bytes. Receivers verify that the key
 * hashes to the envelope's {@code from} PeerID and that the signature is valid
 * before any frame reaches a handler; frames that fail are dropped.
 *
 * <p>Every frame must carry {@link #WIRE_VERSION}; any other {@code ver} is
 * dropped as malformed (spec §9).
 *
 * <p>On a channel the transport has authenticated (spec §5.6), the key and
 * signature may be omitted: a <em>channel-attested</em> frame carries the
 * envelope alone, and {@link #decode(byte[], Optional)} accepts it only when
 * the connection's attested PeerID equals the envelope's {@code from}. Signed
 * frames stay acceptable from any connection, so mixed fleets interoperate.
 */
public final class WireCodec {

    /**
     * The wire protocol version this codec speaks (spec §9): 2 since the
     * ASF-010 destination binding, 3 since the canonical map order of
     * ISSUE-CanonicalMaps (every map's entries sorted by key in the signed bytes). A frame whose {@code ver} differs is
     * malformed to this codec and is dropped before verification, so a peer
     * cannot smuggle a differently-shaped envelope past a receiver by
     * relabelling it; version negotiation is a future protocol change, not a
     * decode-time tolerance.
     */
    public static final int WIRE_VERSION = 3;

    /**
     * An envelope as it travels: signed, or bare on an attested channel.
     *
     * @param envelope        the envelope
     * @param senderPublicKey the sender's raw Ed25519 public key; {@code null}
     *                        on a channel-attested frame
     * @param signature       signature over the envelope's canonical CBOR
     *                        bytes; {@code null} on a channel-attested frame
     */
    public record Signed(Envelope envelope, byte[] senderPublicKey, byte[] signature) {
    }

    private final CborCodec codec;

    /** Creates a codec over the default CBOR codec. */
    public WireCodec() {
        this(CborCodec.defaultCodec());
    }

    /**
     * Creates a codec.
     *
     * @param codec the underlying CBOR codec
     */
    public WireCodec(CborCodec codec) {
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    /**
     * Signs and encodes an envelope into a frame.
     *
     * @param envelope the envelope; its {@code from} must be the identity's peer
     * @param identity the sending peer's identity
     * @return the frame bytes
     */
    public byte[] encode(Envelope envelope, PeerIdentity identity) {
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(identity, "identity");
        if (!envelope.from().equals(identity.peerId())) {
            throw new IllegalArgumentException(
                    "envelope.from " + envelope.from() + " does not match identity " + identity.peerId());
        }
        byte[] canonical = codec.toBytes(envelope);
        Signed signed = new Signed(envelope, identity.rawPublicKey(), identity.sign(canonical));
        return codec.toBytes(signed);
    }

    /**
     * Encodes an envelope as a channel-attested frame: no key, no signature.
     * Send these only on a connection whose remote end is attested for the
     * envelope's {@code from} and has announced it accepts them (spec §5.6);
     * every other receiver drops them.
     *
     * @param envelope the envelope
     * @return the frame bytes
     */
    public byte[] encodeBare(Envelope envelope) {
        Objects.requireNonNull(envelope, "envelope");
        return codec.toBytes(new Signed(envelope, null, null));
    }

    /**
     * Decodes and verifies a frame from an unauthenticated connection: only
     * fully signed frames pass.
     *
     * @param frame the frame bytes
     * @return the verified envelope, or empty when decoding or verification fails
     */
    public Optional<Envelope> decode(byte[] frame) {
        return decode(frame, Optional.empty());
    }

    /**
     * Decodes and verifies a frame. A signed frame verifies against the key it
     * carries, from any connection. A bare frame (no key, no signature) is
     * accepted only when {@code attestedPeer} is present and equals the
     * envelope's {@code from}: the transport already proved that peer is the
     * only party able to send on this channel.
     *
     * @param frame        the frame bytes
     * @param attestedPeer the connection's transport-attested peer, when any
     * @return the accepted envelope, or empty when the frame fails
     */
    public Optional<Envelope> decode(byte[] frame, Optional<PeerId> attestedPeer) {
        Objects.requireNonNull(frame, "frame");
        Objects.requireNonNull(attestedPeer, "attestedPeer");
        Signed signed;
        try {
            signed = codec.fromBytes(frame, Signed.class);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        if (signed == null || signed.envelope() == null
                || signed.envelope().ver() != WIRE_VERSION) {
            return Optional.empty(); // unknown wire version: malformed (spec §9)
        }
        byte[] raw = signed.senderPublicKey();
        if (raw == null && signed.signature() == null) {
            // Channel-attested frame: the transport's authentication stands in
            // for the signature, for exactly the attested peer.
            return attestedPeer.isPresent()
                    && attestedPeer.get().equals(signed.envelope().from())
                    ? Optional.of(signed.envelope())
                    : Optional.empty();
        }
        if (raw == null || signed.signature() == null
                || raw.length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !PeerId.fromPublicKey(raw).equals(signed.envelope().from())) {
            return Optional.empty();
        }
        byte[] canonical = codec.toBytes(signed.envelope());
        boolean valid = Ed25519.verify(Ed25519.publicKeyFromRaw(raw), canonical, signed.signature());
        return valid ? Optional.of(signed.envelope()) : Optional.empty();
    }
}
