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
package ai.badmonkey.agentspaces.identity;

import ai.badmonkey.agentspaces.api.ad.Advertisement;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.common.codec.CborCodec;

import java.security.PublicKey;
import java.util.Objects;

/**
 * Signs and verifies advertisements (spec P3: every advertisement circulates
 * signed). The signature covers the advertisement's canonical CBOR bytes; the
 * v0.1 canonical form is record-component order as produced by
 * {@link CborCodec#defaultCodec()}.
 */
public final class AdvertisementSigner {

    private final CborCodec codec;

    /** Creates a signer over the default codec. */
    public AdvertisementSigner() {
        this(CborCodec.defaultCodec());
    }

    /**
     * Creates a signer over a specific codec.
     *
     * @param codec the codec producing canonical bytes
     */
    public AdvertisementSigner(CborCodec codec) {
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    /**
     * Signs an advertisement with the issuing peer's identity.
     *
     * @param advertisement the advertisement; its issuer must be the identity's peer
     * @param identity      the issuing peer's identity
     * @param <A>           the advertisement type
     * @return the signed advertisement
     * @throws IllegalArgumentException if the advertisement names a different issuer
     */
    public <A extends Advertisement> SignedAdvertisement<A> sign(A advertisement, PeerIdentity identity) {
        Objects.requireNonNull(advertisement, "advertisement");
        Objects.requireNonNull(identity, "identity");
        if (!advertisement.issuer().equals(identity.peerId())) {
            throw new IllegalArgumentException(
                    "advertisement issuer " + advertisement.issuer()
                            + " does not match signing identity " + identity.peerId());
        }
        byte[] canonical = codec.toBytes(advertisement);
        return new SignedAdvertisement<>(advertisement, identity.rawPublicKey(), identity.sign(canonical));
    }

    /**
     * Verifies a signed advertisement using the public key it carries: the key must
     * hash to the advertisement's self-certifying issuer PeerID, and the signature
     * must be valid over the canonical bytes. Receivers MUST verify before caching
     * or acting (spec §4.1).
     *
     * @param signed the signed advertisement
     * @return {@code true} when the issuer key and signature both check out
     */
    public boolean verify(SignedAdvertisement<?> signed) {
        Objects.requireNonNull(signed, "signed");
        byte[] raw = signed.issuerPublicKey();
        if (raw.length != ai.badmonkey.agentspaces.common.crypto.Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !ai.badmonkey.agentspaces.common.id.PeerId.fromPublicKey(raw)
                        .equals(signed.advertisement().issuer())) {
            return false;
        }
        byte[] canonical = codec.toBytes(signed.advertisement());
        return ai.badmonkey.agentspaces.common.crypto.Ed25519.verify(
                ai.badmonkey.agentspaces.common.crypto.Ed25519.publicKeyFromRaw(raw),
                canonical, signed.signature());
    }

    /**
     * Verifies a signed advertisement against a specific public key, ignoring the
     * key carried in the wrapper. Useful when the receiver already holds a pinned
     * key for the issuer.
     *
     * @param signed          the signed advertisement
     * @param issuerPublicKey the pinned issuer public key
     * @return {@code true} when the signature is valid for the canonical bytes
     */
    public boolean verify(SignedAdvertisement<?> signed, PublicKey issuerPublicKey) {
        Objects.requireNonNull(signed, "signed");
        Objects.requireNonNull(issuerPublicKey, "issuerPublicKey");
        byte[] canonical = codec.toBytes(signed.advertisement());
        return ai.badmonkey.agentspaces.common.crypto.Ed25519.verify(issuerPublicKey, canonical, signed.signature());
    }
}
