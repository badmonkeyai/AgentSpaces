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

import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Instant;
import java.util.Objects;

/**
 * Issues and verifies {@link AgentCertificate}s (spec §4.2, QA4 A4-7): the
 * peer signs the canonical CBOR of the unsigned body, and a verifier accepts a
 * certificate only when the offered peer key hashes to the agent's peer, the
 * signature verifies under it, the certificate names the agent the caller is
 * asking about, and it has not lapsed. The same shape as
 * {@link AdvertisementSigner}, for the same reason: one way of signing a record.
 */
public final class AgentCertificates {

    private final CborCodec codec;

    /** Uses the default canonical CBOR codec. */
    public AgentCertificates() {
        this(CborCodec.defaultCodec());
    }

    /**
     * Uses the given codec, which must be the canonical one every replica shares.
     *
     * @param codec the codec
     */
    public AgentCertificates(CborCodec codec) {
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    /**
     * Signs a certificate body as the peer that owns the agent.
     *
     * @param body the unsigned certificate; its agent must belong to {@code peer}
     * @param peer the certifying peer
     * @return the signed certificate
     */
    public AgentCertificate sign(AgentCertificate body, PeerIdentity peer) {
        Objects.requireNonNull(body, "body");
        Objects.requireNonNull(peer, "peer");
        if (!body.agent().peer().equals(peer.peerId())) {
            throw new IllegalArgumentException("certificate for " + body.agent().encoded()
                    + " cannot be issued by " + peer.peerId().display());
        }
        return body.signed(peer.sign(codec.toBytes(body.unsigned())));
    }

    /**
     * Verifies a certificate against the peer key offered with it.
     *
     * @param certificate the certificate
     * @param peerRawKey  the raw Ed25519 key claimed to be the certifying peer's
     * @param expected    the agent the caller is attributing something to
     * @param now         the instant to judge expiry at
     * @return true only when every check passes
     */
    public boolean verify(AgentCertificate certificate, byte[] peerRawKey, AgentId expected,
                          Instant now) {
        Objects.requireNonNull(certificate, "certificate");
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(now, "now");
        if (certificate.peerSignature() == null || peerRawKey == null
                || peerRawKey.length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || certificate.agentPublicKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !certificate.agent().equals(expected)
                || !PeerId.fromPublicKey(peerRawKey).equals(certificate.agent().peer())
                || certificate.expired(now)) {
            return false;
        }
        return Ed25519.verifyRaw(peerRawKey, codec.toBytes(certificate.unsigned()),
                certificate.peerSignature());
    }

    /**
     * Verifies a certificate for a signature made at {@code signingTime} (SPEC
     * §4.2, v0.1.13): every check of {@link #verify}, except that the validity
     * window must contain the signing time rather than the receiver's now, and
     * the signing time may lead the receiver's clock by at most the bounded HLC
     * drift. Judged so, an agent's history stays verifiable after its
     * certificate lapses, while a stamp from beyond the drift ceiling is refused.
     *
     * @param certificate the certificate
     * @param peerRawKey  the raw Ed25519 key claimed to be the certifying peer's
     * @param expected    the agent the caller is attributing something to
     * @param signingTime the instant the certified key signed at (an HLC stamp's physical time)
     * @param receiverNow the receiver's clock
     * @return true only when every check passes
     */
    public boolean verifyAt(AgentCertificate certificate, byte[] peerRawKey, AgentId expected,
                            Instant signingTime, Instant receiverNow) {
        Objects.requireNonNull(signingTime, "signingTime");
        Objects.requireNonNull(receiverNow, "receiverNow");
        if (!certificate.covers(signingTime)
                || signingTime.isAfter(receiverNow.plusMillis(
                        ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock.MAX_DRIFT_MILLIS))) {
            return false;
        }
        // The window already holds signingTime; judge the remaining checks there.
        return verify(certificate, peerRawKey, expected, signingTime);
    }
}
