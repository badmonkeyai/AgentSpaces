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
package ai.badmonkey.agentspaces.peering.membership;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.ByteArrayInputStream;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * What a CA-rooted peer revocation carries to prove itself (SPEC §6.1,
 * v0.1.13): the revoked peer's channel chain, leaf first, as DER, and the CRL
 * revoking the leaf, as DER, so a receiver with no fresh CRL of its own can
 * still verify the CA's statement against its anchors. At most 4 certificates,
 * a CRL of at most 64 KiB, and 96 KiB in all.
 *
 * @param chain the DER certificates, leaf first
 * @param crl   the DER CRL, or null to rely on receivers' own CRLs
 */
public record RevocationEvidence(List<byte[]> chain,
                                 @JsonInclude(JsonInclude.Include.NON_NULL) byte[] crl) {

    /** Most certificates evidence may carry. */
    public static final int MAX_CHAIN = 4;
    /** Most CRL bytes evidence may carry. */
    public static final int MAX_CRL = 64 * 1024;

    /**
     * Evidence for a chain and the CRL revoking its leaf.
     *
     * @param chain the chain, leaf first
     * @param crl   the CRL, or null
     * @return the evidence
     */
    public static RevocationEvidence of(X509Certificate[] chain, X509CRL crl) {
        try {
            List<byte[]> der = new ArrayList<>();
            for (X509Certificate certificate : chain) {
                der.add(certificate.getEncoded());
            }
            return new RevocationEvidence(List.copyOf(der), crl == null ? null : crl.getEncoded());
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("cannot encode revocation evidence", e);
        }
    }

    /** The CBOR bytes a {@code RevocationAdvertisement} carries. */
    public byte[] encode(CborCodec codec) {
        return codec.toBytes(this);
    }

    /**
     * Decodes and bounds evidence off the wire.
     *
     * @param bytes the CBOR bytes
     * @param codec the codec
     * @return the evidence, or empty when malformed or over a bound
     */
    public static Optional<RevocationEvidence> decode(byte[] bytes, CborCodec codec) {
        if (bytes == null) {
            return Optional.empty();
        }
        try {
            RevocationEvidence evidence = codec.fromBytes(bytes, RevocationEvidence.class);
            if (evidence == null || evidence.chain() == null || evidence.chain().isEmpty()
                    || evidence.chain().size() > MAX_CHAIN
                    || (evidence.crl() != null && evidence.crl().length > MAX_CRL)) {
                return Optional.empty();
            }
            return Optional.of(evidence);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /** The parsed chain, or empty when any certificate fails to parse. */
    public Optional<X509Certificate[]> certificates() {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            X509Certificate[] parsed = new X509Certificate[chain.size()];
            for (int i = 0; i < parsed.length; i++) {
                parsed[i] = (X509Certificate) factory.generateCertificate(
                        new ByteArrayInputStream(chain.get(i)));
            }
            return Optional.of(parsed);
        } catch (GeneralSecurityException | RuntimeException e) {
            return Optional.empty();
        }
    }

    /** The parsed CRL, when one is carried and parses. */
    public Optional<X509CRL> parsedCrl() {
        if (crl == null) {
            return Optional.empty();
        }
        try {
            return Optional.of((X509CRL) CertificateFactory.getInstance("X.509")
                    .generateCRL(new ByteArrayInputStream(crl)));
        } catch (GeneralSecurityException | RuntimeException e) {
            return Optional.empty();
        }
    }
}
