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

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.security.GeneralSecurityException;
import java.security.cert.CertPathValidator;
import java.security.cert.CertStore;
import java.security.cert.CertificateFactory;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXParameters;
import java.security.cert.PKIXRevocationChecker;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The enterprise-CA attestation mode (security remediation plan §3/§8, WS4):
 * the peer's channel certificate is issued by the organization's CA with the
 * Ed25519 PeerID as a certified attribute (the subject CN), so attesting a
 * connection means both "the handshake proved possession of this certificate's
 * key" and "the organization's trust root vouches that this PeerID belongs to
 * an enrolled agent". The CA's revocation is then the authoritative eject: a
 * revoked certificate fails the next handshake everywhere, independent of the
 * fabric, with the gossiped {@code RevocationAdvertisement} as the fast-path
 * cache in front of it.
 *
 * <p>Offline tolerance (plan §1.3): revocation runs against the CRLs this
 * trust was constructed with — cached documents an operator refreshes on their
 * own cadence — so validating already-enrolled peers never requires reaching
 * the CA. Reaching the CA mints new trust (enrollment, a fresh CRL); it is not
 * needed to use existing trust. {@link #strictOnline()} opts into the JDK's
 * live OCSP/CRL-distribution-point fetching for deployments that want it.
 *
 * <p>Instances are immutable and safe for concurrent use.
 */
public final class ChannelTrust {

    private final Set<TrustAnchor> anchors;
    private final List<X509CRL> crls;
    private final boolean strictOnline;

    private ChannelTrust(Set<TrustAnchor> anchors, List<X509CRL> crls,
                         boolean strictOnline) {
        this.anchors = anchors;
        this.crls = crls;
        this.strictOnline = strictOnline;
    }

    /**
     * Trust anchored in the given CA certificates, with revocation disabled:
     * the fabric-level {@code RevocationAdvertisement} and short-lived
     * certificates carry revocation instead. The simplest enterprise posture.
     *
     * @param authorities the CA certificates to anchor trust in
     * @return the trust
     */
    public static ChannelTrust of(Collection<X509Certificate> authorities) {
        return new ChannelTrust(anchorsOf(authorities), List.of(), false);
    }

    /**
     * Trust anchored in the given CA certificates, with revocation checked
     * against the given CRLs — cached documents, refreshed out of band, so
     * validation stays offline-tolerant.
     *
     * @param authorities the CA certificates to anchor trust in
     * @param crls        the current certificate revocation lists
     * @return the trust
     */
    public static ChannelTrust withCrls(Collection<X509Certificate> authorities,
                                        Collection<X509CRL> crls) {
        return new ChannelTrust(anchorsOf(authorities), List.copyOf(crls), false);
    }

    /**
     * A copy that additionally lets the JDK fetch OCSP responses and
     * distribution-point CRLs live, soft-failing when the responder is
     * unreachable (availability over a hard online dependency, plan §1.3).
     *
     * @return the online-checking variant
     */
    public ChannelTrust strictOnline() {
        return new ChannelTrust(anchors, crls, true);
    }

    /**
     * Attests a presented certificate chain: the leaf's CN must carry a raw
     * Ed25519 identity key (bounded before decode, ASF-023), and the chain must
     * validate to one of this trust's anchors — with revocation per this
     * trust's configuration. Any failure attests nothing, and the connection
     * stays usable for signed frames only.
     *
     * @param chain the chain the TLS handshake presented, leaf first
     * @return the CA-vouched PeerID, or empty
     */
    public Optional<PeerId> attest(X509Certificate[] chain) {
        if (strictOnline) {
            return attestOnline(chain);
        }
        return status(chain, java.time.Instant.now()) instanceof TrustStatus.Good good
                ? Optional.of(good.peer()) : Optional.empty();
    }

    /**
     * Judges a presented chain at an instant (SPEC §5.6, v0.1.13): whether it
     * validates to one of this trust's anchors then, and whether the CRLs
     * this trust holds revoke it, with the reason. Validation time is the
     * instant given, so a verdict can be reproduced. No network is consulted,
     * even under {@link #strictOnline()}: this is the path revocation evidence
     * is judged on, which must not block.
     *
     * @param chain the chain, leaf first
     * @param at    the instant to judge at
     * @return the status
     */
    public TrustStatus status(X509Certificate[] chain, java.time.Instant at) {
        return status(chain, at, List.of());
    }

    /**
     * As {@link #status(X509Certificate[], java.time.Instant)}, additionally
     * consulting CRLs supplied with the chain, such as a revocation's
     * evidence. A supplied CRL counts only when it verifies under the chain's
     * issuer, as PKIX requires of every CRL it uses.
     *
     * @param chain     the chain, leaf first
     * @param at        the instant to judge at
     * @param extraCrls further CRLs to consult
     * @return the status
     */
    public TrustStatus status(X509Certificate[] chain, java.time.Instant at,
                              Collection<X509CRL> extraCrls) {
        Objects.requireNonNull(at, "at");
        Objects.requireNonNull(extraCrls, "extraCrls");
        if (chain == null || chain.length == 0 || chain[0] == null) {
            return new TrustStatus.Malformed();
        }
        byte[] raw = ChannelCertificate.identityKeyFromCn(chain[0]);
        if (raw == null) {
            return new TrustStatus.Malformed();
        }
        PeerId peer = PeerId.fromPublicKey(raw);
        List<X509Certificate> path = new ArrayList<>();
        for (X509Certificate certificate : chain) {
            boolean isAnchor = anchors.stream().anyMatch(a -> a.getTrustedCert().equals(certificate));
            if (!isAnchor) {
                path.add(certificate); // the path excludes any anchor the peer echoed back
            }
        }
        if (path.isEmpty()) {
            return new TrustStatus.Untrusted("the chain holds only anchors");
        }
        java.util.Date date = java.util.Date.from(at);
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            java.security.cert.CertPath certPath = factory.generateCertPath(path);
            CertPathValidator validator = CertPathValidator.getInstance("PKIX");
            PKIXParameters trustOnly = new PKIXParameters(anchors);
            trustOnly.setDate(date);
            trustOnly.setRevocationEnabled(false);
            try {
                validator.validate(certPath, trustOnly);
            } catch (java.security.cert.CertPathValidatorException e) {
                if (e.getReason() == java.security.cert.CertPathValidatorException.BasicReason.EXPIRED) {
                    return new TrustStatus.Expired();
                }
                if (e.getReason() == java.security.cert.CertPathValidatorException.BasicReason.NOT_YET_VALID) {
                    return new TrustStatus.NotYetValid();
                }
                return new TrustStatus.Untrusted(String.valueOf(e.getMessage()));
            }
            List<X509CRL> all = new ArrayList<>(crls);
            all.addAll(extraCrls);
            if (all.isEmpty()) {
                return new TrustStatus.Good(peer); // revocation is carried by the fabric instead
            }
            PKIXParameters withRevocation = new PKIXParameters(anchors);
            withRevocation.setDate(date);
            withRevocation.addCertStore(CertStore.getInstance("Collection",
                    new CollectionCertStoreParameters(all)));
            withRevocation.setRevocationEnabled(true);
            try {
                validator.validate(certPath, withRevocation);
                return new TrustStatus.Good(peer);
            } catch (java.security.cert.CertPathValidatorException e) {
                if (e.getReason() == java.security.cert.CertPathValidatorException.BasicReason.REVOKED
                        && e.getIndex() == 0) {
                    return revokedEntry(chain, all, peer);
                }
                if (e.getReason() == java.security.cert.CertPathValidatorException.BasicReason
                        .UNDETERMINED_REVOCATION_STATUS) {
                    return new TrustStatus.Undetermined(String.valueOf(e.getMessage()));
                }
                return new TrustStatus.Untrusted(String.valueOf(e.getMessage()));
            }
        } catch (GeneralSecurityException | RuntimeException e) {
            return new TrustStatus.Malformed();
        }
    }

    /**
     * The CRL this trust holds that lists a leaf as revoked, verified under the
     * leaf's issuer: what a node attaches as evidence when it roots a peer
     * revocation in the CA.
     *
     * @param chain the chain, leaf first
     * @return the CRL, when this trust holds one revoking the leaf
     */
    public Optional<X509CRL> crlRevoking(X509Certificate[] chain) {
        if (chain == null || chain.length == 0) {
            return Optional.empty();
        }
        for (X509CRL crl : crls) {
            if (crl.getRevokedCertificate(chain[0]) != null && signedByIssuer(crl, chain)) {
                return Optional.of(crl);
            }
        }
        return Optional.empty();
    }

    private TrustStatus revokedEntry(X509Certificate[] chain, List<X509CRL> all, PeerId peer) {
        for (X509CRL crl : all) {
            java.security.cert.X509CRLEntry entry = crl.getRevokedCertificate(chain[0]);
            if (entry != null && signedByIssuer(crl, chain)) {
                java.security.cert.CRLReason reason = entry.getRevocationReason();
                return new TrustStatus.Revoked(peer,
                        reason == null ? java.security.cert.CRLReason.UNSPECIFIED : reason,
                        entry.getRevocationDate().toInstant());
            }
        }
        return new TrustStatus.Untrusted("revoked, but by no CRL this trust can verify");
    }

    /** Whether a CRL verifies under the leaf's issuer: the next certificate in the chain, or an anchor. */
    private boolean signedByIssuer(X509CRL crl, X509Certificate[] chain) {
        List<java.security.PublicKey> issuers = new ArrayList<>();
        if (chain.length > 1) {
            issuers.add(chain[1].getPublicKey());
        }
        anchors.forEach(anchor -> issuers.add(anchor.getTrustedCert().getPublicKey()));
        for (java.security.PublicKey key : issuers) {
            try {
                crl.verify(key);
                return true;
            } catch (GeneralSecurityException | RuntimeException ignored) {
                // not this issuer
            }
        }
        return false;
    }

    /**
     * The handshake path under {@link #strictOnline()}: the JDK may fetch OCSP
     * responses and distribution-point CRLs, soft-failing when unreachable,
     * and the cached CRLs are consulted too (v0.1.13; they were ignored in
     * this mode before).
     */
    private Optional<PeerId> attestOnline(X509Certificate[] chain) {
        if (chain == null || chain.length == 0) {
            return Optional.empty();
        }
        byte[] raw = ChannelCertificate.identityKeyFromCn(chain[0]);
        if (raw == null) {
            return Optional.empty();
        }
        try {
            List<X509Certificate> path = new ArrayList<>();
            for (X509Certificate certificate : chain) {
                boolean isAnchor = anchors.stream()
                        .anyMatch(a -> a.getTrustedCert().equals(certificate));
                if (!isAnchor) {
                    path.add(certificate);
                }
            }
            if (path.isEmpty()) {
                return Optional.empty();
            }
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            PKIXParameters parameters = new PKIXParameters(anchors);
            CertPathValidator validator = CertPathValidator.getInstance("PKIX");
            PKIXRevocationChecker checker =
                    (PKIXRevocationChecker) validator.getRevocationChecker();
            checker.setOptions(EnumSet.of(PKIXRevocationChecker.Option.SOFT_FAIL));
            parameters.addCertPathChecker(checker);
            parameters.setRevocationEnabled(false); // the checker replaces it
            if (!crls.isEmpty()) {
                parameters.addCertStore(CertStore.getInstance("Collection",
                        new CollectionCertStoreParameters(crls)));
            }
            validator.validate(factory.generateCertPath(path), parameters);
        } catch (GeneralSecurityException e) {
            return Optional.empty(); // untrusted, revoked, expired, or malformed
        }
        return Optional.of(PeerId.fromPublicKey(raw));
    }

    private static Set<TrustAnchor> anchorsOf(Collection<X509Certificate> authorities) {
        if (Objects.requireNonNull(authorities, "authorities").isEmpty()) {
            throw new IllegalArgumentException("at least one CA certificate is required");
        }
        return authorities.stream()
                .map(ca -> new TrustAnchor(ca, null))
                .collect(Collectors.toUnmodifiableSet());
    }
}
