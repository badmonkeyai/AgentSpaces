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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * One group's accepted revocations (security remediation plan §9, the
 * authoritative half of the containment loop). The registry verifies each
 * {@link RevocationAdvertisement}'s signature and its <em>authority</em> — via
 * the pluggable {@link RevocationValidator}, founder-rooted by default — and
 * then holds it for the life of the process: a revoked peer is refused at
 * dispatch permanently, not for a quarantine window. The retained set is also a
 * {@link ReconcilableState}, so anti-entropy converges late joiners from any
 * member's copy long after the original rumor round.
 *
 * <p>Rotation rides the same machinery: a revocation carrying a
 * {@link RevocationAdvertisement#successor() successor} withdraws the old
 * identity and records which new identity replaces it, queryable through
 * {@link #successorOf(PeerId)}. Nothing more is granted to the successor here —
 * it joins and is authorized like any new peer — which is what keeps this
 * rotation shape simple to augment or replace.
 */
public final class RevocationRegistry implements ReconcilableState {

    /**
     * Decides whether a verified revocation is <em>authorized</em>: the seam
     * that swaps trust roots without touching enforcement. The default accepts
     * the group founder; enterprise deployments replace it with a check
     * against an identity-provider assertion or a CA-signed statement.
     */
    @FunctionalInterface
    public interface RevocationValidator {

        /**
         * @param ad    the signature-verified revocation
         * @param group the group's advertisement (carrying the founder)
         * @return whether the issuer had the authority to revoke
         */
        boolean authorized(RevocationAdvertisement ad, GroupAdvertisement group);

        /** The default: only the group founder revokes. */
        static RevocationValidator founderRooted() {
            return (ad, group) -> ad.issuer().equals(group.issuer());
        }

        /**
         * The CA as trust root (SPEC §6.1, v0.1.13, item 9): any member may
         * relay the CA's word, and it is accepted exactly when the revocation's
         * evidence carries a chain whose leaf binds the revoked PeerID, the
         * chain validates to the trust's anchors at the revocation's issue
         * instant, and a CRL signed by the leaf's issuer (the evidence's own, or
         * one this trust holds) lists the leaf for a reason that withdraws the
         * identity. Judging at the issue instant makes every member reach the
         * same verdict, and no network is consulted. A revocation naming a
         * successor is never CA-rooted: only the founder rotates.
         *
         * @param trust the current channel trust (refreshable)
         * @return the validator
         */
        static RevocationValidator caRooted(java.util.function.Supplier<ai.badmonkey.agentspaces.identity.ChannelTrust> trust) {
            Objects.requireNonNull(trust, "trust");
            CborCodec codec = CborCodec.defaultCodec();
            return (ad, group) -> {
                if (ad.evidence() == null || ad.successor() != null) {
                    return false;
                }
                Optional<RevocationEvidence> evidence = RevocationEvidence.decode(ad.evidence(), codec);
                if (evidence.isEmpty()) {
                    return false;
                }
                Optional<java.security.cert.X509Certificate[]> chain = evidence.get().certificates();
                if (chain.isEmpty()) {
                    return false;
                }
                List<java.security.cert.X509CRL> supplied = evidence.get().crl() == null ? List.of()
                        : evidence.get().parsedCrl().map(List::of).orElse(null);
                if (supplied == null) {
                    return false; // a CRL was carried but does not parse
                }
                ai.badmonkey.agentspaces.identity.TrustStatus status =
                        trust.get().status(chain.get(), ad.issued(), supplied);
                return status instanceof ai.badmonkey.agentspaces.identity.TrustStatus.Revoked revoked
                        && revoked.peer().equals(ad.revoked())
                        && revoked.authorizesPeerRevocation();
            };
        }

        /**
         * Accepts what any of the validators accepts.
         *
         * @param validators the validators
         * @return the combined validator
         */
        static RevocationValidator anyOf(RevocationValidator... validators) {
            List<RevocationValidator> all = List.of(validators);
            return (ad, group) -> all.stream().anyMatch(v -> v.authorized(ad, group));
        }
    }

    /**
     * The wire form: the advertisement's exact canonical bytes with the
     * issuer's raw key and signature over them, so verification is
     * byte-stable across hops and languages (the {@code SignedPeerAd} pattern).
     */
    public record SignedRevocation(byte[] adBytes, byte[] publicKey, byte[] signature) {
    }

    /** How far in the future a revocation's {@code issued} may run (ASF-027). */
    private static final Duration MAX_ISSUED_SKEW = Duration.ofMinutes(10);

    /** Bound on retained revocations; beyond it new ones are refused loudly
     * rather than silently evicting older ones (an evicted revocation would
     * re-admit the revoked peer, the worst possible failure mode). */
    private static final int MAX_RETAINED = 4_096;

    private static final System.Logger LOG =
            System.getLogger(RevocationRegistry.class.getName());

    private final GroupAdvertisement group;
    private final RevocationValidator validator;
    private final CborCodec codec;
    private final InstantSource clock;
    private final Consumer<RevocationAdvertisement> onRevoked;
    private final java.util.List<Consumer<RevocationAdvertisement>> listeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Revoked peer -> the latest accepted signed revocation for it. */
    private final Map<PeerId, Accepted> accepted = new ConcurrentHashMap<>();

    private record Accepted(RevocationAdvertisement ad, SignedRevocation signed, int rank, String hash) {
    }

    /**
     * Creates the registry for one group.
     *
     * @param group     the group's advertisement (the founder anchor)
     * @param validator the authority check; see {@link RevocationValidator}
     * @param codec     the CBOR codec
     * @param clock     the time source
     * @param onRevoked invoked once per newly accepted revocation, for
     *                  enforcement side effects (evict, close connections)
     */
    public RevocationRegistry(GroupAdvertisement group, RevocationValidator validator,
                              CborCodec codec, InstantSource clock,
                              Consumer<RevocationAdvertisement> onRevoked) {
        this.group = Objects.requireNonNull(group, "group");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.onRevoked = Objects.requireNonNull(onRevoked, "onRevoked");
    }

    /**
     * Adds a listener invoked once per newly accepted revocation, after the
     * enforcement callback, so layers above peering (discovery, spaces) can
     * react without peering depending on them.
     *
     * @param listener the listener
     */
    public void addListener(Consumer<RevocationAdvertisement> listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /** Whether this peer's identity has been revoked. */
    public boolean revoked(PeerId peer) {
        return accepted.containsKey(peer);
    }

    /**
     * The successor identity recorded for a rotated peer, when its revocation
     * named one.
     *
     * @param rotated the revoked peer
     * @return the replacement identity, when known
     */
    public Optional<PeerId> successorOf(PeerId rotated) {
        Accepted entry = accepted.get(rotated);
        return entry == null ? Optional.empty()
                : Optional.ofNullable(entry.ad().successor());
    }

    /** A snapshot of every revoked peer. */
    public Set<PeerId> allRevoked() {
        return Set.copyOf(accepted.keySet());
    }

    /**
     * Verifies and accepts one signed revocation off the wire.
     *
     * @param payload the CBOR {@link SignedRevocation}
     * @return the advertisement when it was valid, authorized, and new here
     */
    public Optional<RevocationAdvertisement> accept(byte[] payload) {
        SignedRevocation signed;
        try {
            signed = codec.fromBytes(payload, SignedRevocation.class);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        return accept(signed);
    }

    /**
     * Verifies and accepts one signed revocation.
     *
     * @param signed the signed form
     * @return the advertisement when it was valid, authorized, and new here
     */
    public Optional<RevocationAdvertisement> accept(SignedRevocation signed) {
        RevocationAdvertisement ad = verify(signed);
        if (ad == null) {
            return Optional.empty();
        }
        if (accepted.size() >= MAX_RETAINED && !accepted.containsKey(ad.revoked())) {
            LOG.log(System.Logger.Level.ERROR,
                    "revocation registry full (" + MAX_RETAINED + "); refusing new "
                            + "revocation of " + ad.revoked().display()
                            + " — investigate, this should never happen");
            return Optional.empty();
        }
        Accepted candidate = new Accepted(ad, signed, rank(ad),
                HexFormat.of().formatHex(Digests.sha256(signed.adBytes())));
        boolean[] added = {false};
        accepted.compute(ad.revoked(), (peer, previous) -> {
            if (previous == null) {
                added[0] = true;
                return candidate;
            }
            // One record per peer under a total order every replica computes
            // alike (v0.1.13, review M-5): authority first, so a lower-ranked
            // statement never replaces the founder's (no successor erasure);
            // then the newer statement (a rotation may follow a plain revoke);
            // then the larger hash, so digests converge.
            return outranks(candidate, previous) ? candidate : previous;
        });
        if (added[0]) {
            onRevoked.accept(ad);
            for (Consumer<RevocationAdvertisement> listener : listeners) {
                listener.accept(ad);
            }
            return Optional.of(ad);
        }
        return Optional.empty(); // refreshed detail on an already-revoked peer
    }

    /**
     * Authority rank: the founder, then a revocation carrying CA evidence (the
     * validator accepted it, so the CA's word stands behind it), then any other
     * issuer a validator admits. A CA-rooted record therefore never replaces
     * the founder's, such as a rotation naming a successor.
     */
    private int rank(RevocationAdvertisement ad) {
        if (ad.issuer().equals(group.issuer())) {
            return CredentialRevocationRegistry.Authority.FOUNDER;
        }
        return ad.evidence() != null
                ? CredentialRevocationRegistry.Authority.TRUST_ROOT
                : CredentialRevocationRegistry.Authority.OTHER;
    }

    private static boolean outranks(Accepted a, Accepted b) {
        if (a.rank() != b.rank()) {
            return a.rank() > b.rank();
        }
        int byIssued = a.ad().issued().compareTo(b.ad().issued());
        if (byIssued != 0) {
            return byIssued > 0;
        }
        return a.hash().compareTo(b.hash()) > 0;
    }

    /**
     * Verifies one signed revocation without accepting it: exact canonical
     * bytes signed by the key hashing to the issuer, the group matching, the
     * issue stamp inside skew, and the issuer authorized by the validator.
     * Expiry is deliberately NOT checked — a revocation's TTL bounds
     * re-gossip, never how long the withdrawal of trust holds.
     *
     * @param signed the signed form
     * @return the advertisement, or {@code null} when it proves nothing
     */
    public RevocationAdvertisement verify(SignedRevocation signed) {
        if (signed == null || signed.adBytes() == null || signed.publicKey() == null
                || signed.signature() == null
                || signed.publicKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return null;
        }
        RevocationAdvertisement ad;
        try {
            ad = codec.fromBytes(signed.adBytes(), RevocationAdvertisement.class);
        } catch (RuntimeException e) {
            return null;
        }
        if (!PeerId.fromPublicKey(signed.publicKey()).equals(ad.issuer())
                || !Ed25519.verify(Ed25519.publicKeyFromRaw(signed.publicKey()),
                        signed.adBytes(), signed.signature())
                || !ad.group().equals(group.group())
                || ad.issued().isAfter(clock.instant().plus(MAX_ISSUED_SKEW))) {
            return null;
        }
        if (!validator.authorized(ad, group)) {
            return null;
        }
        if (ad.successor() != null && !ad.issuer().equals(group.issuer())) {
            return null; // v0.1.13: only the founder names a successor identity
        }
        return ad;
    }

    // ------------------------------------------------------------ anti-entropy

    @Override
    public byte[] digest() {
        TreeMap<String, String> lines = new TreeMap<>();
        for (Map.Entry<PeerId, Accepted> e : accepted.entrySet()) {
            lines.put("r:" + e.getKey().value(), HexFormat.of()
                    .formatHex(Digests.sha256(e.getValue().signed().adBytes())));
        }
        StringBuilder sb = new StringBuilder();
        lines.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] deltaFor(byte[] remoteDigest) {
        Set<String> remote = new HashSet<>(List.of(
                new String(remoteDigest, StandardCharsets.UTF_8).split("\n")));
        List<SignedRevocation> missing = new ArrayList<>();
        for (Map.Entry<PeerId, Accepted> e : accepted.entrySet()) {
            String line = "r:" + e.getKey().value() + "=" + HexFormat.of()
                    .formatHex(Digests.sha256(e.getValue().signed().adBytes()));
            if (!remote.contains(line)) {
                missing.add(e.getValue().signed());
            }
        }
        return missing.isEmpty() ? new byte[0] : codec.toBytes(missing);
    }

    @Override
    public void applyDelta(byte[] delta) {
        if (delta.length == 0) {
            return;
        }
        SignedRevocation[] entries;
        try {
            entries = codec.fromBytes(delta, SignedRevocation[].class);
        } catch (RuntimeException e) {
            return;
        }
        for (SignedRevocation signed : entries) {
            accept(signed);
        }
    }
}
