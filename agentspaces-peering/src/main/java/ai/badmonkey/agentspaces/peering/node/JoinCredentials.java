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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;

import java.security.PrivateKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/**
 * Join credentials for INVITE groups (spec §5.1, §6.1). Two forms travel under
 * the same {@link #HINT_KEY} hint and are verified by every member against
 * the group's founding advertisement with public data only:
 *
 * <ul>
 *   <li>the <em>founder-issued</em> credential: the founder's signature over
 *   {@code (candidate, group)}, issued per candidate by {@link #issue};</li>
 *   <li>the <em>delegated</em> credential: the founder signs, once, a
 *   {@link #delegate delegation} of an invite signing key with an expiry and
 *   a label; the invite key then signs each candidate ({@link #issueDelegated}).
 *   Whoever holds the invite key's secret can admit themselves until the
 *   delegation expires or the founder revokes it by its {@link #delegationHash
 *   hash} through the ordinary {@code JOIN_CREDENTIAL} revocation, which
 *   refuses every credential issued under it at once.</li>
 * </ul>
 *
 * <p>A delegated credential string is prefixed {@value #DELEGATED_PREFIX} so
 * the two forms never parse as each other. Neither form changes the wire:
 * both are opaque strings in a self-advertisement's resource hints.
 */
public final class JoinCredentials {

    /** The resource-hint key a candidate carries its join credential under. */
    public static final String HINT_KEY = "aspace:join";

    /**
     * The resource-hint key a peer carries its OIDC access token under
     * (TODO-EFG §4, token ingestion). The token rides in the peer's signed
     * self-advertisement exactly as a join credential does, so every member
     * that judges the peer receives it through a channel it already
     * authenticates; receivers hand it to {@code OidcAuthorizer.authorize}
     * through {@link PeerNode.Builder#onCredentialHints}. A token is bound to
     * one PeerID by its {@code agentspaces_peer} claim, so a replayed hint
     * grants nothing to another peer. The token adds its size (typically under
     * 2 KiB) to every self-advertisement the peer gossips.
     */
    public static final String OIDC_HINT_KEY = "aspace:oidc";

    /** The prefix of a delegated credential string. */
    public static final String DELEGATED_PREFIX = "d1:";

    /** The clock skew a delegation's expiry is judged with, the same as advertisements allow. */
    static final Duration EXPIRY_SKEW = Duration.ofMinutes(10);

    private JoinCredentials() {
    }

    /** The bytes a founder signs: the candidate bound to the group. */
    private record CredentialView(PeerId candidate, GroupId group) {
    }

    /** The wire form of a founder-issued credential: the founder's key and its signature. */
    private record Credential(byte[] founderKey, byte[] signature) {
    }

    /** The bytes a founder signs when delegating: the invite key bound to the group, with an expiry and a label. */
    private record DelegationView(byte[] inviteKey, GroupId group, long notAfterMillis, String label) {
    }

    /** The wire form of a delegation: the founder's key, the view, and the founder's signature over the view. */
    private record Delegation(byte[] founderKey, byte[] inviteKey, GroupId group, long notAfterMillis,
                              String label, byte[] signature) {
    }

    /** The wire form of a delegated credential: the delegation and the invite key's signature over the candidate view. */
    private record DelegatedCredential(Delegation delegation, byte[] signature) {
    }

    /**
     * What a delegation says, for a reader that holds its string.
     *
     * @param founder  the founder who signed it
     * @param inviteKey the raw Ed25519 public key it delegates to
     * @param group    the group
     * @param notAfter when it expires
     * @param label    the founder's label for it
     */
    public record DelegationInfo(PeerId founder, byte[] inviteKey, GroupId group, Instant notAfter, String label) {
    }

    /**
     * Issues a join credential for a candidate. Run by a founder (a peer whose
     * PeerID is the group's issuer); hand the returned string to the candidate
     * out of band, and the candidate presents it via
     * {@code PeerNode.joinGroup(..., Map.of(HINT_KEY, credential), ...)}.
     *
     * @param founder   the founder identity (must be the group's issuer)
     * @param group     the group id
     * @param candidate the PeerID being admitted
     * @return the base64 credential string
     */
    public static String issue(PeerIdentity founder, GroupId group, PeerId candidate) {
        Objects.requireNonNull(founder, "founder");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(candidate, "candidate");
        CborCodec codec = CborCodec.defaultCodec();
        byte[] view = codec.toBytes(new CredentialView(candidate, group));
        Credential credential = new Credential(founder.rawPublicKey(), founder.sign(view));
        return Base64.getEncoder().encodeToString(codec.toBytes(credential));
    }

    /**
     * Delegates admission to an invite signing key: the founder signs the
     * key bound to the group with an expiry and a label. The returned string
     * is public data (it admits nobody by itself) and is what
     * {@link #issueDelegated} and {@link #delegationHash} take.
     *
     * @param founder   the founder identity (must be the group's issuer)
     * @param group     the group id
     * @param inviteKey the raw 32-byte Ed25519 public key to delegate to
     * @param notAfter  when the delegation expires
     * @param label     a label the founder recognises ("weekend hack, March")
     * @return the base64 delegation string
     */
    public static String delegate(PeerIdentity founder, GroupId group, byte[] inviteKey,
                                  Instant notAfter, String label) {
        Objects.requireNonNull(founder, "founder");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(inviteKey, "inviteKey");
        Objects.requireNonNull(notAfter, "notAfter");
        if (inviteKey.length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            throw new IllegalArgumentException("an invite key is a raw 32-byte Ed25519 public key");
        }
        CborCodec codec = CborCodec.defaultCodec();
        long notAfterMillis = notAfter.toEpochMilli();
        String text = label == null ? "" : label;
        byte[] view = codec.toBytes(new DelegationView(inviteKey, group, notAfterMillis, text));
        Delegation delegation = new Delegation(founder.rawPublicKey(), inviteKey, group, notAfterMillis,
                text, founder.sign(view));
        return Base64.getEncoder().encodeToString(codec.toBytes(delegation));
    }

    /**
     * Issues a delegated credential for a candidate: the invite signing key's
     * signature over {@code (candidate, group)} under the delegation.
     *
     * @param delegation       the delegation string from {@link #delegate}
     * @param inviteSigningKey the private half of the delegated key
     * @param candidate        the PeerID being admitted
     * @return the credential string, prefixed {@value #DELEGATED_PREFIX}
     * @throws IllegalArgumentException when the delegation string does not parse
     */
    public static String issueDelegated(String delegation, PrivateKey inviteSigningKey, PeerId candidate) {
        Objects.requireNonNull(inviteSigningKey, "inviteSigningKey");
        Objects.requireNonNull(candidate, "candidate");
        CborCodec codec = CborCodec.defaultCodec();
        Delegation parsed = parseDelegation(delegation, codec);
        if (parsed == null) {
            throw new IllegalArgumentException("not a delegation string");
        }
        byte[] view = codec.toBytes(new CredentialView(candidate, parsed.group()));
        DelegatedCredential credential = new DelegatedCredential(parsed, Ed25519.sign(inviteSigningKey, view));
        return DELEGATED_PREFIX + Base64.getEncoder().encodeToString(codec.toBytes(credential));
    }

    /**
     * Reads a delegation string.
     *
     * @param delegation the string from {@link #delegate}
     * @return what it says, or empty when it does not parse
     */
    public static Optional<DelegationInfo> describe(String delegation) {
        Delegation parsed = parseDelegation(delegation, CborCodec.defaultCodec());
        if (parsed == null) {
            return Optional.empty();
        }
        return Optional.of(new DelegationInfo(PeerId.fromPublicKey(parsed.founderKey()), parsed.inviteKey(),
                parsed.group(), Instant.ofEpochMilli(parsed.notAfterMillis()), parsed.label()));
    }

    /**
     * The SHA-256 of a credential's bytes, which a {@code JOIN_CREDENTIAL}
     * revocation names (SPEC §6.1, v0.1.13). For a delegated credential, the
     * hash of the delegated credential's own bytes (revoking one friend); see
     * {@link #delegationHash} for the hash that revokes the whole key.
     *
     * @param encoded the credential string
     * @return the 32-byte hash
     * @throws IllegalArgumentException when the string is not base64
     */
    public static byte[] hash(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        return ai.badmonkey.agentspaces.common.crypto.Digests.sha256(
                Base64.getDecoder().decode(stripPrefix(encoded.trim())));
    }

    /**
     * The SHA-256 of a delegation's bytes: what a {@code JOIN_CREDENTIAL}
     * revocation names to refuse every credential issued under it.
     *
     * @param delegationOrCredential a delegation string, or a delegated credential string
     * @return the 32-byte hash, or null when the string is a founder-issued credential or does not parse
     */
    public static byte[] delegationHash(String delegationOrCredential) {
        if (delegationOrCredential == null) {
            return null;
        }
        CborCodec codec = CborCodec.defaultCodec();
        String trimmed = delegationOrCredential.trim();
        Delegation delegation;
        if (trimmed.startsWith(DELEGATED_PREFIX)) {
            DelegatedCredential credential = parseDelegated(trimmed, codec);
            delegation = credential == null ? null : credential.delegation();
        } else {
            delegation = parseDelegation(trimmed, codec);
        }
        if (delegation == null) {
            return null;
        }
        return ai.badmonkey.agentspaces.common.crypto.Digests.sha256(codec.toBytes(delegation));
    }

    /**
     * What a verifier concludes about a credential (ISSUE-PeerRobustness-2,
     * R2-1.7): {@code EXPIRED} is reserved for the delegated form whose every
     * check passed except the delegation's expiry, so an admission memory can
     * tell a lapsed member from a forgery without parsing the credential twice.
     */
    public enum Verdict {
        /** The credential admits the candidate now. */
        VALID,
        /** A delegated credential that would admit the candidate but for the delegation's expiry. */
        EXPIRED,
        /** Malformed, wrong founder, wrong group, wrong candidate, or a bad signature. */
        INVALID
    }

    /**
     * Verifies a candidate's credential against the group's founding
     * advertisement, judging a delegation's expiry at {@code now}.
     *
     * @param encoded   the credential the candidate presented
     * @param candidate the candidate's PeerID (its self-ad issuer)
     * @param groupAd   the group's founding advertisement
     * @param codec     the codec
     * @param now       the verifier's clock
     * @return {@code true} when the credential admits the candidate
     */
    static boolean verify(String encoded, PeerId candidate, GroupAdvertisement groupAd, CborCodec codec,
                          Instant now) {
        return judge(encoded, candidate, groupAd, codec, now) == Verdict.VALID;
    }

    /**
     * As {@link #verify(String, PeerId, GroupAdvertisement, CborCodec, Instant)},
     * distinguishing a delegation that would verify but for its expiry. The
     * founder-issued form has no expiry and is {@code VALID} or {@code INVALID}.
     *
     * @param encoded   the credential the candidate presented
     * @param candidate the candidate's PeerID (its self-ad issuer)
     * @param groupAd   the group's founding advertisement
     * @param codec     the codec
     * @param now       the verifier's clock
     * @return the verdict
     */
    static Verdict judge(String encoded, PeerId candidate, GroupAdvertisement groupAd, CborCodec codec,
                         Instant now) {
        if (encoded == null) {
            return Verdict.INVALID;
        }
        String trimmed = encoded.trim();
        if (trimmed.startsWith(DELEGATED_PREFIX)) {
            return judgeDelegated(trimmed, candidate, groupAd, codec, now);
        }
        try {
            Credential credential = codec.fromBytes(Base64.getDecoder().decode(trimmed), Credential.class);
            if (credential == null || credential.founderKey() == null
                    || credential.signature() == null
                    || credential.founderKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
                return Verdict.INVALID;
            }
            // Self-certifying: the signer must be the group's founder (issuer).
            if (!PeerId.fromPublicKey(credential.founderKey()).equals(groupAd.issuer())) {
                return Verdict.INVALID;
            }
            byte[] view = codec.toBytes(new CredentialView(candidate, groupAd.group()));
            return Ed25519.verify(Ed25519.publicKeyFromRaw(credential.founderKey()),
                    view, credential.signature()) ? Verdict.VALID : Verdict.INVALID;
        } catch (RuntimeException e) {
            return Verdict.INVALID;
        }
    }

    /**
     * Whether a credential admits a candidate to a group, judged at
     * {@code now}: what every member checks when the candidate's
     * self-advertisement arrives, available to a service that must know
     * before it joins (a beacon asked to register a group checks that the
     * credential it was handed admits its own peer).
     *
     * @param encoded   the credential string
     * @param candidate the candidate's PeerID
     * @param groupAd   the group's founding advertisement
     * @param now       the clock
     * @return {@code true} when the credential admits the candidate
     */
    public static boolean admits(String encoded, PeerId candidate, GroupAdvertisement groupAd, Instant now) {
        return verify(encoded, candidate, groupAd, CborCodec.defaultCodec(), now);
    }

    /**
     * Verifies a candidate's credential against the group's founding
     * advertisement at the current time.
     *
     * @param encoded   the credential the candidate presented
     * @param candidate the candidate's PeerID
     * @param groupAd   the group's founding advertisement
     * @param codec     the codec
     * @return {@code true} when the credential admits the candidate
     */
    static boolean verify(String encoded, PeerId candidate, GroupAdvertisement groupAd, CborCodec codec) {
        return verify(encoded, candidate, groupAd, codec, Instant.now());
    }

    /**
     * The delegated checks with the expiry judged last: {@code EXPIRED} only
     * when the delegation is well formed, the founder's, for this group, and
     * signed by both the founder and the invite key, and the candidate's
     * signature verifies, so nothing an admission memory re-admits on that
     * verdict could have been refused for any other reason.
     */
    private static Verdict judgeDelegated(String encoded, PeerId candidate, GroupAdvertisement groupAd,
                                          CborCodec codec, Instant now) {
        try {
            DelegatedCredential credential = parseDelegated(encoded, codec);
            if (credential == null || credential.signature() == null) {
                return Verdict.INVALID;
            }
            Delegation delegation = credential.delegation();
            if (!delegationVerifies(delegation, groupAd, codec)) {
                return Verdict.INVALID;
            }
            byte[] view = codec.toBytes(new CredentialView(candidate, groupAd.group()));
            if (!Ed25519.verifyRaw(delegation.inviteKey(), view, credential.signature())) {
                return Verdict.INVALID;
            }
            return now.minus(EXPIRY_SKEW).toEpochMilli() > delegation.notAfterMillis()
                    ? Verdict.EXPIRED : Verdict.VALID;
        } catch (RuntimeException e) {
            return Verdict.INVALID;
        }
    }

    /** Every check on a delegation except its expiry. */
    private static boolean delegationVerifies(Delegation delegation, GroupAdvertisement groupAd,
                                              CborCodec codec) {
        if (delegation == null || delegation.founderKey() == null || delegation.inviteKey() == null
                || delegation.signature() == null || delegation.group() == null
                || delegation.founderKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || delegation.inviteKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return false;
        }
        if (!PeerId.fromPublicKey(delegation.founderKey()).equals(groupAd.issuer())) {
            return false;
        }
        if (!delegation.group().equals(groupAd.group())) {
            return false;
        }
        byte[] view = codec.toBytes(new DelegationView(delegation.inviteKey(), delegation.group(),
                delegation.notAfterMillis(), delegation.label()));
        return Ed25519.verifyRaw(delegation.founderKey(), view, delegation.signature());
    }

    private static Delegation parseDelegation(String delegation, CborCodec codec) {
        if (delegation == null) {
            return null;
        }
        try {
            Delegation parsed = codec.fromBytes(Base64.getDecoder().decode(delegation.trim()), Delegation.class);
            return parsed == null || parsed.founderKey() == null || parsed.inviteKey() == null
                    || parsed.signature() == null || parsed.group() == null ? null : parsed;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static DelegatedCredential parseDelegated(String encoded, CborCodec codec) {
        try {
            DelegatedCredential parsed = codec.fromBytes(
                    Base64.getDecoder().decode(stripPrefix(encoded)), DelegatedCredential.class);
            return parsed == null || parsed.delegation() == null ? null : parsed;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String stripPrefix(String encoded) {
        return encoded.startsWith(DELEGATED_PREFIX) ? encoded.substring(DELEGATED_PREFIX.length()) : encoded;
    }
}
