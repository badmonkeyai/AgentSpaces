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

import java.util.Base64;
import java.util.Objects;

/**
 * Founder-signed join credentials for {@code INVITE} groups (spec §5.1). A
 * founder issues a credential binding a specific candidate PeerID to a specific
 * group; the candidate carries it in its self-advertisement's resource hints
 * under {@link #HINT_KEY}, and every member verifies it before admitting the
 * candidate. The credential is self-certifying: it carries the founder's public
 * key, and admission requires that key to hash to the group's issuer (the
 * founder that created the group), so no out-of-band key distribution is needed.
 *
 * <p>The credential authorizes membership; it is not a capability. A holder
 * still proves control of its own PeerID by signing its self-advertisement as
 * usual, so a leaked credential cannot be replayed under a different identity.
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

    private JoinCredentials() {
    }

    /** The bytes a founder signs: the candidate bound to the group. */
    private record CredentialView(PeerId candidate, GroupId group) {
    }

    /** The wire form of a credential: the founder's key and its signature. */
    private record Credential(byte[] founderKey, byte[] signature) {
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
     * The SHA-256 of a credential's bytes, which a {@code JOIN_CREDENTIAL}
     * revocation names (SPEC §6.1, v0.1.13).
     *
     * @param encoded the base64 credential string
     * @return the 32-byte hash
     * @throws IllegalArgumentException when the string is not base64
     */
    public static byte[] hash(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        return ai.badmonkey.agentspaces.common.crypto.Digests.sha256(
                Base64.getDecoder().decode(encoded.trim()));
    }

    /**
     * Verifies a candidate's credential against the group's founding
     * advertisement.
     *
     * @param encoded   the base64 credential the candidate presented
     * @param candidate the candidate's PeerID (its self-ad issuer)
     * @param groupAd   the group's founding advertisement
     * @param codec     the codec
     * @return {@code true} when the credential admits the candidate
     */
    static boolean verify(String encoded, PeerId candidate,
                          GroupAdvertisement groupAd, CborCodec codec) {
        if (encoded == null) {
            return false;
        }
        try {
            Credential credential = codec.fromBytes(
                    Base64.getDecoder().decode(encoded), Credential.class);
            if (credential == null || credential.founderKey() == null
                    || credential.signature() == null
                    || credential.founderKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
                return false;
            }
            // Self-certifying: the signer must be the group's founder (issuer).
            if (!PeerId.fromPublicKey(credential.founderKey()).equals(groupAd.issuer())) {
                return false;
            }
            byte[] view = codec.toBytes(new CredentialView(candidate, groupAd.group()));
            return Ed25519.verify(Ed25519.publicKeyFromRaw(credential.founderKey()),
                    view, credential.signature());
        } catch (RuntimeException e) {
            return false;
        }
    }
}
