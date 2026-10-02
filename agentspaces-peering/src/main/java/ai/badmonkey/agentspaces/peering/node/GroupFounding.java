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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Founds self-certifying groups and verifies their founding advertisements
 * (spec §4.4, §5.1). A GroupID is the hash of the group's founding document,
 * so possession of the document proves the name and, just as importantly, no
 * one can publish a <em>different</em> policy under the same name: a receiver
 * that re-derives the id from the document it was handed detects any
 * substitution.
 *
 * <p>The founding document is defined precisely as follows.
 * <ol>
 *   <li>The <em>founding fields</em> are the advertisement's immutable
 *       fields, in this order: {@code name}, {@code founder} (the issuer
 *       PeerID), {@code issued}, {@code ttl}, {@code membershipPolicy},
 *       {@code defaultStrategy}, {@code gossip} — the record
 *       {@link FoundingFields}, encoded as canonical CBOR by
 *       {@link CborCodec#defaultCodec()}.</li>
 *   <li>The founder signs those canonical bytes with its Ed25519 identity key
 *       (RFC 8032; deterministic, so the same founder founding the same
 *       fields always derives the same id).</li>
 *   <li>The <em>founding document</em> is the record {@link FoundingDocument}
 *       (the fields plus that signature), again as canonical CBOR, and
 *       {@code GroupId = GroupId.fromFounding(canonical CBOR of the founding
 *       document)}.</li>
 *   <li>The advertisement's {@code id} is {@code "aspace://" + groupId}.</li>
 * </ol>
 *
 * <p>{@link #verify} accepts a {@link SignedGroupAdvertisement} only when the
 * carried key hashes to the advertisement's issuer, the signature verifies
 * under that key over the founding fields, the derived id equals the
 * advertisement's {@code group}, and the {@code id} URI matches. Any group
 * advertisement that arrives from the network MUST pass this check before a
 * node acts on it; locally configured literal-id groups (the older
 * {@code PeerNode.joinGroup(GroupAdvertisement, ...)}) are exempt only because
 * their advertisement is local configuration the network cannot swap.
 */
public final class GroupFounding {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    /** The URI scheme prefix a founding advertisement's {@code id} carries. */
    public static final String URI_PREFIX = "aspace://";

    private GroupFounding() {
    }

    /**
     * The immutable founding fields of a group: everything a member relies on
     * that no later party may change. Field order is the canonical order.
     *
     * @param name             the human-readable group name
     * @param founder          the founding peer
     * @param issued           the founding instant
     * @param ttl              the advertisement's cache time-to-live
     * @param membershipPolicy who may join
     * @param defaultStrategy  the default conflict strategy of the group's spaces
     * @param gossip           the group's gossip parameters
     */
    public record FoundingFields(String name,
                                 PeerId founder,
                                 Instant issued,
                                 Duration ttl,
                                 GroupAdvertisement.MembershipPolicy membershipPolicy,
                                 ConflictStrategyType defaultStrategy,
                                 GroupAdvertisement.GossipParameters gossip) {
        public FoundingFields {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(founder, "founder");
            Objects.requireNonNull(issued, "issued");
            Objects.requireNonNull(ttl, "ttl");
            Objects.requireNonNull(membershipPolicy, "membershipPolicy");
            Objects.requireNonNull(defaultStrategy, "defaultStrategy");
            Objects.requireNonNull(gossip, "gossip");
        }
    }

    /**
     * The founding document whose canonical CBOR hashes to the GroupID: the
     * founding fields and the founder's signature over their canonical bytes.
     *
     * @param fields    the founding fields
     * @param signature the founder's Ed25519 signature over {@code CBOR(fields)}
     */
    public record FoundingDocument(FoundingFields fields, byte[] signature) {
        public FoundingDocument {
            Objects.requireNonNull(fields, "fields");
            Objects.requireNonNull(signature, "signature");
        }
    }

    /**
     * Founds a group: signs the founding fields, derives the self-certifying
     * GroupID, and returns the signed founding advertisement to join with and
     * to serve to newcomers.
     *
     * @param founder          the founding identity (becomes the issuer)
     * @param name             the group name
     * @param membershipPolicy who may join
     * @param defaultStrategy  the default conflict strategy of the group's spaces
     * @param gossip           the gossip parameters
     * @param issued           the founding instant
     * @param ttl              the advertisement's cache time-to-live
     * @return the signed, self-certifying founding advertisement
     */
    public static SignedGroupAdvertisement found(PeerIdentity founder, String name,
                                                 GroupAdvertisement.MembershipPolicy membershipPolicy,
                                                 ConflictStrategyType defaultStrategy,
                                                 GroupAdvertisement.GossipParameters gossip,
                                                 Instant issued, Duration ttl) {
        Objects.requireNonNull(founder, "founder");
        FoundingFields fields = new FoundingFields(name, founder.peerId(), issued, ttl,
                membershipPolicy, defaultStrategy, gossip);
        byte[] signature = founder.sign(CODEC.toBytes(fields));
        GroupId groupId = derive(fields, signature);
        GroupAdvertisement ad = new GroupAdvertisement(URI_PREFIX + groupId.value(),
                founder.peerId(), groupId, issued, ttl, name, membershipPolicy,
                defaultStrategy, gossip);
        return new SignedGroupAdvertisement(ad, founder.rawPublicKey(), signature);
    }

    /**
     * Derives the self-certifying GroupID of a founding document.
     *
     * @param fields    the founding fields
     * @param signature the founder's signature over the fields' canonical bytes
     * @return the GroupID
     */
    public static GroupId derive(FoundingFields fields, byte[] signature) {
        return GroupId.fromFounding(CODEC.toBytes(new FoundingDocument(fields, signature)));
    }

    /**
     * Extracts the founding fields of an advertisement, in canonical order.
     *
     * @param ad the advertisement
     * @return its founding fields
     */
    public static FoundingFields fieldsOf(GroupAdvertisement ad) {
        Objects.requireNonNull(ad, "ad");
        return new FoundingFields(ad.name(), ad.issuer(), ad.issued(), ad.ttl(),
                ad.membershipPolicy(), ad.defaultStrategy(), ad.gossip());
    }

    /**
     * Verifies that a signed group advertisement is the genuine founding
     * document of the group it names: the founder key hashes to the issuer,
     * the signature verifies over the founding fields, the derived id equals
     * {@code advertisement().group()}, and the {@code id} URI names that group.
     *
     * @param signed the signed advertisement
     * @return {@code true} only when every check passes
     */
    public static boolean verify(SignedGroupAdvertisement signed) {
        if (signed == null || signed.advertisement() == null
                || signed.founderPublicKey() == null || signed.signature() == null) {
            return false;
        }
        GroupAdvertisement ad = signed.advertisement();
        byte[] key = signed.founderPublicKey();
        if (key.length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !PeerId.fromPublicKey(key).equals(ad.issuer())) {
            return false;
        }
        FoundingFields fields;
        try {
            fields = fieldsOf(ad);
            if (!Ed25519.verify(Ed25519.publicKeyFromRaw(key), CODEC.toBytes(fields),
                    signed.signature())) {
                return false;
            }
        } catch (RuntimeException e) {
            return false;
        }
        return derive(fields, signed.signature()).equals(ad.group())
                && (URI_PREFIX + ad.group().value()).equals(ad.id());
    }
}
