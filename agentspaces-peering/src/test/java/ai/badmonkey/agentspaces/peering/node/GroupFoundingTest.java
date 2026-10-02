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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Self-certifying GroupIDs (spec §4.4, §5.1): the id is the hash of the
 * founding document, so it is deterministic for a founder and its fields,
 * changes with any founding field, and cannot be claimed by a forged or
 * mismatched signature.
 */
class GroupFoundingTest {

    private final PeerIdentity founder = PeerIdentity.generate();
    private final GroupAdvertisement.GossipParameters gossip =
            new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1));

    private SignedGroupAdvertisement found(GroupAdvertisement.MembershipPolicy policy) {
        return GroupFounding.found(founder, "fleet", policy, ConflictStrategyType.LEASE_RACE,
                gossip, Instant.EPOCH, Duration.ofDays(1));
    }

    /** Spec §4.4: founding the same fields with the same key derives the same id, and the document verifies. */
    @Test
    void derivationIsDeterministicAndVerifies() {
        SignedGroupAdvertisement first = found(GroupAdvertisement.MembershipPolicy.OPEN);
        SignedGroupAdvertisement second = found(GroupAdvertisement.MembershipPolicy.OPEN);

        assertThat(first.advertisement().group()).isEqualTo(second.advertisement().group());
        assertThat(first.advertisement().id())
                .isEqualTo("aspace://" + first.advertisement().group().value());
        assertThat(first.advertisement().issuer()).isEqualTo(founder.peerId());
        assertThat(GroupFounding.verify(first)).isTrue();
        assertThat(GroupFounding.derive(GroupFounding.fieldsOf(first.advertisement()),
                first.signature())).isEqualTo(first.advertisement().group());
        // The wire form round-trips and still verifies.
        CborCodec codec = CborCodec.defaultCodec();
        assertThat(GroupFounding.verify(codec.fromBytes(codec.toBytes(first),
                SignedGroupAdvertisement.class))).isTrue();
    }

    /** Spec §5.1: the membership policy is a founding field, so a different policy is a different group. */
    @Test
    void aTamperedPolicyChangesTheIdAndTheOldIdRefusesIt() {
        SignedGroupAdvertisement open = found(GroupAdvertisement.MembershipPolicy.OPEN);
        SignedGroupAdvertisement invite = found(GroupAdvertisement.MembershipPolicy.INVITE);
        assertThat(open.advertisement().group()).isNotEqualTo(invite.advertisement().group());

        // The founder's own INVITE document relabelled with the OPEN group's id.
        GroupAdvertisement a = invite.advertisement();
        GroupAdvertisement relabelled = new GroupAdvertisement(open.advertisement().id(),
                a.issuer(), open.advertisement().group(), a.issued(), a.ttl(), a.name(),
                a.membershipPolicy(), a.defaultStrategy(), a.gossip());
        assertThat(GroupFounding.verify(new SignedGroupAdvertisement(relabelled,
                invite.founderPublicKey(), invite.signature()))).isFalse();

        // The OPEN document with its policy flipped in place: the signature no longer covers it.
        GroupAdvertisement o = open.advertisement();
        GroupAdvertisement flipped = new GroupAdvertisement(o.id(), o.issuer(), o.group(),
                o.issued(), o.ttl(), o.name(), GroupAdvertisement.MembershipPolicy.INVITE,
                o.defaultStrategy(), o.gossip());
        assertThat(GroupFounding.verify(new SignedGroupAdvertisement(flipped,
                open.founderPublicKey(), open.signature()))).isFalse();
    }

    /** Spec §4.1/§4.4: a signature by anyone but the founder is refused, even over otherwise genuine fields. */
    @Test
    void aForgedSignatureIsRefused() {
        SignedGroupAdvertisement genuine = found(GroupAdvertisement.MembershipPolicy.OPEN);
        PeerIdentity forger = PeerIdentity.generate();
        byte[] forgedSignature = forger.sign(CborCodec.defaultCodec().toBytes(
                GroupFounding.fieldsOf(genuine.advertisement())));

        // Forger's signature under the founder's key.
        assertThat(GroupFounding.verify(new SignedGroupAdvertisement(genuine.advertisement(),
                genuine.founderPublicKey(), forgedSignature))).isFalse();
        // A flipped bit in the genuine signature.
        byte[] damaged = genuine.signature().clone();
        damaged[7] ^= 0x01;
        assertThat(GroupFounding.verify(new SignedGroupAdvertisement(genuine.advertisement(),
                genuine.founderPublicKey(), damaged))).isFalse();
    }

    /** Spec §4.1: the carried key must hash to the issuer; a forger's key with a valid self-signature is refused. */
    @Test
    void anIssuerKeyMismatchIsRefused() {
        SignedGroupAdvertisement genuine = found(GroupAdvertisement.MembershipPolicy.OPEN);
        PeerIdentity forger = PeerIdentity.generate();
        byte[] forgerSignature = forger.sign(CborCodec.defaultCodec().toBytes(
                GroupFounding.fieldsOf(genuine.advertisement())));

        // Consistent key and signature, but the key is not the issuer's.
        assertThat(GroupFounding.verify(new SignedGroupAdvertisement(genuine.advertisement(),
                forger.rawPublicKey(), forgerSignature))).isFalse();
        // A key of the wrong length never verifies.
        assertThat(GroupFounding.verify(new SignedGroupAdvertisement(genuine.advertisement(),
                new byte[31], genuine.signature()))).isFalse();
        // And a founder's document whose id URI names another group is refused too.
        GroupAdvertisement g = genuine.advertisement();
        GroupAdvertisement wrongUri = new GroupAdvertisement("aspace://" + GroupId.of("zOther").value(),
                g.issuer(), g.group(), g.issued(), g.ttl(), g.name(), g.membershipPolicy(),
                g.defaultStrategy(), g.gossip());
        assertThat(GroupFounding.verify(new SignedGroupAdvertisement(wrongUri,
                genuine.founderPublicKey(), genuine.signature()))).isFalse();
    }
}
