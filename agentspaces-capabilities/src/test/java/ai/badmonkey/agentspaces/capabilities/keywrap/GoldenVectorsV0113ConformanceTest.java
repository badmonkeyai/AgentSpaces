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
package ai.badmonkey.agentspaces.capabilities.keywrap;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.ad.CredentialRevocation;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap;
import ai.badmonkey.agentspaces.common.crypto.X25519;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.identity.AgentCertificates;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The v0.1.13 golden vectors against the production classes (TODO-9-10-11
 * Phase 3): what the generator wrote is what production encodes, and each
 * vector carries the verdict the clients must reproduce. The fold-path
 * verdicts for agent-signed states live in {@code SignedAgentVectorsClusterTest}.
 */
class GoldenVectorsV0113ConformanceTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static final HexFormat HEX = HexFormat.of();
    private static Map<String, Object> golden;
    private final CborCodec codec = CborCodec.defaultCodec();

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void loadVectors() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (GOLDEN_REQUIRED) {
            assertThat(found).as("golden.json is required but none of " + GOLDEN_PATHS + " exists").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
    }

    private static byte[] bytes(String key) {
        assertThat(golden).as("vector " + key).containsKey(key);
        return HEX.parseHex((String) golden.get(key));
    }

    private static String text(String key) {
        return (String) golden.get(key);
    }

    private PeerId peer() {
        return PeerId.of(text("peer_id"));
    }

    private AgentId python() {
        return new AgentId(peer(), "python");
    }

    @Test
    void theEpochCommitmentAndWrapBindingAreTheProductionBytes() {
        GroupKey epochTwo = GroupKey.fromBytes(bytes("content_key_epoch_2"));
        byte[] commitment = codec.toBytes(new GroupKeyDistributor.EpochCommitment(text("group_id"), 2L,
                "2025-01-01T00:00:00Z", Digests.sha256(epochTwo.rawBytes())));
        assertThat(HEX.formatHex(commitment)).isEqualTo(text("epoch_commitment_cbor"));
        assertThat(Ed25519.verifyRaw(bytes("public_key_raw"), commitment, bytes("epoch_proof_signature"))).isTrue();
        PeerId forger = PeerId.fromPublicKey(bytes("adv_forger_public_key_raw"));
        byte[] binding = codec.toBytes(new GroupKeyDistributor.WrapBinding(text("group_id"), peer().value(),
                forger.value(), 7L, 2L, peer().value(), python().encoded()));
        assertThat(HEX.formatHex(binding)).isEqualTo(text("wrap_binding_v2_cbor"));
        assertThat(HEX.formatHex(codec.toBytes(new GroupKeyDistributor.WrapBinding(text("group_id"),
                peer().value(), forger.value(), 7L, null, null, null))))
                .as("a v2 binding without epoch fields omits them").doesNotContain(
                        HEX.formatHex("epoch".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void anEpochTwoRecordNamesItsEpochAndOpensUnderTheEpochAad() {
        byte[] recordBytes = bytes("entry_record_key_epoch_cbor");
        EntryRecord record = codec.fromBytes(recordBytes, EntryRecord.class);
        assertThat(record.keyEpoch()).isEqualTo(2L);
        assertThat(codec.toBytes(record)).as("decode and re-encode is stable").isEqualTo(recordBytes);
        assertThat(Ed25519.verifyRaw(bytes("public_key_raw"), bytes("sign_view_key_epoch_cbor"), record.sig()))
                .isTrue();
        String aad = text("space_id") + "|11111111-2222-3333-4444-555555555555|2";
        assertThat(text("content_key_aad_epoch_2")).isEqualTo(aad);
        assertThat(GroupKey.fromBytes(bytes("content_key_epoch_2"))
                .decrypt(record.payload(), aad.getBytes(StandardCharsets.UTF_8)))
                .hasValueSatisfying(plain -> assertThat(plain).isEqualTo(bytes("task_payload_cbor")));
        assertThat(GroupKey.fromBytes(bytes("content_key_epoch_2"))
                .decrypt(record.payload(), (aad.substring(0, aad.length() - 1) + "1").getBytes(StandardCharsets.UTF_8)))
                .as("relabelled to another epoch, it does not open").isEmpty();
    }

    @Test
    void theEncryptionKeyCertificateVerifiesAndOmitsNothingItNeeds() throws Exception {
        AgentCertificate certificate = codec.fromBytes(bytes("agent_certificate_with_encryption_cbor"),
                AgentCertificate.class);
        assertThat(certificate.encryptionPublicKey()).isEqualTo(bytes("agent_encryption_public_key_raw"));
        assertThat(codec.toBytes(certificate.unsigned()))
                .isEqualTo(bytes("agent_certificate_with_encryption_unsigned_cbor"));
        Instant verifyAt = Instant.parse(text("certificate_verify_at"));
        assertThat(new AgentCertificates(codec).verify(certificate, bytes("public_key_raw"), python(), verifyAt))
                .isTrue();
        java.security.PrivateKey x25519 = java.security.KeyFactory.getInstance("X25519").generatePrivate(
                new java.security.spec.PKCS8EncodedKeySpec(bytes("agent_encryption_private_key_pkcs8")));
        java.security.KeyPair other = X25519.generate();
        assertThat(X25519.agree(x25519, X25519.rawPublicKey(other.getPublic())))
                .as("the private key pairs with the certified public key")
                .isEqualTo(X25519.agree(other.getPrivate(), certificate.encryptionPublicKey()));
        AgentCertificate plain = codec.fromBytes(bytes("agent_certificate_cbor"), AgentCertificate.class);
        assertThat(plain.encryptionPublicKey()).as("the v0.1.11 certificate is unchanged").isNull();
    }

    @Test
    void theCredentialRevocationsVerifyUnderTheAgentsOwnPeerWithTheFreezeRule() {
        GroupId group = GroupId.of(text("group_id"));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + group.value(),
                PeerIdentity.generate().peerId(), group, Instant.EPOCH, Duration.ofDays(1), "golden",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        CredentialRevocationRegistry registry = new CredentialRevocationRegistry(groupAd,
                CredentialRevocationRegistry.Validator.defaults(), codec, InstantSource.system());
        for (String kind : List.of("agent", "agent_key")) {
            byte[] adBytes = bytes("credential_revocation_" + kind + "_cbor");
            CredentialRevocation decoded = codec.fromBytes(adBytes, CredentialRevocation.class);
            assertThat(codec.toBytes(decoded)).as(kind + " re-encodes stably").isEqualTo(adBytes);
            RevocationRegistry.SignedRevocation signed = codec.fromBytes(
                    bytes("signed_credential_revocation_" + kind + "_cbor"), RevocationRegistry.SignedRevocation.class);
            assertThat(signed.adBytes()).isEqualTo(adBytes);
            assertThat(registry.accept(signed)).as(kind + " accepted from the agent's own peer").isPresent();
        }
        Instant issued = Instant.parse("2026-01-01T00:00:00Z");
        assertThat(registry.refuses(python(), null, issued.minus(Duration.ofMinutes(90))))
                .as("retired an hour before issue: earlier history stands").isFalse();
        assertThat(registry.refuses(python(), null, issued.minus(Duration.ofMinutes(30)))).isTrue();
        assertThat(registry.refuses(python(), bytes("agent_public_key_raw"), Instant.EPOCH))
                .as("the compromised key is refused whatever its time").isTrue();
    }

    @Test
    void theCardWithActionsAndACertificateIsAdmittedAtItsIssueTime() {
        byte[] cardBytes = bytes("agent_card_with_actions_cbor");
        AgentCard card = codec.fromBytes(cardBytes, AgentCard.class);
        assertThat(codec.toBytes(card)).isEqualTo(cardBytes);
        assertThat(card.actions()).singleElement().satisfies(action -> {
            assertThat(action.name()).isEqualTo("research");
            assertThat(action.kind()).isEqualTo(CardAction.TAKE);
            assertThat(action.space()).isEqualTo("tasks");
        });
        assertThat(card.agentCertificate()).isNotNull();
        AdCache cache = new AdCache(codec, InstantSource.fixed(Instant.parse(text("certificate_verify_at"))));
        assertThat(cache.accept(new AdCache.StoredAd("AgentCard", cardBytes, bytes("public_key_raw"),
                bytes("agent_card_with_actions_signature")))).isPresent();
    }

    // ------------------------------------------------------------ audit item 10

    private CredentialRevocationRegistry registryFoundedBy(PeerId founder) {
        GroupId group = GroupId.of(text("group_id"));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + group.value(), founder, group,
                Instant.EPOCH, Duration.ofDays(1), "golden", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        return new CredentialRevocationRegistry(groupAd, CredentialRevocationRegistry.Validator.defaults(), codec,
                InstantSource.fixed(Instant.parse("2026-06-01T00:00:00Z")));
    }

    @Test
    void theLeafAndJoinCredentialRevocationsAreTheProductionBytesAndOnlyAFounderIssuesThem() {
        GroupId group = GroupId.of(text("group_id"));
        Instant issued = Instant.parse("2026-01-01T00:00:00Z");
        byte[] leafFingerprint = Digests.sha256("golden-leaf-der".getBytes(StandardCharsets.UTF_8));
        byte[] credentialHash = Digests.sha256("golden-join-credential".getBytes(StandardCharsets.UTF_8));
        assertThat(codec.toBytes(CredentialRevocation.of(peer(), group, issued, Duration.ofDays(30),
                CredentialRevocation.Target.x509Leaf("CN=golden-leaf,O=Bad Monkey", "0a1b2c3d", leafFingerprint),
                CredentialRevocation.SUPERSEDED))).isEqualTo(bytes("credential_revocation_x509_leaf_cbor"));
        assertThat(codec.toBytes(CredentialRevocation.of(peer(), group, issued, Duration.ofDays(30),
                CredentialRevocation.Target.joinCredential(credentialHash),
                CredentialRevocation.PRIVILEGE_WITHDRAWN))).isEqualTo(bytes("credential_revocation_join_credential_cbor"));

        CredentialRevocationRegistry founded = registryFoundedBy(peer());
        CredentialRevocationRegistry stranger = registryFoundedBy(PeerIdentity.generate().peerId());
        for (String kind : List.of("x509_leaf", "join_credential")) {
            byte[] adBytes = bytes("credential_revocation_" + kind + "_cbor");
            CredentialRevocation decoded = codec.fromBytes(adBytes, CredentialRevocation.class);
            assertThat(codec.toBytes(decoded)).as(kind + " re-encodes stably").isEqualTo(adBytes);
            assertThat(decoded.id()).isEqualTo(CredentialRevocation.idFor(group, decoded.target()));
            byte[] signed = bytes("signed_credential_revocation_" + kind + "_cbor");
            assertThat(founded.accept(signed)).as(kind + " from the founder").isPresent();
            assertThat(stranger.accept(signed)).as(kind + " from a peer that did not found the group").isEmpty();
        }
        assertThat(founded.leafRevoked(leafFingerprint)).isTrue();
        assertThat(founded.joinCredentialRevoked(credentialHash)).isTrue();
        assertThat(stranger.leafRevoked(leafFingerprint)).isFalse();
        assertThat(stranger.joinCredentialRevoked(credentialHash)).isFalse();
    }

    @Test
    void aRevocationOfAnAgentByAPeerThatDoesNotHostItIsRefused() {
        RevocationRegistry.SignedRevocation signed = codec.fromBytes(
                bytes("adv_credential_revocation_cross_issuer_cbor"), RevocationRegistry.SignedRevocation.class);
        CredentialRevocation ad = codec.fromBytes(signed.adBytes(), CredentialRevocation.class);
        PeerId forger = PeerId.fromPublicKey(bytes("adv_forger_public_key_raw"));
        assertThat(ad.issuer()).isEqualTo(forger);
        assertThat(ad.target().agent()).isEqualTo(python());
        assertThat(Ed25519.verifyRaw(signed.publicKey(), signed.adBytes(), signed.signature()))
                .as("authentic: the forger really signed it").isTrue();
        for (PeerId founder : List.of(peer(), PeerIdentity.generate().peerId())) {
            CredentialRevocationRegistry registry = registryFoundedBy(founder);
            assertThat(registry.verify(signed)).as("no authority, founder " + founder.display()).isNull();
            assertThat(registry.accept(signed)).isEmpty();
            assertThat(registry.refuses(python(), bytes("agent_public_key_raw"), Instant.EPOCH)).isFalse();
        }
        // The control: the same target revoked by the agent's own peer is accepted.
        assertThat(registryFoundedBy(PeerIdentity.generate().peerId())
                .accept(bytes("signed_credential_revocation_agent_cbor"))).isPresent();
    }

    @Test
    void aWrapOpensOnlyUnderTheBindingItsRequesterExpects() throws Exception {
        java.security.PrivateKey agentX25519 = java.security.KeyFactory.getInstance("X25519").generatePrivate(
                new java.security.spec.PKCS8EncodedKeySpec(bytes("agent_encryption_private_key_pkcs8")));
        byte[] agentPublic = bytes("agent_encryption_public_key_raw");
        PeerId forger = PeerId.fromPublicKey(bytes("adv_forger_public_key_raw"));
        // What GroupKeyDistributor.receive computes for this exchange: the requester's own view.
        byte[] expected = codec.toBytes(new GroupKeyDistributor.WrapBinding(text("group_id"), peer().value(),
                forger.value(), 7L, 2L, peer().value(), python().encoded()));
        assertThat(expected).isEqualTo(bytes("wrap_binding_v2_cbor"));
        byte[] wrapBytes = bytes("key_wrap_v2_cbor");
        GroupKeyWrap.WrappedKey wrap = codec.fromBytes(wrapBytes, GroupKeyWrap.WrappedKey.class);
        assertThat(codec.toBytes(wrap)).isEqualTo(wrapBytes);
        assertThat(GroupKeyWrap.unwrapBound(wrap, agentX25519, agentPublic, expected))
                .hasValueSatisfying(key -> assertThat(key.rawBytes()).isEqualTo(bytes("content_key_epoch_2")));

        Map<String, GroupKeyDistributor.WrapBinding> wrong = Map.of(
                "adv_key_wrap_wrong_group", new GroupKeyDistributor.WrapBinding(text("founding_group_id"),
                        peer().value(), forger.value(), 7L, 2L, peer().value(), python().encoded()),
                "adv_key_wrap_wrong_epoch", new GroupKeyDistributor.WrapBinding(text("group_id"),
                        peer().value(), forger.value(), 7L, 3L, peer().value(), python().encoded()),
                "adv_key_wrap_wrong_rotator", new GroupKeyDistributor.WrapBinding(text("group_id"),
                        peer().value(), forger.value(), 7L, 2L, forger.value(), python().encoded()));
        for (Map.Entry<String, GroupKeyDistributor.WrapBinding> hostile : wrong.entrySet()) {
            String name = hostile.getKey();
            byte[] binding = codec.toBytes(hostile.getValue());
            assertThat(binding).as(name + " binding").isEqualTo(bytes(name + "_binding_cbor"));
            GroupKeyWrap.WrappedKey sealed = codec.fromBytes(bytes(name + "_cbor"), GroupKeyWrap.WrappedKey.class);
            assertThat(GroupKeyWrap.unwrapBound(sealed, agentX25519, agentPublic, binding))
                    .as(name + " is a genuine wrap of the epoch key, for the wrong exchange").isPresent();
            assertThat(GroupKeyWrap.unwrapBound(sealed, agentX25519, agentPublic, expected))
                    .as(name + " does not open under the requester's binding").isEmpty();
        }
    }

    @Test
    void aRecordUnderARenewedCertificateIsJudgedAtItsSigningTime() {
        AgentCertificate first = codec.fromBytes(bytes("agent_certificate_cbor"), AgentCertificate.class);
        AgentCertificate renewed = codec.fromBytes(bytes("renewed_agent_certificate_cbor"), AgentCertificate.class);
        Instant verifyAt = Instant.parse(text("certificate_verify_at"));
        Instant signedAt = Instant.parse(text("renewed_certificate_signed_at"));
        // The production renewal reproduces the renewed certificate byte for byte.
        Instant[] clock = {verifyAt};
        PeerIdentity golden = PeerIdentity.of(new java.security.KeyPair(
                Ed25519.publicKeyFromRaw(bytes("public_key_raw")), Ed25519.privateKeyFromPkcs8(bytes("private_key_pkcs8"))));
        ai.badmonkey.agentspaces.api.spi.AgentIdentity renewing = golden.renewingSubordinate("python",
                new java.security.KeyPair(Ed25519.publicKeyFromRaw(bytes("agent_public_key_raw")),
                        Ed25519.privateKeyFromPkcs8(bytes("agent_private_key_pkcs8"))),
                Duration.ofHours(24), () -> clock[0]);
        assertThat(codec.toBytes(renewing.certificate().orElseThrow())).isEqualTo(bytes("agent_certificate_cbor"));
        clock[0] = verifyAt.plus(Duration.ofHours(13));
        renewing.renewIfDue(clock[0]);
        clock[0] = signedAt;
        assertThat(renewing.certificateCovering(signedAt).map(codec::toBytes))
                .hasValueSatisfying(b -> assertThat(b).isEqualTo(bytes("renewed_agent_certificate_cbor")));

        ai.badmonkey.agentspaces.space.replicated.SpaceWire.EntryStateDto dto = codec.fromBytes(
                bytes("entry_state_renewed_certificate_cbor"),
                ai.badmonkey.agentspaces.space.replicated.SpaceWire.EntryStateDto.class);
        assertThat(codec.toBytes(dto)).isEqualTo(bytes("entry_state_renewed_certificate_cbor"));
        assertThat(codec.toBytes(dto.agentCertificate())).isEqualTo(bytes("renewed_agent_certificate_cbor"));
        assertThat(Instant.ofEpochMilli(dto.record().issued().physical())).isEqualTo(signedAt);
        assertThat(Ed25519.verifyRaw(renewed.agentPublicKey(), bytes("sign_view_renewed_certificate_cbor"),
                dto.record().sig())).isTrue();
        AgentCertificates certificates = new AgentCertificates(codec);
        Instant longAfter = signedAt.plus(Duration.ofHours(30));
        assertThat(certificates.verifyAt(renewed, bytes("public_key_raw"), python(), signedAt, longAfter))
                .as("judged at the signing time, after the renewed certificate lapsed").isTrue();
        assertThat(certificates.verify(renewed, bytes("public_key_raw"), python(), longAfter))
                .as("judged at receipt it would be refused").isFalse();
        assertThat(certificates.verifyAt(first, bytes("public_key_raw"), python(), signedAt, signedAt))
                .as("the first certificate had lapsed at the signing time").isFalse();
    }
}
