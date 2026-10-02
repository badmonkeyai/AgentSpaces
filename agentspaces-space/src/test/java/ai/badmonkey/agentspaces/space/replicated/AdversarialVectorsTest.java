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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.AgentCertificates;
import ai.badmonkey.agentspaces.peering.node.GroupFounding;
import ai.badmonkey.agentspaces.peering.node.SignedGroupAdvertisement;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Java half of the adversarial conformance vectors (security review §6 /
 * remediation plan Workstream 1): the {@code adv_*} entries in the shared
 * {@code golden.json} are hostile inputs every implementation must reject —
 * by failed decode, failed verification, or a bounds check. The Python and
 * TypeScript suites prove their rejections against the same bytes, so all
 * three implementations refuse identical inputs identically.
 *
 * <p>The end-to-end enforcement paths (a hostile delta arriving over gossip)
 * are proven in {@code ForgedStateRejectionTest} and
 * {@code NonMemberIsolationTest}; this test pins the codec- and contract-level
 * behavior to the exact shared vector bytes.
 */
class AdversarialVectorsTest {

    /** The vectors live with the non-JVM clients; the clients tree sits either
     * inside the repo or beside it at the workspace root. */
    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static Map<String, Object> golden;
    private static final CborCodec codec = CborCodec.defaultCodec();

    /** Set {@code -Dgolden.required=true} (CI) to fail, rather than skip, when the vectors are absent. */
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void loadVectors() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (GOLDEN_REQUIRED) {
            assertThat(found).as("golden.json is required (golden.required=true) but none of "
                    + GOLDEN_PATHS + " exists").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
    }

    private static byte[] hex(String name) {
        return HexFormat.of().parseHex((String) golden.get(name));
    }

    @Test
    void hostileClaimFieldsFailToDecodeAtAll() {
        // A NaN bid and a non-positive epoch are refused by TakeClaim's own
        // validation, so the whole delta fails decoding and is dropped.
        for (String name : new String[] {
                "adv_claim_nan_bid_delta", "adv_claim_epoch_zero_delta"}) {
            assertThatThrownBy(() ->
                    codec.fromBytes(hex(name), SpaceWire.Delta.class))
                    .as(name).isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void aTamperedClaimSignatureNeverVerifies() {
        SpaceWire.Delta delta = codec.fromBytes(
                hex("adv_claim_bad_signature_delta"), SpaceWire.Delta.class);
        SpaceWire.SignedClaim signed = delta.claim();
        assertThat(Ed25519.verify(Ed25519.publicKeyFromRaw(signed.holderKey()),
                codec.toBytes(signed.claim()), signed.signature())).isFalse();
    }

    @Test
    void aTransplantedClaimFailsItsSignedEntryBinding() {
        SpaceWire.Delta delta = codec.fromBytes(
                hex("adv_claim_transplanted_delta"), SpaceWire.Delta.class);
        // The signature itself is valid; the signed entryId disagrees with the
        // claimEntry key it travels under, which is what verifyClaim rejects.
        SpaceWire.SignedClaim signed = delta.claim();
        assertThat(Ed25519.verify(Ed25519.publicKeyFromRaw(signed.holderKey()),
                codec.toBytes(signed.claim()), signed.signature())).isTrue();
        assertThat(signed.claim().entryId()).isNotEqualTo(delta.claimEntry());
    }

    @Test
    void boundedClaimVectorsExceedTheMergeBounds() {
        // Decodable and validly signed, but outside what mergeClaim admits
        // (ASF-004); the end-to-end rejection is proven in
        // ForgedStateRejectionTest.
        SpaceWire.Delta jump = codec.fromBytes(
                hex("adv_claim_epoch_jump_delta"), SpaceWire.Delta.class);
        assertThat(jump.claim().claim().epoch())
                .isGreaterThan(ReplicatedSpace.MAX_CLAIM_EPOCH_JUMP);
        SpaceWire.Delta eternal = codec.fromBytes(
                hex("adv_claim_eternal_delta"), SpaceWire.Delta.class);
        assertThat(eternal.claim().claim().expiresAtMillis())
                .isGreaterThan(System.currentTimeMillis()
                        + ReplicatedSpace.MAX_CLAIM_HOLD_MILLIS);
    }

    /**
     * TECH-SPEC §7.2's two-key rule (QA4 A4-7 phase 1): with a certificate attached
     * the certificate must verify under the peer key, name the record's issuer and
     * be unexpired, and the record must verify under the certificate's agent key.
     * Each hostile delta decodes and breaks exactly one of those; the certified
     * golden delta passes. The end-to-end drop is proven in
     * SignedAgentVectorsClusterTest.
     */
    @Test
    void everyHostileCertificateDeltaFailsTheTwoKeyRule() {
        Instant verifyAt = Instant.parse((String) golden.get("certificate_verify_at"));
        assertThat(twoKeyRule(hex("delta_with_certificate_cbor"), verifyAt))
                .as("the certified golden delta verifies").isTrue();
        for (String name : new String[] {
                "adv_cert_wrong_peer_delta", "adv_cert_wrong_agent_delta",
                "adv_cert_expired_delta", "adv_cert_uncertified_key_delta",
                "adv_cert_peer_signed_record_delta"}) {
            assertThat(twoKeyRule(hex(name), verifyAt)).as(name).isFalse();
        }
        // The expiry is the only thing wrong with the expired certificate: a day
        // earlier it would have verified.
        assertThat(twoKeyRule(hex("adv_cert_expired_delta"), verifyAt.minus(java.time.Duration.ofDays(1)).plus(java.time.Duration.ofMinutes(30))))
                .isTrue();
    }

    private static boolean twoKeyRule(byte[] deltaBytes, Instant now) {
        SpaceWire.Delta delta = codec.fromBytes(deltaBytes, SpaceWire.Delta.class);
        SpaceWire.EntryStateDto dto = delta.state();
        EntryRecord record = dto.record();
        AgentCertificate certificate = dto.agentCertificate();
        assertThat(certificate).as("every certificate vector carries one").isNotNull();
        byte[] view = codec.toBytes(new GoldenVectorsConformanceTest.SignView(record.entryId(),
                record.spaceId(), record.type(), record.payload(), record.payloadRef(),
                record.issuer(), record.issued(), record.tags(), record.keyEpoch()));
        return PeerId.fromPublicKey(dto.issuerPublicKey()).equals(record.issuer().peer())
                && new AgentCertificates(codec).verify(certificate, dto.issuerPublicKey(),
                        record.issuer(), now)
                && Ed25519.verifyRaw(certificate.agentPublicKey(), view, record.sig());
    }

    /**
     * TECH-SPEC §7.6's two-key rule for claims (QA4 A4-7 phase 3): with a
     * certificate attached, holderKey stays the peer key and the signature must
     * verify under the certificate's agent key. Each hostile delta breaks exactly
     * one condition; the certified golden delta passes. End to end in
     * SignedAgentVectorsClusterTest.
     */
    @Test
    void everyHostileCertifiedClaimFailsTheTwoKeyRule() {
        Instant verifyAt = Instant.parse((String) golden.get("certificate_verify_at"));
        assertThat(claimRule(hex("claim_delta_with_certificate_cbor"), verifyAt)).isTrue();
        assertThat(claimRule(hex("claim_delta_cbor"), verifyAt)).as("peer-signed, no certificate").isTrue();
        for (String name : new String[] {"adv_claim_cert_peer_signed_delta",
                "adv_claim_cert_expired_delta", "adv_claim_cert_wrong_holder_delta"}) {
            assertThat(claimRule(hex(name), verifyAt)).as(name).isFalse();
        }
    }

    private static boolean claimRule(byte[] deltaBytes, Instant now) {
        SpaceWire.Delta delta = codec.fromBytes(deltaBytes, SpaceWire.Delta.class);
        SpaceWire.SignedClaim signed = delta.claim();
        byte[] bytes = codec.toBytes(signed.claim());
        if (!signed.holderAttested()) {
            return false;
        }
        if (signed.holderCertificate() == null) {
            return Ed25519.verifyRaw(signed.holderKey(), bytes, signed.signature());
        }
        return new AgentCertificates(codec).verify(signed.holderCertificate(), signed.holderKey(),
                signed.claim().holder(), now)
                && Ed25519.verifyRaw(signed.holderCertificate().agentPublicKey(), bytes, signed.signature());
    }

    @Test
    void deepNestingIsRefusedBounded() {
        byte[] deep = hex("adv_deep_nesting_cbor");
        assertThat(new WireCodec(codec).decode(deep)).isEmpty();
        assertThatThrownBy(() -> codec.fromBytes(deep, SpaceWire.Delta.class))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void aTamperedFrameSignatureNeverVerifies() {
        assertThat(new WireCodec(codec).decode(hex("adv_frame_bad_signature")))
                .isEmpty();
    }

    @Test
    void aMisaddressedFrameCarriesItsSignedDestination() {
        // The signature verifies; the dispatch gate then drops any frame whose
        // destination is not this peer (enforced in PeerNode, ASF-010).
        var envelope = new WireCodec(codec).decode(hex("adv_frame_wrong_destination"));
        assertThat(envelope).isPresent();
        assertThat(envelope.get().to().value())
                .isEqualTo(golden.get("adv_frame_wrong_destination_to"))
                .isNotEqualTo(golden.get("peer_id"));
    }

    /** SPEC §5.1: a GROUP_AD whose `group` and `id` name a GroupID its founding document does not derive to is refused, although the founder's signature is genuine. */
    @Test
    void aGroupAdvertisementNamingTheWrongGroupIdIsRefused() {
        assumeTrue(golden.containsKey("adv_group_ad_wrong_id_cbor"), "v0.1.10 vectors absent");
        SignedGroupAdvertisement signed = codec.fromBytes(
                hex("adv_group_ad_wrong_id_cbor"), SignedGroupAdvertisement.class);
        assertThat(GroupFounding.verify(signed)).isFalse();
        // The defect is the id alone: key hashes to the issuer, signature is
        // valid over the founding fields, but the derived id differs.
        GroupFounding.FoundingFields fields = GroupFounding.fieldsOf(signed.advertisement());
        assertThat(PeerId.fromPublicKey(signed.founderPublicKey()))
                .isEqualTo(signed.advertisement().issuer());
        assertThat(Ed25519.verifyRaw(signed.founderPublicKey(), codec.toBytes(fields),
                signed.signature())).isTrue();
        assertThat(GroupFounding.derive(fields, signed.signature()))
                .isEqualTo(GroupId.of((String) golden.get("founding_group_id")))
                .isNotEqualTo(signed.advertisement().group());
        assertThat(signed.advertisement().group().value()).isEqualTo(golden.get("group_id"));
    }

    /** SPEC §5.1: a GROUP_AD with the correct GroupID but a signature by another key is refused. */
    @Test
    void aGroupAdvertisementWithAForgedFounderSignatureIsRefused() {
        assumeTrue(golden.containsKey("adv_group_ad_forged_signature_cbor"), "v0.1.10 vectors absent");
        SignedGroupAdvertisement signed = codec.fromBytes(
                hex("adv_group_ad_forged_signature_cbor"), SignedGroupAdvertisement.class);
        assertThat(GroupFounding.verify(signed)).isFalse();
        // Everything but the signature is the genuine founding advertisement.
        SignedGroupAdvertisement genuine = codec.fromBytes(
                hex("signed_group_ad_cbor"), SignedGroupAdvertisement.class);
        assertThat(signed.advertisement()).isEqualTo(genuine.advertisement());
        assertThat(signed.founderPublicKey()).isEqualTo(genuine.founderPublicKey());
        assertThat(signed.signature()).isNotEqualTo(genuine.signature());
        byte[] fields = codec.toBytes(GroupFounding.fieldsOf(signed.advertisement()));
        assertThat(Ed25519.verifyRaw(signed.founderPublicKey(), fields, signed.signature())).isFalse();
        // The forger's own key, pinned in the file, did produce it.
        assertThat(Ed25519.verifyRaw(hex("adv_forger_public_key_raw"), fields, signed.signature()))
                .isTrue();
    }

    @Test
    void theFrameCapMatchesTheSharedConstant() {
        // TcpTransport.MAX_FRAME is private; the shared constant pins its value.
        assertThat(((Number) golden.get("max_frame_bytes")).intValue())
                .isEqualTo(8 * 1024 * 1024);
    }
}
