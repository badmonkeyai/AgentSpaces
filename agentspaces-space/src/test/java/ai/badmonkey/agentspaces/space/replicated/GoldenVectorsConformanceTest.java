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

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.identity.AgentCertificates;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Java half of the non-adversarial golden vectors. {@code golden.json} is
 * produced by {@code tools/golden/GoldenVectors.java} from this codec, and the
 * Python and TypeScript clients prove themselves against it byte for byte;
 * until now nothing proved that the <em>current</em> Java codec still emits
 * those bytes (or that the mirrored {@code SignView}/{@code StateSignView}
 * records in the generator match the private ones in {@link ReplicatedSpace}).
 * Every assertion here re-derives a vector from its inputs with the production
 * classes and compares to the pinned bytes, so a wire-shape drift on the Java
 * side fails here before it breaks the other two implementations.
 */
class GoldenVectorsConformanceTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final CborCodec codec = CborCodec.defaultCodec();
    private static final WireCodec wire = new WireCodec(codec);
    private static final HexFormat HEX = HexFormat.of();
    /** Set {@code -Dgolden.required=true} (CI) to fail, rather than skip, when the vectors are absent. */
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");

    private static Map<String, Object> golden;
    private static PeerIdentity identity;
    private static PeerId self;
    private static GroupId group;
    private static SpaceId spaceId;
    private static HlcTimestamp stamp;
    private static EntryId entryId;
    private static AgentId issuer;

    /** Mirrors ReplicatedSpace's private SignView (see tools/golden/GoldenVectors.java). */
    record SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                    String payloadRef, AgentId issuer, HlcTimestamp issued,
                    Map<String, String> tags,
                    // v0.1.13, omitted when null
                    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                    Long keyEpoch) {
    }

    /** Mirrors ReplicatedSpace's private StateSignView (SPEC §11a.4 field order). */
    record StateSignView(SpaceId spaceId, EntryId entryId, List<Dot> adds,
                         List<Dot> removes, HlcTimestamp leaseStamp, LeaseInfo leaseValue,
                         boolean completed,
                         // v0.1.13, omitted when null
                         @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                         String signer,
                         @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                         HlcTimestamp signedAt) {
    }

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
        identity = PeerIdentity.of(new KeyPair(
                Ed25519.publicKeyFromRaw(hex("public_key_raw")),
                Ed25519.privateKeyFromPkcs8(hex("private_key_pkcs8"))));
        self = identity.peerId();
        group = GroupId.of(text("group_id"));
        spaceId = SpaceId.of(text("space_id"));
        stamp = HlcTimestamp.parse(text("hlc_encoded"));
        entryId = EntryId.of("11111111-2222-3333-4444-555555555555");
        issuer = identity.agent("python");
    }

    private static byte[] hex(String name) {
        return HEX.parseHex(text(name));
    }

    private static String text(String name) {
        return (String) golden.get(name);
    }

    private static void assertBytes(byte[] actual, String vector) {
        assertThat(HEX.formatHex(actual)).as(vector).isEqualTo(text(vector));
    }

    /** SPEC §4.1/§4.4: PeerID, GroupID and SpaceID derive to the pinned strings from the fixed key and founding bytes. */
    @Test
    void peerGroupAndSpaceIdsMatchTheGoldenVectors() {
        assertThat(self.value()).isEqualTo(text("peer_id"));
        assertThat(PeerId.fromPublicKey(hex("public_key_raw")).value()).isEqualTo(text("peer_id"));
        assertThat(GroupId.fromFounding("research-fleet-demo-v1".getBytes(StandardCharsets.UTF_8)).value())
                .isEqualTo(text("group_id"));
        assertThat(SpaceId.local(text("group_id") + "/tasks").value()).isEqualTo(text("space_id"));
    }

    /** TECH §1.4: the HLC canonical string form is {@code physical:logical:node}. */
    @Test
    void hlcEncodingMatches() {
        assertThat(stamp).isEqualTo(new HlcTimestamp(1735689600000L, 3, self.value()));
        assertThat(stamp.encoded()).isEqualTo(text("hlc_encoded"));
    }

    /** SPEC §9: PING body, envelope canonical bytes, RFC 8032 signature and the full signed frame are byte-identical, and the frame verifies. */
    @Test
    void pingEnvelopeSignatureAndFrameBytesMatch() {
        byte[] body = codec.toBytes(new Bodies.Ping(42));
        assertBytes(body, "ping_body_cbor");

        Envelope ping = new Envelope(2, group, Envelope.Kind.PING, self, self, stamp, body);
        byte[] envelopeCbor = codec.toBytes(ping);
        assertBytes(envelopeCbor, "ping_envelope_cbor");
        assertBytes(identity.sign(envelopeCbor), "ping_envelope_signature");
        assertBytes(wire.encode(ping, identity), "ping_frame_cbor");

        Optional<Envelope> decoded = wire.decode(hex("ping_frame_cbor"));
        assertThat(decoded).isPresent();
        assertThat(decoded.get().kind()).isEqualTo(Envelope.Kind.PING);
        assertThat(decoded.get().from()).isEqualTo(self);
        assertThat(decoded.get().to()).isEqualTo(self);
        assertThat(decoded.get().ver()).isEqualTo(2);
    }

    /** SPEC §4.1/§5: the membership introduction (PeerAdvertisement, SignedPeerAd, peers-stream rumor body) is byte-identical. */
    @Test
    void membershipIntroBytesMatch() {
        PeerAdvertisement ad = new PeerAdvertisement(
                "aspace://" + group.value() + "/peer/" + self.value(),
                self, group, Instant.parse("2026-01-01T00:00:00Z"), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:7460", 0)),
                Set.of(), Map.of());
        byte[] adBytes = codec.toBytes(ad);
        assertBytes(adBytes, "peer_ad_cbor");
        byte[] signedAd = codec.toBytes(new PeerNode.SignedPeerAd(
                adBytes, identity.rawPublicKey(), identity.sign(adBytes)));
        assertBytes(signedAd, "signed_peer_ad_cbor");
        assertBytes(codec.toBytes(new Bodies.Rumor("peers",
                "peer:" + self.value() + ":1767225600000", 6, signedAd)), "intro_rumor_body_cbor");
    }

    /** TECH §7.2: the record SignView bytes and the record signature match. */
    @Test
    void recordSignViewAndSignatureMatch() {
        byte[] payload = hex("task_payload_cbor");
        byte[] viewBytes = codec.toBytes(new SignView(entryId, spaceId,
                "GoldenVectors$ResearchTask#v1", payload, null, issuer, stamp, Map.of(), null));
        assertBytes(viewBytes, "sign_view_cbor");
        assertBytes(identity.sign(viewBytes), "record_signature");
    }

    /** SPEC §11a.4 + v0.1.8 canonical form: the StateSignView bytes carry definite-length dot arrays and the state signature matches. */
    @Test
    void stateSignViewUsesDefiniteLengthArraysAndTheStateSignatureMatches() {
        LeaseInfo lease = new LeaseInfo(issuer, 1735693200000L, LeaseKind.WRITE);
        List<Dot> adds = List.of(new Dot(self.value(), 1));
        byte[] viewBytes = codec.toBytes(new StateSignView(spaceId, entryId, adds, List.of(),
                stamp, lease, false, null, null));
        assertBytes(viewBytes, "state_sign_view_cbor");
        assertBytes(identity.sign(viewBytes), "state_signature");

        // "adds" (text(4)) is followed by a definite array of one, "removes"
        // (text(7)) by a definite empty array: 0x81 and 0x80, never 0x9F.
        String hex = HEX.formatHex(viewBytes);
        assertThat(hex).contains("6461646473" + "81");
        assertThat(hex).contains("6772656d6f766573" + "80");
        assertThat(hex).doesNotContain("64616464739f").doesNotContain("6772656d6f7665739f");
        // The view itself is an indefinite-length map.
        assertThat(viewBytes[0]).isEqualTo((byte) 0xBF);
        assertThat(viewBytes[viewBytes.length - 1]).isEqualTo((byte) 0xFF);
    }

    /** SPEC §11a.4 wire shape: the signed EntryStateDto and the Delta that carries it are byte-identical. */
    @Test
    void entryStateDtoAndDeltaBytesMatch() {
        LeaseInfo lease = new LeaseInfo(issuer, 1735693200000L, LeaseKind.WRITE);
        EntryRecord record = new EntryRecord(entryId, spaceId, "GoldenVectors$ResearchTask#v1",
                hex("task_payload_cbor"), null, issuer, stamp, lease, Map.of(),
                hex("record_signature"));
        SpaceWire.EntryStateDto dto = new SpaceWire.EntryStateDto(record,
                identity.rawPublicKey(), List.of(new Dot(self.value(), 1)), List.of(),
                stamp, lease, false, hex("state_signature"));
        assertBytes(codec.toBytes(dto), "entry_state_dto_cbor");
        assertBytes(codec.toBytes(new SpaceWire.Delta(dto, null, null)), "delta_cbor");
    }

    /** TECH §7.6: the take claim's canonical bytes, holder signature and claim delta are byte-identical. */
    @Test
    void takeClaimBytesAndSignatureMatch() {
        TakeClaim claim = new TakeClaim(entryId, spaceId, 1, stamp, issuer, 0.0, 1735693200000L);
        byte[] claimBytes = codec.toBytes(claim);
        assertBytes(claimBytes, "take_claim_cbor");
        byte[] claimSig = identity.sign(claimBytes);
        assertBytes(claimSig, "take_claim_signature");
        assertBytes(codec.toBytes(new SpaceWire.Delta(null, entryId,
                new SpaceWire.SignedClaim(claim, identity.rawPublicKey(), claimSig))),
                "claim_delta_cbor");
    }

    /**
     * SPEC §4.2 / §11a.3, TECH-SPEC §7.2 (QA4 A4-7 phase 1): the peer-signed agent
     * certificate, the agent-key record signature over the same SignView, and the
     * EntryStateDto that carries both are byte-identical, and the certificate
     * verifies under the peer key at the pinned instant.
     */
    @Test
    void subordinateAgentCertificateAndAgentSignedRecordMatch() {
        Instant verifyAt = Instant.parse(text("certificate_verify_at"));
        KeyPair agentKeys = new KeyPair(
                Ed25519.publicKeyFromRaw(hex("agent_public_key_raw")),
                Ed25519.privateKeyFromPkcs8(hex("agent_private_key_pkcs8")));
        AgentIdentity agent = identity.subordinate("python", agentKeys, verifyAt, Duration.ofHours(24));
        AgentCertificate certificate = agent.certificate().orElseThrow();
        assertThat(agent.id()).isEqualTo(issuer);
        assertThat(agent.isSubordinate()).isTrue();
        assertBytes(agent.publicKey(), "agent_public_key_raw");
        assertBytes(codec.toBytes(certificate.unsigned()), "agent_certificate_unsigned_cbor");
        assertBytes(certificate.peerSignature(), "agent_certificate_signature");
        assertBytes(codec.toBytes(certificate), "agent_certificate_cbor");
        assertThat(codec.fromBytes(hex("agent_certificate_cbor"), AgentCertificate.class))
                .isEqualTo(certificate);
        assertThat(new AgentCertificates(codec).verify(certificate, identity.rawPublicKey(), issuer,
                verifyAt)).isTrue();

        byte[] viewBytes = hex("sign_view_cbor");
        byte[] agentSig = agent.sign(viewBytes);
        assertBytes(agentSig, "record_signature_agent_key");
        assertThat(Ed25519.verifyRaw(certificate.agentPublicKey(), viewBytes, agentSig)).isTrue();
        assertThat(Ed25519.verifyRaw(identity.rawPublicKey(), viewBytes, agentSig))
                .as("the peer key does not verify an agent-signed record").isFalse();

        byte[] payload = hex("task_payload_cbor");
        LeaseInfo lease = new LeaseInfo(issuer, 1735693200000L, LeaseKind.WRITE);
        EntryRecord record = new EntryRecord(entryId, spaceId, "GoldenVectors$ResearchTask#v1",
                payload, null, issuer, stamp, lease, Map.of(), agentSig);
        SpaceWire.EntryStateDto dto = new SpaceWire.EntryStateDto(record, identity.rawPublicKey(),
                List.of(new Dot(self.value(), 1)), List.of(), stamp, lease, false,
                hex("state_signature"), certificate);
        assertBytes(codec.toBytes(dto), "entry_state_with_certificate_cbor");
        assertBytes(codec.toBytes(new SpaceWire.Delta(dto, null, null)), "delta_with_certificate_cbor");
        SpaceWire.EntryStateDto decoded = codec.fromBytes(hex("entry_state_with_certificate_cbor"),
                SpaceWire.EntryStateDto.class);
        assertThat(decoded.agentCertificate()).isEqualTo(certificate);
        assertThat(codec.fromBytes(hex("entry_state_dto_cbor"), SpaceWire.EntryStateDto.class)
                .agentCertificate()).as("the pre-certificate DTO decodes with no certificate").isNull();
    }

    /** SPEC §9: the DIGEST body shape is byte-identical. */
    @Test
    void digestBodyMatches() {
        assertBytes(codec.toBytes(new Bodies.Digest(
                Map.of("peers", "x".getBytes(StandardCharsets.UTF_8)))), "digest_body_cbor");
    }

    /** SPEC §9: every non-adversarial CBOR vector is well-formed CBOR to this codec. */
    @Test
    void everyNonAdversarialGoldenCborVectorDecodesInJava() {
        int decoded = 0;
        for (Map.Entry<String, Object> e : golden.entrySet()) {
            if (!e.getKey().endsWith("_cbor") || e.getKey().startsWith("adv_")) {
                continue;
            }
            assertThat(codec.fromBytes(HEX.parseHex((String) e.getValue()), Object.class))
                    .as(e.getKey()).isNotNull();
            decoded++;
        }
        assertThat(decoded).isGreaterThanOrEqualTo(28);
    }
}
