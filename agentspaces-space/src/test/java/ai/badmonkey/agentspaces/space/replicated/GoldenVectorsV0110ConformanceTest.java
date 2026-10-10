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

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.node.GroupFounding;
import ai.badmonkey.agentspaces.peering.node.SignedGroupAdvertisement;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Java half of the v0.1.10 golden vectors (QA2 workstream C): the group
 * founding document and its self-certifying GroupID (SPEC §4.4, §5.1), the
 * {@code GROUP_AD_WANT}/{@code GROUP_AD} bootstrap frames (§9), the
 * advertisements with their v0.1.10 additive fields (§6.1), and the aggregate
 * and gossip-learn pipe frames (§8). Every vector is re-derived from its
 * inputs with the production classes where the space module can reach them,
 * and through mirrors of the private capability records otherwise; the
 * generator ({@code tools/golden/GoldenVectors.java}) checks those mirrors
 * against the real records by reflection on every run.
 *
 * <p>Set {@code -Dgolden.required=true} (CI) to fail, rather than skip, when
 * the vectors are absent.
 */
class GoldenVectorsV0110ConformanceTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static final CborCodec codec = CborCodec.defaultCodec();
    private static final WireCodec wire = new WireCodec(codec);
    private static final HexFormat HEX = HexFormat.of();
    private static final Instant ISSUED = Instant.parse("2026-01-01T00:00:00Z");
    private static final String TASK_SCHEMA = "GoldenVectors$ResearchTask#v1";

    private static Map<String, Object> golden;
    private static PeerIdentity identity;
    private static PeerId self;
    private static GroupId legacyGroup;
    private static GroupId foundingGroup;
    private static HlcTimestamp stamp;
    private static AgentId issuer;

    // Mirrors of PushSumAggregate's private wire records (TECH-SPEC §8.2);
    // the generator asserts these shapes against the real records.
    record Share(String epochId, double value, double weight) {
    }

    record Extremum(String epochId, boolean max, double value) {
    }

    record Histogram(String epochId, double lo, double hi, double[] buckets) {
    }

    record Roster(String epochId, long[] members) {
    }

    record Frame(Share share, Extremum extremum, Histogram histogram, Roster roster) {
    }

    /** Mirrors GossipLearner's private Exchange record (TECH-SPEC §8.5). */
    record Exchange(String modelId, long token, String kind, long round, byte[] inline, String cid) {
    }

    /** Mirrors ReplicatedSpace's private SignView: the bytes a record signature covers (TECH §7.2). */
    record SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                    String payloadRef, AgentId issuer, HlcTimestamp issued,
                    Map<String, String> tags,
                    // v0.1.13, omitted when null
                    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                    Long keyEpoch) {
    }

    /** Mirrors AdCache.StoredAd (agentspaces-discovery, not on this module's classpath). */
    record StoredAd(String adType, byte[] adBytes, byte[] publicKey, byte[] signature) {
    }

    /** Mirrors ai.badmonkey.agentspaces.agent.join.JoinTicket (agentspaces-agent, not on this module's classpath; issue #16). */
    record JoinTicket(String join, String key) {
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
        assumeTrue(golden.containsKey("founding_group_id"),
                "golden.json predates the v0.1.10 vectors");
        identity = PeerIdentity.of(new KeyPair(
                Ed25519.publicKeyFromRaw(hex("public_key_raw")),
                Ed25519.privateKeyFromPkcs8(hex("private_key_pkcs8"))));
        self = identity.peerId();
        legacyGroup = GroupId.of(text("group_id"));
        foundingGroup = GroupId.of(text("founding_group_id"));
        stamp = HlcTimestamp.parse(text("hlc_encoded"));
        issuer = identity.agent("python");
    }

    private static byte[] hex(String name) {
        return HEX.parseHex(text(name));
    }

    private static String text(String name) {
        Object value = golden.get(name);
        assertThat(value).as("vector " + name + " present").isNotNull();
        return (String) value;
    }

    private static void assertBytes(byte[] actual, String vector) {
        assertThat(HEX.formatHex(actual)).as(vector).isEqualTo(text(vector));
    }

    /** Decodes a vector as {@code type} and proves the re-encoding is byte-exact. */
    private static <T> T roundTrip(String vector, Class<T> type) {
        T decoded = codec.fromBytes(hex(vector), type);
        assertThat(decoded).as(vector + " decodes").isNotNull();
        assertBytes(codec.toBytes(decoded), vector);
        return decoded;
    }

    private static GroupAdvertisement.GossipParameters gossip() {
        return new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1));
    }

    private static GroupFounding.FoundingFields foundingFields() {
        return new GroupFounding.FoundingFields("golden-fleet", self, ISSUED, Duration.ofDays(1),
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                gossip());
    }

    /** SPEC §4.4: founding fields, founder signature, founding document and the derived GroupID are byte-identical. */
    @Test
    void foundingFieldsDocumentAndDerivedGroupIdMatch() {
        GroupFounding.FoundingFields fields = foundingFields();
        byte[] fieldsBytes = codec.toBytes(fields);
        assertBytes(fieldsBytes, "founding_fields_cbor");
        byte[] signature = identity.sign(fieldsBytes);
        assertBytes(signature, "founding_signature");
        byte[] document = codec.toBytes(new GroupFounding.FoundingDocument(fields, signature));
        assertBytes(document, "founding_document_cbor");

        assertThat(GroupFounding.derive(fields, signature)).isEqualTo(foundingGroup);
        assertThat(GroupId.fromFounding(hex("founding_document_cbor"))).isEqualTo(foundingGroup);
        assertThat(Multibase.base58btc(Digests.sha256(document))).isEqualTo(text("founding_group_id"));
        assertThat(text("founding_ad_id")).isEqualTo(GroupFounding.URI_PREFIX + text("founding_group_id"));
        assertThat(foundingGroup).isNotEqualTo(legacyGroup);
    }

    /** SPEC §5.1: GroupFounding.found reproduces the signed founding advertisement exactly, and it verifies. */
    @Test
    void signedGroupAdvertisementMatchesAndVerifies() {
        SignedGroupAdvertisement founded = GroupFounding.found(identity, "golden-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                gossip(), ISSUED, Duration.ofDays(1));
        assertBytes(codec.toBytes(founded.advertisement()), "group_ad_cbor");
        assertBytes(codec.toBytes(founded), "signed_group_ad_cbor");
        assertThat(founded.advertisement().group()).isEqualTo(foundingGroup);
        assertThat(founded.advertisement().id()).isEqualTo(text("founding_ad_id"));
        assertThat(GroupFounding.fieldsOf(founded.advertisement())).isEqualTo(foundingFields());

        GroupAdvertisement ad = roundTrip("group_ad_cbor", GroupAdvertisement.class);
        assertThat(ad.issuer()).isEqualTo(self);
        assertThat(ad.membershipPolicy()).isEqualTo(GroupAdvertisement.MembershipPolicy.OPEN);
        assertThat(ad.defaultStrategy()).isEqualTo(ConflictStrategyType.LEASE_RACE);
        assertThat(ad.gossip()).isEqualTo(gossip());

        SignedGroupAdvertisement signed = roundTrip("signed_group_ad_cbor", SignedGroupAdvertisement.class);
        assertThat(signed.founderPublicKey()).isEqualTo(identity.rawPublicKey());
        assertThat(signed.signature()).isEqualTo(hex("founding_signature"));
        assertThat(GroupFounding.verify(signed)).isTrue();
    }

    /** SPEC §9: the GROUP_AD_WANT envelope and frame are byte-identical, decode, and carry the wanted group with an empty body. */
    @Test
    void groupAdWantFrameMatchesAndDecodes() {
        Envelope want = new Envelope(WireCodec.WIRE_VERSION, foundingGroup, Envelope.Kind.GROUP_AD_WANT, self, self,
                stamp, new byte[0]);
        assertBytes(codec.toBytes(want), "group_ad_want_envelope_cbor");
        assertBytes(wire.encode(want, identity), "group_ad_want_frame_cbor");

        Optional<Envelope> decoded = wire.decode(hex("group_ad_want_frame_cbor"));
        assertThat(decoded).isPresent();
        assertThat(decoded.get().ver()).isEqualTo(WireCodec.WIRE_VERSION);
        assertThat(decoded.get().kind()).isEqualTo(Envelope.Kind.GROUP_AD_WANT);
        assertThat(decoded.get().group()).isEqualTo(foundingGroup);
        assertThat(decoded.get().from()).isEqualTo(self);
        assertThat(decoded.get().to()).isEqualTo(self);
        assertThat(decoded.get().stamp()).isEqualTo(stamp);
        assertThat(decoded.get().body()).isEmpty();
        assertBytes(codec.toBytes(roundTrip("group_ad_want_envelope_cbor", Envelope.class)),
                "group_ad_want_envelope_cbor");
    }

    /** SPEC §9: the GROUP_AD envelope and frame are byte-identical; the body is the signed founding advertisement and verifies. */
    @Test
    void groupAdFrameMatchesAndCarriesTheVerifiableFounding() {
        Envelope answer = new Envelope(WireCodec.WIRE_VERSION, foundingGroup, Envelope.Kind.GROUP_AD, self, self,
                stamp, hex("signed_group_ad_cbor"));
        assertBytes(codec.toBytes(answer), "group_ad_envelope_cbor");
        assertBytes(wire.encode(answer, identity), "group_ad_frame_cbor");

        Optional<Envelope> decoded = wire.decode(hex("group_ad_frame_cbor"));
        assertThat(decoded).isPresent();
        assertThat(decoded.get().kind()).isEqualTo(Envelope.Kind.GROUP_AD);
        assertThat(decoded.get().group()).isEqualTo(foundingGroup);
        assertThat(decoded.get().to()).isEqualTo(self);
        assertThat(decoded.get().body()).isEqualTo(hex("signed_group_ad_cbor"));
        SignedGroupAdvertisement founding = codec.fromBytes(decoded.get().body(),
                SignedGroupAdvertisement.class);
        assertThat(GroupFounding.verify(founding)).isTrue();
        assertThat(founding.advertisement().group()).isEqualTo(decoded.get().group());
        roundTrip("group_ad_envelope_cbor", Envelope.class);
    }

    /** SPEC §6.1: the AgentCard with space bindings, its signature and its StoredAd wire form are byte-identical, and spaceFor routes. */
    @Test
    void agentCardSignatureAndStoredFormMatch() {
        AgentCard card = new AgentCard(
                "aspace://" + legacyGroup.value() + "/agent/" + self.value() + "/python",
                self, legacyGroup, ISSUED, Duration.ofMinutes(15), issuer,
                "Researches a topic and writes a finding",
                List.of("research"), List.of(TASK_SCHEMA), List.of("GoldenVectors$Finding#v1"),
                Map.of("tokens", "low"), Map.of(TASK_SCHEMA, "tasks"));
        byte[] cardBytes = codec.toBytes(card);
        assertBytes(cardBytes, "agent_card_cbor");
        SignedAdvertisement<AgentCard> signed = new AdvertisementSigner(codec).sign(card, identity);
        assertBytes(signed.signature(), "agent_card_signature");
        assertBytes(codec.toBytes(new StoredAd("AgentCard", cardBytes, identity.rawPublicKey(),
                signed.signature())), "signed_agent_card_cbor");

        AgentCard decoded = roundTrip("agent_card_cbor", AgentCard.class);
        assertThat(decoded).isEqualTo(card);
        assertThat(decoded.spaceFor(TASK_SCHEMA)).contains("tasks");
        assertThat(decoded.spaceFor("GoldenVectors$Finding#v1")).isEmpty();
        assertThat(decoded.agent()).isEqualTo(issuer);

        StoredAd stored = roundTrip("signed_agent_card_cbor", StoredAd.class);
        assertThat(stored.adType()).isEqualTo("AgentCard");
        assertThat(stored.adBytes()).isEqualTo(cardBytes);
        assertThat(new AdvertisementSigner(codec).verify(new SignedAdvertisement<>(
                codec.fromBytes(stored.adBytes(), AgentCard.class), stored.publicKey(),
                stored.signature()))).isTrue();
    }

    /** SPEC §6.1/§7.5: the SpaceAdvertisement's admission and replication fields round-trip and its founding SpaceID derives. */
    @Test
    void spaceAdvertisementAdmissionReplicationAndSpaceIdMatch() {
        SpaceAdvertisement ad = new SpaceAdvertisement(
                "aspace://" + legacyGroup.value() + "/tasks", self, legacyGroup, ISSUED,
                Duration.ofMinutes(15), "tasks", List.of(TASK_SCHEMA),
                ConflictStrategyType.LEASE_RACE, SpaceAdvertisement.Admission.ALLOWLIST,
                SpaceAdvertisement.Replication.TAG_SHARDED);
        byte[] adBytes = codec.toBytes(ad);
        assertBytes(adBytes, "space_ad_cbor");
        assertThat(SpaceId.fromFounding(adBytes).value()).isEqualTo(text("space_ad_space_id"));

        SpaceAdvertisement decoded = roundTrip("space_ad_cbor", SpaceAdvertisement.class);
        assertThat(decoded).isEqualTo(ad);
        assertThat(decoded.admission()).isEqualTo(SpaceAdvertisement.Admission.ALLOWLIST);
        assertThat(decoded.replication()).isEqualTo(SpaceAdvertisement.Replication.TAG_SHARDED);
        assertThat(decoded.strategy()).isEqualTo(ConflictStrategyType.LEASE_RACE);
        assertThat(decoded.schemaHints()).containsExactly(TASK_SCHEMA);
    }

    /** SPEC §7.5/§11a.4: a SpaceCredential entry record decodes, re-encodes byte-exact, carries the reserved type name, and its record signature verifies under the issuer's key. */
    @Test
    void spaceCredentialRecordDecodesVerifiesAndNamesTheReservedType() {
        assumeTrue(golden.containsKey("space_credential_record_cbor"),
                "golden.json predates the v0.1.11 credential vector");
        EntryRecord record = roundTrip("space_credential_record_cbor", EntryRecord.class);

        String credentialType = new SimpleSchemaRegistry().register(SpaceCredential.class);
        assertThat(credentialType).isEqualTo(SpaceCredential.class.getName() + "#v1");
        assertThat(record.type()).isEqualTo(credentialType);
        assertThat(record.spaceId().value()).isEqualTo(text("space_id"));
        assertThat(record.issuer()).isEqualTo(issuer);
        assertThat(record.issued()).isEqualTo(stamp);
        assertThat(record.lease().kind()).isEqualTo(LeaseKind.WRITE);
        assertThat(record.lease().expiresAtMillis())
                .as("the lease is the credential's validity: PT1H after issue")
                .isEqualTo(stamp.physical() + Duration.ofHours(1).toMillis());

        SpaceCredential credential = codec.fromBytes(record.payload(), SpaceCredential.class);
        assertThat(credential.agent()).isEqualTo(issuer);
        assertThat(credential.scopes()).containsExactly(SpaceAdmission.Scope.WRITE, SpaceAdmission.Scope.TAKE);
        assertThat(codec.toBytes(credential)).as("payload re-encodes byte-exact").isEqualTo(record.payload());

        byte[] view = codec.toBytes(new SignView(record.entryId(), record.spaceId(), record.type(),
                record.payload(), record.payloadRef(), record.issuer(), record.issued(), record.tags(),
                record.keyEpoch()));
        assertThat(Ed25519.verify(Ed25519.publicKeyFromRaw(hex("public_key_raw")), view, record.sig()))
                .as("the issuer's record signature verifies").isTrue();
        assertThat(identity.sign(view)).as("deterministic Ed25519: re-signing reproduces it")
                .isEqualTo(record.sig());
    }

    /**
     * SPEC §7.1 reserved types / §10.3 (issue #16): a JoinTicket entry record decodes, names the
     * reserved type, carries the join and key tags, its payload is the two-field ticket, and its
     * record signature verifies over the SignView with the tags in wire order (join, key).
     * The whole record is not re-encoded byte-exact here on purpose: EntryRecord copies its tags
     * through Map.copyOf, whose iteration order is salted per JVM, so a two-tag record re-encodes
     * in either order; the vector pins the (join, key) order a client sees on the wire.
     */
    @Test
    @SuppressWarnings("unchecked")
    void joinTicketRecordRoundTripsAndNamesTheReservedType() {
        assumeTrue(golden.containsKey("join_ticket_record_cbor"),
                "golden.json predates the issue #16 join ticket vector");
        byte[] bytes = hex("join_ticket_record_cbor");
        EntryRecord record = codec.fromBytes(bytes, EntryRecord.class);

        assertThat(record.type()).isEqualTo("ai.badmonkey.agentspaces.agent.join.JoinTicket#v1");
        assertThat(record.entryId()).isEqualTo(EntryId.of("33333333-4444-5555-6666-777777777777"));
        assertThat(record.spaceId().value()).isEqualTo(text("space_id"));
        assertThat(record.issuer()).isEqualTo(issuer);
        assertThat(record.issued()).isEqualTo(stamp);
        assertThat(record.payloadRef()).isNull();
        assertThat(record.keyEpoch()).isNull();
        assertThat(record.tags()).containsOnly(
                Map.entry("join", "golden.join"), Map.entry("key", "golden-key"));
        assertThat(record.lease().kind()).isEqualTo(LeaseKind.WRITE);
        assertThat(record.lease().holder()).isEqualTo(issuer);
        assertThat(record.lease().expiresAtMillis())
                .as("the ticket's lease: PT10M after issue")
                .isEqualTo(stamp.physical() + Duration.ofMinutes(10).toMillis());

        Map<String, Object> payload = codec.fromBytes(record.payload(), Map.class);
        assertThat(payload).as("payload is the two-field ticket map, join then key")
                .containsExactly(Map.entry("join", "golden.join"), Map.entry("key", "golden-key"));
        JoinTicket ticket = codec.fromBytes(record.payload(), JoinTicket.class);
        assertThat(ticket).isEqualTo(new JoinTicket("golden.join", "golden-key"));
        assertThat(codec.toBytes(ticket)).as("payload re-encodes byte-exact").isEqualTo(record.payload());

        // Structural round trip of the whole record (tag order aside, see above).
        EntryRecord again = codec.fromBytes(codec.toBytes(record), EntryRecord.class);
        assertThat(again.entryId()).isEqualTo(record.entryId());
        assertThat(again.type()).isEqualTo(record.type());
        assertThat(again.payload()).isEqualTo(record.payload());
        assertThat(again.lease()).isEqualTo(record.lease());
        assertThat(again.tags()).isEqualTo(record.tags());
        assertThat(again.sig()).isEqualTo(record.sig());

        // The signature covers the tags as they travel, in canonical order (wire v3,
        // ISSUE-CanonicalMaps: shorter key first, then bytewise): key, then join.
        Map<String, Object> wireForm = codec.fromBytes(bytes, Map.class);
        Map<String, String> wireTags = (Map<String, String>) wireForm.get("tags");
        assertThat(List.copyOf(wireTags.keySet())).containsExactly("key", "join");
        byte[] view = codec.toBytes(new SignView(record.entryId(), record.spaceId(), record.type(),
                record.payload(), record.payloadRef(), record.issuer(), record.issued(), wireTags,
                record.keyEpoch()));
        assertThat(Ed25519.verify(Ed25519.publicKeyFromRaw(hex("public_key_raw")), view, record.sig()))
                .as("the joiner's record signature verifies").isTrue();
        assertThat(identity.sign(view)).as("deterministic Ed25519: re-signing reproduces it")
                .isEqualTo(record.sig());
    }

    /** SPEC §8 / TECH §8.2: each aggregate Frame variant is byte-identical and decodes with exactly that variant set. */
    @Test
    void aggregateFrameVariantsMatch() {
        long token = ByteBuffer.wrap(
                Digests.sha256(self.value().getBytes(StandardCharsets.UTF_8)), 0, 8).getLong();
        assertThat(Long.toString(token)).isEqualTo(text("aggregate_roster_token"));

        assertBytes(codec.toBytes(new Frame(new Share("golden", 4.5, 0.5), null, null, null)),
                "aggregate_share_frame_cbor");
        assertBytes(codec.toBytes(new Frame(null, new Extremum("golden", true, 4.5), null, null)),
                "aggregate_extremum_frame_cbor");
        assertBytes(codec.toBytes(new Frame(null, null,
                new Histogram("golden", 0.0, 10.0, new double[] {1.0, 0.0, 2.0, 0.5}), null)),
                "aggregate_histogram_frame_cbor");
        assertBytes(codec.toBytes(new Frame(null, null, null,
                new Roster("golden", new long[] {token}))), "aggregate_roster_frame_cbor");

        Frame share = roundTrip("aggregate_share_frame_cbor", Frame.class);
        assertThat(share.share()).isEqualTo(new Share("golden", 4.5, 0.5));
        assertThat(share.extremum()).isNull();
        assertThat(share.histogram()).isNull();
        assertThat(share.roster()).isNull();

        Frame extremum = roundTrip("aggregate_extremum_frame_cbor", Frame.class);
        assertThat(extremum.extremum()).isEqualTo(new Extremum("golden", true, 4.5));
        assertThat(extremum.share()).isNull();

        Frame histogram = roundTrip("aggregate_histogram_frame_cbor", Frame.class);
        assertThat(histogram.histogram().epochId()).isEqualTo("golden");
        assertThat(histogram.histogram().lo()).isEqualTo(0.0);
        assertThat(histogram.histogram().hi()).isEqualTo(10.0);
        assertThat(histogram.histogram().buckets()).containsExactly(1.0, 0.0, 2.0, 0.5);
        assertThat(histogram.share()).isNull();

        Frame roster = roundTrip("aggregate_roster_frame_cbor", Frame.class);
        assertThat(roster.roster().epochId()).isEqualTo("golden");
        assertThat(roster.roster().members()).containsExactly(token);
        assertThat(roster.share()).isNull();
    }

    /** SPEC §8 / TECH §8.5: the WeightAveraging encoding, its CID, and the three Exchange kinds are byte-identical. */
    @Test
    void gossipLearnEncodingContentIdAndExchangesMatch() {
        double[] model = {1.0, 2.5, -3.0};
        ByteBuffer buffer = ByteBuffer.allocate(model.length * Double.BYTES).order(ByteOrder.BIG_ENDIAN);
        for (double d : model) {
            buffer.putDouble(d);
        }
        byte[] encoding = buffer.array();
        assertBytes(encoding, "learn_model_encoding");
        assertThat(BlockExchange.cidOf(encoding)).isEqualTo(text("learn_content_id"));
        assertThat(Multibase.base58btc(Digests.sha256(encoding))).isEqualTo(text("learn_content_id"));

        assertBytes(codec.toBytes(new Exchange("golden-model", 7L, "OFFER", 2L, encoding, null)),
                "learn_exchange_offer_cbor");
        assertBytes(codec.toBytes(new Exchange("golden-model", 7L, "ACCEPT", 3L, encoding, null)),
                "learn_exchange_accept_cbor");
        assertBytes(codec.toBytes(new Exchange("golden-model", 7L, "BUSY", 0L, null, null)),
                "learn_exchange_busy_cbor");

        Exchange offer = roundTrip("learn_exchange_offer_cbor", Exchange.class);
        assertThat(offer.kind()).isEqualTo("OFFER");
        assertThat(offer.token()).isEqualTo(7L);
        assertThat(offer.round()).isEqualTo(2L);
        assertThat(offer.inline()).isEqualTo(encoding);
        assertThat(offer.cid()).isNull();
        Exchange accept = roundTrip("learn_exchange_accept_cbor", Exchange.class);
        assertThat(accept.kind()).isEqualTo("ACCEPT");
        assertThat(accept.round()).isEqualTo(3L);
        Exchange busy = roundTrip("learn_exchange_busy_cbor", Exchange.class);
        assertThat(busy.kind()).isEqualTo("BUSY");
        assertThat(busy.inline()).isNull();
        assertThat(busy.cid()).isNull();
    }

    /** SPEC §9: the pinned wire version is the one WireCodec accepts. */
    @Test
    void wireVersionIsPinned() {
        assertThat(((Number) golden.get("wire_version")).intValue())
                .isEqualTo(WireCodec.WIRE_VERSION).isEqualTo(3);
    }

    /** Every v0.1.10 {@code *_cbor} vector, adversarial ones included, is well-formed CBOR to this codec. */
    @Test
    void everyNewCborVectorDecodes() {
        List<String> expected = List.of("founding_fields_cbor", "founding_document_cbor",
                "group_ad_cbor", "signed_group_ad_cbor", "group_ad_want_envelope_cbor",
                "group_ad_want_frame_cbor", "group_ad_envelope_cbor", "group_ad_frame_cbor",
                "adv_group_ad_wrong_id_cbor", "adv_group_ad_forged_signature_cbor",
                "agent_card_cbor", "signed_agent_card_cbor", "space_ad_cbor",
                "aggregate_share_frame_cbor", "aggregate_extremum_frame_cbor",
                "aggregate_histogram_frame_cbor", "aggregate_roster_frame_cbor",
                "learn_exchange_offer_cbor", "learn_exchange_accept_cbor",
                "learn_exchange_busy_cbor", "space_credential_record_cbor",
                "join_ticket_record_cbor");
        for (String name : expected) {
            assertThat(codec.fromBytes(hex(name), Object.class)).as(name).isNotNull();
        }
    }

    /**
     * SPEC §6.1 / §4.2 (QA4 A4-7 phase 3): the AgentCard's appended {@code agentPublicKey}
     * is on the wire only when present, so the keyed card decodes to the golden card
     * plus the agent key, its bytes and signature are pinned, and a card without the
     * key is still exactly {@code agent_card_cbor}.
     */
    @Test
    void anAttestedAgentCardCarriesItsKeyAndAnUnkeyedOneIsUnchanged() {
        AgentCard plain = codec.fromBytes(hex("agent_card_cbor"), AgentCard.class);
        assertThat(plain.agentPublicKey()).isNull();
        assertThat(plain.attested()).isFalse();
        assertBytes(codec.toBytes(plain), "agent_card_cbor");
        AgentCard keyed = new AgentCard(plain.id(), plain.issuer(), plain.group(), plain.issued(),
                plain.ttl(), plain.agent(), plain.description(), plain.goals(), plain.consumes(),
                plain.produces(), plain.costHints(), plain.spaceBindings(), hex("agent_public_key_raw"));
        assertBytes(codec.toBytes(keyed), "agent_card_with_key_cbor");
        assertBytes(new AdvertisementSigner(codec).sign(keyed, identity).signature(),
                "agent_card_with_key_signature");
        AgentCard decoded = codec.fromBytes(hex("agent_card_with_key_cbor"), AgentCard.class);
        assertThat(decoded).isEqualTo(keyed);
        assertThat(decoded.attested()).isTrue();
        assertThat(PeerId.fromPublicKey(decoded.agentPublicKey()))
                .as("the agent key is not the peer key").isNotEqualTo(decoded.issuer());
    }
}
