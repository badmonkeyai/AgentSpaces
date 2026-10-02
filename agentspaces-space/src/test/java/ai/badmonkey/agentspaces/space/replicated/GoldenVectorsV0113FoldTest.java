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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The v0.1.13 transition vectors of audit item 10 through a real
 * {@link ReplicatedSpace}: agent-signed renew, cancel and complete land; a
 * record under a renewed certificate lands at its signing time; and each
 * hostile twin (a signing time outside the certificate's window or beyond the
 * drift ceiling, a claim predating its entry, a relabelled key epoch) is
 * dropped at the door, leaving the replica as it was.
 */
class GoldenVectorsV0113FoldTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static final EntryId ENTRY = EntryId.of("11111111-2222-3333-4444-555555555555");
    private static Map<String, Object> golden;

    private final CborCodec codec = CborCodec.defaultCodec();
    private TestClock clock;
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private GroupRuntime sender;
    private ReplicatedSpace receiver;

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

    /** Two peers in the golden group, clocks at {@code at}; the receiver's "tasks" space is the golden space_id. */
    private void cluster(Instant at) throws IOException {
        clock = TestClock.startingAt(at);
        GroupId groupId = GroupId.of((String) golden.get("group_id"));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
                "golden", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        PeerIdentity receiving = PeerIdentity.generate();
        PeerNode a = node(receiving, 1, "a");
        GroupRuntime aRuntime = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        receiver = ReplicatedSpace.builder(aRuntime, "tasks", receiving, "reader").clock(clock).build();
        PeerNode b = node(PeerIdentity.generate(), 2, "b");
        sender = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(4);
        assertThat(receiver.id().value()).as("the fixture space is the golden space")
                .isEqualTo(golden.get("space_id"));
    }

    private void atVerifyTime() throws IOException {
        cluster(Instant.parse((String) golden.get("certificate_verify_at")));
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(PeerIdentity identity, long seed, String address) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private byte[] hex(String vector) {
        assertThat(golden).as("vector " + vector).containsKey(vector);
        return HexFormat.of().parseHex((String) golden.get(vector));
    }

    private void gossip(String vector) {
        sender.gossip().publish("space:tasks", vector, hex(vector));
        tickAll(2);
    }

    private SpaceWire.EntryStateDto stateOf(String vector) {
        return codec.fromBytes(hex(vector), SpaceWire.Delta.class).state();
    }

    @Test
    void anAgentSignedRenewalLandsAndAdvancesTheLease() throws IOException {
        atVerifyTime();
        gossip("delta_agent_signed_cbor");
        gossip("delta_agent_renew_cbor");
        SpaceWire.EntryStateDto renewal = stateOf("delta_agent_renew_cbor");
        assertThat(renewal.leaseStamp()).isGreaterThan(stateOf("delta_agent_signed_cbor").leaseStamp());
        assertThat(receiver.signedState(ENTRY)).hasValueSatisfying(dto -> {
            assertThat(dto.agentSigned()).isTrue();
            assertThat(dto.leaseStamp()).isEqualTo(renewal.leaseStamp());
            assertThat(dto.leaseValue().expiresAtMillis()).isEqualTo(renewal.leaseValue().expiresAtMillis());
            assertThat(codec.toBytes(dto)).isEqualTo(hex("entry_state_agent_renew_cbor"));
        });
    }

    @Test
    void anAgentSignedCancelLandsAndWithdrawsTheEntry() throws IOException {
        atVerifyTime();
        gossip("delta_agent_signed_cbor");
        gossip("delta_agent_cancel_cbor");
        assertThat(receiver.signedState(ENTRY)).hasValueSatisfying(dto -> {
            assertThat(dto.agentSigned()).isTrue();
            assertThat(dto.removes()).containsExactlyElementsOf(dto.adds());
            assertThat(codec.toBytes(dto)).isEqualTo(hex("entry_state_agent_cancel_cbor"));
        });
    }

    @Test
    void anAgentSignedCompletionCarriesItsStateCertificateAndLands() throws IOException {
        atVerifyTime();
        gossip("delta_agent_signed_cbor");
        SpaceWire.EntryStateDto completion = stateOf("delta_agent_complete_cbor");
        assertThat(completion.completed()).isTrue();
        assertThat(codec.toBytes(completion.stateCertificate())).isEqualTo(hex("agent_certificate_cbor"));
        gossip("delta_agent_complete_cbor");
        assertThat(receiver.claimProof(ENTRY)).hasValueSatisfying(proof ->
                assertThat(proof.agentAttested()).isTrue());
        assertThat(receiver.signedState(ENTRY)).hasValueSatisfying(dto -> {
            assertThat(dto.completed()).isTrue();
            assertThat(dto.signer()).endsWith("/python");
            assertThat(codec.toBytes(dto)).isEqualTo(hex("entry_state_agent_complete_cbor"));
        });
    }

    @Test
    void aStateSignedOutsideItsCertificatesWindowOrBeyondTheDriftCeilingIsRefused() throws IOException {
        atVerifyTime();
        for (String vector : new String[] {"adv_agent_state_out_of_window_delta", "adv_agent_state_drift_delta"}) {
            gossip(vector);
            assertThat(receiver.knownEntries()).as(vector).isZero();
        }
        gossip("delta_agent_signed_cbor");
        assertThat(receiver.knownEntries()).as("the control still lands").isEqualTo(1);
    }

    @Test
    void anOutOfWindowAgentClaimIsRefused() throws IOException {
        atVerifyTime();
        gossip("delta_cbor");
        gossip("adv_claim_cert_out_of_window_delta");
        assertThat(receiver.claimProof(ENTRY)).isEmpty();
        gossip("claim_delta_with_certificate_cbor");
        assertThat(receiver.claimProof(ENTRY)).hasValueSatisfying(proof ->
                assertThat(proof.agentAttested()).isTrue());
    }

    @Test
    void aClaimPredatingItsEntryBeyondTheAllowanceCannotJumpTheQueue() throws IOException {
        atVerifyTime();
        gossip("delta_cbor");
        gossip("claim_delta_cbor");
        SpaceWire.SignedClaim honest = receiver.claimProof(ENTRY).orElseThrow();
        SpaceWire.SignedClaim predating = codec.fromBytes(hex("adv_claim_predates_issue_delta"),
                SpaceWire.Delta.class).claim();
        assertThat(TakeClaim.merge(honest.claim(), predating.claim()))
                .as("unchecked, the lattice would pick the predating claim").isEqualTo(predating.claim());
        assertThat(stateOf("delta_cbor").record().issued().physical() - predating.claim().stamp().physical())
                .isEqualTo(ReplicatedSpace.MAX_CLAIM_PREDATES_ISSUE_MILLIS + 1);
        gossip("adv_claim_predates_issue_delta");
        assertThat(receiver.claimProof(ENTRY)).hasValueSatisfying(proof ->
                assertThat(proof.claim()).isEqualTo(honest.claim()));
    }

    @Test
    void aRecordWhoseKeyEpochWasRelabelledAfterSigningIsRefused() throws IOException {
        atVerifyTime();
        gossip("adv_key_epoch_relabelled_delta");
        assertThat(receiver.knownEntries()).isZero();
        gossip("delta_key_epoch_cbor");
        assertThat(receiver.knownEntries()).as("the epoch-2 control lands").isEqualTo(1);
        assertThat(receiver.signedState(ENTRY)).hasValueSatisfying(dto ->
                assertThat(dto.record().keyEpoch()).isEqualTo(2L));
    }

    @Test
    void aRecordUnderARenewedCertificateLandsAtItsSigningTime() throws IOException {
        cluster(Instant.parse((String) golden.get("renewed_certificate_signed_at")));
        gossip("delta_renewed_certificate_cbor");
        EntryId renewedEntry = EntryId.of("44444444-5555-6666-7777-888888888888");
        assertThat(receiver.signedState(renewedEntry)).hasValueSatisfying(dto -> {
            assertThat(dto.agentSigned()).isTrue();
            assertThat(codec.toBytes(dto.agentCertificate())).isEqualTo(hex("renewed_agent_certificate_cbor"));
        });
    }

    @Test
    void aRecordUnderARenewedCertificateIsRefusedByAReceiverFarBehindItsStamp() throws IOException {
        // The same bytes at certificate_verify_at: the signing time leads the
        // receiver's clock by more than the drift ceiling.
        atVerifyTime();
        gossip("delta_renewed_certificate_cbor");
        assertThat(receiver.knownEntries()).isZero();
    }
}
