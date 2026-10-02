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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
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
import org.junit.jupiter.api.BeforeEach;
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
 * The golden certificate vectors through a real {@link ReplicatedSpace}: the
 * same bytes the Python and TypeScript clients fold in their golden suites are
 * gossiped to a peer whose clock sits at {@code certificate_verify_at}. The
 * certified delta lands; each of the five hostile deltas is dropped at the
 * door (TECH-SPEC §7.2's two-key rule, QA4 A4-7 phase 1). This is what makes
 * "all three reject identically" a statement about the fold path, not only
 * about the verification helpers.
 */
class SignedAgentVectorsClusterTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static final String[] HOSTILE = {
            "adv_cert_wrong_peer_delta", "adv_cert_wrong_agent_delta", "adv_cert_expired_delta",
            "adv_cert_uncertified_key_delta", "adv_cert_peer_signed_record_delta"};
    private static Map<String, Object> golden;

    private TestClock clock;
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private GroupId groupId;
    private GroupAdvertisement groupAd;
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

    /** Two peers in the golden group; the receiver's "tasks" space is the golden space_id. */
    @BeforeEach
    void cluster() throws IOException {
        clock = TestClock.startingAt(Instant.parse((String) golden.get("certificate_verify_at")));
        groupId = GroupId.of((String) golden.get("group_id"));
        groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
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

    private void gossip(String vector) {
        sender.gossip().publish("space:tasks", vector,
                HexFormat.of().parseHex((String) golden.get(vector)));
        tickAll(2);
    }

    /**
     * SPEC §11a.4 v0.1.13 (A6, review B-1 of TODO-9-10-11): this v0.1.11 vector
     * carries an agent-signed record under a <em>peer-signed</em> state. Its
     * verdict changes on purpose: an agent-attested entry's transitions must be
     * signed by the agent, so the peer (or a sibling agent) cannot act in its
     * name, and the delta is refused. Phase 3 relabels it hostile and adds an
     * agent-signed control vector.
     */
    @Test
    void theV0111CertifiedDeltaWithAPeerSignedStateIsNowRefused() {
        gossip("delta_with_certificate_cbor");
        assertThat(receiver.knownEntries()).isZero();
    }

    /**
     * TODO-9-10-11 Phase 3 (B-1): the agent-signed control lands and is stored
     * agent-attested; the relabelled legacy vector and the two hostile
     * agent-state variants are refused.
     */
    @Test
    void theAgentSignedStateLandsAndItsHostileVariantsAreRefused() {
        for (String vector : new String[] {"adv_legacy_state_on_attested_entry_delta_cbor",
                "adv_agent_state_peer_key_delta", "adv_agent_state_wrong_signer_delta"}) {
            gossip(vector);
            assertThat(receiver.knownEntries()).as(vector).isZero();
        }
        gossip("delta_agent_signed_cbor");
        assertThat(receiver.knownEntries()).isEqualTo(1);
        ai.badmonkey.agentspaces.api.entry.EntryId entryId =
                ai.badmonkey.agentspaces.api.entry.EntryId.of("11111111-2222-3333-4444-555555555555");
        assertThat(receiver.signedState(entryId)).hasValueSatisfying(dto -> {
            assertThat(dto.agentSigned()).isTrue();
            assertThat(dto.signer()).endsWith("/python");
        });
        assertThat(golden.get("adv_legacy_state_on_attested_entry_delta_cbor"))
                .as("the relabelled vector is the v0.1.11 one, byte for byte")
                .isEqualTo(golden.get("delta_with_certificate_cbor"));
    }

    @Test
    void thePreCertificateGoldenDeltaStillLands() {
        gossip("delta_cbor");
        assertThat(receiver.knownEntries()).isEqualTo(1);
    }

    @Test
    void everyHostileCertificateDeltaIsDroppedAtTheDoor() {
        for (String vector : HOSTILE) {
            gossip(vector);
            assertThat(receiver.knownEntries()).as(vector).isZero();
        }
        // The control still lands afterwards: the drops were verdicts, not a stuck replica.
        gossip("delta_cbor");
        assertThat(receiver.knownEntries()).isEqualTo(1);
    }

    /** QA4 A4-7 phase 3: the agent-signed claim proof lands, stored agent-attested; each hostile proof is dropped. */
    @Test
    void theCertifiedClaimProofLandsAndHostileOnesAreDropped() {
        gossip("delta_cbor");
        ai.badmonkey.agentspaces.api.entry.EntryId entryId =
                ai.badmonkey.agentspaces.api.entry.EntryId.of("11111111-2222-3333-4444-555555555555");
        for (String vector : new String[] {"adv_claim_cert_peer_signed_delta",
                "adv_claim_cert_expired_delta", "adv_claim_cert_wrong_holder_delta"}) {
            gossip(vector);
            assertThat(receiver.claimProof(entryId)).as(vector).isEmpty();
        }
        gossip("claim_delta_with_certificate_cbor");
        assertThat(receiver.claimProof(entryId)).hasValueSatisfying(proof -> {
            assertThat(proof.agentAttested()).isTrue();
            assertThat(proof.holderAttested()).isTrue();
            assertThat(proof.claim().holder().localName()).isEqualTo("python");
        });
    }
}
