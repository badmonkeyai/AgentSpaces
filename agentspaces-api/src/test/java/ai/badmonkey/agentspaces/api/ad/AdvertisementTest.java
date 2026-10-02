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
package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdvertisementTest {

    private static final Instant ISSUED = Instant.parse("2026-08-26T14:00:00Z");
    private static final PeerId PEER = PeerId.of("zPeer");
    private static final GroupId GROUP = GroupId.of("zGroup");

    @Test
    void ttlBoundsFreshness() {
        AgentCard card = card(Duration.ofMinutes(15));

        assertThat(card.expiresAt()).isEqualTo(ISSUED.plus(Duration.ofMinutes(15)));
        assertThat(card.expired(ISSUED.plus(Duration.ofMinutes(14)))).isFalse();
        assertThat(card.expired(ISSUED.plus(Duration.ofMinutes(15)))).isTrue();
        assertThat(card.expired(ISSUED.plus(Duration.ofMinutes(16)))).isTrue();
    }

    @Test
    void collectionsAreDefensivelyCopied() {
        AgentCard card = card(Duration.ofMinutes(15));

        assertThatThrownBy(() -> card.goals().add("mutate"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> card.costHints().put("k", "v"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void groupAdvertisementValidatesGossipParameters() {
        assertThatThrownBy(() -> new GroupAdvertisement.GossipParameters(0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GroupAdvertisement.GossipParameters(3, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);

        GroupAdvertisement.GossipParameters defaults = GroupAdvertisement.GossipParameters.defaults();
        assertThat(defaults.fanout()).isEqualTo(3);
    }

    @Test
    void signedAdvertisementRequiresKeyAndSignature() {
        assertThatThrownBy(() -> new SignedAdvertisement<>(card(Duration.ofMinutes(1)),
                new byte[32], new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SignedAdvertisement<>(card(Duration.ofMinutes(1)),
                new byte[0], new byte[64]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void advertisementFamilyConstructs() {
        PeerAdvertisement peerAd = new PeerAdvertisement("aspace://g/peer/1", PEER, GROUP, ISSUED,
                Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("tcp", "10.0.0.5:7401", 0)),
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS), Map.of("battery", "0.83"));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://g", PEER, GROUP, ISSUED,
                Duration.ofHours(24), "research-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        SpaceAdvertisement spaceAd = new SpaceAdvertisement("aspace://g/tasks", PEER, GROUP, ISSUED,
                Duration.ofHours(24), "tasks", List.of("ResearchTask#v1"),
                ConflictStrategyType.LEASE_RACE);
        CapabilityAdvertisement capAd = new CapabilityAdvertisement("aspace://g/cap/vote/1", PEER,
                GROUP, ISSUED, Duration.ofMinutes(15), "aspace:cap/vote", "0.1",
                "space:votes", Map.of("mode", "QUORUM"), Map.of());
        AssetCard assetCard = new AssetCard("aspace://g/asset/sensor-log", PEER, GROUP, ISSUED,
                Duration.ofMinutes(15), "sensor-log", "s3://fleet/sensor-log.parquet",
                "Hourly lidar sweeps", "parquet{ts:int64,range:float32[]}", "PT1H",
                Map.of("bytes", "1048576"), Map.of("binding", "aspace:cap/blocks"));
        PeerId successor = PeerId.of("zSuccessor");
        RevocationAdvertisement revocation = new RevocationAdvertisement(
                "aspace://g/revoke/zPeer", PEER, GROUP, ISSUED, Duration.ofHours(24),
                PEER, "key rotated", successor);
        RevocationAdvertisement terminal = new RevocationAdvertisement(
                "aspace://g/revoke/zPeer/2", PEER, GROUP, ISSUED, Duration.ofHours(24),
                PEER, null, null);

        assertThat(peerAd.roles()).contains(PeerAdvertisement.PeerRole.RENDEZVOUS);
        assertThat(groupAd.membershipPolicy()).isEqualTo(GroupAdvertisement.MembershipPolicy.OPEN);
        assertThat(spaceAd.strategy()).isEqualTo(ConflictStrategyType.LEASE_RACE);
        assertThat(capAd.capabilityType()).isEqualTo("aspace:cap/vote");
        assertThat(assetCard.asset()).isEqualTo("sensor-log");
        assertThat(assetCard.expiresAt()).isEqualTo(ISSUED.plus(Duration.ofMinutes(15)));
        assertThatThrownBy(() -> assetCard.access().put("k", "v"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(revocation.revoked()).isEqualTo(PEER);
        assertThat(revocation.successor()).isEqualTo(successor);
        assertThat(terminal.reason()).as("a null reason normalizes to empty").isEmpty();
        assertThat(terminal.successor()).as("a revocation may name no successor").isNull();
        assertThatThrownBy(() -> new RevocationAdvertisement("aspace://g/revoke/x", PEER, GROUP,
                ISSUED, Duration.ofHours(24), null, "no target", null))
                .isInstanceOf(NullPointerException.class);
    }

    /** SPEC §7.5: the v0.1.11 admission rules CREDENTIAL and AUTHORIZER are additive constants a SpaceAdvertisement carries. */
    @Test
    void spaceAdvertisementCarriesTheCredentialAndAuthorizerAdmissionRules() {
        SpaceAdvertisement credential = new SpaceAdvertisement("aspace://g/tasks", PEER, GROUP,
                ISSUED, Duration.ofHours(24), "tasks", List.of("ResearchTask#v1"),
                ConflictStrategyType.LEASE_RACE, SpaceAdvertisement.Admission.CREDENTIAL,
                SpaceAdvertisement.Replication.FULL);
        SpaceAdvertisement authorizer = new SpaceAdvertisement("aspace://g/tasks", PEER, GROUP,
                ISSUED, Duration.ofHours(24), "tasks", List.of("ResearchTask#v1"),
                ConflictStrategyType.LEASE_RACE, SpaceAdvertisement.Admission.AUTHORIZER,
                SpaceAdvertisement.Replication.FULL);

        assertThat(credential.admission()).isEqualTo(SpaceAdvertisement.Admission.CREDENTIAL);
        assertThat(authorizer.admission()).isEqualTo(SpaceAdvertisement.Admission.AUTHORIZER);
        assertThat(SpaceAdvertisement.Admission.values())
                .as("the pre-v0.1.11 constants keep their names and order")
                .startsWith(SpaceAdvertisement.Admission.GROUP, SpaceAdvertisement.Admission.ALLOWLIST)
                .hasSize(4);
        assertThat(Authorizer.Operation.values())
                .as("the space operations the AUTHORIZER rule asks about")
                .contains(Authorizer.Operation.SPACE_WRITE, Authorizer.Operation.SPACE_TAKE);
    }

    private static AgentCard card(Duration ttl) {
        return new AgentCard("aspace://g/agent/researcher", PEER, GROUP, ISSUED, ttl,
                AgentId.parse("zPeer/researcher"), "Researches topics",
                List.of("research"), List.of("ResearchTask#v1"), List.of("Finding#v1"),
                Map.of("model", "small"));
    }

    /** SPEC §6.1 v0.1.13: a card's actions are validated: unique names, a bounded count, bounded descriptions. */
    @Test
    void cardActionsAreValidated() {
        PeerId peer = PeerId.fromPublicKey(new byte[32]);
        GroupId group = GroupId.of("zActions");
        AgentCard base = new AgentCard("aspace://zActions/agent/a", peer, group, Instant.EPOCH,
                Duration.ofMinutes(15), new AgentId(peer, "a"), "", List.of(), List.of(), List.of(),
                Map.of());
        CardAction one = new CardAction("one", "", List.of("A#v1"), List.of(), null, CardAction.TAKE);
        assertThat(base.withActions(List.of(one)).actions()).containsExactly(one);
        assertThat(base.withActions(null).actions()).isNull();
        assertThatThrownBy(() -> base.withActions(List.of(one, one)))
                .hasMessageContaining("unique");
        List<CardAction> tooMany = new java.util.ArrayList<>();
        for (int i = 0; i <= AgentCard.MAX_ACTIONS; i++) {
            tooMany.add(new CardAction("a" + i, "", List.of(), List.of(), null, CardAction.TAKE));
        }
        assertThatThrownBy(() -> base.withActions(tooMany)).hasMessageContaining("at most");
        assertThatThrownBy(() -> new CardAction("long", "x".repeat(CardAction.MAX_DESCRIPTION_LENGTH + 1),
                List.of(), List.of(), null, CardAction.TAKE)).hasMessageContaining("longer");
        // A kind this reader does not know (a newer peer's) still decodes, so the
        // card is kept, but it is never invoked.
        CardAction future = new CardAction("later", "", List.of("A#v1"), List.of(), null, "stream");
        assertThat(future.invocable()).isFalse();
        assertThat(CardAction.KINDS).doesNotContain("stream");
        assertThat(CardAction.KINDS).containsAll(CardAction.INVOCABLE);
    }
}
