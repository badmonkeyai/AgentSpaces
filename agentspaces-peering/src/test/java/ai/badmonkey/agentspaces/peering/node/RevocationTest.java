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
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The authoritative eject (remediation plan §9): a founder-signed revocation is
 * honored fleet-wide, a non-founder's is refused everywhere, a late joiner
 * converges on it through anti-entropy, and rotation is a revocation naming a
 * successor.
 */
class RevocationTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zRevoke");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRevoke", founderId.peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerIdentity identity, PeerNode node, GroupRuntime runtime) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer peer(PeerIdentity identity, String address, long seed,
                      String... seedAddresses) throws IOException {
        return peer(identity, address, seed, UnaryOperator.identity(), seedAddresses);
    }

    /** As {@link #peer(PeerIdentity, String, long, String...)}, with a hook to customize the builder. */
    private Peer peer(PeerIdentity identity, String address, long seed,
                      UnaryOperator<PeerNode.Builder> customize,
                      String... seedAddresses) throws IOException {
        PeerNode node = customize.apply(
                PeerNode.builder(identity).clock(clock).randomSeed(seed)).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        nodes.add(node);
        return new Peer(identity, node, runtime);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
        }
    }

    @Test
    void aFounderRevocationEjectsFleetWideAndSurvivesForLateJoiners() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer bystander = peer(PeerIdentity.generate(), "b", 2, "f");
        Peer victim = peer(PeerIdentity.generate(), "v", 3, "f");
        tickAll(6);
        assertThat(bystander.runtime().membership().member(victim.node().peerId()))
                .as("victim is a member before the revocation").isPresent();

        assertThat(founder.runtime().revoke(victim.node().peerId(), "compromised"))
                .isPresent();
        tickAll(4);

        // Every node refuses the identity and evicted it from its view.
        for (Peer honest : List.of(founder, bystander)) {
            assertThat(honest.runtime().revocations().revoked(victim.node().peerId()))
                    .isTrue();
            assertThat(honest.runtime().membership().member(victim.node().peerId()))
                    .isEmpty();
        }

        // The victim cannot regain membership: even its fresh self-introductions
        // are refused at dispatch before any admission runs.
        tickAll(6);
        assertThat(founder.runtime().membership().member(victim.node().peerId()))
                .isEmpty();

        // A peer that joins long after the rumor round converges on the
        // revocation through anti-entropy alone.
        Peer late = peer(PeerIdentity.generate(), "late", 4, "f");
        tickAll(8);
        assertThat(late.runtime().revocations().revoked(victim.node().peerId()))
                .as("anti-entropy delivered the revocation to the late joiner")
                .isTrue();
    }

    @Test
    void aNonFounderRevocationIsRefusedEverywhere() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer attacker = peer(PeerIdentity.generate(), "a", 2, "f");
        Peer victim = peer(PeerIdentity.generate(), "v", 3, "f");
        tickAll(6);

        assertThat(attacker.runtime().revoke(victim.node().peerId(), "grudge"))
                .as("the issuer's own registry already refuses it").isEmpty();
        tickAll(4);

        assertThat(founder.runtime().revocations().revoked(victim.node().peerId())).isFalse();
        assertThat(founder.runtime().membership().member(victim.node().peerId())).isPresent();
    }

    @Test
    void rotationRevokesTheOldIdentityAndRecordsTheSuccessor() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer old = peer(PeerIdentity.generate(), "old", 2, "f");
        tickAll(6);

        PeerIdentity fresh = PeerIdentity.generate();
        assertThat(founder.runtime().rotate(old.node().peerId(), fresh.peerId(),
                "rotation")).isPresent();
        tickAll(4);

        assertThat(founder.runtime().revocations().revoked(old.node().peerId())).isTrue();
        assertThat(founder.runtime().revocations().successorOf(old.node().peerId()))
                .contains(fresh.peerId());

        // The successor joins like any new peer and is admitted normally.
        Peer successor = peer(fresh, "fresh", 5, "f");
        tickAll(6);
        assertThat(founder.runtime().membership().member(successor.node().peerId()))
                .isPresent();
        assertThat(founder.runtime().revocations().revoked(successor.node().peerId()))
                .isFalse();
    }

    /** Spec v0.1.9: the authority check is pluggable; a substituted trust root replaces the founder fleet-wide. */
    @Test
    void aPluggableValidatorReplacesTheFounderAsAuthority() throws Exception {
        PeerIdentity officerId = PeerIdentity.generate();
        UnaryOperator<PeerNode.Builder> officerRooted = builder -> builder.revocationValidator(
                (ad, group) -> ad.issuer().equals(officerId.peerId()));
        Peer founder = peer(founderId, "f", 1, officerRooted);
        Peer officer = peer(officerId, "o", 2, officerRooted, "f");
        Peer victim = peer(PeerIdentity.generate(), "v", 3, officerRooted, "f");
        Peer other = peer(PeerIdentity.generate(), "w", 4, officerRooted, "f");
        tickAll(6);

        assertThat(founder.runtime().revoke(other.node().peerId(), "no longer my call"))
                .as("under the substituted root the founder's registry refuses its own statement")
                .isEmpty();
        assertThat(officer.runtime().revoke(victim.node().peerId(), "compromised")).isPresent();
        tickAll(4);

        for (Peer honest : List.of(founder, officer, other)) {
            assertThat(honest.runtime().revocations().revoked(victim.node().peerId())).isTrue();
            assertThat(honest.runtime().revocations().revoked(other.node().peerId())).isFalse();
            assertThat(honest.runtime().membership().member(victim.node().peerId())).isEmpty();
        }
    }

    /** Spec v0.1.9: enforcement closes the revoked peer's connection and silences every stream it publishes on. */
    @Test
    void revocationClosesTheVictimsConnectionAndSilencesItsStreams() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer bystander = peer(PeerIdentity.generate(), "b", 2, "f");
        Peer victim = peer(PeerIdentity.generate(), "v", 3, "f");
        tickAll(6);
        List<String> news = new CopyOnWriteArrayList<>();
        bystander.runtime().gossip().onStream("news", (from, id, payload) ->
                news.add(new String(payload, StandardCharsets.UTF_8)));
        victim.runtime().gossip().publish("news", "n1", "before".getBytes(StandardCharsets.UTF_8));
        assertThat(news).containsExactly("before");
        assertThat(bystander.node().channelModes()).containsKey(victim.node().peerId());

        assertThat(founder.runtime().revoke(victim.node().peerId(), "compromised")).isPresent();
        tickAll(2);

        assertThat(bystander.node().channelModes())
                .as("the link to the revoked peer is cut").doesNotContainKey(victim.node().peerId());
        assertThat(founder.node().channelModes()).doesNotContainKey(victim.node().peerId());
        victim.runtime().gossip().publish("news", "n2", "after".getBytes(StandardCharsets.UTF_8));
        tickAll(2);
        victim.runtime().gossip().publish("news", "n3", "after-2".getBytes(StandardCharsets.UTF_8));
        assertThat(news).as("nothing the revoked identity publishes is delivered").containsExactly("before");
    }

    /** Spec v0.1.9: the TTL bounds re-gossip, never the withdrawal; a joiner arriving after the TTL still converges. */
    @Test
    void aRevocationOutlivesItsGossipTtl() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer bystander = peer(PeerIdentity.generate(), "b", 2, "f");
        Peer victim = peer(PeerIdentity.generate(), "v", 3, "f");
        tickAll(6);
        assertThat(founder.runtime().revoke(victim.node().peerId(), "compromised")).isPresent();
        tickAll(4);
        assertThat(bystander.runtime().revocations().revoked(victim.node().peerId())).isTrue();

        // Far past the 30-day re-gossip TTL the withdrawal still holds everywhere...
        clock.advance(Duration.ofDays(31));
        tickAll(6);
        for (Peer honest : List.of(founder, bystander)) {
            assertThat(honest.runtime().revocations().revoked(victim.node().peerId())).isTrue();
            assertThat(honest.runtime().membership().member(victim.node().peerId())).isEmpty();
        }

        // ...and a peer joining only now converges on it from a member's retained set.
        Peer late = peer(PeerIdentity.generate(), "late", 5, "f");
        tickAll(8);
        assertThat(late.runtime().revocations().revoked(victim.node().peerId()))
                .as("anti-entropy serves retained revocations regardless of their TTL").isTrue();
    }
}
