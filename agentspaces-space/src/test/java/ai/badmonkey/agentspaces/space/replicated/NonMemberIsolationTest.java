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
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.MembershipValidator;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The data plane serves admitted members only (ASF-003): a keypair that knows
 * the GroupId but was never admitted gets no replication — its digests are
 * never answered, its pulls never served — so a closed group's plaintext state
 * cannot be exfiltrated by joining the transport without passing admission.
 */
class NonMemberIsolationTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zClosed");
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zClosed", founderId.peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "closed-fleet",
            GroupAdvertisement.MembershipPolicy.POLICY, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final MembershipValidator badgeCheck =
            (candidate, group, credentials) -> "ok".equals(credentials.get("badge"));

    private record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer peer(PeerIdentity identity, String address, long seed,
                      Map<String, String> hints, MembershipValidator validator,
                      String... seedAddresses) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seeds, hints, validator);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, address)
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        return new Peer(node, runtime, space);
    }

    @Test
    void aMemberSpammingHostileClaimsIsQuarantined() throws Exception {
        GroupAdvertisement open = new GroupAdvertisement(
                "aspace://zClosed/open", founderId.peerId(), groupId,
                java.time.Instant.EPOCH, Duration.ofDays(1), "open-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());

        PeerNode a = PeerNode.builder(founderId).clock(clock).randomSeed(1).build();
        a.listen(network.register("qa"), "qa");
        GroupRuntime ra = a.joinGroup(open, GroupMembership.Config.defaults(),
                java.util.List.of());
        ReplicatedSpace spaceA = ReplicatedSpace.builder(ra, "tasks", founderId, "qa")
                .clock(clock).settleWindow(Duration.ZERO).build();
        PeerIdentity evilId = PeerIdentity.generate();
        PeerNode evil = PeerNode.builder(evilId).clock(clock).randomSeed(2).build();
        evil.listen(network.register("qevil"), "qevil");
        GroupRuntime revil = evil.joinGroup(open, GroupMembership.Config.defaults(),
                java.util.List.of(new PeerAdvertisement.Endpoint("mem", "qa", 0)));
        ReplicatedSpace spaceEvil = ReplicatedSpace.builder(revil, "tasks", evilId, "qevil")
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(a);
        nodes.add(evil);
        for (int i = 0; i < 6; i++) {
            nodes.forEach(PeerNode::tick);
        }

        // Seed an entry so hostile claims have a target, and prove the channel
        // works before the attack: evil replicates it.
        spaceA.write(new TaskEntry("victim-entry", 1), Lease.of(Duration.ofMinutes(30)));
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        assertThat(spaceEvil.read(Template.of(TaskEntry.class))).isPresent();

        // Evil publishes a stream of validly signed claims that fail the merge
        // bounds — witnessed, attributable misbehavior. Each is a distinct
        // hostile claim; past the strike threshold, a quarantines evil.
        var codec = ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();
        var evilHlc = new ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock(clock, "qevil");
        var targetId = ai.badmonkey.agentspaces.api.entry.EntryId
                .of("11111111-2222-3333-4444-555555555555");
        for (int i = 0; i < 20; i++) {
            TakeClaim hostile = new TakeClaim(targetId, spaceA.id(), 2,
                    evilHlc.now(), evilId.agent("qevil"), 0.0, Long.MAX_VALUE);
            SpaceWire.SignedClaim signed = new SpaceWire.SignedClaim(hostile,
                    evilId.rawPublicKey(), evilId.sign(codec.toBytes(hostile)));
            revil.gossip().publish("space:tasks", "hostile:" + i,
                    codec.toBytes(new SpaceWire.Delta(null, targetId, signed)));
        }
        for (int i = 0; i < 2; i++) {
            nodes.forEach(PeerNode::tick);
        }

        // Quarantined: evil's subsequent honest write no longer replicates to a.
        spaceEvil.write(new TaskEntry("post-quarantine", 9),
                Lease.of(Duration.ofMinutes(30)));
        for (int i = 0; i < 6; i++) {
            nodes.forEach(PeerNode::tick);
        }
        assertThat(spaceA.readAll(Template.of(TaskEntry.class), 10))
                .as("the quarantined peer's frames are refused")
                .extracting(TaskEntry::topic)
                .containsExactly("victim-entry");
    }

    @Test
    void aNeverAdmittedPeerGetsNoReplication() throws Exception {
        Peer founder = peer(founderId, "f", 1, Map.of(), badgeCheck);
        Peer good = peer(PeerIdentity.generate(), "good", 2,
                Map.of("badge", "ok"), badgeCheck, "f");
        PeerIdentity outsiderId = PeerIdentity.generate();
        Peer outsider = peer(outsiderId, "out", 3, Map.of(), null, "f");

        // The outsider controls its own node: it forces the founder into its
        // local view so its anti-entropy digests target the founder directly —
        // the exact exfiltration channel the dispatch gate must close.
        outsider.runtime().membership().onPeerAdvertisement(new PeerAdvertisement(
                "aspace://" + groupId.value() + "/peer/" + founderId.peerId().value(),
                founderId.peerId(), groupId, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "f", 0)),
                Set.of(), Map.of()));

        founder.space().write(new TaskEntry("secret-work", 1),
                Lease.of(Duration.ofMinutes(30)));
        for (int i = 0; i < 10; i++) {
            nodes.forEach(PeerNode::tick);
        }

        // The credentialed member replicates; the outsider never does.
        assertThat(founder.runtime().membership().allMembers())
                .extracting(GroupMembership.Member::id)
                .contains(good.node().peerId())
                .doesNotContain(outsider.node().peerId());
        assertThat(good.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(outsider.space().read(Template.of(TaskEntry.class)))
                .as("no digest answer, no pull, no rumor service for a non-member")
                .isEmpty();
    }
}
