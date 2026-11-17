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
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tag-shard partial replication (spec §7.5): a sharded replica stores and
 * matches only entries whose tags satisfy its shard predicate, while full
 * replicas hold everything; anti-entropy stays quiescent once converged.
 */
class ShardedSpaceTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zShardFleet");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zShardFleet", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "shard-fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed,
                         Predicate<Map<String, String>> shard,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace.Builder builder =
                ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                        .clock(clock)
                        .settleWindow(Duration.ZERO);
        if (shard != null) {
            builder.tagShard(shard);
        }
        ReplicatedSpace space = builder.build();
        nodes.add(node);
        return new Peer(node, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    @Test
    void aShardedReplicaHoldsOnlyItsShard() throws Exception {
        Peer writer = newPeer("a", 1, null);
        Peer euOnly = newPeer("b", 2, tags -> "eu".equals(tags.get("region")), "a");
        Peer full = newPeer("c", 3, null, "a");
        tickAll(4);

        writer.space().write(new TaskEntry("eu ledgers", 1), MINUTES_30,
                Map.of("region", "eu"));
        writer.space().write(new TaskEntry("us ledgers", 2), MINUTES_30,
                Map.of("region", "us"));
        writer.space().write(new TaskEntry("apac ledgers", 3), MINUTES_30,
                Map.of("region", "apac"));
        tickAll(4);

        assertThat(full.space().knownEntries()).isEqualTo(3);
        assertThat(full.space().readAll(Template.of(TaskEntry.class), 10)).hasSize(3);

        assertThat(euOnly.space().knownEntries()).isEqualTo(1);
        assertThat(euOnly.space().readAll(Template.of(TaskEntry.class), 10))
                .containsExactly(new TaskEntry("eu ledgers", 1));
    }

    @Test
    void untaggedEntriesAreOutOfEveryKeyedShard() throws Exception {
        Peer writer = newPeer("a", 1, null);
        Peer euOnly = newPeer("b", 2, tags -> "eu".equals(tags.get("region")), "a");
        tickAll(4);

        writer.space().write(new TaskEntry("untagged", 1), MINUTES_30);
        tickAll(4);

        assertThat(euOnly.space().knownEntries()).isZero();
    }

    @Test
    void theShardedReplicaStaysQuietOnceConverged() throws Exception {
        Peer writer = newPeer("a", 1, null);
        Peer euOnly = newPeer("b", 2, tags -> "eu".equals(tags.get("region")), "a");
        tickAll(4);

        writer.space().write(new TaskEntry("us ledgers", 2), MINUTES_30,
                Map.of("region", "us"));
        tickAll(6);

        // The dropped entry is acknowledged in the shard's digest, so extra
        // anti-entropy rounds neither store it nor disturb convergence.
        assertThat(euOnly.space().knownEntries()).isZero();
        tickAll(6);
        assertThat(euOnly.space().knownEntries()).isZero();
        assertThat(writer.space().knownEntries()).isEqualTo(1);
    }

    @Test
    void aShardedReplicasOwnWritesAreAlwaysStored() throws Exception {
        Peer writer = newPeer("a", 1, null);
        Peer euOnly = newPeer("b", 2, tags -> "eu".equals(tags.get("region")), "a");
        tickAll(4);

        euOnly.space().write(new TaskEntry("my own out-of-shard note", 4), MINUTES_30,
                Map.of("region", "us"));
        tickAll(4);

        // The writer keeps what it wrote, and full replicas receive it.
        assertThat(euOnly.space().readAll(Template.of(TaskEntry.class), 10)).hasSize(1);
        assertThat(writer.space().readAll(Template.of(TaskEntry.class), 10))
                .containsExactly(new TaskEntry("my own out-of-shard note", 4));
    }

    /**
     * P36 (srad/replication-by-entry.md phase 4): the trie audit of annex I.6 between a full replica and a sharded one.
     * The sharded replica's dropped entries stand in its slots as the state line it remembered, so after one repair
     * its audited set equals the full replica's, both stream hashes match, and later rounds open no audit and fetch
     * nothing. A changed out-of-shard entry makes the sets differ by one line, which the next round fetches by id,
     * drops again, and settles.
     */
    @Test
    void anAuditBetweenAShardedAndAFullReplicaSettlesWithTheDroppedLinesInTheSlots() throws Exception {
        Peer full = newPeer("a", 1, null);
        Peer euOnly = newPeer("b", 2, tags -> "eu".equals(tags.get("region")), "a");
        tickAll(6);
        ai.badmonkey.agentspaces.peering.gossip.GossipBus fullBus = gossip(full);
        ai.badmonkey.agentspaces.peering.gossip.GossipBus shardBus = gossip(euOnly);
        assertThat(shardBus.audits(full.node().peerId())).as("both on wire.ae=2").isTrue();

        network.partition("a", "b");
        List<ai.badmonkey.agentspaces.api.space.EntryHandle> us = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            full.space().write(new TaskEntry("eu-" + i, i), MINUTES_30, Map.of("region", "eu"));
            us.add(full.space().write(new TaskEntry("us-" + i, i), MINUTES_30, Map.of("region", "us")));
        }
        network.heal();
        for (int round = 0; round < 4; round++) {
            shardBus.antiEntropyWith(full.node().peerId());
            fullBus.antiEntropyWith(euOnly.node().peerId());
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(euOnly.space().knownEntries()).as("the shard holds its own").isEqualTo(60);
        assertThat(euOnly.space().stats().shardDroppedLines()).as("and remembers each dropped line").isEqualTo(60);
        assertThat(euOnly.space().streamHash()).as("the audited sets are equal")
                .isEqualTo(full.space().streamHash());

        long audits = shardBus.auditSignals().roundsAudited() + fullBus.auditSignals().roundsAudited();
        long fetched = shardBus.auditSignals().idsFetched() + fullBus.auditSignals().idsFetched();
        tickAll(6);
        assertThat(shardBus.auditSignals().roundsAudited() + fullBus.auditSignals().roundsAudited())
                .as("equal hashes open no audit").isEqualTo(audits);
        assertThat(shardBus.auditSignals().idsFetched() + fullBus.auditSignals().idsFetched()).isEqualTo(fetched);

        us.get(7).renew(Duration.ofMinutes(45)); // a new state of an out-of-shard entry: one line differs
        for (int round = 0; round < 3; round++) {
            shardBus.antiEntropyWith(full.node().peerId());
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(euOnly.space().knownEntries()).as("the changed entry was fetched and dropped again")
                .isEqualTo(60);
        assertThat(euOnly.space().streamHash()).isEqualTo(full.space().streamHash());
        assertThat(shardBus.auditSignals().refusals()).doesNotContainKeys("undecodable", "fingerprint-mismatch");
    }

    private static ai.badmonkey.agentspaces.peering.gossip.GossipBus gossip(Peer peer) {
        return peer.node().groups().iterator().next().gossip();
    }
}
