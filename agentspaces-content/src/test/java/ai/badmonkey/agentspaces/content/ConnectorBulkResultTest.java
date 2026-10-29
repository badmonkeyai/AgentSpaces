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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.connect.ConnectorRuntime;
import ai.badmonkey.agentspaces.connect.DataQueryClient;
import ai.badmonkey.agentspaces.connect.DataQueryEntries;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataResult;
import ai.badmonkey.agentspaces.connect.DataSpaces;
import ai.badmonkey.agentspaces.connect.TableAssetProvider;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario 6 of srad/large-networks-bulk-data.md (LB-E-05): a connector's result above the
 * escalation bound leaves the connector as an artifact of the content swarm, the completion
 * carries its root, and the requester's {@link DataQueryClient} reads every row once the
 * artifact is complete on its replica.
 */
class ConnectorBulkResultTest {

    private static final long THRESHOLD = 256 * 1024;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zBulkResult");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zBulkResult", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "bulk-result",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime, DiscoveryService discovery,
                         ReplicatedSpace data, Content content) {
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
        nodes.forEach(PeerNode::close);
    }

    private Wired newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks)
                .random(new Random(seed)).build();
        runtime.onTick(content::tick);
        content.start();
        ReplicatedSpace data = DataSpaces.builder(runtime, DataSpaces.DEFAULT_NAME, identity, address, blocks,
                        new SpacePayloads(content), THRESHOLD)
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        return new Wired(node, identity, runtime, discovery, data, content);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(250));
        }
    }

    @Test
    @Timeout(120)
    void aResultAboveTheBoundTravelsAsAnArtifactAndTheClientReadsEveryRow() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        List<Map<String, String>> rows = new ArrayList<>();
        String padding = "x".repeat(200);
        for (int i = 0; i < 2_000; i++) {
            rows.add(Map.of("id", String.valueOf(i), "region", "eu", "payload", padding));
        }
        String hash = DataQueryEntries.canonicalHash("orders", Map.of("region", "eu"));
        assertThat(codec.toBytes(new DataResult(hash, "orders", rows, null, "pg-connector")).length)
                .as("the rows serialize past the escalation bound").isGreaterThan((int) THRESHOLD);
        TableAssetProvider provider = new TableAssetProvider("orders", "postgres://ops/public.orders",
                "Bulk order export", "com.example.OrderRow#v1", Duration.ofMinutes(5), rows);
        ConnectorRuntime connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", provider, groupId, clock, Duration.ofMinutes(1));
        closeables.add(connector);
        connector.start();

        DataQueryClient client = new DataQueryClient(agent.data(), "analyst", Set.of(connectorPeer.identity().peerId()));
        // The fetch waits on the requester's thread; the fabric delivers inline, and the swarm's
        // scheduler runs on the ticks this loop drives until the answer is in.
        Thread ticker = Thread.ofVirtual().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                tickAll(1);
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        try {
            Optional<DataQueryClient.Fetched> fetched = client.fetch("orders", Map.of("region", "eu"),
                    Duration.ofSeconds(60));
            assertThat(fetched).isPresent();
            assertThat(fetched.get().result().rows()).hasSize(2_000).containsExactlyElementsOf(rows);
            assertThat(fetched.get().result().servedBy()).isEqualTo("pg-connector");
        } finally {
            ticker.interrupt();
        }
        // The completion on the wire is a root, not the rows: the artifact is in both stores.
        assertThat(connectorPeer.content().list(10)).singleElement()
                .satisfies(m -> assertThat(m.mediaType()).isEqualTo("application/cbor"));
        String root = connectorPeer.content().list(10).get(0).root();
        assertThat(agent.content().store().have(root).cardinality())
                .isEqualTo(agent.content().store().manifest(root).orElseThrow().pieces());
        assertThat(agent.content().store().manifest(root).orElseThrow().size()).isGreaterThan(THRESHOLD);
    }
}
