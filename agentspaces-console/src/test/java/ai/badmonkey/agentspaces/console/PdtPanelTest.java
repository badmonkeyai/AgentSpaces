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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.refs.PConflict;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The PDT panel over two simulated peers (srad/agentspaces-pdt.md PD-C-08): a
 * swap on one peer shows as a ref with one head on both, the reader's delta
 * figure says how the version reached it, a fork decided on read shows as a
 * conflict with its recent decision, the store and collector figures are
 * present, and the document serializes as JSON.
 */
class PdtPanelTest {

    private static final Codec<String> STR = Codec.of(String.class);
    private static final Duration DEADLINE = Duration.ofSeconds(20);

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final ObjectMapper json = new ObjectMapper();
    private final GroupId groupId = GroupId.of("zPdtPanel");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zPdtPanel", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();

    private record Peer(String name, PeerNode node, GroupRuntime runtime, Content content, NodeStore store, Pdt pdt) {
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.pdt().close();
            p.content().close();
            p.node().close();
        });
    }

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(runtime, "refs", identity, "refs")
                .clock(clock).settleWindow(Duration.ZERO).build();
        NodeStore store = new NodeStore(new MemoryBlockStore(), clock);
        Pdt pdt = Pdt.builder(refSpace).store(store).clock(clock).loadTimeout(DEADLINE)
                .collectEvery(Duration.ofSeconds(1)).build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(pdt.treeStore()).advertisements(peer -> Optional.ofNullable(ads.get(peer))).build();
        ads.put(identity.peerId(), content.describe(groupId));
        runtime.onTick(content::tick);
        content.start();
        pdt.attach(content).start();
        runtime.onTick(pdt::tick);
        Peer peer = new Peer(address, node, runtime, content, store, pdt);
        peers.add(peer);
        return peer;
    }

    private void tickAll() {
        for (Peer p : peers) {
            p.node().tick();
        }
        clock.advance(Duration.ofMillis(250));
    }

    private boolean runUntil(Duration max, BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            tickAll();
            Thread.sleep(2);
        }
        return true;
    }

    private JsonNode render(PdtPanel panel) throws IOException {
        return json.readTree(json.writeValueAsString(panel.data()));
    }

    @Test
    void thePanelShowsRefsHeadsDeltasConflictsStoreAndCollectorOnEveryPeer() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.runtime().membership().allMembers().size() == 1
                && b.runtime().membership().allMembers().size() == 1)).isTrue();
        PdtPanel panelA = PdtPanel.of(a.node(), () -> Map.of("fleet", a.pdt()));
        PdtPanel panelB = PdtPanel.of(b.node(), () -> Map.of("fleet", b.pdt()));
        assertThat(panelA.id()).isEqualTo("pdt");
        assertThat(panelA.title()).isEqualTo("Persistent trees");

        // before any ref: the groups appear with empty refs and the store figures
        JsonNode empty = render(panelA);
        assertThat(empty.get("peer").asText()).isEqualTo(a.node().peerId().value());
        assertThat(empty.get("groups")).hasSize(1);
        JsonNode group = empty.get("groups").get(0);
        assertThat(group.get("group").asText()).isEqualTo("fleet");
        assertThat(group.get("refs")).isEmpty();
        assertThat(group.get("store").get("nodesHeld").asInt()).isZero();
        assertThat(group.get("collector").get("phase").asText()).isEqualTo("IDLE");
        assertThat(group.get("collector").get("lastCollection")).isEmpty();

        Ref<PMap<String, String>> onA = a.pdt().mapRef("facts", STR, STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef("facts", STR, STR);
        panelA.dataFor("fleet");
        onA.onConflict(panelA.dataFor("fleet")::onConflict);
        onB.onConflict(panelB.dataFor("fleet")::onConflict);
        assertThatThrownBy(() -> panelA.dataFor("other")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no Pdt facade");

        PMap<String, String> v1 = onA.swap(m -> m.assoc("k", "one").assoc("j", "two"));
        assertThat(runUntil(Duration.ofSeconds(20), () -> onB.deref().get("k").isPresent())).isTrue();

        JsonNode docA = render(panelA).get("groups").get(0);
        assertThat(docA.get("refs")).hasSize(1);
        JsonNode facts = docA.get("refs").get(0);
        assertThat(facts.get("name").asText()).isEqualTo("facts");
        assertThat(facts.get("heads").asInt()).isEqualTo(1);
        assertThat(facts.get("versionsKnown").asInt()).isGreaterThanOrEqualTo(1);
        JsonNode head = facts.get("headList").get(0);
        assertThat(head.get("root").asText()).isEqualTo(v1.root());
        assertThat(head.get("seq").asLong()).isEqualTo(1);
        assertThat(head.get("size").asLong()).isEqualTo(2);
        assertThat(head.get("kind").asText()).isEqualTo("map");
        assertThat(head.get("issuer").asText()).contains(a.node().peerId().value());
        assertThat(head.get("delta").get("how").asText()).isEqualTo("inline");
        assertThat(head.get("delta").get("nodes").asInt()).isGreaterThanOrEqualTo(1);
        assertThat(docA.get("store").get("nodesHeld").asInt()).isGreaterThanOrEqualTo(1);
        assertThat(docA.get("store").get("bytesHeld").asLong()).isPositive();
        assertThat(docA.get("store").get("pinnedRoots").asInt()).isGreaterThanOrEqualTo(1);
        assertThat(docA.get("store").get("retainVersions").asInt()).isEqualTo(64);
        assertThat(docA.get("counters").get("refs").asInt()).isEqualTo(1);

        // the reader took the version inline (a small delta travels on the head) and counts it so
        JsonNode docB = render(panelB).get("groups").get(0);
        assertThat(docB.get("refs").get(0).get("headList").get(0).get("root").asText()).isEqualTo(v1.root());
        assertThat(docB.get("deltas").get("inline").asLong() + docB.get("deltas").get("fetched").asLong()).isGreaterThanOrEqualTo(1);
        assertThat(docB.get("conflicts").get("count").asLong()).isZero();

        // a fork: partitioned, both peers change the same key off v1; healed, each sees two heads
        network.partition("a", "b");
        onA.swap(m -> m.assoc("k", "from-a"));
        onB.swap(m -> m.assoc("k", "from-b"));
        tickAll();
        network.heal();
        assertThat(runUntil(Duration.ofSeconds(20), () -> onA.heads().heads().size() == 2 && onB.heads().heads().size() == 2)).isTrue();
        String merged = onA.deref().get("k").orElseThrow();
        assertThat(merged).isIn("from-a", "from-b");
        assertThat(onB.deref().get("k")).contains(merged);
        JsonNode forkA = render(panelA).get("groups").get(0);
        assertThat(forkA.get("refs").get(0).get("heads").asInt()).isEqualTo(2);
        assertThat(forkA.get("conflicts").get("count").asLong()).isGreaterThanOrEqualTo(1);
        JsonNode recent = forkA.get("conflicts").get("recent");
        assertThat(recent.size()).isGreaterThanOrEqualTo(1);
        assertThat(recent.get(0).get("ref").asText()).isEqualTo("facts");
        assertThat(recent.get(0).get("key").asText()).isEqualTo("k");
        assertThat(recent.get(0).get("reason").asText()).isEqualTo(PConflict.LATER_HLC);
        assertThat(recent.get(0).get("winnerRoot").asText()).startsWith("bafyrei");

        // a collection run to its end is the last collection; a tiny store finishes inside one tick
        PdtPanelData data = panelA.dataFor("fleet");
        assertThat(render(panelA).get("groups").get(0).get("collector").get("lastCollection")).isEmpty();
        data.recordCollection(a.pdt().collector().collect());
        JsonNode cycle = render(panelA).get("groups").get(0).get("collector").get("lastCollection");
        assertThat(cycle.get("marked").asInt()).isGreaterThanOrEqualTo(1);
        assertThat(cycle.get("nodesRemoved").asInt()).isGreaterThanOrEqualTo(0);
        assertThat(cycle.get("nodesHeld").asInt()).isGreaterThanOrEqualTo(1);
        assertThat(cycle.get("observedAt").asText()).isNotEmpty();
        assertThat(render(panelA).get("groups").get(0).get("collector").get("phase").asText()).isEqualTo("IDLE");

        // a recorded decision beyond the bound drops the oldest
        for (int i = 0; i < PdtPanelData.RECENT_CONFLICTS + 3; i++) {
            data.onConflict(new PConflict("facts", "k" + i, v1.root(), v1.root(), PConflict.WHOLE_HEAD));
        }
        JsonNode bounded = render(panelA).get("groups").get(0).get("conflicts").get("recent");
        assertThat(bounded).hasSize(PdtPanelData.RECENT_CONFLICTS);
        assertThat(bounded.get(0).get("key").asText()).isEqualTo("k" + (PdtPanelData.RECENT_CONFLICTS + 2));
    }
}
