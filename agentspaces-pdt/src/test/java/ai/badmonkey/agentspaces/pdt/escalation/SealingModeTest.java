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
package ai.badmonkey.agentspaces.pdt.escalation;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.content.SpacePayloads;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The security row of the escalation (srad/agentspaces-pdt.md PD-Q-SEC-05, 6.7 row
 * 8): the unsealed escalation of PD-E-04 gives no keyless member a node in
 * {@code group-key} mode. A sealed group of three members on the sim fleet of
 * {@code PdtSwarmTest}: the writer and one reader hold the content key on their
 * spaces and on their swarms; the third member has no key at all but is admitted,
 * replicates the records, and opts its space into the trees. It never obtains a
 * node: its tree requests are rejected (reason 3, no sealing in common), its
 * payload fetch never completes, its store holds nothing under the root, and the
 * entry stays invisible to it; the keyed reader decodes the entry as usual.
 */
class SealingModeTest {

    private static final long THRESHOLD = 64 * 1024;
    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    public record Doc(String id, Map<String, String> fields, int revision) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zPdtSeal");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zPdtSeal", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();
    private final GroupKey groupKey = GroupKey.generate();

    private record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, BlockExchange blocks,
                        Content content, NodeStore store, Pdt pdt, Map<String, ReplicatedSpace> spaces) {
        PeerId id() {
            return node.peerId();
        }
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.spaces().values().forEach(ReplicatedSpace::close);
            p.pdt().close();
            p.content().close();
            p.node().close();
        });
    }

    /** A member; with a key, the ring goes on the swarm and on the opted-in space (null for a keyless member). */
    private Peer newPeer(String address, long seed, GroupKey key, String... seedAddresses) throws IOException {
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
        Pdt pdt = Pdt.builder(refSpace).store(store).clock(clock).loadTimeout(Duration.ofSeconds(2))
                .collectEvery(Duration.ZERO).build();
        Content.Builder content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(pdt.treeStore()).advertisements(peer -> Optional.ofNullable(ads.get(peer)));
        if (key != null) {
            content.keyRing(GroupKeyRing.of(key));
        }
        Content built = content.build();
        ads.put(identity.peerId(), built.describe(groupId));
        runtime.onTick(built::tick);
        built.start();
        pdt.attach(built).start();
        runtime.onTick(pdt::tick);
        ReplicatedSpace.Builder docs = ReplicatedSpace.builder(runtime, "docs", identity, "app")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks)
                .bulk(pdt.payloads(), THRESHOLD).sealBeforeEscalate(false)
                .bulkFallback(new SpacePayloads(built));
        if (key != null) {
            docs.keyRing(GroupKeyRing.of(key));
        }
        Map<String, ReplicatedSpace> spaces = new HashMap<>();
        spaces.put("content", contentSpace);
        spaces.put("refs", refSpace);
        spaces.put("docs", docs.build());
        Peer peer = new Peer(address, identity, node, runtime, blocks, built, store, pdt, spaces);
        peers.add(peer);
        return peer;
    }

    private void tickAll() {
        for (Peer p : peers) {
            p.node().tick();
        }
        clock.advance(Duration.ofMillis(250));
    }

    private void settle(int ticks) {
        for (int i = 0; i < ticks; i++) {
            tickAll();
        }
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

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static Map<String, String> fields(int n, long seed) {
        Random random = new Random(seed);
        Map<String, String> fields = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            char[] chars = new char[80];
            for (int c = 0; c < chars.length; c++) {
                chars[c] = (char) ('a' + random.nextInt(26));
            }
            fields.put("field-" + i, new String(chars));
        }
        return fields;
    }

    @Test
    void aKeylessMemberOfASealedGroupObtainsNoNodeOfAnEscalatedEntry() throws Exception {
        Peer a = newPeer("a", 1, groupKey);
        Peer b = newPeer("b", 2, groupKey, "a");
        Peer k = newPeer("k", 3, null, "a");
        ReplicatedSpace docsA = a.spaces().get("docs");
        ReplicatedSpace docsB = b.spaces().get("docs");
        ReplicatedSpace docsK = k.spaces().get("docs");
        settle(8);
        assertThat(a.runtime().membership().allMembers()).hasSize(2);
        assertThat(a.content().advertisesTrees(k.id())).as("the keyless member advertises the trees").isTrue();
        assertThat(a.pdt().payloads().unable(List.of(b.id(), k.id()))).as("so the writer takes the tree seam").isEmpty();

        Doc doc = new Doc("doc-1", fields(3_000, 9), 1);
        EntryHandle handle = docsA.write(doc, HOUR);
        EntryRecord record = docsA.signedState(handle.entryId()).orElseThrow().record();
        String root = record.payloadRef();
        assertThat(Cids.parse(root).codec()).isEqualTo(Cids.DAG_CBOR);
        assertThat(record.payload()).isNull();
        assertThat(record.sealedEpoch()).as("the record names the epoch (0, the configured key)").isEqualTo(0L);

        // the keyed reader decodes the entry as usual, and the record reaches the keyless member too
        assertThat(runUntil(Duration.ofSeconds(20), () -> docsB.read(Template.of(Doc.class)).isPresent())).isTrue();
        assertThat(docsB.read(Template.of(Doc.class)).orElseThrow()).isEqualTo(doc);
        assertThat(runUntil(Duration.ofSeconds(10), () -> docsK.signedState(handle.entryId()).isPresent())).isTrue();

        // the keyless member asks; a keyed holder answers only sealed NODES (PD-P-03), which k cannot
        // open, so past its deadline and a retry it holds nothing and no plaintext node crossed to it
        List<byte[]> toK = new java.util.concurrent.CopyOnWriteArrayList<>();
        network.tap("a", "k", toK::add);
        network.tap("b", "k", toK::add);
        byte[] rootNode = a.store().get(root).orElseThrow();
        byte[] tail = java.util.Arrays.copyOfRange(rootNode, rootNode.length - 48, rootNode.length);
        settle(60);
        network.untap("a", "k");
        network.untap("b", "k");
        assertThat(toK).as("frames reached k").isNotEmpty();
        for (byte[] frame : toK) {
            assertThat(indexOf(frame, tail)).as("no plaintext node crosses the wire to k").isEqualTo(-1);
        }
        assertThat(docsK.read(Template.of(Doc.class))).as("invisible to the keyless member").isEmpty();
        assertThat(k.pdt().payloads().stats().fetched()).as(k.pdt().payloads().toString()).isZero();
        assertThat(k.store().has(root)).as("no root node").isFalse();
        assertThat(k.store().size()).as("no node at all under the root").isZero();
        assertThat(k.content().stats().nodesReceived()).as("no node opened at k: " + k.content().stats()).isZero();

        // the keyless member is a holder of nothing, so the keyed reader's second fetch still comes from a or b
        Map<String, String> changed = new TreeMap<>(doc.fields());
        changed.put("field-7", "changed");
        docsA.write(new Doc("doc-2", changed, 2), HOUR);
        assertThat(runUntil(Duration.ofSeconds(20), () -> docsB.readAll(Template.of(Doc.class), 4).size() == 2)).isTrue();
        assertThat(k.content().stats().nodesServed()).isZero();
        assertThat(k.content().stats().nodesReceived()).isZero();
    }
}
