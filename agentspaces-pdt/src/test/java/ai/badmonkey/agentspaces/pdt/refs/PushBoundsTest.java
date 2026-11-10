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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 5, P2-Q-SEC-03 (threat 3 of §6.7): pushed bundles cannot flood. A bundle outside its grammar or its
 * bound is dropped, counted, and never forwarded; a bundle is stored only after its nodes verify against the root
 * of a live head that announced the push, so nodes that no head names, or that the head's root does not reach, never
 * enter the store; a receiver takes at most its budget per stream; and a writer pushes within its own budget, the
 * rest pull-only.
 */
class PushBoundsTest {

    private static final String NAME = "pushed";
    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static RefFleet.Spec pushing(PushPublisher.Config config) {
        return RefFleet.Spec.defaults().pdt(b -> b.push(config));
    }

    private static PMap<String, String> grow(PMap<String, String> base, int bytes, long seed) {
        Random random = new Random(seed);
        PMap<String, String> out = base;
        for (int i = 0; i * 120 < bytes; i++) {
            out = out.assoc("k-" + seed + "-" + i, RefFleet.value(100, random));
        }
        return out;
    }

    private static String stream() {
        return PushReceiver.stream(RefFleet.REF_SPACE, NAME);
    }

    @Test
    void aBundleOutsideItsGrammarOrBoundIsDroppedCountedAndNeverForwarded() throws Exception {
        RefFleet.Peer mallory = fleet.newPeer("m", 300, pushing(PushPublisher.Config.on()));
        RefFleet.Peer follower = fleet.newPeer("f", 301, pushing(PushPublisher.Config.on()), "m");
        fleet.settle(10);
        follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        byte[] node = new byte[]{(byte) 0xa0};
        String root = Cids.of(node, Cids.DAG_CBOR);

        // a field the schema does not name, then a bundle whose part is past its parts, then one over 1 MiB
        Map<String, Object> extra = new java.util.LinkedHashMap<>();
        extra.put("v", 1);
        extra.put("root", root);
        extra.put("seq", 1L);
        extra.put("part", 0);
        extra.put("parts", 1);
        extra.put("nodes", List.of(node));
        extra.put("x", 1);
        mallory.runtime().gossip().publish(stream(), "bad-1", CODEC.toBytes(extra));
        mallory.runtime().gossip().publish(stream(), "bad-2", CODEC.toBytes(new PushBundle(1, root, 1, 2, 2, List.of(node))));
        byte[] huge = new byte[PushBundle.MAX_BYTES + 1];
        mallory.runtime().gossip().publish(stream(), "bad-3", CODEC.toBytes(new PushBundle(1, root, 1, 0, 1, List.of(huge))));
        fleet.settle(4);

        assertThat(follower.pdt().refs().pushStats().bundlesDropped()).isEqualTo(3);
        assertThat(follower.store().has(root)).isFalse();
        assertThat(follower.runtime().gossip().rumorsSentByStream()).as("the follower forwarded none of them")
                .doesNotContainKey(stream());
    }

    @Test
    void nodesNoLiveHeadNamesOrItsRootDoesNotReachNeverEnterTheStore() throws Exception {
        RefFleet.Peer writer = fleet.newPeer("w", 310, pushing(PushPublisher.Config.on()));
        RefFleet.Peer mallory = fleet.newPeer("m", 311, pushing(PushPublisher.Config.on()), "w");
        RefFleet.Peer follower = fleet.newPeer("f", 312, pushing(PushPublisher.Config.on()), "w");
        fleet.settle(10);
        follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);

        // well-formed bundles for a root that no head names: held aside, then dropped when the grace ends
        PMap<String, String> forged = mallory.pdt().mapRef("elsewhere", RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 60_000, 3));
        List<byte[]> nodes = mallory.store().reachable(forged.root()).stream().map(c -> mallory.store().get(c).orElseThrow()).toList();
        mallory.runtime().gossip().publish(stream(), "orphan", CODEC.toBytes(new PushBundle(1, forged.root(), 1, 0, 1, nodes)));
        fleet.settle(4);
        assertThat(follower.store().has(forged.root())).as("no head names the root").isFalse();
        fleet.clock.advance(PushPublisher.Config.on().grace().multipliedBy(PushReceiver.ORPHAN_GRACES + 1));
        fleet.settle(2);
        assertThat(follower.pdt().refs().pushStats().nodesDropped()).isEqualTo(nodes.size());

        // a genuine push, then foreign nodes under the same root's bundle: only what the head's root reaches is stored
        Ref<PMap<String, String>> ref = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> real = ref.swap(m -> grow(m, 60_000, 4));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> follower.store().complete(real.root()))).isTrue();
        mallory.runtime().gossip().publish(stream(), "stowaway", CODEC.toBytes(new PushBundle(1, real.root(), 2, 0, 1, nodes)));
        fleet.settle(4);
        for (byte[] foreign : nodes) {
            assertThat(follower.store().has(Cids.of(foreign, Cids.DAG_CBOR))).as("a stowaway node").isFalse();
        }
    }

    @Test
    void aReceiverTakesAtMostItsBudgetPerStreamAndAWriterPushesWithinItsOwn() throws Exception {
        // a budget of 64 KiB a second, up to 128 KiB at once, on both sides
        PushPublisher.Config tight = new PushPublisher.Config(true, 64 * 1024, 128 * 1024, Duration.ofSeconds(2));
        RefFleet.Peer writer = fleet.newPeer("w", 320, pushing(tight));
        RefFleet.Peer follower = fleet.newPeer("f", 321, pushing(tight), "w");
        fleet.settle(10);
        follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> ref = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> first = ref.swap(m -> grow(m, 100_000, 5));
        PMap<String, String> second = ref.swap(m -> grow(m, 100_000, 6));
        assertThat(writer.pdt().refs().pushStats().deltasPushed()).as("the first fits the writer's budget").isEqualTo(1);
        assertThat(writer.pdt().refs().pushStats().deltasPullOnly()).as("the second does not, yet").isEqualTo(1);
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> fleet.allHold(second.root()))).isTrue();
        assertThat(follower.store().complete(first.root())).isTrue();

        // a member that ignores its own budget meets the receiver's
        RefFleet.Peer flooder = fleet.newPeer("x", 322, pushing(PushPublisher.Config.on()), "w");
        fleet.settle(6);
        long before = follower.pdt().refs().pushStats().bundlesDropped();
        PMap<String, String> flood = flooder.pdt().mapRef("flood", RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 300_000, 7));
        List<byte[]> nodes = flooder.store().reachable(flood.root()).stream().map(c -> flooder.store().get(c).orElseThrow()).toList();
        for (int i = 0; i < 4; i++) {
            flooder.runtime().gossip().publish(stream(), "flood-" + i, CODEC.toBytes(new PushBundle(1, flood.root(), i, 0, 1, nodes)));
        }
        fleet.settle(2);
        assertThat(follower.pdt().refs().pushStats().bundlesDropped() - before).as("past 128 KiB at once").isGreaterThanOrEqualTo(3);
    }

    // ---------------------------------------------------------------- PDT 2 phase 5 adversary (P2-Q-SEC-03)

    /**
     * A bundle's bound counted node bytes alone, so a bundle of 150,000 four-byte nodes passed it at 600 KB while the
     * follower held each node under a 64-character key in a map entry: about 160 bytes of heap a node, some 25 MB for
     * one bundle and well past a gigabyte for one stream's budget. A root now holds at most {@code maxNodes} nodes.
     */
    @Test
    void aBundleOfTinyNodesPastTheNodeBoundIsDropped() throws Exception {
        RefFleet.Peer mallory = fleet.newPeer("m", 350, pushing(PushPublisher.Config.on()));
        RefFleet.Peer follower = fleet.newPeer("f", 351, pushing(PushPublisher.Config.on()), "m");
        fleet.settle(10);
        follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        List<byte[]> tiny = new ArrayList<>();
        for (int i = 0; i < 150_000; i++) {
            tiny.add(ByteBuffer.allocate(4).putInt(i).array());
        }
        String root = Cids.of(tiny.get(0), Cids.DAG_CBOR);
        mallory.runtime().gossip().publish(stream(), "tiny", CODEC.toBytes(new PushBundle(1, root, 1, 0, 1, tiny)));
        fleet.settle(2);

        PushPublisher.Stats stats = follower.pdt().refs().pushStats();
        assertThat(stats.bundlesReceived()).as("nothing held").isZero();
        assertThat(stats.bundlesDropped()).isEqualTo(1);
    }

    /**
     * A follower walked the whole stored part of an announced root on every bundle under it, and on after the root
     * completed, so any member could send a stream of tiny bundles naming the root and make each follower decode the
     * delta once per bundle. A bundle now resumes the walk only at the links the push still lacks.
     */
    @Test
    void bundlesThatAddNothingToAnAnnouncedRootCostTheFollowerNoWalk() throws Exception {
        Probe probe = probe(360);
        probe.receiver.expect(probe.stream, probe.root, 1);
        probe.receiver.onBundle(probe.stream, probe.from, probe.bundle(probe.root, probe.allBut(probe.late)));
        assertThat(probe.store.complete(probe.root)).isFalse();

        probe.reads.set(0);
        for (int i = 0; i < 200; i++) {
            probe.receiver.onBundle(probe.stream, probe.from, probe.bundle(probe.root, List.of(junk("walk-" + i))));
        }
        assertThat(probe.reads.get()).as("junk under an announced root walks nothing").isZero();

        probe.receiver.onBundle(probe.stream, probe.from, probe.bundle(probe.root, List.of(probe.node(probe.late))));
        assertThat(probe.store.complete(probe.root)).as("the late node completes the root").isTrue();
        probe.reads.set(0);
        for (int i = 0; i < 50; i++) {
            probe.receiver.onBundle(probe.stream, probe.from, probe.bundle(probe.root, List.of(junk("after-" + i))));
        }
        assertThat(probe.reads.get()).as("a complete root takes no more walking").isZero();
    }

    /** Bundles of roots no head announced pushed out an announced root's nodes, so four tiny bundles undid a push. */
    @Test
    void rootsNoHeadAnnouncedNeverDisplaceAnAnnouncedOne() throws Exception {
        Probe probe = probe(370);
        probe.receiver.expect(probe.stream, probe.root, 1);
        probe.receiver.onBundle(probe.stream, probe.from, probe.bundle(probe.root, probe.allBut(probe.late)));
        for (int i = 0; i <= PushReceiver.PENDING_ROOTS; i++) {
            byte[] node = junk("decoy-" + i);
            probe.receiver.onBundle(probe.stream, probe.from, probe.bundle(Cids.of(node, Cids.DAG_CBOR), List.of(node)));
        }
        probe.receiver.onBundle(probe.stream, probe.from, probe.bundle(probe.root, List.of(probe.node(probe.late))));
        assertThat(probe.store.complete(probe.root)).as("the announced root kept its nodes").isTrue();
    }

    /**
     * A follower's forward filter read only the grammar, so a bundle it dropped past its stream budget still went on
     * to its peers: the budget bounded what the follower kept and not the flood it relayed (threat 3 of §6.7).
     */
    @Test
    void aBundlePastTheFollowersBudgetIsDroppedAndNeverForwarded() throws Exception {
        PushPublisher.Config tight = new PushPublisher.Config(true, 64 * 1024, 128 * 1024, Duration.ofSeconds(2));
        RefFleet.Peer flooder = fleet.newPeer("x", 380, pushing(PushPublisher.Config.on()));
        RefFleet.Peer follower = fleet.newPeer("f", 381, pushing(tight), "x");
        fleet.newPeer("b", 382, pushing(PushPublisher.Config.on()), "x");
        fleet.settle(10);
        follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> flood = flooder.pdt().mapRef("flood", RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 80_000, 11));
        List<byte[]> nodes = flooder.store().reachable(flood.root()).stream().map(c -> flooder.store().get(c).orElseThrow()).toList();
        for (int i = 0; i < 4; i++) {
            flooder.runtime().gossip().publish(stream(), "flood-" + i, CODEC.toBytes(new PushBundle(1, flood.root(), i, 0, 1, nodes)));
        }
        fleet.settle(2);

        PushPublisher.Stats stats = follower.pdt().refs().pushStats();
        assertThat(stats.bundlesDropped()).as("past 128 KiB at once").isGreaterThanOrEqualTo(3);
        assertThat(follower.runtime().gossip().rumorsSentByStream().getOrDefault(stream(), 0L))
                .as("the follower forwarded only the bundle it took").isLessThanOrEqualTo(stats.bundlesReceived());
    }

    private static byte[] junk(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** A receiver driven by hand over a store that counts its reads, and a delta of a writer's map to push into it. */
    private Probe probe(long seed) throws Exception {
        RefFleet.Peer writer = fleet.newPeer("w", seed);
        RefFleet.Peer host = fleet.newPeer("h", seed + 1, "w");
        fleet.settle(10);
        PMap<String, String> value = writer.pdt().mapRef("source", RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 60_000, seed));
        List<String> cids = new ArrayList<>(writer.store().reachable(value.root()));
        String late = cids.stream().filter(c -> !c.equals(value.root())).findFirst().orElseThrow();
        AtomicLong reads = new AtomicLong();
        NodeStore store = new NodeStore(new MemoryBlockStore(), fleet.clock) {
            @Override
            public Optional<byte[]> get(String cid) {
                reads.incrementAndGet();
                return super.get(cid);
            }
        };
        PushReceiver receiver = new PushReceiver(store, host.content(), fleet.clock, PushPublisher.Config.on());
        String stream = PushReceiver.stream(RefFleet.REF_SPACE, "probe");
        receiver.follow(stream);
        return new Probe(writer, receiver, store, reads, stream, writer.id(), value.root(), cids, late);
    }

    private record Probe(RefFleet.Peer writer, PushReceiver receiver, NodeStore store, AtomicLong reads, String stream,
                         PeerId from, String root, List<String> cids, String late) {
        byte[] node(String cid) {
            return writer.store().get(cid).orElseThrow();
        }

        List<byte[]> allBut(String left) {
            return cids.stream().filter(c -> !c.equals(left)).map(this::node).toList();
        }

        byte[] bundle(String root, List<byte[]> nodes) {
            return CODEC.toBytes(new PushBundle(1, root, 1, 0, 1, nodes));
        }
    }
}
