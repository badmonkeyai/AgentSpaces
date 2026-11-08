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

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.reduce.Reductions;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.BulkPayloads;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.DataSpaces;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Escalation in a space over the persistent trees, end to end on the sim fleet of
 * {@code PdtSwarmTest} (srad/agentspaces-pdt.md §3.7, §6.4 S6, PD-E-01, E-04, E-05,
 * E-06, E-07, E-08, PD-Q-AVL-01): each peer carries a {@link Pdt} facade whose
 * {@link PdtPayloads} an application space opts into with
 * {@code bulk(pdt.payloads(), 64 KiB).sealBeforeEscalate(false)}.
 */
class PdtPayloadsSpaceTest {

    private static final long THRESHOLD = 64 * 1024;
    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    /** A record as the examples declare them: a large map field among small ones. */
    public record Doc(String id, Map<String, String> fields, int revision) {
    }

    public record Payment(String account, long cents) {
    }

    /** A reduce accumulator of about 200 KiB: the ledger grows by one line per step. */
    public record Balance(String account, Map<String, String> ledger, long cents, int payments) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zPdtEsc");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zPdtEsc", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();
    private final Set<String> dead = new HashSet<>();
    private final GroupKey groupKey = GroupKey.generate();

    private record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, BlockExchange blocks,
                        Content content, NodeStore store, Pdt pdt, Map<String, ReplicatedSpace> spaces) {
        PeerId id() {
            return node.peerId();
        }

        ReplicatedSpace space(String name) {
            return spaces.get(name);
        }
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            if (!dead.contains(p.name())) {
                p.spaces().values().forEach(ReplicatedSpace::close);
                p.pdt().close();
                p.content().close();
                p.node().close();
            }
        });
    }

    private Peer newPeer(String address, long seed, boolean trees, String... seedAddresses) throws IOException {
        return newPeer(address, seed, trees, DEADLINE, seedAddresses);
    }

    /** As {@link #newPeer(String, long, boolean, String...)} with the facade's load timeout, which bounds a tree fetch. */
    private Peer newPeer(String address, long seed, boolean trees, Duration deadline, String... seedAddresses) throws IOException {
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
        Pdt pdt = Pdt.builder(refSpace).store(store).clock(clock).loadTimeout(deadline).collectEvery(Duration.ZERO).build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(pdt.treeStore()).advertisements(peer -> Optional.ofNullable(ads.get(peer))).build();
        CapabilityAdvertisement ad = content.describe(groupId);
        if (!trees) {
            // a 0.5.0 member: the same advertisement without the trees parameter (PD-E-06)
            Map<String, String> parameters = new HashMap<>(ad.parameters());
            parameters.remove(Content.TREES_PARAMETER);
            ad = new CapabilityAdvertisement(ad.id(), ad.issuer(), ad.group(), ad.issued(), ad.ttl(), ad.capabilityType(),
                    ad.version(), ad.binding(), parameters, ad.costHints());
        }
        ads.put(identity.peerId(), ad);
        runtime.onTick(content::tick);
        content.start();
        pdt.attach(content).start();
        runtime.onTick(pdt::tick);
        Map<String, ReplicatedSpace> spaces = new HashMap<>();
        spaces.put("content", contentSpace);
        spaces.put("refs", refSpace);
        Peer peer = new Peer(address, identity, node, runtime, blocks, content, store, pdt, spaces);
        peers.add(peer);
        return peer;
    }

    /** An application space opted into the trees (PD-E-01, PD-E-04), with the flat swarm as the fallback (PD-E-06). */
    private ReplicatedSpace optedIn(Peer peer, String name, boolean sealed, Consumer<ReplicatedSpace.Builder> more) {
        ReplicatedSpace.Builder builder = ReplicatedSpace.builder(peer.runtime(), name, peer.identity(), "app")
                .clock(clock).settleWindow(Duration.ZERO).blocks(peer.blocks())
                .bulk(peer.pdt().payloads(), THRESHOLD).sealBeforeEscalate(false)
                .bulkFallback(new SpacePayloads(peer.content()));
        if (sealed) {
            builder.keyRing(GroupKeyRing.of(groupKey));
        }
        more.accept(builder);
        ReplicatedSpace space = builder.build();
        peer.spaces().put(name, space);
        return space;
    }

    private void tickAll() {
        for (Peer p : peers) {
            if (!dead.contains(p.name())) {
                p.node().tick();
            }
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

    private static String refOf(ReplicatedSpace space, String id) {
        for (Space.Entry<Doc> entry : space.readAllEntries(Template.of(Doc.class), 16)) {
            if (entry.value().id().equals(id)) {
                return recordOf(space, entry).payloadRef();
            }
        }
        return null;
    }

    /** The signed record behind an entry, as the replica keeps it for anti-entropy. */
    private static EntryRecord recordOf(ReplicatedSpace space, Space.Entry<?> entry) {
        return space.signedState(entry.entryId()).orElseThrow().record();
    }

    // ------------------------------------------------------------ PD-E-01, E-04, E-05

    @Test
    void aLargeEntryEscalatesToATreeAndASecondWriteShipsOnlyItsDelta() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        ReplicatedSpace docsA = optedIn(a, "docs", true, builder -> { });
        ReplicatedSpace docsB = optedIn(b, "docs", true, builder -> { });
        settle(8);
        assertThat(a.runtime().membership().allMembers()).hasSize(1);
        assertThat(a.content().advertisesTrees(b.id())).isTrue();

        Map<String, String> fields = fields(3_400, 1);
        Doc v1 = new Doc("doc-1", fields, 1);
        int payloadBytes = codec.toBytes(v1).length;
        assertThat(payloadBytes).isGreaterThan(300 * 1024);
        EntryHandle h1 = docsA.write(v1, HOUR);
        PdtPayloads.Stats aAfterV1 = a.pdt().payloads().stats();
        assertThat(aAfterV1.published()).isEqualTo(1);
        assertThat(aAfterV1.pinned()).isEqualTo(1);
        EntryRecord record1 = recordOf(docsA, docsA.readAllEntries(Template.of(Doc.class), 4).get(0));
        String ref1 = record1.payloadRef();
        assertThat(Cids.parse(ref1).codec()).as("the payloadRef is a dag-cbor root (PD-E-02)").isEqualTo(Cids.DAG_CBOR);
        assertThat(record1.payload()).isNull();
        assertThat(record1.sealedEpoch()).as("the record names the epoch, the tree is plaintext (PD-E-04)").isEqualTo(0L);
        assertThat(a.store().complete(ref1)).isTrue();

        assertThat(runUntil(Duration.ofSeconds(20), () -> docsB.read(Template.of(Doc.class)).isPresent()
                && b.pdt().payloads().stats().fetched() == 1))
                .as("b: " + b.pdt().payloads() + " content " + b.content().stats()).isTrue();
        Doc onB = docsB.read(Template.of(Doc.class)).orElseThrow();
        assertThat(onB).isEqualTo(v1);
        PdtPayloads.Stats bAfterV1 = b.pdt().payloads().stats();
        System.out.printf("v1: a wrote %d bytes in store of %d bytes; b received %d bytes, b store %d bytes; a content %s; b content %s%n",
                aAfterV1.bytesWritten(), a.store().bytesUsed(), bAfterV1.bytesReceived(), b.store().bytesUsed(),
                a.content().stats(), b.content().stats());
        assertThat(bAfterV1.fetched()).as(bAfterV1.toString()).isEqualTo(1);
        assertThat(bAfterV1.bytesReceived()).as("the first version crosses whole").isGreaterThan(aAfterV1.bytesWritten() / 2);
        assertThat(bAfterV1.pinned()).as("the reader pins the root while the entry lives (PD-E-05)").isEqualTo(1);

        Map<String, String> changed = new TreeMap<>(fields);
        changed.put("field-1500", "changed");
        docsA.write(new Doc("doc-2", changed, 2), HOUR);
        PdtPayloads.Stats aAfterV2 = a.pdt().payloads().stats();
        long delta = aAfterV2.bytesWritten() - aAfterV1.bytesWritten();
        assertThat(runUntil(Duration.ofSeconds(20), () -> docsB.readAll(Template.of(Doc.class), 4).size() == 2
                && b.pdt().payloads().stats().fetched() == 2))
                .as("b: " + b.pdt().payloads()).isTrue();
        PdtPayloads.Stats bAfterV2 = b.pdt().payloads().stats();
        long received = bAfterV2.bytesReceived() - bAfterV1.bytesReceived();
        System.out.printf("escalation: v1 %d payload bytes, tree %d bytes; one field changed: delta %d bytes written, %d received%n",
                payloadBytes, aAfterV1.bytesWritten(), delta, received);
        assertThat(received).as("the second version moves as its delta (PD-E-01)").isLessThan(16 * 1024)
                .isLessThanOrEqualTo(delta + delta / 2);
        assertThat(docsB.readAll(Template.of(Doc.class), 4)).extracting(Doc::id).containsExactlyInAnyOrder("doc-1", "doc-2");

        // PD-E-05: a live entry pins its root on both sides; a cancelled or lapsed entry releases it once collected
        String ref2 = refOf(docsA, "doc-2");
        assertThat(a.pdt().payloads().pinned()).containsExactlyInAnyOrder(ref1, ref2);
        assertThat(b.pdt().payloads().pinned()).containsExactlyInAnyOrder(ref1, ref2);
        h1.cancel();
        clock.advance(Duration.ofHours(3)); // past the lease and the tombstone horizon of both entries
        settle(4);
        docsA.sweepNow();
        docsB.sweepNow();
        assertThat(a.pdt().payloads().pinned()).as("the writer released both roots").isEmpty();
        assertThat(b.pdt().payloads().pinned()).as("the reader released both roots").isEmpty();
        assertThat(a.store().complete(ref2)).as("the nodes stay until the collector runs (retention)").isTrue();
    }

    // ------------------------------------------------------------ PD-E-06

    @Test
    void aMemberWithoutTheTreeExchangeSendsTheWriterToTheFallbackAndARequiredSpaceRefuses() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        Peer c = newPeer("c", 3, false, "a");
        AtomicInteger fallbackPublishes = new AtomicInteger();
        SpacePayloads flat = new SpacePayloads(a.content());
        BulkPayloads counting = new BulkPayloads() {
            @Override
            public String publish(byte[] payload, Lease lease) {
                fallbackPublishes.incrementAndGet();
                return flat.publish(payload, lease);
            }

            @Override
            public Optional<byte[]> local(String ref) {
                return flat.local(ref);
            }

            @Override
            public void ensure(String ref, PeerId issuer, Runnable onArrival) {
                flat.ensure(ref, issuer, onArrival);
            }

            @Override
            public void release(String ref) {
                flat.release(ref);
            }
        };
        ReplicatedSpace docsA = optedIn(a, "docs", false, builder -> builder.bulkFallback(counting));
        ReplicatedSpace docsB = optedIn(b, "docs", false, builder -> { });
        ReplicatedSpace docsC = optedIn(c, "docs", false, builder -> { });
        ReplicatedSpace strictA = optedIn(a, "strict", false, builder -> builder.bulkRequired(true));
        settle(8);
        assertThat(a.runtime().membership().allMembers()).hasSize(2);
        assertThat(a.content().advertisesTrees(c.id())).isFalse();
        assertThat(a.pdt().payloads().unable(List.of(b.id(), c.id()))).containsExactly(c.id());

        Doc big = new Doc("doc-1", fields(3_000, 2), 1);
        docsA.write(big, HOUR);
        assertThat(fallbackPublishes.get()).as("the flat seam carried the write").isEqualTo(1);
        assertThat(a.pdt().payloads().stats().published()).isZero();
        String ref = recordOf(docsA, docsA.readAllEntries(Template.of(Doc.class), 4).get(0)).payloadRef();
        assertThat(Cids.parse(ref).codec()).as("a flat artifact root").isEqualTo(Cids.RAW);
        assertThat(runUntil(Duration.ofSeconds(20), () -> docsB.read(Template.of(Doc.class)).isPresent()
                && docsC.read(Template.of(Doc.class)).isPresent())).isTrue();
        assertThat(docsC.read(Template.of(Doc.class)).orElseThrow()).isEqualTo(big);

        assertThatThrownBy(() -> strictA.write(big, HOUR)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("strict").hasMessageContaining("agentspaces.pdt.spaces.strict.require")
                .hasMessageContaining(c.id().display());
        // a small entry is never escalated, so the strict space still takes it
        strictA.write(new Doc("small", Map.of("k", "v"), 1), HOUR);
        assertThat(strictA.readAll(Template.of(Doc.class), 4)).hasSize(1);
    }

    // ------------------------------------------------------------ PD-E-07

    @Test
    void aDataSpaceOfTheConnectorSdkRidesTheTrees() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        ReplicatedSpace dataA = DataSpaces.builder(a.runtime(), "data", a.identity(), "connector", a.blocks(),
                a.pdt().payloads(), THRESHOLD).sealBeforeEscalate(false).clock(clock).settleWindow(Duration.ZERO).build();
        ReplicatedSpace dataB = DataSpaces.builder(b.runtime(), "data", b.identity(), "connector", b.blocks(),
                b.pdt().payloads(), THRESHOLD).sealBeforeEscalate(false).clock(clock).settleWindow(Duration.ZERO).build();
        a.spaces().put("data", dataA);
        b.spaces().put("data", dataB);
        settle(8);
        Doc result = new Doc("rows", fields(2_000, 3), 1);
        dataA.write(result, HOUR);
        String ref = recordOf(dataA, dataA.readAllEntries(Template.of(Doc.class), 4).get(0)).payloadRef();
        assertThat(Cids.parse(ref).codec()).as("a bulk result is a tree root (PD-E-07)").isEqualTo(Cids.DAG_CBOR);
        assertThat(runUntil(Duration.ofSeconds(20), () -> dataB.read(Template.of(Doc.class)).isPresent()
                && b.pdt().payloads().stats().fetched() == 1)).isTrue();
        assertThat(dataB.read(Template.of(Doc.class)).orElseThrow()).isEqualTo(result);
        assertThat(b.pdt().payloads().stats().fetched()).as(b.pdt().payloads().toString()).isEqualTo(1);
    }

    // ------------------------------------------------------------ PD-E-08

    @AgentSpec(name = "teller", description = "Keeps a ledger per account", goals = {"balance"})
    public static class Teller {
        @SpaceReduce(space = "ledger", key = "account", lease = "5s", pollTimeout = "50ms", accumulatorLease = "1h")
        public Balance apply(Balance balance, Payment payment) {
            Map<String, String> ledger = balance == null ? fields(2_200, 4) : new TreeMap<>(balance.ledger());
            int count = (balance == null ? 0 : balance.payments()) + 1;
            ledger.put("line-" + count, payment.cents() + " cents");
            return new Balance(payment.account(), ledger, (balance == null ? 0 : balance.cents()) + payment.cents(), count);
        }
    }

    @Test
    void aSpaceReduceAccumulatorMovesByDeltaPerStep() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        ReplicatedSpace ledgerA = optedIn(a, "ledger", false, builder -> { });
        ReplicatedSpace ledgerB = optedIn(b, "ledger", false, builder -> { });
        settle(8);
        AgentBinder binder = new AgentBinder(a.identity(), groupId, null, clock).space("ledger", ledgerA);
        try {
            binder.bind(new Teller());
            int steps = 50;
            long aWritten = 0;
            long bReceived = 0;
            long worstRatioPermille = 0;
            List<String> rows = new ArrayList<>();
            for (int i = 1; i <= steps; i++) {
                final int step = i;
                ledgerA.write(new Payment("acct", i), HOUR);
                // one step at a time: b has seen the version and fetched its tree before the next payment
                assertThat(runUntil(Duration.ofSeconds(20), () -> Reductions.current(ledgerB, Balance.class, "teller.apply", "acct")
                        .map(e -> e.value().payments() == step).orElse(false)
                        && b.pdt().payloads().stats().fetched() == step))
                        .as("step " + step + ": a " + a.pdt().payloads() + " b " + b.pdt().payloads() + " " + rows).isTrue();
                PdtPayloads.Stats aStats = a.pdt().payloads().stats();
                PdtPayloads.Stats bStats = b.pdt().payloads().stats();
                long wrote = aStats.bytesWritten() - aWritten;
                long got = bStats.bytesReceived() - bReceived;
                rows.add(step + ":" + wrote + "/" + got);
                if (step > 1) {
                    worstRatioPermille = Math.max(worstRatioPermille, got * 1000 / Math.max(1, wrote));
                    assertThat(got).as("step " + step + " (PD-E-08): a wrote " + wrote + " bytes; " + rows)
                            .isLessThanOrEqualTo(wrote + wrote / 2);
                }
                aWritten = aStats.bytesWritten();
                bReceived = bStats.bytesReceived();
            }
            Balance last = Reductions.current(ledgerB, Balance.class, "teller.apply", "acct").orElseThrow().value();
            assertThat(last.cents()).isEqualTo(steps * (steps + 1L) / 2);
            assertThat(codec.toBytes(last).length).isGreaterThan(190 * 1024);
            PdtPayloads.Stats aStats = a.pdt().payloads().stats();
            PdtPayloads.Stats bStats = b.pdt().payloads().stats();
            System.out.printf("reduce: %d steps, a published %d trees and wrote %d bytes, b fetched %d and received %d bytes,"
                    + " worst step ratio %d permille; per step wrote/received %s%n",
                    steps, aStats.published(), aStats.bytesWritten(), bStats.fetched(), bStats.bytesReceived(), worstRatioPermille, rows);
            assertThat(bStats.bytesReceived()).as("over the run b received at most 1.5x what a wrote")
                    .isLessThanOrEqualTo(aStats.bytesWritten() + aStats.bytesWritten() / 2);
        } finally {
            binder.close();
        }
    }

    // ------------------------------------------------------------ PD-Q-AVL-01

    /** The WRITTEN events a subscriber saw, by entry id. */
    private static List<String> written(ReplicatedSpace space) {
        List<String> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        space.notify(Template.of(Doc.class), event -> {
            if (event.kind() == ai.badmonkey.agentspaces.api.space.SpaceEvent.Kind.WRITTEN) {
                seen.add(event.entry().id());
            }
        }, HOUR);
        return seen;
    }

    /** Ticks with the a to b link held until b holds the record of the entry, releasing one held frame at a time. */
    private boolean recordReaches(ReplicatedSpace docsB, EntryHandle handle) {
        for (int i = 0; i < 60 && docsB.signedState(handle.entryId()).isEmpty(); i++) {
            tickAll();
            while (docsB.signedState(handle.entryId()).isEmpty() && network.releaseOne("a", "b")) {
                // one frame at a time, so the rumor lands and the tree's NODES stay held
            }
        }
        return docsB.signedState(handle.entryId()).isPresent();
    }

    @Test
    void theWrittenEventIsDeferredUntilTheTreeArrives() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        ReplicatedSpace docsA = optedIn(a, "docs", false, builder -> { });
        ReplicatedSpace docsB = optedIn(b, "docs", false, builder -> { });
        settle(8);
        List<String> seenAtB = written(docsB);

        network.hold("a", "b");
        EntryHandle handle = docsA.write(new Doc("doc-1", fields(3_000, 5), 1), HOUR);
        assertThat(recordReaches(docsB, handle)).as("b holds the record while the tree is held back").isTrue();
        settle(8);
        assertThat(docsB.read(Template.of(Doc.class))).as("invisible until the tree arrives (PD-Q-AVL-01)").isEmpty();
        assertThat(seenAtB).as("no WRITTEN before the tree arrives").isEmpty();
        assertThat(b.content().stats().treeFetchesActive()).as("the fetch is under way: " + b.content().stats()).isEqualTo(1);

        network.release("a", "b", SimNetwork.Order.FIFO, 0);
        assertThat(runUntil(Duration.ofSeconds(20), () -> docsB.read(Template.of(Doc.class)).isPresent())).isTrue();
        // the event follows the fetch's completion callback, a tick or so after the nodes are readable
        assertThat(runUntil(Duration.ofSeconds(10), () -> !seenAtB.isEmpty()))
                .as("the deferred WRITTEN: " + b.pdt().payloads() + " " + b.content().stats()).isTrue();
        settle(4);
        assertThat(seenAtB).as("exactly one WRITTEN once the tree is complete").containsExactly("doc-1");
        assertThat(b.pdt().payloads().stats().fetched()).isEqualTo(1);
    }

    @Test
    void aTreeFetchThatGivesUpIsRetriedUntilTheHolderIsReachable() throws Exception {
        Peer a = newPeer("a", 1, true, Duration.ofSeconds(1));
        Peer b = newPeer("b", 2, true, Duration.ofSeconds(1), "a");
        ReplicatedSpace docsA = optedIn(a, "docs", false, builder -> { });
        ReplicatedSpace docsB = optedIn(b, "docs", false, builder -> { });
        settle(8);
        List<String> seenAtB = written(docsB);

        network.hold("a", "b");
        EntryHandle handle = docsA.write(new Doc("doc-1", fields(3_000, 6), 1), HOUR);
        assertThat(recordReaches(docsB, handle)).isTrue();
        // past the one-second deadline: the fetch gives up, the seam reports it, and the space schedules a retry
        assertThat(runUntil(Duration.ofSeconds(20), () -> b.pdt().payloads().stats().failed() >= 1))
                .as("b: " + b.pdt().payloads() + " " + b.content().stats()).isTrue();
        assertThat(docsB.bulkRetriesPending()).as("the failed fetch waits for its next attempt").isEqualTo(1);
        assertThat(docsB.read(Template.of(Doc.class))).isEmpty();
        assertThat(seenAtB).isEmpty();

        network.release("a", "b", SimNetwork.Order.FIFO, 0);
        assertThat(runUntil(Duration.ofSeconds(30), () -> docsB.read(Template.of(Doc.class)).isPresent()))
                .as("the retry lands once the holder is reachable: " + b.pdt().payloads() + " retries "
                        + docsB.bulkRetriesPending()).isTrue();
        assertThat(docsB.read(Template.of(Doc.class)).orElseThrow().id()).isEqualTo("doc-1");
        assertThat(runUntil(Duration.ofSeconds(10), () -> !seenAtB.isEmpty()))
                .as("the deferred WRITTEN after the retry: " + b.pdt().payloads() + " " + b.content().stats()).isTrue();
        assertThat(docsB.bulkRetriesPending()).isZero();
        settle(4);
        assertThat(seenAtB).as("the deferred WRITTEN fires once, after the retry").containsExactly("doc-1");

        // and a later entry still flows
        docsA.write(new Doc("doc-2", fields(3_100, 7), 2), HOUR);
        assertThat(runUntil(Duration.ofSeconds(20), () -> docsB.readAll(Template.of(Doc.class), 4).size() == 2)).isTrue();
    }

    // ------------------------------------------------------------ PD-E-01: the codec distinction

    @Test
    void eachSeamRefusesTheOtherCodecAndAReplicaWithOnlyTheFlatSeamWaitsForAntiEntropy() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        ReplicatedSpace docsA = optedIn(a, "docs", false, builder -> { });
        // b is not opted in: today's flat seam alone, pre-sealing as before
        ReplicatedSpace docsB = ReplicatedSpace.builder(b.runtime(), "docs", b.identity(), "app")
                .clock(clock).settleWindow(Duration.ZERO).blocks(b.blocks())
                .bulk(new SpacePayloads(b.content()), THRESHOLD).build();
        b.spaces().put("docs", docsB);
        settle(8);

        EntryHandle handle = docsA.write(new Doc("doc-1", fields(3_000, 8), 1), HOUR);
        String treeRoot = recordOf(docsA, docsA.readAllEntries(Template.of(Doc.class), 4).get(0)).payloadRef();
        assertThat(Cids.parse(treeRoot).codec()).isEqualTo(Cids.DAG_CBOR);
        String rawRoot = new SpacePayloads(a.content()).publish(new byte[70 * 1024], HOUR);
        assertThat(Cids.parse(rawRoot).codec()).isEqualTo(Cids.RAW);

        SpacePayloads flat = new SpacePayloads(a.content());
        assertThat(flat.accepts(rawRoot)).isTrue();
        assertThat(flat.accepts(treeRoot)).as("the flat seam refuses a dag-cbor root").isFalse();
        assertThat(flat.local(treeRoot)).isEmpty();
        assertThatThrownBy(() -> flat.ensure(treeRoot, a.id(), () -> { })).isInstanceOf(IllegalArgumentException.class);
        assertThat(a.pdt().payloads().accepts(treeRoot)).isTrue();
        assertThat(a.pdt().payloads().accepts(rawRoot)).as("the tree seam refuses a raw root").isFalse();
        assertThat(a.pdt().payloads().accepts("bzzz")).isFalse();
        assertThat(flat.accepts("bzzz")).isFalse();

        assertThat(docsA.seamFor(treeRoot)).isSameAs(a.pdt().payloads());
        assertThat(docsA.seamFor(rawRoot)).as("the fallback carries the raw root").isNotNull().isNotSameAs(a.pdt().payloads());
        assertThat(docsB.seamFor(treeRoot)).as("no seam at b carries a tree root").isNull();
        assertThat(docsB.seamFor(rawRoot)).isNotNull();

        assertThat(runUntil(Duration.ofSeconds(10), () -> docsB.signedState(handle.entryId()).isPresent())).isTrue();
        settle(8);
        assertThat(docsB.read(Template.of(Doc.class))).as("the entry waits, no flat fetch opened").isEmpty();
        assertThat(b.content().stats().fetchesActive() + b.content().stats().treeFetchesActive())
                .as(b.content().stats().toString()).isZero();
    }

    // ------------------------------------------------------------ PD-E-02, PD-N-04: a value the spill cannot break up

    @Test
    void aTextStringAboveTheNodeBoundTakesTheFallbackSeamAndAStrictSpaceRefusesIt() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        ReplicatedSpace docsA = optedIn(a, "docs", false, builder -> { });
        ReplicatedSpace docsB = optedIn(b, "docs", false, builder -> { });
        ReplicatedSpace strictA = optedIn(a, "strict", false, builder -> builder.bulkRequired(true));
        settle(8);

        char[] chars = new char[2 * 1024 * 1024];
        java.util.Arrays.fill(chars, 'x');
        Doc big = new Doc("doc-1", Map.of("text", new String(chars)), 1);
        docsA.write(big, HOUR);
        assertThat(a.pdt().payloads().stats().published()).as("the tree seam could not spill a 2 MiB text").isZero();
        String ref = recordOf(docsA, docsA.readAllEntries(Template.of(Doc.class), 4).get(0)).payloadRef();
        assertThat(Cids.parse(ref).codec()).as("the flat fallback carried it").isEqualTo(Cids.RAW);
        assertThat(runUntil(Duration.ofSeconds(30), () -> docsB.read(Template.of(Doc.class)).isPresent())).isTrue();
        assertThat(docsB.read(Template.of(Doc.class)).orElseThrow()).isEqualTo(big);

        assertThatThrownBy(() -> strictA.write(big, HOUR)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot become a tree").hasMessageContaining("1048569");
        assertThat(strictA.readAll(Template.of(Doc.class), 4)).isEmpty();
    }
}
