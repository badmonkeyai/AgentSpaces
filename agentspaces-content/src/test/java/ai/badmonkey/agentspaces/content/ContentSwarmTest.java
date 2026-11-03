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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The content swarm over the in-memory fabric (srad/large-networks-bulk-data.md
 * §2.5 scenarios 1, 7 and the resume and in-place rows). The test network
 * delivers frames inline, so a fetch runs as fast as the ticks that replicate the
 * manifest and the holdings; timeouts run on the test clock.
 */
class ContentSwarmTest {

    private static final int PIECE = 256 * 1024;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zContent");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zContent", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(String name, PeerNode node, GroupRuntime runtime, ReplicatedSpace space,
                        BlockExchange blocks, Content content, PieceStore store) {
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.content().close();
            p.node().close();
        });
    }

    private Peer newPeer(String address, long seed, PieceStore store, String... seedAddresses) throws IOException {
        return newPeer(address, seed, store, b -> b, seedAddresses);
    }

    private Peer newPeer(String address, long seed, PieceStore store,
                         java.util.function.UnaryOperator<Content.Builder> customise,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        PieceStore pieceStore = store == null ? new MemoryPieceStore(clock, 0) : store;
        Content content = customise.apply(Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(clock).store(pieceStore).blocks(blocks).random(new Random(seed))).build();
        runtime.onTick(content::tick);
        content.start();
        Peer peer = new Peer(address, node, runtime, space, blocks, content, pieceStore);
        peers.add(peer);
        return peer;
    }

    private final java.util.Set<String> dead = new java.util.HashSet<>();

    private void kill(Peer peer) {
        dead.add(peer.name());
        peer.content().close();
        peer.node().close();
    }

    private void tickAll() {
        for (Peer p : peers) {
            if (!dead.contains(p.name())) {
                p.node().tick();
            }
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

    private static byte[] randomBytes(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private static byte[] readAll(Fetch fetch) throws IOException {
        try (SeekableByteChannel channel = fetch.open()) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            return buffer.array();
        }
    }

    private static PublishOptions options() {
        return PublishOptions.defaults().withPieceSize(PIECE).withName("artifact.bin");
    }

    // ------------------------------------------------------------ scenarios

    @Test
    void singleSourceFetchVerifiesStoresAndSeeds() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(3 * PIECE + 12_345, 7);
        Manifest manifest = a.content().publish(artifact, options());
        assertThat(manifest.pieces()).isEqualTo(4);
        assertThat(manifest.table()).hasSize(4 * 32);
        assertThat(manifest.root()).startsWith("bafkrei");

        Fetch fetch = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(readAll(fetch)).isEqualTo(artifact);
        assertThat(b.store().have(manifest.root()).cardinality()).isEqualTo(4);
        assertThat(fetch.progress().bytesBySource()).containsKey(a.node().peerId());

        // b is now a seeder: its complete holding reaches a's replica
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.content().describe(manifest.root())
                .map(Content.Description::completeHolders).orElse(0) == 2)).isTrue();
        assertThat(a.content().stats().bytesServed()).isGreaterThanOrEqualTo(artifact.length);
    }

    @Test
    void swarmCompletesFromAPartialHolderAfterTheSeederDies() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        Peer c = newPeer("c", 3, null, "a");
        Peer d = newPeer("d", 4, null, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(16 * PIECE, 11);
        Manifest manifest = a.content().publish(artifact, options());

        // only b can hear a's answers; c and d must get everything from b
        network.partition("a", "c");
        network.partition("a", "d");
        Fetch onB = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), onB::complete)).as(onB.trace().toString()).isTrue();

        kill(a); // the seeder dies; its holding is still in every replica for a while
        Fetch onC = c.content().fetch(manifest.root());
        Fetch onD = d.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(30), () -> onC.complete() && onD.complete()))
                .as("c: " + onC.trace() + " d: " + onD.trace()).isTrue();
        assertThat(readAll(onC)).isEqualTo(artifact);
        assertThat(readAll(onD)).isEqualTo(artifact);
        assertThat(onC.progress().bytesBySource().getOrDefault(b.node().peerId(), 0L)
                + onC.progress().bytesBySource().getOrDefault(d.node().peerId(), 0L))
                .as("c drew from b or d, never from the dead seeder").isEqualTo(artifact.length);
        assertThat(onC.progress().bytesBySource().getOrDefault(a.node().peerId(), 0L)).isZero();
    }

    @Test
    void resumeAfterRestartRequestsOnlyMissingPieces(@TempDir Path dir) throws Exception {
        Peer a = newPeer("a", 1, null);
        FilePieceStore storeB = new FilePieceStore(dir.resolve("b"), codec, clock, 0);
        Peer b = newPeer("b", 2, storeB, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(8 * PIECE, 21);
        Manifest manifest = a.content().publish(artifact, options());
        // replicate the manifest and holding before holding a's answers back
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.content().spaces().manifest(manifest.root()).isPresent()
                && !b.content().spaces().holdings(manifest.root()).isEmpty())).isTrue();

        network.hold("a", "b");
        Fetch first = b.content().fetch(manifest.root());
        for (int i = 0; i < 3; i++) {
            tickAll();
        }
        // release held frames one at a time (gossip shares the queue with the answers) until
        // a piece has landed; the answers are absorbed on handler threads, so give each a moment
        long settle = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (storeB.have(manifest.root()).cardinality() == 0 && System.nanoTime() < settle) {
            if (!network.releaseOne("a", "b")) {
                tickAll(); // let b ask again and a answer into the held queue
            }
            Thread.sleep(5);
        }
        int before = storeB.have(manifest.root()).cardinality();
        assertThat(before).as("some pieces verified from the released answers").isBetween(1, 7);
        first.cancel();
        b.content().close();
        network.release("a", "b", ai.badmonkey.agentspaces.test.SimNetwork.Order.FIFO, 0);

        // "restart": a new store over the same directory, a new capability on the same peer
        FilePieceStore reopened = new FilePieceStore(dir.resolve("b"), codec, clock, 0);
        assertThat(reopened.have(manifest.root()).cardinality()).isEqualTo(before);
        Content again = Content.builder(new CapabilityPipes(b.runtime(), codec), b.runtime(), b.space(), b.node().peerId())
                .codec(codec).clock(clock).store(reopened).blocks(b.blocks()).random(new Random(9)).build();
        b.runtime().onTick(again::tick);
        again.start();
        Fetch second = again.fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), second::complete)).as(second.trace().toString()).isTrue();
        assertThat(second.trace().get(0)).contains(before + " already verified");
        assertThat(second.progress().bytesBySource().getOrDefault(a.node().peerId(), 0L))
                .as("only the missing pieces crossed").isEqualTo((long) (8 - before) * PIECE);
        assertThat(readAll(second)).isEqualTo(artifact);
        again.close();
    }

    @Test
    void badPieceIsDiscardedTheSourceStruckAndTheFetchCompletesElsewhere() throws Exception {
        Peer a = newPeer("a", 1, null);
        MemoryPieceStore evilInner = new MemoryPieceStore(clock, 0);
        CorruptingStore corrupting = new CorruptingStore(evilInner);
        Peer e = newPeer("e", 5, corrupting, "a");
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(6 * PIECE, 31);
        Manifest manifest = a.content().publish(artifact, options());
        // the evil holder claims the whole artifact: it copies a's bytes into its own store
        evilInner.open(manifest);
        for (int i = 0; i < manifest.pieces(); i++) {
            evilInner.write(manifest.root(), i, java.util.Arrays.copyOfRange(artifact, i * PIECE, i * PIECE + manifest.pieceLength(i)));
        }
        e.content().start();
        corrupting.armed = true;
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.content().spaces().holdings(manifest.root()).size() == 2)).isTrue();

        Fetch fetch = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(30), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(readAll(fetch)).isEqualTo(artifact);
        assertThat(b.content().stats().piecesStruck()).isGreaterThanOrEqualTo(1);
        assertThat(fetch.progress().bytesBySource().getOrDefault(e.node().peerId(), 0L)).isZero();
        assertThat(fetch.trace()).anySatisfy(line -> assertThat(line).contains("failed its hash"));
    }

    @Test
    void largeTableTravelsThroughTheBlockExchange() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        int pieceSize = 64 * 1024;
        byte[] artifact = randomBytes(70 * pieceSize + 5, 41); // 71 pieces, above the 64 inline bound
        Manifest manifest = a.content().publish(artifact, PublishOptions.defaults().withPieceSize(pieceSize));
        assertThat(manifest.table()).isNull();
        assertThat(manifest.tableRef()).startsWith("z");

        Fetch fetch = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(readAll(fetch)).isEqualTo(artifact);
        assertThat(b.blocks().local(manifest.tableRef())).as("the table block is cached here").isPresent();
    }

    @Test
    void inPlacePublishServesTheFileWhereItLies(@TempDir Path dir) throws Exception {
        InPlaceStore inPlace = new InPlaceStore();
        MemoryPieceStore cache = new MemoryPieceStore(clock, 0);
        Peer a = newPeer("a", 1, new CompositeStore(List.of(inPlace, cache), false));
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(5 * PIECE + 99, 51);
        Path file = dir.resolve("model.bin");
        Files.write(file, artifact);
        Manifest manifest = a.content().publish(file, options().withInPlace(true));
        assertThat(cache.bytesUsed()).as("nothing copied").isZero();
        assertThat(inPlace.roots()).containsExactly(manifest.root());

        Fetch fetch = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(readAll(fetch)).isEqualTo(artifact);
    }

    @Test
    void describeAndListReadTheReplicaWithoutAFetch() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        Manifest manifest = a.content().publish(randomBytes(PIECE, 61), options().withName("one.bin"));
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.content().describe(manifest.root()).isPresent())).isTrue();
        Optional<Content.Description> description = b.content().describe(manifest.root());
        assertThat(description).isPresent();
        assertThat(description.get().completeHolders()).isEqualTo(1);
        assertThat(description.get().held().cardinality()).isZero();
        assertThat(description.get().replicated()).isTrue();
        assertThat(b.content().list(10)).extracting(Manifest::name).containsExactly("one.bin");
    }

    @Test
    void sealedRootTravelsSealedAndAKeylessMemberCannotFetchIt() throws Exception {
        ai.badmonkey.agentspaces.common.crypto.GroupKey key = ai.badmonkey.agentspaces.common.crypto.GroupKey.generate();
        Peer a = newPeer("a", 1, null, b -> b.keyRing(ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(key)));
        Peer b = newPeer("b", 2, null, bb -> bb.keyRing(ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(key)), "a");
        Peer k = newPeer("k", 3, null, "a"); // a member with no content key
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(4 * PIECE + 7, 71);
        Manifest manifest = a.content().publish(artifact, options());
        assertThat(manifest.sealing()).isEqualTo(Manifest.SEALING_GROUP_KEY);

        List<byte[]> onTheWire = new java.util.concurrent.CopyOnWriteArrayList<>();
        network.tap("a", "b", onTheWire::add);
        Fetch onB = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), onB::complete)).as(onB.trace().toString()).isTrue();
        assertThat(readAll(onB)).isEqualTo(artifact);
        network.untap("a", "b");
        byte[] firstPiece = java.util.Arrays.copyOf(artifact, PIECE);
        for (byte[] frame : onTheWire) {
            assertThat(indexOf(frame, java.util.Arrays.copyOf(firstPiece, 4096)))
                    .as("no plaintext range crosses the wire").isEqualTo(-1);
        }

        Fetch onK = k.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(10), onK::isDoneOrFailed)).isTrue();
        assertThat(onK.complete()).isFalse();
        assertThat(onK.trace()).anySatisfy(line -> assertThat(line).contains("no content key"));
    }

    @Test
    void aFetchGrantNarrowsWhoIsServed() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        Peer c = newPeer("c", 3, null, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        // a's authorizer grants CONTENT_FETCH to c only
        ai.badmonkey.agentspaces.api.spi.Authorizer onlyC = new ai.badmonkey.agentspaces.api.spi.Authorizer() {
            @Override
            public boolean permits(ai.badmonkey.agentspaces.common.id.PeerId peer,
                                   Operation operation, String scope) {
                return operation != Operation.CONTENT_FETCH || peer.equals(c.node().peerId());
            }
        };
        Content gated = Content.builder(new CapabilityPipes(a.runtime(), codec), a.runtime(), a.space(), a.node().peerId())
                .codec(codec).clock(clock).store(a.store()).blocks(a.blocks()).authorizer(onlyC).random(new Random(1)).build();
        a.content().close();
        a.runtime().onTick(gated::tick);
        byte[] artifact = randomBytes(2 * PIECE, 81);
        Manifest manifest = gated.publish(artifact, options());

        Fetch onB = b.content().fetch(manifest.root());
        runUntil(Duration.ofSeconds(2), onB::complete);
        assertThat(onB.complete()).as("b is refused by a's grant and nobody else holds the root").isFalse();
        assertThat(gated.stats().wantsRefused()).isGreaterThanOrEqualTo(1);
        assertThat(onB.trace()).anySatisfy(line -> assertThat(line).contains("rejected (3)"));
        Fetch onC = c.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), onC::complete)).as(onC.trace().toString()).isTrue();
        // c now seeds under its own ungated policy, so b completes from c: the grant binds the holder that holds it
        assertThat(runUntil(Duration.ofSeconds(20), onB::complete)).as(onB.trace().toString()).isTrue();
        assertThat(onB.progress().bytesBySource().getOrDefault(a.node().peerId(), 0L)).isZero();
        gated.close();
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

    /** A holder whose every range is wrong by one byte. */
    private static final class CorruptingStore implements PieceStore {
        private final PieceStore inner;
        /** Off while the holder starts, so its sampled check (LB-T-10) passes and it lies only when serving. */
        volatile boolean armed;

        CorruptingStore(PieceStore inner) {
            this.inner = inner;
        }

        @Override
        public String kind() {
            return "corrupting";
        }

        @Override
        public boolean writable() {
            return inner.writable();
        }

        @Override
        public java.util.Set<String> roots() {
            return inner.roots();
        }

        @Override
        public Optional<Manifest> manifest(String root) {
            return inner.manifest(root);
        }

        @Override
        public void open(Manifest manifest) {
            inner.open(manifest);
        }

        @Override
        public java.util.BitSet have(String root) {
            return inner.have(root);
        }

        @Override
        public Optional<byte[]> read(String root, int index, int offset, int length) {
            return inner.read(root, index, offset, length).map(bytes -> {
                if (!armed) {
                    return bytes;
                }
                byte[] bad = bytes.clone();
                if (bad.length > 0) {
                    bad[0] ^= 0x01;
                }
                return bad;
            });
        }

        @Override
        public void write(String root, int index, byte[] piece) {
            inner.write(root, index, piece);
        }

        @Override
        public SeekableByteChannel openArtifact(String root) throws IOException {
            return inner.openArtifact(root);
        }

        @Override
        public void pin(String root) {
            inner.pin(root);
        }

        @Override
        public void unpin(String root) {
            inner.unpin(root);
        }

        @Override
        public boolean pinned(String root) {
            return inner.pinned(root);
        }

        @Override
        public long bytesUsed() {
            return inner.bytesUsed();
        }

        @Override
        public long quota() {
            return inner.quota();
        }

        @Override
        public void evict() {
            inner.evict();
        }

        @Override
        public void forget(String root) {
            inner.forget(root);
        }
    }

    // ------------------------------------------------------------ the API rows (LB-M-01, M-04, M-09, K-01, S-01, H-05, E-06)

    @Test
    void aStreamPublishMatchesTheBytesPublishAndTheEtaEndsAtZero() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] data = new byte[3 * 65_536 + 99];
        new Random(5).nextBytes(data);
        PublishOptions options = PublishOptions.defaults().withPieceSize(65_536).withName("stream.bin");
        Manifest fromStream = a.content().publish(new java.io.ByteArrayInputStream(data), options);
        assertThat(fromStream.root()).isEqualTo(ContentRoots.hash(data, 65_536).root());
        Fetch fetch = b.content().fetch(fromStream.root());
        assertThat(runUntil(Duration.ofSeconds(30), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(fetch.progress().etaMillis()).isZero();
        assertThat(new Fetch.Progress(0, 4, 0, java.util.Map.of(), 0, true).etaMillis()).isEqualTo(-1);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> a.content().publish(
                        new java.io.ByteArrayInputStream(data), options.withInPlace(true)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Path");
    }

    @Test
    void lossyBindingsGetSmallerPieces() {
        var reliable = java.util.Set.of(ai.badmonkey.agentspaces.api.spi.Transport.Capability.RELIABLE,
                ai.badmonkey.agentspaces.api.spi.Transport.Capability.ORDERED);
        var datagram = java.util.Set.of(ai.badmonkey.agentspaces.api.spi.Transport.Capability.DATAGRAM);
        assertThat(PublishOptions.pieceSizeFor(reliable)).isEqualTo(1024 * 1024);
        assertThat(PublishOptions.pieceSizeFor(datagram)).isEqualTo(256 * 1024);
        assertThat(PublishOptions.pieceSizeFor(java.util.Set.of())).isEqualTo(256 * 1024);
    }

    @Test
    void attrsAreTagsSoATemplateAndASearchFindTheRun() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        Manifest run41 = a.content().publish(new byte[1000], PublishOptions.defaults().withPieceSize(65_536)
                .withName("model-41.bin").withMediaType("application/x-model").withAttrs(java.util.Map.of("run", "41")));
        clock.advance(Duration.ofSeconds(1));
        Manifest run42 = a.content().publish(new byte[2000], PublishOptions.defaults().withPieceSize(65_536)
                .withName("model-42.bin").withMediaType("application/x-model").withAttrs(java.util.Map.of("run", "42")));
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.content().list(10).size() == 2)).isTrue();
        assertThat(run42.tags()).containsEntry("run", "42").containsEntry(Manifest.ROOT_TAG, run42.root());
        var byRun = b.content().list(ai.badmonkey.agentspaces.api.space.Template.of(Manifest.class)
                .whereTag("run", ai.badmonkey.agentspaces.api.space.Matchers.eq("42")), 10);
        assertThat(byRun).extracting(Manifest::root).containsExactly(run42.root());
        assertThat(b.content().search("MODEL", "application/x-model", null, null, null, 10))
                .extracting(d -> d.manifest().root()).containsExactly(run42.root(), run41.root());
        assertThat(b.content().search(null, null, java.util.Map.of("run", "41"), a.node().peerId(), null, 10))
                .singleElement().satisfies(d -> {
                    assertThat(d.manifest().root()).isEqualTo(run41.root());
                    assertThat(d.completeHolders()).isEqualTo(1);
                });
        clock.advance(Duration.ofSeconds(1));
        assertThat(b.content().search(null, null, null, null, Duration.ofMillis(500), 10)).isEmpty();
    }

    @Test
    void aCatalogueBeyondSixtyFourRootsKeepsEveryHoldingAlive() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        List<String> roots = new ArrayList<>();
        for (int i = 0; i < Content.HOLDINGS_PER_TICK + 6; i++) {
            byte[] data = new byte[100 + i];
            data[0] = (byte) i;
            roots.add(a.content().publish(data, PublishOptions.defaults().withPieceSize(65_536)).root());
        }
        // ten holding lease lengths of ticks: every root must have come round often enough to stay held
        for (int i = 0; i < 4 * 60 * 10; i++) {
            tickAll();
        }
        for (String root : roots) {
            assertThat(b.content().describe(root)).as(root).hasValueSatisfying(d -> assertThat(d.completeHolders()).isEqualTo(1));
        }
    }

    @Test
    void aSpaceWithoutTheSwarmRefusesAPayloadAboveItsCapNamingTheProperty() throws Exception {
        Peer a = newPeer("a", 1, null);
        ReplicatedSpace capped = ReplicatedSpace.builder(a.runtime(), "capped", PeerIdentity.generate(), "app")
                .clock(clock).settleWindow(Duration.ZERO).blocks(a.blocks()).maxPayload(256 * 1024).build();
        record Blob(byte[] bytes) {
        }
        capped.write(new Blob(new byte[100 * 1024]), ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(1)));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> capped.write(new Blob(new byte[300 * 1024]),
                        ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(1))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("agentspaces.content.enabled");
        capped.close();
    }

    // ------------------------------------------------------------ the sealed mode (LB-G-03, LB-T-05)

    @Test
    void aSealedHashingRootIsHeldAndServedByAKeylessMemberAndOpenedOnlyWithTheKey() throws Exception {
        ai.badmonkey.agentspaces.common.crypto.GroupKey key = ai.badmonkey.agentspaces.common.crypto.GroupKey.generate();
        Peer a = newPeer("a", 1, null, b -> b.keyRing(ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(key)));
        Peer k = newPeer("k", 3, null, "a"); // keyless
        Peer b = newPeer("b", 2, null, bb -> bb.keyRing(ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(key)), "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(3 * PIECE + 501, 72);
        Manifest manifest = a.content().publish(artifact, options().withSealedHashing(true));
        assertThat(manifest.hashing()).isEqualTo(Manifest.HASHING_SEALED);
        assertThat(manifest.sealing()).isEqualTo(Manifest.SEALING_GROUP_KEY);
        assertThat(manifest.root()).isNotEqualTo(ContentRoots.hash(artifact, PIECE).root())
                .as("the root is over sealed bytes, so it names this group's sealing, not the plaintext");

        // the keyless member fetches, verifies, and stores the sealed pieces, and cannot open them
        Fetch onK = k.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), onK::complete)).as(onK.trace().toString()).isTrue();
        assertThat(k.store().have(manifest.root()).cardinality()).isEqualTo(manifest.pieces());
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> k.content().openPlain(manifest.root()))
                .isInstanceOf(IOException.class).hasMessageContaining("no content key");
        byte[] stored = k.store().read(manifest.root(), 0, 0, 4096).orElseThrow();
        assertThat(indexOf(stored, java.util.Arrays.copyOf(artifact, 64))).as("held sealed").isEqualTo(-1);

        // the publisher leaves; the key holder fetches from the keyless holder and opens the plaintext
        kill(a);
        Fetch onB = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(30), onB::complete)).as(onB.trace().toString()).isTrue();
        // the dead publisher may still be listed while its holding lapses; every byte came from k
        assertThat(onB.progress().bytesBySource().get(k.node().peerId())).isEqualTo((long) manifest.size());
        assertThat(onB.progress().bytesBySource().getOrDefault(a.node().peerId(), 0L)).isZero();
        assertThat(readAll(onB)).isEqualTo(artifact);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> b.content().share(manifest.root(),
                        ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofHours(1))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("re-seals");
    }

    @Test
    void sharingASealedHashingRootReSealsItUnderTheOtherGroupsKeyAsANewRoot() throws Exception {
        ai.badmonkey.agentspaces.common.crypto.GroupKey keyA = ai.badmonkey.agentspaces.common.crypto.GroupKey.generate();
        ai.badmonkey.agentspaces.common.crypto.GroupKey keyB = ai.badmonkey.agentspaces.common.crypto.GroupKey.generate();
        Peer bridge = newPeer("bridge", 1, null, b -> b.keyRing(ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(keyA)));
        Peer other = newPeer("other", 2, null, "bridge");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(2 * PIECE + 33, 73);
        Manifest inA = bridge.content().publish(artifact, options().withSealedHashing(true));

        // the second group: the bridge and a mission member, under key B
        GroupId missionId = GroupId.of("zMission");
        GroupAdvertisement mission = new GroupAdvertisement("aspace://zMission", PeerIdentity.generate().peerId(), missionId,
                java.time.Instant.EPOCH, Duration.ofDays(1), "mission",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        java.util.function.BiFunction<Peer, List<PeerAdvertisement.Endpoint>, Content> joinMission = (peer, seeds) -> {
            GroupRuntime runtime = peer.node().joinGroup(mission, GroupMembership.Config.defaults(), seeds);
            BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
            ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", PeerIdentity.generate(), "content")
                    .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
            Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, space, peer.node().peerId())
                    .codec(codec).clock(clock).store(peer.store()).blocks(blocks)
                    .keyRing(ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(keyB)).random(new Random(9)).build();
            runtime.onTick(content::tick);
            content.start();
            return content;
        };
        Content bridgeInMission = joinMission.apply(bridge, List.of());
        Content otherInMission = joinMission.apply(other, List.of(new PeerAdvertisement.Endpoint("mem", "bridge", 0)));
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        Manifest inMission = bridgeInMission.share(inA.root(), ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofHours(1)),
                bridge.content());
        assertThat(inMission.root()).isNotEqualTo(inA.root());
        assertThat(inMission.hashing()).isEqualTo(Manifest.HASHING_SEALED);
        Fetch fetch = otherInMission.fetch(inMission.root());
        assertThat(runUntil(Duration.ofSeconds(30), fetch::complete)).as(fetch.trace().toString()).isTrue();
        try (SeekableByteChannel channel = otherInMission.openPlain(inMission.root())) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            assertThat(buffer.array()).isEqualTo(artifact);
        }
        bridgeInMission.close();
        otherInMission.close();
    }

    // ------------------------------------------------------------ H-08, P-06, T-10, T-11, K-06..K-08, G-06

    private Content joinOtherGroup(Peer peer, GroupAdvertisement ad, String... seeds) {
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", seed, 0));
        }
        GroupRuntime runtime = peer.node().joinGroup(ad, GroupMembership.Config.defaults(), endpoints);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", PeerIdentity.generate(), "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, space, peer.node().peerId())
                .codec(codec).clock(clock).store(peer.store()).blocks(blocks).random(new Random(11)).build();
        runtime.onTick(content::tick);
        content.start();
        return content;
    }

    private static GroupAdvertisement otherGroup(String id) {
        return new GroupAdvertisement("aspace://" + id, PeerIdentity.generate().peerId(), GroupId.of(id),
                java.time.Instant.EPOCH, Duration.ofDays(1), id,
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    @Test
    void aFetcherPushesHaveToItsSourcesBetweenHoldingRefreshes() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        Manifest manifest = a.content().publish(randomBytes(20 * 65_536, 81), PublishOptions.defaults().withPieceSize(65_536));
        List<byte[]> toA = new java.util.concurrent.CopyOnWriteArrayList<>();
        network.tap("b", "a", toA::add);
        Fetch fetch = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        network.untap("b", "a");
        long haves = toA.stream().filter(frame -> {
            try {
                var signed = codec.fromBytes(frame, ai.badmonkey.agentspaces.peering.wire.WireCodec.Signed.class);
                if (signed == null || signed.envelope().kind() != ai.badmonkey.agentspaces.peering.wire.Envelope.Kind.PIPE_DATA) {
                    return false;
                }
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> pipe = codec.fromBytes(signed.envelope().body(), java.util.Map.class);
                return Content.TYPE.equals(pipe.get("capability"))
                        && Messages.tagOf(codec, (byte[]) pipe.get("payload")).orElse(-1) == Messages.HAVE;
            } catch (RuntimeException e) {
                return false;
            }
        }).count();
        assertThat(haves).as("a HAVE every eight pieces and one at completion").isGreaterThanOrEqualTo(3);
    }

    @Test
    void aDownloadBudgetHoldsAFetchBackAndTheTraceSaysSo() throws Exception {
        Peer a = newPeer("a", 1, null);
        ContentPolicy metered = new ContentPolicy() {
            @Override
            public long downloadBytesPerDay() {
                return 2 * 65_536;
            }
        };
        Peer b = newPeer("b", 2, null, bb -> bb.policy(metered), "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        Manifest manifest = a.content().publish(randomBytes(6 * 65_536, 82), PublishOptions.defaults().withPieceSize(65_536));
        Fetch fetch = b.content().fetch(manifest.root());
        for (int i = 0; i < 40; i++) {
            tickAll();
        }
        assertThat(fetch.complete()).isFalse();
        assertThat(fetch.progress().bytesReceived()).isLessThanOrEqualTo(2L * 65_536);
        assertThat(fetch.trace()).anySatisfy(line -> assertThat(line).contains("download budget"));
    }

    @Test
    void aDamagedRootFailsItsSampleOnStartAndIsForgotten(@TempDir Path dir) throws Exception {
        FilePieceStore store = new FilePieceStore(dir, codec, clock, 0);
        Peer a = newPeer("a", 1, store);
        Manifest manifest = a.content().publish(randomBytes(3 * PIECE + 5, 83), options());
        store.flush();
        Path data;
        try (var dirs = Files.list(dir)) {
            data = dirs.filter(Files::isDirectory).findFirst().orElseThrow().resolve("data");
        }
        Files.write(data, new byte[(int) Files.size(data)]); // the disk lost the bytes
        FilePieceStore reopened = new FilePieceStore(dir, codec, clock, 0);
        assertThat(reopened.roots()).containsExactly(manifest.root());
        Peer again = newPeer("again", 9, reopened);
        assertThat(again.content().rootsDroppedOnSample()).isEqualTo(1);
        assertThat(reopened.roots()).isEmpty();
    }

    @Test
    void anInPlaceFileChangedUnderneathIsCaughtAtItsFirstServe(@TempDir Path dir) throws Exception {
        InPlaceStore inPlace = new InPlaceStore();
        Peer a = newPeer("a", 1, new CompositeStore(List.of(inPlace, new MemoryPieceStore(clock, 0)), false));
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(2 * PIECE + 9, 84);
        Path file = dir.resolve("map.bin");
        Files.write(file, artifact);
        Manifest manifest = a.content().publish(file, options().withInPlace(true));
        Files.write(file, randomBytes(artifact.length, 85)); // edited after publication
        Fetch fetch = b.content().fetch(manifest.root());
        for (int i = 0; i < 20; i++) {
            tickAll();
        }
        assertThat(fetch.complete()).isFalse();
        assertThat(a.content().rootsDroppedOnSample()).isEqualTo(1);
        assertThat(inPlace.roots()).isEmpty();
    }

    /** An external backend over bytes held "remotely" by key, counting every range read (CC-L-01, CC-L-02). */
    private static final class RemoteStore implements ExternalStore {
        final java.util.Map<String, byte[]> objects = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.Map<String, Manifest> manifests = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.Map<String, String> keys = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();

        @Override public void register(Manifest manifest, String key) {
            manifests.put(manifest.root(), manifest);
            keys.put(manifest.root(), key);
        }
        @Override public String kind() { return "remote"; }
        @Override public boolean writable() { return false; }
        @Override public java.util.Set<String> roots() { return java.util.Set.copyOf(manifests.keySet()); }
        @Override public java.util.Optional<Manifest> manifest(String root) { return java.util.Optional.ofNullable(manifests.get(root)); }
        @Override public void open(Manifest manifest) { }
        @Override public java.util.BitSet have(String root) {
            java.util.BitSet bits = new java.util.BitSet();
            Manifest m = manifests.get(root);
            if (m != null) {
                bits.set(0, m.pieces());
            }
            return bits;
        }
        @Override public java.util.Optional<byte[]> read(String root, int index, int offset, int length) {
            Manifest m = manifests.get(root);
            if (m == null) {
                return java.util.Optional.empty();
            }
            reads.incrementAndGet();
            byte[] object = objects.get(keys.get(root));
            long at = (long) index * m.pieceSize() + offset;
            int n = (int) Math.min(Math.min(length, m.pieceLength(index) - offset), object.length - at);
            return java.util.Optional.of(java.util.Arrays.copyOfRange(object, (int) at, (int) at + n));
        }
        @Override public void write(String root, int index, byte[] piece) { throw new IllegalStateException("read-only"); }
        @Override public SeekableByteChannel openArtifact(String root) throws IOException {
            return new ByteArrayChannel(objects.get(keys.get(root)));
        }
        @Override public void pin(String root) { }
        @Override public void unpin(String root) { }
        @Override public boolean pinned(String root) { return manifests.containsKey(root); }
        @Override public long bytesUsed() { return 0; }
        @Override public long quota() { return 0; }
        @Override public void evict() { }
        @Override public void forget(String root) { manifests.remove(root); keys.remove(root); }
    }

    @Test
    void anExternalStoreServesWhereItLiesAndTheCacheTakesEachPieceOnce() throws Exception {
        RemoteStore remote = new RemoteStore();
        MemoryPieceStore cache = new MemoryPieceStore(clock, 0);
        Peer a = newPeer("a", 1, new CompositeStore(List.of(cache, remote), true));
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(6 * PIECE + 17, 61);
        remote.objects.put("models/ckpt-7", artifact);
        Manifest manifest = a.content().publishExternal(remote, "models/ckpt-7",
                new java.io.ByteArrayInputStream(artifact), options());
        assertThat(manifest.root()).isEqualTo(ContentRoots.hash(artifact, PIECE).root());
        assertThat(remote.roots()).containsExactly(manifest.root());
        assertThat(a.content().describe(manifest.root())).isPresent();

        Fetch fetch = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(readAll(fetch)).isEqualTo(artifact);
        assertThat(remote.reads.get()).as("one whole-piece read per piece, sub-ranges from the cache")
                .isEqualTo(manifest.pieces());
    }

    @Test
    void anExternalObjectChangedUnderneathIsForgottenInEveryBackend() throws Exception {
        RemoteStore remote = new RemoteStore();
        MemoryPieceStore cache = new MemoryPieceStore(clock, 0);
        Peer a = newPeer("a", 1, new CompositeStore(List.of(cache, remote), true));
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(3 * PIECE + 5, 62);
        remote.objects.put("tiles/7", artifact);
        Manifest manifest = a.content().publishExternal(remote, "tiles/7",
                ContentRoots.hash(artifact, PIECE), options());
        remote.objects.put("tiles/7", randomBytes(artifact.length, 63)); // replaced after publication
        Fetch fetch = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), () -> a.content().rootsDroppedOnSample() > 0))
                .as("the first serve catches the change").isTrue();
        assertThat(fetch.complete()).isFalse();
        assertThat(a.content().rootsDroppedOnSample()).isEqualTo(1);
        assertThat(remote.roots()).isEmpty();
        assertThat(cache.roots()).as("the read-through copy of the bad piece goes too").doesNotContain(manifest.root());
    }

    @Test
    void publishExternalRefusesABackendTheStoreDoesNotComposeAndAMismatchedPieceSize() throws Exception {
        Peer a = newPeer("a", 1, null);
        RemoteStore stray = new RemoteStore();
        byte[] artifact = randomBytes(PIECE + 1, 64);
        stray.objects.put("k", artifact);
        assertThatThrownBy(() -> a.content().publishExternal(stray, "k", ContentRoots.hash(artifact, PIECE), options()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("not part of this swarm's store");
        RemoteStore remote = new RemoteStore();
        Peer c = newPeer("c", 3, new CompositeStore(List.of(new MemoryPieceStore(clock, 0), remote), false));
        assertThatThrownBy(() -> c.content().publishExternal(remote, "k", ContentRoots.hash(artifact, PIECE / 2), options()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another piece size");
        assertThat(new InPlaceStore().external()).isTrue();
        assertThat(new MemoryPieceStore(clock, 0).external()).isFalse();
    }

    @Test
    void piecesAtRestAreEncryptedAndReadBackByRange(@TempDir Path dir) throws Exception {
        byte[] key = EncryptedPieceStore.keyIn(dir);
        assertThat(EncryptedPieceStore.keyIn(dir)).as("the same key on the next start").isEqualTo(key);
        EncryptedPieceStore store = new EncryptedPieceStore(new FilePieceStore(dir, codec, clock, 0), key);
        Peer a = newPeer("a", 1, store);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] artifact = randomBytes(2 * PIECE + 77, 86);
        Manifest manifest = a.content().publish(artifact, options());
        store.flush();
        try (var walk = Files.walk(dir)) {
            for (Path f : walk.filter(Files::isRegularFile).filter(f -> f.getFileName().toString().equals("data")).toList()) {
                assertThat(indexOf(Files.readAllBytes(f), java.util.Arrays.copyOf(artifact, 64))).as("no plaintext on disk").isEqualTo(-1);
            }
        }
        assertThat(store.read(manifest.root(), 1, 1000, 32)).hasValueSatisfying(r ->
                assertThat(r).isEqualTo(java.util.Arrays.copyOfRange(artifact, PIECE + 1000, PIECE + 1032)));
        Fetch fetch = b.content().fetch(manifest.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(readAll(fetch)).isEqualTo(artifact);
        try (SeekableByteChannel channel = store.openArtifact(manifest.root())) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            assertThat(buffer.array()).isEqualTo(artifact);
        }
    }

    @Test
    void aBridgesCollectionListsOnlyWhatItServesInThatGroupAndTheConsoleCountsIt() throws Exception {
        Peer bridge = newPeer("bridge", 1, null);
        Manifest onlyHere = bridge.content().publish(randomBytes(1000, 87), options().withName("first-group-only.bin"));
        Manifest shared = bridge.content().publish(randomBytes(2000, 88), options().withName("terrain-sector-7.bin")
                .withMediaType("application/x-tiles").withAttrs(java.util.Map.of("sector", "7")));
        Content inMission = joinOtherGroup(bridge, otherGroup("zMissionK"));
        CollectionAssetProvider provider = new CollectionAssetProvider(inMission,
                List.of(CollectionAssetProvider.Collection.all("tiles", "Terrain tiles.")));
        assertThat(provider.rowCount(provider.collections().get(0))).as("nothing shared yet").isZero();

        var written = Content.shareAll(bridge.content(), List.of(inMission), m -> m.name() != null && m.name().startsWith("terrain"),
                ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofHours(1)));
        assertThat(written.get(inMission.group())).extracting(Manifest::root).containsExactly(shared.root());
        assertThat(provider.rowCount(provider.collections().get(0))).isEqualTo(1);
        var cards = provider.describeAssets(inMission.group(), bridge.node().peerId(), clock.instant());
        assertThat(cards.get(0).description()).contains("Terrain tiles.", "terrain-sector-7.bin", "application/x-tiles", "sector 7");
        assertThat(inMission.describe(shared.root())).hasValueSatisfying(d -> assertThat(d.advertised()).isTrue());
        // the store is shared, so the mission swarm can describe the first group's root, but lists none of it
        assertThat(inMission.describe(onlyHere.root())).hasValueSatisfying(d -> {
            assertThat(d.advertised()).isFalse();
            assertThat(d.replicated()).isFalse();
        });
        @SuppressWarnings("unchecked")
        List<java.util.Map<String, Object>> rows = (List<java.util.Map<String, Object>>) new ContentPanelData(inMission).data().get("collections");
        assertThat(rows).singleElement().satisfies(row -> assertThat(row).containsEntry("roots", 1));
        inMission.close();
    }
}
