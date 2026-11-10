/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.CardSpill;
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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ISSUE-AppCardOriginFormats §9.8 (SPEC §6.1a.5; AppCard origin and formats, 2026-11-09): {@code put} publishes a parts
 * document with media type {@code application/cbor}, sealing {@code none}, and 65,536-byte pieces under the card's id;
 * {@code get} on a second member returns the same bytes; a holder that serves other bytes fails the piece check and
 * never reaches the reader; an artifact longer or shorter than the stub states is refused from its manifest before the
 * reader fetches it; and a document no holder serves fails at the store's deadline and frees its fetch.
 */
class ContentSpillStoreTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSpill", PeerIdentity.generate().peerId(),
            GroupId.of("zSpill"), Instant.EPOCH, Duration.ofDays(1), "spill", GroupAdvertisement.MembershipPolicy.OPEN,
            ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<Content> swarms = new ArrayList<>();

    @AfterEach
    void tearDown() {
        swarms.forEach(Content::close);
        nodes.forEach(PeerNode::close);
    }

    private Content swarm(String address, long seed, PieceStore store, boolean start) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                nodes.isEmpty() ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "s0", 0)));
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(clock).store(store).blocks(blocks).random(new Random(seed)).build();
        runtime.onTick(content::tick);
        if (start) {
            content.start();
        }
        nodes.add(node);
        swarms.add(content);
        return content;
    }

    private boolean until(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(250));
            Thread.sleep(2);
        }
        return condition.getAsBoolean();
    }

    private static byte[] document(int length) {
        byte[] bytes = new byte[length];
        new Random(length).nextBytes(bytes);
        return bytes;
    }

    @Test
    void putPublishesThePartsDocumentAndASecondMemberGetsItsBytes() throws Exception {
        Content a = swarm("s0", 1, new MemoryPieceStore(clock, 0), true);
        Content b = swarm("s1", 2, new MemoryPieceStore(clock, 0), true);
        until(() -> nodes.get(0).groups().iterator().next().membership().allMembers().size() == 1);
        byte[] parts = document(3 * CardSpill.PIECE_SIZE + 17);
        String root = new ContentSpillStore(a).put("aspace://zSpill/app/zPeerA/board", parts).join();
        Manifest manifest = a.store().manifest(root).orElseThrow();
        assertThat(manifest.mediaType()).isEqualTo("application/cbor");
        assertThat(manifest.sealing()).isEqualTo(Manifest.SEALING_NONE);
        assertThat(manifest.pieceSize()).isEqualTo(CardSpill.PIECE_SIZE);
        assertThat(manifest.pieces()).isEqualTo(4);
        assertThat(manifest.name()).isEqualTo("aspace://zSpill/app/zPeerA/board");
        assertThat(CardSpill.isRoot(root)).as("the root a stub may carry").isTrue();
        assertThat(new ContentSpillStore(a).put("aspace://zSpill/app/zPeerA/board", parts).join()).as("same bytes, same root").isEqualTo(root);

        CompletableFuture<byte[]> fetched = new ContentSpillStore(b).get(root, parts.length);
        assertThat(until(fetched::isDone)).isTrue();
        assertThat(fetched.join()).isEqualTo(parts);
        new ContentSpillStore(a).release(root);
        assertThat(a.servesHere(root)).as("released roots stay served until the store evicts them").isTrue();
    }

    @Test
    void aHolderThatServesOtherBytesFailsThePieceCheck() throws Exception {
        Content a = swarm("s0", 1, new MemoryPieceStore(clock, 0), true);
        MemoryPieceStore evilInner = new MemoryPieceStore(clock, 0);
        ContentSwarmTest.CorruptingStore corrupting = new ContentSwarmTest.CorruptingStore(evilInner);
        Content evil = swarm("s1", 5, corrupting, false);
        Content b = swarm("s2", 2, new MemoryPieceStore(clock, 0), true);
        until(() -> nodes.get(0).groups().iterator().next().membership().allMembers().size() == 2);
        byte[] parts = document(12 * CardSpill.PIECE_SIZE);
        String root = new ContentSpillStore(a).put("card", parts).join();
        Manifest manifest = a.store().manifest(root).orElseThrow();
        evilInner.open(manifest);
        for (int i = 0; i < manifest.pieces(); i++) {
            evilInner.write(root, i, Arrays.copyOfRange(parts, i * CardSpill.PIECE_SIZE, i * CardSpill.PIECE_SIZE + manifest.pieceLength(i)));
        }
        evil.start();
        corrupting.armed = true;
        assertThat(until(() -> b.spaces().holdings(root).size() == 2)).isTrue();
        CompletableFuture<byte[]> fetched = new ContentSpillStore(b).get(root, parts.length);
        assertThat(until(fetched::isDone)).isTrue();
        assertThat(fetched.join()).as("only verified pieces reach the reader").isEqualTo(parts);
        assertThat(b.stats().piecesStruck()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void aDocumentNoHolderServesFailsAtTheDeadlineAndFreesItsFetch() throws Exception {
        // AppCard origin and formats, 2026-11-09: a stub that names a root nobody serves, or a document its publisher
        // released, must not hold the reader or one of the swarm's fetch slots for good
        swarm("s0", 1, new MemoryPieceStore(clock, 0), true);
        Content b = swarm("s1", 2, new MemoryPieceStore(clock, 0), true);
        until(() -> nodes.get(0).groups().iterator().next().membership().allMembers().size() == 1);
        byte[] parts = document(100);
        String unserved = ContentRoots.rootOf(ContentRoots.hash(parts, CardSpill.PIECE_SIZE).table());
        CompletableFuture<byte[]> fetched = new ContentSpillStore(b, Duration.ofMillis(300)).get(unserved, parts.length);
        assertThat(until(fetched::isDone)).isTrue();
        assertThatThrownBy(fetched::join).hasCauseInstanceOf(TimeoutException.class);
        assertThat(until(() -> b.activeFetches().isEmpty())).as("the fetch leaves its slot").isTrue();
        assertThat(ContentSpillStore.DEFAULT_FETCH_TIMEOUT).isEqualTo(Duration.ofSeconds(30));
        assertThatThrownBy(() -> new ContentSpillStore(b, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anArtifactOfAnotherLengthThanTheStubStatesIsRefusedFromItsManifest() throws Exception {
        // AppCard origin and formats, 2026-11-09: a stub under the fetch bound cannot make the reader fetch a larger artifact
        Content a = swarm("s0", 1, new MemoryPieceStore(clock, 0), true);
        byte[] parts = document(12 * CardSpill.PIECE_SIZE);
        String root = new ContentSpillStore(a).put("card", parts).join();
        Content b = swarm("s1", 2, new MemoryPieceStore(clock, 0), true);
        assertThat(b.spaces().manifest(root)).as("b joined after the put, so the manifest is still on its way").isEmpty();
        CompletableFuture<byte[]> early = new ContentSpillStore(b).get(root, 100);
        assertThat(until(early::isDone)).isTrue();
        assertThatThrownBy(early::join).hasCauseInstanceOf(IllegalArgumentException.class).hasMessageContaining("expects 100");
        assertThat(until(() -> b.activeFetches().isEmpty())).as("the fetch stops").isTrue();
        assertThat(b.stats().bytesReceived()).isLessThan(parts.length);

        assertThat(b.spaces().manifest(root)).isPresent();
        CompletableFuture<byte[]> known = new ContentSpillStore(b).get(root, parts.length + 1L);
        assertThat(known).as("a manifest already held refuses at once").isCompletedExceptionally();
        assertThatThrownBy(known::join).hasCauseInstanceOf(IllegalArgumentException.class);
        assertThat(b.activeFetches()).as("and starts no fetch").isEmpty();
    }

    @Test
    void aFetchThatExpectsALengthStopsAtAManifestOfAnotherBeforeAnyPiece() throws Exception {
        // AppCard origin and formats, 2026-11-09: the session checks the manifest whichever way it arrives (the
        // manifest space, the store, or a holder's reply) before it asks for a piece
        Content a = swarm("s0", 1, new MemoryPieceStore(clock, 0), true);
        byte[] parts = document(6 * CardSpill.PIECE_SIZE);
        String root = new ContentSpillStore(a).put("card", parts).join();
        Content b = swarm("s1", 2, new MemoryPieceStore(clock, 0), true);
        assertThat(FetchOptions.defaults().expectedSize()).as("any length by default").isZero();
        Fetch wrong = b.fetch(root, FetchOptions.defaults().withExpectedSize(parts.length - 1L));
        assertThat(until(wrong::isDoneOrFailed)).isTrue();
        assertThat(wrong.complete()).isFalse();
        assertThat(wrong.failure()).get().isInstanceOf(IllegalArgumentException.class);
        assertThat(wrong.progress().bytesReceived()).isZero();
        assertThat(until(() -> b.activeFetches().isEmpty())).isTrue();
        Fetch right = b.fetch(root, FetchOptions.defaults().withExpectedSize(parts.length));
        assertThat(until(right::isDoneOrFailed)).isTrue();
        assertThat(right.complete()).isTrue();
    }

    @Test
    void aResolverThatSharesAnotherResolversFetchStillChecksItsOwnLength() throws Exception {
        // AppCard origin and formats, 2026-11-09: two stubs may name one root under different sizes; the second get
        // joins the session the first began, so the store checks the length again when the fetch ends
        Content a = swarm("s0", 1, new MemoryPieceStore(clock, 0), true);
        byte[] parts = document(2 * CardSpill.PIECE_SIZE + 5);
        String root = new ContentSpillStore(a).put("card", parts).join();
        Content b = swarm("s1", 2, new MemoryPieceStore(clock, 0), true);
        CompletableFuture<byte[]> right = new ContentSpillStore(b).get(root, parts.length);
        CompletableFuture<byte[]> wrong = new ContentSpillStore(b).get(root, parts.length - 5L);
        assertThat(until(() -> right.isDone() && wrong.isDone())).isTrue();
        assertThat(right.join()).isEqualTo(parts);
        assertThatThrownBy(wrong::join).hasCauseInstanceOf(IllegalArgumentException.class).hasMessageContaining("the stub states");
    }
}
