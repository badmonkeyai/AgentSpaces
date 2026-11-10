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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.Messages;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Intra-chunk deltas over the tree exchange (PDT 2 phase 4, P2-Z-01, P2-Z-02, P2-F-05; srad/agentspaces-pdt2.md
 * §3.8): a level-2 holder answers a level-2 asker's {@code TREE_WANT} with a {@code dcz} frame in place of a
 * changed chunk when the asker's haves hold the frame's base and the frame is under half the chunk; the asker
 * rebuilds the chunk, verifies it against its CID, and stores the chunk. Every other case moves the chunk whole.
 */
class NodeDeltasTest {

    private static final int TICK = 250;
    private static final int CHUNKS = 8;
    private static final int CHUNK_BYTES = 64 * 1024;
    private static final int EDITED = 5;

    /** Two versions of a one-level tree over raw chunks: the second changes one byte of chunk {@link #EDITED}. */
    private record Versions(NodeFixtures.Tree v1, NodeFixtures.Tree v2, String base, String chunk, byte[] frame) {
    }

    private static Versions versions() {
        Random random = new Random(5);
        List<byte[]> chunks = new ArrayList<>();
        for (int i = 0; i < CHUNKS; i++) {
            byte[] chunk = new byte[CHUNK_BYTES];
            random.nextBytes(chunk);
            chunks.add(chunk);
        }
        NodeFixtures.Tree v1 = tree(chunks);
        byte[] base = chunks.get(EDITED);
        byte[] edited = base.clone();
        edited[20_000] ^= 0x5a;
        chunks.set(EDITED, edited);
        NodeFixtures.Tree v2 = tree(chunks);
        return new Versions(v1, v2, Cids.of(base, Cids.RAW), Cids.of(edited, Cids.RAW), Dcz.encode(base, edited).orElseThrow());
    }

    private static NodeFixtures.Tree tree(List<byte[]> chunks) {
        Map<String, byte[]> nodes = new LinkedHashMap<>();
        List<String> links = new ArrayList<>();
        for (byte[] chunk : chunks) {
            String cid = Cids.of(chunk, Cids.RAW);
            nodes.put(cid, chunk);
            links.add(cid);
        }
        byte[] root = NodeFixtures.node(links, new byte[]{'r'});
        nodes.put(NodeFixtures.cid(root), root);
        return new NodeFixtures.Tree(NodeFixtures.cid(root), nodes, 1);
    }

    /** A root over one interior node of the chunks and one tail chunk, so a single TREE_WANT walks every chunk. */
    private static NodeFixtures.Tree underOneWalk(List<byte[]> chunks, byte[] tail) {
        NodeFixtures.Tree inner = tree(chunks);
        Map<String, byte[]> nodes = new LinkedHashMap<>(inner.nodes());
        String tailCid = Cids.of(tail, Cids.RAW);
        nodes.put(tailCid, tail);
        byte[] root = NodeFixtures.node(List.of(inner.root(), tailCid), new byte[]{'w'});
        nodes.put(NodeFixtures.cid(root), root);
        return new NodeFixtures.Tree(NodeFixtures.cid(root), nodes, 2);
    }

    /** A fabric with one holder of both versions that keeps the frame, and an asker that holds the first version. */
    private static SimTreeFabric.Holder holder(SimTreeFabric fabric, Versions v) {
        return levelTwoHolder(fabric, v.v1(), v.v2(), List.of(new Messages.NodeDelta(v.chunk(), v.base(), v.frame())));
    }

    /** A level-2 holder of both versions that keeps these frames, for a level-2 asker. */
    private static SimTreeFabric.Holder levelTwoHolder(SimTreeFabric fabric, NodeFixtures.Tree v1, NodeFixtures.Tree v2,
                                                       List<Messages.NodeDelta> frames) {
        SimTreeFabric.Holder holder = fabric.holder("zHolder").holds(v1).holds(v2);
        frames.forEach(holder.store::keepDelta);
        holder.levelTwo = true;
        holder.askerReadsLevelTwo = true;
        fabric.readsDeltas = true;
        return holder;
    }

    private static TreeFetchSession start(SimTreeFabric fabric, MemoryTreeStore mine, String root, List<String> haves) {
        TreeFetchSession session = new TreeFetchSession(root, haves, List.of(), fabric, mine, FetchOptions.defaults(), 16, 32);
        fabric.session = session;
        session.start();
        fabric.pump();
        return session;
    }

    private static MemoryTreeStore holding(NodeFixtures.Tree tree) {
        return new MemoryTreeStore().putAll(tree);
    }

    private static List<Messages.NodeDelta> deltasSent(SimTreeFabric.Holder holder) {
        List<Messages.NodeDelta> out = new ArrayList<>();
        for (Object m : holder.sent) {
            if (m instanceof Messages.Nodes nodes && nodes.deltas() != null) {
                out.addAll(nodes.deltas());
            }
        }
        return out;
    }

    @Test
    void aLevelTwoAskerWhoseHavesHoldTheBaseGetsTheFrameAndStoresTheVerifiedChunk() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        MemoryTreeStore mine = holding(v.v1());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(50, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(mine.get(v.chunk())).get().isEqualTo(v.v2().nodes().get(v.chunk()));
        assertThat(deltasSent(holder)).extracting(Messages.NodeDelta::cid).containsExactly(v.chunk());
        assertThat(holder.server.deltasServed()).isEqualTo(1);
        assertThat(session.deltasApplied()).isEqualTo(1);
        long rootBytes = v.v2().nodes().get(v.v2().root()).length;
        assertThat(session.progress().bytesReceived()).as("the root and the frame; the chunk moved as %d bytes", v.frame().length)
                .isEqualTo(rootBytes + v.frame().length).isLessThan(8 * 1024);
        assertThat(fabric.strikes).isEmpty();
    }

    @Test
    void aLevelOneAskerGetsTheChunkWhole() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        holder.askerReadsLevelTwo = false;
        MemoryTreeStore mine = holding(v.v1());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(50, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(deltasSent(holder)).isEmpty();
        assertThat(session.progress().bytesReceived()).isGreaterThan(CHUNK_BYTES);
    }

    @Test
    void aLevelOneAskerSentFramesAnywayIgnoresThemAndAsksForTheChunks() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        fabric.readsDeltas = false;
        MemoryTreeStore mine = holding(v.v1());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(50, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(deltasSent(holder)).hasSize(1);
        assertThat(session.deltasApplied() + session.deltasRefused()).isZero();
        assertThat(holder.askedByNodeWantFor(v.chunk())).isTrue();
        assertThat(fabric.strikes).isEmpty();
    }

    @Test
    void aLevelOneHolderSendsNoFrame() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        holder.levelTwo = false;
        MemoryTreeStore mine = holding(v.v1());
        start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(50, TICK)).isTrue();
        assertThat(deltasSent(holder)).isEmpty();
        assertThat(mine.complete(v.v2().root())).isTrue();
    }

    @Test
    void anAskerWhoseHavesDoNotShowTheBaseGetsTheChunkWhole() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        MemoryTreeStore mine = holding(v.v1());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of());

        assertThat(fabric.runUntilDone(50, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(deltasSent(holder)).isEmpty();
    }

    @Test
    void aFrameThatIsNotUnderHalfTheChunkTravelsAsTheChunk() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        byte[] large = new byte[CHUNK_BYTES / 2];
        System.arraycopy(v.frame(), 0, large, 0, v.frame().length);
        holder.store.keepDelta(new Messages.NodeDelta(v.chunk(), v.base(), large));
        MemoryTreeStore mine = holding(v.v1());
        start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(50, TICK)).isTrue();
        assertThat(deltasSent(holder)).isEmpty();
        assertThat(mine.complete(v.v2().root())).isTrue();
    }

    @Test
    void anAskerThatLacksTheBaseIgnoresTheFrameAndAsksForTheChunk() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        MemoryTreeStore mine = holding(v.v1());
        mine.remove(v.base());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(50, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(deltasSent(holder)).hasSize(1);
        assertThat(session.deltasApplied()).isZero();
        assertThat(holder.askedByNodeWantFor(v.chunk())).as("P2-Z-01: the chunk is asked for by NODE_WANT").isTrue();
        assertThat(fabric.strikes).isEmpty();
    }

    @Test
    void aFrameThatDoesNotOpenIsRefusedWithoutAStrikeAndTheChunkIsAskedForWhole() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        byte[] vcdiff = v.frame().clone();
        vcdiff[0] = Dcz.VCDIFF;
        holder.store.keepDelta(new Messages.NodeDelta(v.chunk(), v.base(), vcdiff));
        MemoryTreeStore mine = holding(v.v1());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(50, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(session.deltasRefused()).isEqualTo(1);
        assertThat(fabric.strikes).isEmpty();
        assertThat(session.trace()).anySatisfy(line -> assertThat(line).contains("VCDIFF"));
    }

    @Test
    void aHolderThatRepeatsAFrameThatDoesNotOpenGetsOneOpeningPerSession() {
        // PDT 2 phase 4, P2-Q-SEC-05: a frame that does not open strikes nobody, yet each one costs the asker a read
        // and a hash of its base, so a session opens one such frame per source and asks for the rest whole
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        byte[] vcdiff = v.frame().clone();
        vcdiff[0] = Dcz.VCDIFF;
        List<Messages.NodeDelta> repeated = Collections.nCopies(Messages.MAX_WANT_CIDS,
                new Messages.NodeDelta(v.chunk(), v.base(), vcdiff));
        holder.tamper = m -> m instanceof Messages.Nodes nodes && nodes.deltas() != null
                ? new Messages.Nodes(Messages.NODES, nodes.req(), nodes.nodes(), null, null, nodes.missing(), repeated) : m;
        MemoryTreeStore mine = holding(v.v1());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(50, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(session.deltasRefused()).as("one frame opened of %d", repeated.size()).isEqualTo(1);
        assertThat(fabric.strikes).isEmpty();
    }

    @Test
    void aWalkWithMoreFramesThanOneNodesMayCarrySplitsThemAcrossMessages() {
        // PDT 2 phase 4, P2-Z-01: a receiver drops a NODES past MAX_WANT_CIDS deltas as a violation and asks again,
        // so a holder that packed more frames into one NODES would answer that walk the same way forever
        int edited = Messages.MAX_WANT_CIDS + 44;
        Random random = new Random(6);
        List<byte[]> before = new ArrayList<>();
        List<byte[]> after = new ArrayList<>();
        for (int i = 0; i < edited; i++) {
            byte[] chunk = new byte[4096];
            random.nextBytes(chunk);
            before.add(chunk);
            byte[] changed = chunk.clone();
            changed[i] ^= 0x5a;
            after.add(changed);
        }
        byte[] tail = new byte[4096];
        random.nextBytes(tail);
        NodeFixtures.Tree v1 = underOneWalk(before, tail);
        NodeFixtures.Tree v2 = underOneWalk(after, tail);
        List<Messages.NodeDelta> frames = new ArrayList<>();
        for (int i = 0; i < edited; i++) {
            frames.add(new Messages.NodeDelta(Cids.of(after.get(i), Cids.RAW), Cids.of(before.get(i), Cids.RAW),
                    Dcz.encode(before.get(i), after.get(i)).orElseThrow()));
        }

        SimTreeFabric.Holder alone = levelTwoHolder(new SimTreeFabric(), v1, v2, frames);
        String walked = NodeFixtures.linksOf(v2.nodes().get(v2.root())).get(0);
        alone.server.serve(PeerId.of("zAsker"),
                new Messages.TreeWant(1, walked, List.of(v1.root()), Messages.MAX_TREE_BUDGET));
        assertThat(deltasSent(alone)).hasSize(edited);
        assertThat(alone.sent).filteredOn(m -> m instanceof Messages.Nodes)
                .allSatisfy(m -> assertThat(((Messages.Nodes) m).violation()).isEmpty());

        SimTreeFabric fabric = new SimTreeFabric();
        levelTwoHolder(fabric, v1, v2, frames);
        MemoryTreeStore mine = holding(v1);
        TreeFetchSession session = start(fabric, mine, v2.root(), List.of(v1.root()));
        assertThat(fabric.runUntilDone(50, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v2.root())).isTrue();
        assertThat(session.deltasApplied()).isEqualTo(edited);
    }

    @Test
    void aFrameThatRebuildsOtherBytesThanItsCidNamesStrikesTheHolderAndStoresNothing() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        SimTreeFabric.Holder honest = fabric.holder("zHonest").holds(v.v1()).holds(v.v2());
        byte[] base = v.v1().nodes().get(v.base());
        byte[] other = base.clone();
        other[3] ^= 1;
        holder.store.keepDelta(new Messages.NodeDelta(v.chunk(), v.base(), Dcz.encode(base, other).orElseThrow()));
        MemoryTreeStore mine = holding(v.v1());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(100, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(mine.get(v.chunk())).get().isEqualTo(v.v2().nodes().get(v.chunk()));
        assertThat(fabric.strikes).hasSize(1);
        assertThat(fabric.strikes.get(0)).contains("zHolder").contains("rebuilds");
        assertThat(honest.server.nodesServed()).isPositive();
    }

    @Test
    void aFrameForAChunkNoVerifiedNodeLinksStrikesTheHolder() {
        Versions v = versions();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder holder = holder(fabric, v);
        fabric.holder("zHonest").holds(v.v1()).holds(v.v2());
        byte[] base = v.v1().nodes().get(v.base());
        byte[] foreign = new byte[1000];
        new Random(9).nextBytes(foreign);
        String foreignCid = Cids.of(foreign, Cids.RAW);
        Messages.NodeDelta stray = new Messages.NodeDelta(foreignCid, v.base(), Dcz.encode(base, foreign).orElseThrow());
        holder.tamper = m -> m instanceof Messages.Nodes nodes && nodes.deltas() != null
                ? new Messages.Nodes(Messages.NODES, nodes.req(), nodes.nodes(), null, null, nodes.missing(), List.of(stray)) : m;
        MemoryTreeStore mine = holding(v.v1());
        TreeFetchSession session = start(fabric, mine, v.v2().root(), List.of(v.v1().root()));

        assertThat(fabric.runUntilDone(100, TICK)).as(session.trace().toString()).isTrue();
        assertThat(mine.complete(v.v2().root())).isTrue();
        assertThat(mine.has(foreignCid)).isFalse();
        assertThat(fabric.strikes).hasSize(1);
        assertThat(fabric.strikes.get(0)).contains("zHolder").contains("no verified node links");
    }
}
