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
package ai.badmonkey.agentspaces.pdt.blob;

import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Dcz;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.seal.NodeSealer;
import ai.badmonkey.agentspaces.pdt.seal.SealedNodeStore;
import ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The writer's side table of intra-chunk deltas (PDT 2 phase 4, P2-Z-02; srad/agentspaces-pdt2.md §3.8): an edit
 * computes a {@code dcz} frame for each new chunk over the chunk it replaced, keeps it keyed by {@code (base, cid)}
 * for {@code deltaRetention} (one hour), and keeps only a frame under half its chunk.
 */
class ChunkDeltasTest {

    private final TestClock clock = TestClock.create();
    private final NodeStore store = new NodeStore(new MemoryBlockStore(), clock);

    /** A member at level 2 reads the frames (PDT 2 phase 4 measures, §10.14); without one an edit keeps none. */
    @BeforeEach
    void someMemberReadsLevelTwo() {
        store.deltas().audience(() -> true);
    }

    private static byte[] seeded(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private record Edit(PBlob before, PBlob after, BlobTree.Located old, BlobTree.Located edited) {
    }

    private static Edit oneByteEdit(NodeStore store, PBlob before, long offset) {
        byte old = before.read(offset, 1)[0];
        PBlob after = before.splice(offset, 1, new byte[]{(byte) (old ^ 0x5a)});
        return new Edit(before, after, before.tree().seek(offset), after.tree().seek(offset));
    }

    @Test
    void aOneByteEditKeepsAFrameKeyedByTheReplacedChunkThatRebuildsTheNewOne() {
        PBlob v1 = PBlob.of(seeded(2 << 20, 1), store);
        Edit edit = oneByteEdit(store, v1, 1_000_001L);
        assertThat(edit.edited().cid()).isNotEqualTo(edit.old().cid());

        List<Messages.NodeDelta> deltas = store.deltas().deltas(edit.edited().cid());
        assertThat(deltas).hasSize(1);
        Messages.NodeDelta delta = deltas.get(0);
        assertThat(delta.cid()).isEqualTo(edit.edited().cid());
        assertThat(delta.base()).isEqualTo(edit.old().cid());
        byte[] base = store.get(delta.base()).orElseThrow();
        byte[] chunk = store.get(delta.cid()).orElseThrow();
        assertThat(Dcz.decode(base, delta.dcz())).isEqualTo(chunk);
        assertThat(delta.dcz().length).as("a one-byte edit of a %d-byte chunk", chunk.length).isLessThan(200);
        assertThat(PdtSwarm.treeStore(store).deltas(delta.cid())).as("the tree exchange reads the same table").containsExactly(delta);
    }

    @Test
    void aFrameLapsesAfterTheRetention() {
        PBlob v1 = PBlob.of(seeded(1 << 20, 2), store);
        Edit edit = oneByteEdit(store, v1, 300_000L);
        assertThat(store.deltas().retention()).isEqualTo(Duration.ofHours(1)).isEqualTo(ChunkDeltas.DEFAULT_RETENTION);
        clock.advance(Duration.ofMinutes(59));
        assertThat(store.deltas().deltas(edit.edited().cid())).hasSize(1);
        clock.advance(Duration.ofMinutes(2));
        assertThat(store.deltas().deltas(edit.edited().cid())).isEmpty();
        assertThat(store.deltas().size()).isZero();
    }

    @Test
    void anEditThroughASiblingStoreKeepsItsFramesInTheSameTable() {
        PBlob v1 = PBlob.of(seeded(1 << 20, 3), store);
        NodeStore sibling = store.over(store.backend());
        Edit edit = oneByteEdit(sibling, PBlob.load(sibling, v1.root()), 700_000L);
        assertThat(store.deltas().deltas(edit.edited().cid())).hasSize(1);
        assertThat(sibling.deltas()).isSameAs(store.deltas());
    }

    @Test
    void aStoreWithDeltasOffAndASealedStoreKeepNoFrames() {
        store.deltas().enabled(false);
        Edit off = oneByteEdit(store, PBlob.of(seeded(1 << 20, 4), store), 500_000L);
        assertThat(store.deltas().deltas(off.edited().cid())).isEmpty();

        SealedNodeStore sealed = new SealedNodeStore(new MemoryBlockStore(), clock,
                new NodeSealer(GroupKeyRing.empty(), clock));
        assertThat(sealed.deltas().enabled()).as("a sealed chunk's CID names its envelope, which no frame rebuilds").isFalse();
    }

    @Test
    void aRewrittenRangeKeepsOnlyFramesUnderHalfTheirChunk() {
        PBlob v1 = PBlob.of(seeded(2 << 20, 5), store);
        PBlob v2 = v1.splice(600_000L, 400_000L, seeded(400_000, 6));
        int kept = 0;
        for (BlobTree.Entry chunk : v2.chunks()) {
            for (Messages.NodeDelta delta : store.deltas().deltas(chunk.cid())) {
                kept++;
                assertThat(2L * delta.dcz().length).isLessThan(chunk.length());
            }
        }
        assertThat(kept).as("random new bytes compress against no old chunk").isLessThanOrEqualTo(2);
        assertThat(store.deltas().bytes()).isLessThanOrEqualTo(ChunkDeltas.MAX_BYTES);
    }

    @Test
    void anEditKeepsFramesOnlyWhileSomeMemberCouldReceiveThem() {
        // PDT 2 phase 4 measures (owner default of 2026-11-09, srad/agentspaces-pdt2.md §10.14): a table records only
        // while its audience answers true, and a store no swarm attached has none
        NodeStore alone = new NodeStore(new MemoryBlockStore(), clock);
        assertThat(alone.deltas().recording()).isFalse();
        Edit first = oneByteEdit(alone, PBlob.of(seeded(1 << 20, 7), alone), 400_000L);
        assertThat(alone.deltas().size()).as("no audience").isZero();

        AtomicBoolean levelTwo = new AtomicBoolean();
        alone.deltas().audience(levelTwo::get);
        Edit second = oneByteEdit(alone, first.after(), 500_000L);
        assertThat(alone.deltas().size()).as("no member reads level 2").isZero();
        assertThat(alone.deltas().record(second.old().cid(), alone.get(second.old().cid()).orElseThrow(), second.edited().cid(),
                alone.get(second.edited().cid()).orElseThrow())).as("a direct record follows the same rule").isFalse();

        levelTwo.set(true);
        assertThat(alone.deltas().recording()).isTrue();
        Edit third = oneByteEdit(alone, second.after(), 600_000L);
        assertThat(alone.deltas().deltas(third.edited().cid())).as("a member reads level 2").hasSize(1);
        alone.deltas().enabled(false);
        assertThat(alone.deltas().recording()).as("the builder option still turns it off").isFalse();
    }
}
