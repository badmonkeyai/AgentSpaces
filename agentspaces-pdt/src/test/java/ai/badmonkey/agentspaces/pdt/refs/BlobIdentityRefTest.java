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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.blob.BaoOutboard;
import ai.badmonkey.agentspaces.pdt.blob.Blake3;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A blob ref names its identity (PDT 2 phase 4, P2-B-01, P2-B-02, P2-F-06; srad/agentspaces-pdt2.md §3.5, rule 10 of
 * §6.10.3): every head of a blob ref carries {@code pdt.blake3} and {@code pdt.bao}, the writer keeps the outboard,
 * which no head carries inline and no blob node links, and a reader reaches it through {@code pdt.bao} alone to read
 * a verified window.
 */
class BlobIdentityRefTest {

    private static final String NAME = "model";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static PRef latest(RefFleet.Peer peer) {
        return RefFleet.headEntries(peer, NAME).stream().map(Space.Entry::value)
                .max(Comparator.comparingLong(PRef::seq)).orElseThrow();
    }

    private static byte[] seeded(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    @Test
    void aBlobHeadNamesItsIdentityAndAReaderVerifiesAWindowThroughTheOutboard() throws Exception {
        RefFleet.Peer w = fleet.newPeer("w", 1);
        RefFleet.Peer r = fleet.newPeer("r", 2, "w");
        fleet.settle(12);
        Ref<PBlob> onW = w.pdt().blobRef(NAME);
        Ref<PBlob> onR = r.pdt().blobRef(NAME);
        byte[] bytes = seeded(600_000, 9);
        PBlob v1 = onW.swap(empty -> PBlob.of(bytes, empty.store()));
        BaoOutboard.Built outboard = BaoOutboard.of(new ByteArrayInputStream(bytes));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onR.heads().roots().equals(List.of(v1.root())))).isTrue();

        PRef head = latest(r);
        assertThat(head.v()).as("the b node keeps its schema, so the head stays version 1 (rule 5)").isEqualTo(PRef.VERSION);
        assertThat(head.attrs().get(Manifest.BLAKE3_ATTR)).isEqualTo(Cids.identity(Blake3.digest(bytes)));
        assertThat(head.attrs().get(Manifest.BAO_ATTR)).isEqualTo(outboard.root());
        assertThat(w.store().has(outboard.root())).isTrue();
        if (head.nodes() != null) {
            for (byte[] node : head.nodes()) {
                assertThat(outboard.nodes()).as("no bo node rides inline").doesNotContainKey(Cids.of(node, Cids.DAG_CBOR));
            }
        }
        for (int i = 0; i < 8; i++) {
            fleet.tickAll();
        }
        assertThat(w.store().has(outboard.root())).as("the collector keeps the outboard of a live version").isTrue();

        PBlob read = onR.deref();
        assertThat(read.claim()).isPresent();
        assertThat(read.claim().orElseThrow().identity()).isEqualTo(outboard.identity());
        assertThat(r.store().has(outboard.root())).as("a reader fetches the outboard only to verify").isFalse();
        try (InputStream in = read.openVerified(250_000, 40_000)) {
            assertThat(in.readAllBytes()).isEqualTo(Arrays.copyOfRange(bytes, 250_000, 290_000));
        }
        assertThat(r.store().has(outboard.root())).isTrue();
    }

    @Test
    void aRewriterOfALapsedBlobHeadHoldsTheOutboardItNamesSoAWindowVerifiesAfterTheWriterLeaves() throws Exception {
        // PDT 2 phase 4 gate, P2-B-01 and P2-B-03 with PD-R-08: a member that rewrites a lapsed blob head names the
        // pair only for an outboard it holds, so a verified window still reads once the writer is gone
        RefFleet.Spec shortLease = RefFleet.Spec.defaults().pdt(b -> b.headLease(Duration.ofSeconds(20)).loadTimeout(Duration.ofSeconds(2)));
        RefFleet.Peer a = fleet.newPeer("a", 7, shortLease);
        RefFleet.Peer b = fleet.newPeer("b", 8, shortLease, "a");
        RefFleet.Peer c = fleet.newPeer("c", 9, shortLease, "a");
        fleet.settle(12);
        Ref<PBlob> onA = a.pdt().blobRef(NAME);
        Ref<PBlob> onB = b.pdt().blobRef(NAME);
        Ref<PBlob> onC = c.pdt().blobRef(NAME);
        byte[] bytes = seeded(600_000, 13);
        PBlob v1 = onA.swap(empty -> PBlob.of(bytes, empty.store()));
        String bao = BaoOutboard.of(new ByteArrayInputStream(bytes)).root();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root())
                && onB.heads().roots().equals(List.of(v1.root())) && onC.heads().roots().equals(List.of(v1.root())))).isTrue();
        assertThat(b.store().has(bao) || c.store().has(bao)).as("followers fetch no outboard").isFalse();

        fleet.kill(a);
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> b.pdt().stats().headsRewritten() + c.pdt().stats().headsRewritten() >= 1))
                .as("a follower rewrote the lapsed head").isTrue();
        fleet.settle(4);
        for (RefFleet.Peer rewriter : List.of(b, c)) {
            if (rewriter.pdt().stats().headsRewritten() > 0) {
                assertThat(latest(rewriter).attrs().get(Manifest.BAO_ATTR)).isEqualTo(bao);
                assertThat(rewriter.store().has(bao)).as("%s names an outboard it holds", rewriter.name()).isTrue();
            }
        }
        RefFleet.Peer reader = b.pdt().stats().headsRewritten() > 0 ? c : b;
        Ref<PBlob> onReader = reader == b ? onB : onC;
        try (InputStream in = onReader.deref().openVerified(250_000, 40_000)) {
            assertThat(in.readAllBytes()).isEqualTo(Arrays.copyOfRange(bytes, 250_000, 290_000));
        }
    }

    @Test
    void aFacadeBuiltWithoutDeltasKeepsNoFrames() throws Exception {
        // PDT 2 phase 4, P2-Z-02 and P2-Q-MNT-01: deltas is a builder option, on by default; both members advertise
        // level 2, so a frame could travel between them (PDT 2 phase 4 measures, §10.14)
        RefFleet.Spec levelTwo = RefFleet.Spec.defaults().content(b -> b.pdtLevel(2));
        RefFleet.Peer on = fleet.newPeer("on", 5, levelTwo);
        RefFleet.Peer off = fleet.newPeer("off", 6,
                levelTwo.pdt(b -> b.deltas(false).deltaRetention(Duration.ofMinutes(5))), "on");
        fleet.settle(12);
        assertThat(on.store().deltas().enabled()).isTrue();
        assertThat(off.store().deltas().enabled()).isFalse();
        assertThat(off.store().deltas().retention()).isEqualTo(Duration.ofMinutes(5));
        Ref<PBlob> onOff = off.pdt().blobRef(NAME);
        onOff.swap(empty -> PBlob.of(seeded(300_000, 11), empty.store()));
        onOff.swap(blob -> blob.splice(100_000, 1, new byte[]{7}));
        assertThat(off.store().deltas().size()).isZero();
        Ref<PBlob> onOn = on.pdt().blobRef("other");
        onOn.swap(empty -> PBlob.of(seeded(300_000, 12), empty.store()));
        onOn.swap(blob -> blob.splice(100_000, 1, new byte[]{7}));
        assertThat(on.store().deltas().size()).as("a swap's edit keeps its frame in the facade's store").isEqualTo(1);
    }

    @Test
    void anEditedBlobNamesTheIdentityOfItsNewBytes() throws Exception {
        RefFleet.Peer w = fleet.newPeer("w", 3);
        RefFleet.Peer r = fleet.newPeer("r", 4, "w");
        fleet.settle(12);
        Ref<PBlob> onW = w.pdt().blobRef(NAME);
        Ref<PBlob> onR = r.pdt().blobRef(NAME);
        byte[] bytes = seeded(400_000, 10);
        onW.swap(empty -> PBlob.of(bytes, empty.store()));
        PBlob v2 = onW.swap(blob -> blob.splice(123_456, 1, new byte[]{42}));
        bytes[123_456] = 42;
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onR.heads().roots().equals(List.of(v2.root())))).isTrue();
        assertThat(latest(r).attrs().get(Manifest.BLAKE3_ATTR)).isEqualTo(Cids.identity(Blake3.digest(bytes)));
        try (InputStream in = onR.deref().openVerified(123_000, 1000)) {
            assertThat(in.readAllBytes()).isEqualTo(Arrays.copyOfRange(bytes, 123_000, 124_000));
        }
    }
}
