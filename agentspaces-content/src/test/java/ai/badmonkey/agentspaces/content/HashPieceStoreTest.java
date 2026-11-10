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
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 1, P2-H-01 to P2-H-04 (srad/agentspaces-pdt2.md §3.3): the hash-keyed piece store
 * keeps one file per piece hash, an index per root, and a reference count per hash, so two
 * artifacts that share a piece hold it once, a second artifact starts with the pieces it shares
 * with a held one, and a file store converts by hashing each piece once.
 */
class HashPieceStoreTest {

    private static final int PIECE = 64 * 1024;
    private static final PeerId ORIGIN = PeerIdentity.generate().peerId();

    private final CborCodec codec = CborCodec.defaultCodec();
    private final TestClock clock = TestClock.create();

    @TempDir
    Path dir;

    /** Ten pieces; the first {@code shared} come from seed 1, the rest from {@code seed}. */
    private static byte[] artifact(int shared, long seed) {
        byte[] bytes = new byte[10 * PIECE];
        byte[] common = new byte[10 * PIECE];
        new Random(1).nextBytes(common);
        new Random(seed).nextBytes(bytes);
        System.arraycopy(common, 0, bytes, 0, shared * PIECE);
        return bytes;
    }

    private static Manifest manifest(byte[] artifact) {
        ContentRoots.Hashed hashed = ContentRoots.hash(artifact, PIECE);
        return new Manifest(Manifest.VERSION, hashed.root(), hashed.size(), PIECE, hashed.pieces(PIECE), hashed.table(),
                null, null, "a.bin", null, Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN, ORIGIN, 1L, null);
    }

    private static byte[] piece(byte[] artifact, int index) {
        return java.util.Arrays.copyOfRange(artifact, index * PIECE, Math.min(artifact.length, (index + 1) * PIECE));
    }

    private static void writeAll(PieceStore store, Manifest m, byte[] artifact) {
        store.open(m);
        for (int i = 0; i < m.pieces(); i++) {
            store.write(m.root(), i, piece(artifact, i));
        }
    }

    private List<Path> pieceFiles() throws IOException {
        Path pieces = dir.resolve(HashPieceStore.PIECES);
        if (!Files.isDirectory(pieces)) {
            return List.of();
        }
        try (Stream<Path> walk = Files.walk(pieces)) {
            return walk.filter(Files::isRegularFile).toList();
        }
    }

    private static byte[] readAll(SeekableByteChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
        while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
            // drain
        }
        return buffer.array();
    }

    @Test
    void piecesLiveUnderTheirHashAndTwoArtifactsShareOneFile() throws IOException {
        HashPieceStore store = new HashPieceStore(dir, codec, clock, 0);
        byte[] a = artifact(6, 2);
        byte[] b = artifact(6, 3);
        Manifest ma = manifest(a);
        Manifest mb = manifest(b);
        writeAll(store, ma, a);
        String hex = HexFormat.of().formatHex(Digests.sha256(piece(a, 0)));
        assertThat(dir.resolve(HashPieceStore.PIECES).resolve(hex.substring(0, 2)).resolve(hex)).isRegularFile();
        assertThat(dir.resolve(HashPieceStore.ROOTS).resolve(store.dirName(ma.root())).resolve(HashPieceStore.INDEX)).isRegularFile();
        assertThat(pieceFiles()).hasSize(10);
        writeAll(store, mb, b);
        assertThat(pieceFiles()).as("the six shared pieces are held once").hasSize(14);
        assertThat(store.references(hex)).isEqualTo(2);
        assertThat(store.bytesUsed()).as("bytes counted once per hash").isEqualTo(14L * PIECE);
        assertThat(store.read(mb.root(), 2, 10, 100)).contains(java.util.Arrays.copyOfRange(b, 2 * PIECE + 10, 2 * PIECE + 110));
        try (SeekableByteChannel channel = store.openArtifact(mb.root())) {
            assertThat(readAll(channel)).isEqualTo(b);
            channel.position(5L * PIECE + 7);
            ByteBuffer one = ByteBuffer.allocate(PIECE);
            assertThat(channel.read(one)).isEqualTo(PIECE);
            assertThat(one.array()).isEqualTo(java.util.Arrays.copyOfRange(b, 5 * PIECE + 7, 6 * PIECE + 7));
        }
        store.forget(ma.root());
        assertThat(pieceFiles()).as("forget unlinks, and deletes only what no other root names").hasSize(10);
        assertThat(store.references(hex)).isEqualTo(1);
        try (SeekableByteChannel channel = store.openArtifact(mb.root())) {
            assertThat(readAll(channel)).isEqualTo(b);
        }
        store.forget(mb.root());
        assertThat(pieceFiles()).isEmpty();
        assertThat(store.roots()).isEmpty();
    }

    @Test
    void aWriteVerifiesAgainstTheTableAndAHeldHashLinks() throws IOException {
        HashPieceStore store = new HashPieceStore(dir, codec, clock, 0);
        byte[] a = artifact(0, 2);
        Manifest ma = manifest(a);
        store.open(ma);
        assertThatThrownBy(() -> store.write(ma.root(), 0, piece(a, 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("piece 0");
        assertThat(store.have(ma.root()).isEmpty()).isTrue();
        store.write(ma.root(), 1, piece(a, 1));
        long modified = Files.getLastModifiedTime(pieceFiles().get(0)).toMillis();
        store.write(ma.root(), 1, piece(a, 1));
        assertThat(pieceFiles()).hasSize(1);
        assertThat(Files.getLastModifiedTime(pieceFiles().get(0)).toMillis()).isEqualTo(modified);
    }

    /** P2-H-02: the second artifact starts with the 60 % it shares; the fetch asks for the other 40 %. */
    @Test
    void openSeedsHaveFromHeldHashes() throws IOException {
        HashPieceStore store = new HashPieceStore(dir, codec, clock, 0);
        byte[] a = artifact(6, 2);
        byte[] b = artifact(6, 3);
        writeAll(store, manifest(a), a);
        Manifest mb = manifest(b);
        store.open(mb);
        BitSet have = store.have(mb.root());
        assertThat(have.cardinality()).isEqualTo(6);
        assertThat(have.nextClearBit(0)).isEqualTo(6);
        assertThat(store.read(mb.root(), 3, 0, PIECE)).contains(piece(b, 3));
    }

    @Test
    void aRestartReadsTheIndexesBack() throws IOException {
        byte[] a = artifact(6, 2);
        byte[] b = artifact(6, 3);
        Manifest ma = manifest(a);
        Manifest mb = manifest(b);
        HashPieceStore first = new HashPieceStore(dir, codec, clock, 0);
        writeAll(first, ma, a);
        first.open(mb);
        for (int i = 6; i < 8; i++) {
            first.write(mb.root(), i, piece(b, i));
        }
        first.pin(mb.root());
        first.flush();
        HashPieceStore again = new HashPieceStore(dir, codec, clock, 0);
        assertThat(again.roots()).containsExactlyInAnyOrder(ma.root(), mb.root());
        assertThat(again.manifest(mb.root())).contains(mb);
        assertThat(again.have(mb.root()).cardinality()).isEqualTo(8);
        assertThat(again.pinned(mb.root())).isTrue();
        assertThat(again.bytesUsed()).isEqualTo(12L * PIECE);
        try (SeekableByteChannel channel = again.openArtifact(ma.root())) {
            assertThat(readAll(channel)).isEqualTo(a);
        }
        again.forget(ma.root());
        assertThat(pieceFiles()).as("the reference counts came back with the indexes").hasSize(8);
    }

    /** P2-H-04: a file store converts by hashing each held piece once; shared pieces land once. */
    @Test
    void importFromAFileStoreHashesEachPieceOnce() throws IOException {
        FilePieceStore files = new FilePieceStore(dir.resolve("file"), codec, clock, 0);
        byte[] a = artifact(6, 2);
        byte[] b = artifact(6, 3);
        Manifest ma = manifest(a);
        Manifest mb = manifest(b);
        writeAll(files, ma, a);
        files.open(mb);
        for (int i = 0; i < 9; i++) {
            files.write(mb.root(), i, piece(b, i));
        }
        files.pin(ma.root());
        HashPieceStore store = new HashPieceStore(dir.resolve("hash"), codec, clock, 0);
        HashPieceStore.Import result = store.importFrom(files);
        assertThat(result.roots()).isEqualTo(2);
        assertThat(result.piecesHashed()).as("each held piece hashed once").isEqualTo(19);
        assertThat(result.piecesStored()).as("the shared pieces are stored once").isEqualTo(13);
        assertThat(store.have(mb.root())).isEqualTo(files.have(mb.root()));
        assertThat(store.pinned(ma.root())).isTrue();
        try (SeekableByteChannel channel = store.openArtifact(ma.root())) {
            assertThat(readAll(channel)).isEqualTo(a);
        }
        assertThat(store.read(mb.root(), 8, 0, PIECE)).contains(piece(b, 8));
    }

    @Test
    void evictionDropsTheLeastRecentlyServedUnpinnedRoot() throws IOException {
        HashPieceStore store = new HashPieceStore(dir, codec, clock, 12L * PIECE);
        byte[] a = artifact(0, 2);
        byte[] b = artifact(0, 3);
        writeAll(store, manifest(a), a);
        clock.advance(java.time.Duration.ofSeconds(1));
        writeAll(store, manifest(b), b);
        assertThat(store.roots()).containsExactly(manifest(b).root());
        assertThat(store.bytesUsed()).isEqualTo(10L * PIECE);
        assertThat(pieceFiles()).hasSize(10);
    }
}
