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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link TreePublisher} (PD-B-05): entries by relative path with size, mode, and media
 * type; symbolic links inside the root as Git does them; links that leave the root
 * refused; and a re-publish that shares every unchanged file's blob. Most cases use a
 * stand-in writer that names the sorted entries by the CID of their canonical CBOR, so
 * they exercise the walk alone; the last case publishes through the default
 * {@link TreePublisher#pmapWriter()} and reads the {@code PMap} back.
 */
class TreePublisherTest {

    /** A stand-in for the PMap writer: deterministic in the entries, writes nothing. */
    private static final TreePublisher.MapWriter STAND_IN = (entries, store) -> {
        Map<String, Object> plain = new LinkedHashMap<>();
        entries.forEach((k, v) -> plain.put(k, v.toMap()));
        return Cids.of(CborCodec.defaultCodec().toBytes(plain), Cids.DAG_CBOR);
    };

    private static void write(Path dir, String relative, byte[] bytes) throws IOException {
        Path file = dir.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.write(file, bytes);
    }

    @Test
    void publishesEveryRegularFileByRelativePath(@TempDir Path dir) throws IOException {
        NodeStore store = NodeStore.inMemory();
        byte[] big = new byte[3 * 1024 * 1024];
        new Random(42).nextBytes(big);
        write(dir, "README.md", "# hello\n".getBytes(StandardCharsets.UTF_8));
        write(dir, "src/main/App.java", "class App {}".getBytes(StandardCharsets.UTF_8));
        write(dir, "data/big.bin", big);
        write(dir, "data/empty", new byte[0]);
        write(dir, "bin/run.sh", "#!/bin/sh\n".getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(dir.resolve("bin/run.sh"), java.util.EnumSet.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
        Files.createDirectories(dir.resolve("empty-dir"));

        TreePublisher publisher = new TreePublisher(store, STAND_IN);
        TreePublisher.Published tree = publisher.publish(dir);
        SortedMap<String, FileEntry> entries = tree.entries();
        assertThat(entries.keySet()).containsExactly("README.md", "bin/run.sh", "data/big.bin", "data/empty", "src/main/App.java");
        FileEntry readme = entries.get("README.md");
        assertThat(readme.size()).isEqualTo(8);
        assertThat(readme.mode()).isEqualTo(FileEntry.REGULAR);
        assertThat(readme.mediaType()).isEqualTo("text/markdown");
        assertThat(readme.root()).isEqualTo(Cids.of("# hello\n".getBytes(StandardCharsets.UTF_8), Cids.RAW));
        assertThat(entries.get("bin/run.sh").mode()).isEqualTo(FileEntry.EXECUTABLE);
        assertThat(entries.get("bin/run.sh").mediaType()).isEqualTo("application/x-sh");
        assertThat(entries.get("src/main/App.java").mediaType()).isEqualTo("text/x-java-source");
        assertThat(entries.get("data/empty").root()).isEqualTo(BlobTree.EMPTY_ROOT);
        assertThat(entries.get("data/empty").mediaType()).isEqualTo(FileEntry.OCTET_STREAM);
        FileEntry bigEntry = entries.get("data/big.bin");
        assertThat(bigEntry.size()).isEqualTo(big.length);
        assertThat(bigEntry.root()).isEqualTo(PBlob.of(big, NodeStore.inMemory()).root());
        assertThat(PBlob.load(store, bigEntry.root()).bytes()).isEqualTo(big);
        assertThat(tree.changedFiles()).isEqualTo(4);
        assertThat(tree.writtenBytes()).isEqualTo(store.bytesUsed());
        assertThat(tree.root()).isEqualTo(STAND_IN.write(entries, store));
        assertThat(FileEntry.fromMap(readme.toMap())).isEqualTo(readme);
    }

    @Test
    void sharesUnchangedFilesWithThePreviousTree(@TempDir Path dir) throws IOException {
        NodeStore store = NodeStore.inMemory();
        byte[] big = new byte[4 * 1024 * 1024];
        new Random(42).nextBytes(big);
        byte[] other = new byte[2 * 1024 * 1024];
        new Random(43).nextBytes(other);
        write(dir, "a/big.bin", big);
        write(dir, "b/other.bin", other);
        write(dir, "notes.txt", "one".getBytes(StandardCharsets.UTF_8));
        TreePublisher publisher = new TreePublisher(store, STAND_IN);
        TreePublisher.Published first = publisher.publish(dir);

        TreePublisher.Published same = publisher.publish(dir, first);
        assertThat(same.root()).isEqualTo(first.root());
        assertThat(same.writtenBytes()).isZero();
        assertThat(same.changedFiles()).isZero();

        big[2_000_000] ^= 0x33;
        write(dir, "a/big.bin", big);
        write(dir, "notes.txt", "two".getBytes(StandardCharsets.UTF_8));
        TreePublisher.Published second = publisher.publish(dir, first);
        assertThat(second.root()).isNotEqualTo(first.root());
        assertThat(second.entries().get("b/other.bin")).isEqualTo(first.entries().get("b/other.bin"));
        assertThat(second.entries().get("a/big.bin").root()).isNotEqualTo(first.entries().get("a/big.bin").root());
        assertThat(second.entries().get("a/big.bin").root()).isEqualTo(PBlob.of(big, NodeStore.inMemory()).root());
        assertThat(second.changedFiles()).isEqualTo(2);
        assertThat(second.writtenBytes()).isLessThan(FastCdc.MAX_SIZE + 20_000);
        System.out.println("TreePublisher re-publish after a one-byte edit of 4 MiB and a 3-byte file: wrote "
                + second.writtenBytes() + " bytes");

        Files.delete(dir.resolve("b/other.bin"));
        TreePublisher.Published third = publisher.publish(dir, second);
        assertThat(third.entries().keySet()).containsExactly("a/big.bin", "notes.txt");
        assertThat(third.writtenBytes()).isZero();
    }

    @Test
    void symbolicLinksInsideTheRootBecomeLinkEntries(@TempDir Path dir) throws IOException {
        NodeStore store = NodeStore.inMemory();
        write(dir, "docs/guide.txt", "guide".getBytes(StandardCharsets.UTF_8));
        Files.createSymbolicLink(dir.resolve("guide-link"), Path.of("docs/guide.txt"));
        Files.createSymbolicLink(dir.resolve("docs-link"), Path.of("docs"));
        Files.createSymbolicLink(dir.resolve("dangling"), Path.of("docs/missing.txt"));
        TreePublisher.Published tree = new TreePublisher(store, STAND_IN).publish(dir);
        assertThat(tree.entries().keySet()).containsExactly("dangling", "docs-link", "docs/guide.txt", "guide-link");
        FileEntry link = tree.entries().get("guide-link");
        assertThat(link.mode()).isEqualTo(FileEntry.SYMLINK);
        assertThat(link.symlink()).isTrue();
        assertThat(link.mediaType()).isEqualTo("inode/symlink");
        assertThat(link.size()).isEqualTo("docs/guide.txt".length());
        assertThat(PBlob.load(store, link.root()).bytes()).isEqualTo("docs/guide.txt".getBytes(StandardCharsets.UTF_8));
        assertThat(tree.entries().get("docs-link").mode()).isEqualTo(FileEntry.SYMLINK);
        assertThat(tree.entries().get("dangling").mode()).isEqualTo(FileEntry.SYMLINK);
    }

    @Test
    void refusesSymbolicLinksThatLeaveTheRoot(@TempDir Path dir) throws IOException {
        NodeStore store = NodeStore.inMemory();
        Path outside = Files.createTempFile("pdt-outside", ".txt");
        try {
            Path root = dir.resolve("root");
            Files.createDirectories(root.resolve("sub"));
            write(root, "ok.txt", "ok".getBytes(StandardCharsets.UTF_8));
            Files.createSymbolicLink(root.resolve("sub/escape"), outside);
            TreePublisher publisher = new TreePublisher(store, STAND_IN);
            assertThatThrownBy(() -> publisher.publish(root))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("leaves the root");
            Files.delete(root.resolve("sub/escape"));
            Files.createSymbolicLink(root.resolve("sub/up"), Path.of("../../"));
            assertThatThrownBy(() -> publisher.publish(root))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("leaves the root");
            Files.delete(root.resolve("sub/up"));
            Files.createSymbolicLink(root.resolve("sub/dangling-up"), Path.of("../../nowhere.txt"));
            assertThatThrownBy(() -> publisher.publish(root))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("leaves the root");
            Files.delete(root.resolve("sub/dangling-up"));
            Files.createSymbolicLink(root.resolve("sub/back-in"), Path.of("../sub/../ok.txt"));
            assertThat(publisher.publish(root).entries()).containsKeys("ok.txt", "sub/back-in");
        } finally {
            Files.deleteIfExists(outside);
        }
    }

    @Test
    void refusesANonDirectoryAndValidatesEntries(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("file.txt");
        Files.writeString(file, "x");
        TreePublisher publisher = new TreePublisher(NodeStore.inMemory(), STAND_IN);
        assertThatThrownBy(() -> publisher.publish(file)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FileEntry(BlobTree.EMPTY_ROOT, -1, FileEntry.REGULAR, "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FileEntry(BlobTree.EMPTY_ROOT, 0, "040000", "text/plain"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("mode");
        assertThatThrownBy(() -> new FileEntry("not-a-cid", 0, FileEntry.REGULAR, "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FileEntry.fromMap(Map.of("root", BlobTree.EMPTY_ROOT)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(TreePublisher.mediaType(Path.of("x.PNG"))).isEqualTo("image/png");
        assertThat(TreePublisher.mediaType(Path.of("Makefile"))).isEqualTo(FileEntry.OCTET_STREAM);
        assertThat(TreePublisher.mediaType(Path.of("trailing."))).isEqualTo(FileEntry.OCTET_STREAM);
    }

    @Test
    void theDefaultWriterPublishesAPersistentMapThatReadsBack(@TempDir Path dir) throws IOException {
        NodeStore store = NodeStore.inMemory();
        byte[] big = new byte[2 * 1024 * 1024];
        new Random(42).nextBytes(big);
        write(dir, "README.md", "# hello\n".getBytes(StandardCharsets.UTF_8));
        write(dir, "data/big.bin", big);
        write(dir, "src/App.java", "class App {}".getBytes(StandardCharsets.UTF_8));
        TreePublisher publisher = new TreePublisher(store);
        TreePublisher.Published first = publisher.publish(dir);

        PMap<String, FileEntry> map = TreePublisher.load(store, first.root());
        assertThat(map.count()).isEqualTo(3);
        assertThat(map.toMap()).containsExactlyInAnyOrderEntriesOf(first.entries());
        assertThat(map.get("data/big.bin")).contains(first.entries().get("data/big.bin"));
        assertThat(store.complete(first.root())).isTrue();
        // An entry names its blob by CID text, so the blob tree is not reachable from the map root;
        // a complete tree is the map plus every entry's blob.
        assertThat(store.reachable(first.root())).doesNotContain(first.entries().get("data/big.bin").root());
        assertThat(store.complete(first.entries().get("data/big.bin").root())).isTrue();

        assertThat(new TreePublisher(NodeStore.inMemory()).publish(dir).root())
                .as("the map root depends on the entries alone").isEqualTo(first.root());

        write(dir, "README.md", "# hello again\n".getBytes(StandardCharsets.UTF_8));
        TreePublisher.Published second = publisher.publish(dir, first);
        assertThat(second.root()).isNotEqualTo(first.root());
        PMap<String, FileEntry> next = TreePublisher.load(store, second.root());
        assertThat(next.diff(map).changed().keySet()).containsExactly("README.md");
        assertThat(next.get("data/big.bin")).isEqualTo(map.get("data/big.bin"));
    }
}
