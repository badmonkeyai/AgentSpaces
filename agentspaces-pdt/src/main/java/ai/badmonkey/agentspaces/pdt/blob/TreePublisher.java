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

import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Publishes a directory as a map from relative path to {@link FileEntry}
 * (srad/agentspaces-pdt.md §3.3, PD-B-05). Every regular file becomes a {@link PBlob};
 * a symbolic link inside the root becomes an entry of mode {@code 120000} whose blob
 * holds the link target, after Git; a symbolic link that leaves the root is refused;
 * empty directories have no entry. A publish with a previous result hands each
 * file's previous blob to {@link PBlob#of(Path, NodeStore, PBlob)}, so an unchanged
 * file writes nothing and a changed file writes its delta.
 *
 * <p>The map itself is written through a {@link MapWriter}. The default of
 * {@link #pmapWriter()} builds a {@link PMap}{@code <String, FileEntry>} from the sorted
 * entries through one editor and returns its root; {@link #load} reads that map back.
 * This class walks files, builds blobs, and shares; the writer owns the map layout, and a
 * test may substitute one.
 */
public final class TreePublisher {

    /** The seam to the persistent map: turns the entries into a map root. */
    @FunctionalInterface
    public interface MapWriter {

        /**
         * Writes the map and returns its root.
         *
         * @param entries relative path (forward slashes) to entry, sorted
         * @param store   the store the map's nodes go to
         * @return the map's root CID
         */
        String write(SortedMap<String, FileEntry> entries, NodeStore store);
    }

    /**
     * A published tree.
     *
     * @param root         the map root
     * @param entries      relative path to entry, sorted
     * @param writtenBytes the chunk and interior bytes the blobs wrote to the store
     * @param changedFiles how many files wrote at least one node
     */
    public record Published(String root, SortedMap<String, FileEntry> entries, long writtenBytes, int changedFiles) {

        /** Validates the parts. */
        public Published {
            Objects.requireNonNull(root, "root");
            entries = Collections.unmodifiableSortedMap(new TreeMap<>(entries));
        }
    }

    private static final Map<String, String> MEDIA_TYPES = Map.ofEntries(
            Map.entry("txt", "text/plain"), Map.entry("md", "text/markdown"), Map.entry("csv", "text/csv"),
            Map.entry("html", "text/html"), Map.entry("htm", "text/html"), Map.entry("css", "text/css"),
            Map.entry("js", "text/javascript"), Map.entry("mjs", "text/javascript"), Map.entry("ts", "text/typescript"),
            Map.entry("json", "application/json"), Map.entry("xml", "application/xml"), Map.entry("yaml", "application/yaml"),
            Map.entry("yml", "application/yaml"), Map.entry("toml", "application/toml"), Map.entry("cbor", "application/cbor"),
            Map.entry("pdf", "application/pdf"), Map.entry("zip", "application/zip"), Map.entry("gz", "application/gzip"),
            Map.entry("jar", "application/java-archive"), Map.entry("wasm", "application/wasm"),
            Map.entry("png", "image/png"), Map.entry("jpg", "image/jpeg"), Map.entry("jpeg", "image/jpeg"),
            Map.entry("gif", "image/gif"), Map.entry("svg", "image/svg+xml"), Map.entry("webp", "image/webp"),
            Map.entry("mp3", "audio/mpeg"), Map.entry("mp4", "video/mp4"), Map.entry("wav", "audio/wav"),
            Map.entry("java", "text/x-java-source"), Map.entry("py", "text/x-python"), Map.entry("rb", "text/x-ruby"),
            Map.entry("clj", "text/x-clojure"), Map.entry("groovy", "text/x-groovy"), Map.entry("sh", "application/x-sh"));

    private final NodeStore store;
    private final MapWriter writer;

    /**
     * A publisher whose map is the persistent map of {@link #pmapWriter()}.
     *
     * @param store where blobs and the map go
     */
    public TreePublisher(NodeStore store) {
        this(store, pmapWriter());
    }

    /**
     * A publisher.
     *
     * @param store  where blobs and the map go
     * @param writer the map seam
     */
    public TreePublisher(NodeStore store, MapWriter writer) {
        this.store = Objects.requireNonNull(store, "store");
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    /**
     * The map writer of PD-B-05: a {@code PMap<String, FileEntry>} keyed by relative path,
     * built through one editor so the publish writes each new map node once.
     */
    public static MapWriter pmapWriter() {
        return (entries, store) -> {
            Editor<String, FileEntry> editor = PMap.empty(store, Codec.of(String.class), FileEntry.codec()).editor();
            entries.forEach(editor::assoc);
            return editor.done().value().root();
        };
    }

    /**
     * The map a {@link #pmapWriter()} publish wrote.
     *
     * @param store the store that holds it
     * @param root  the map root of a {@link Published}
     * @return the map from relative path to entry
     */
    public static PMap<String, FileEntry> load(NodeStore store, String root) {
        return PMap.load(store, root, Codec.of(String.class), FileEntry.codec());
    }

    /**
     * Publishes a directory from scratch.
     *
     * @param dir the directory
     * @return the tree
     * @throws IllegalArgumentException when a symbolic link leaves the root
     * @throws UncheckedIOException     when the walk fails
     */
    public Published publish(Path dir) {
        return publish(dir, null);
    }

    /**
     * Publishes a directory, sharing every unchanged file's blob with a previous tree.
     *
     * @param dir      the directory
     * @param previous the previous tree, or null
     * @return the tree
     * @throws IllegalArgumentException when a symbolic link leaves the root
     * @throws UncheckedIOException     when the walk fails
     */
    public Published publish(Path dir, Published previous) {
        Objects.requireNonNull(dir, "dir");
        Path root = dir.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException(root + " is not a directory");
        }
        Path real = realPath(root);
        SortedMap<String, FileEntry> entries = new TreeMap<>();
        long[] written = {0};
        int[] changed = {0};
        try {
            Files.walkFileTree(root, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    String key = key(root, file);
                    PBlob previousBlob = previousBlob(previous, key);
                    FileEntry entry;
                    PBlob blob;
                    if (attrs.isSymbolicLink()) {
                        Path target = readLink(file);
                        checkInside(real, file, target);
                        byte[] bytes = target.toString().getBytes(StandardCharsets.UTF_8);
                        blob = PBlob.of(bytes, store);
                        entry = new FileEntry(blob.root(), bytes.length, FileEntry.SYMLINK, "inode/symlink");
                    } else if (attrs.isRegularFile()) {
                        blob = PBlob.of(file, store, previousBlob);
                        entry = new FileEntry(blob.root(), blob.size(), mode(file), mediaType(file));
                    } else {
                        return FileVisitResult.CONTINUE;
                    }
                    entries.put(key, entry);
                    if (blob.writtenBytes() > 0) {
                        written[0] += blob.writtenBytes();
                        changed[0]++;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("publishing " + root, e);
        }
        String mapRoot = writer.write(entries, store);
        return new Published(mapRoot, entries, written[0], changed[0]);
    }

    private PBlob previousBlob(Published previous, String key) {
        if (previous == null) {
            return null;
        }
        FileEntry entry = previous.entries().get(key);
        if (entry == null || entry.symlink()) {
            return null;
        }
        try {
            return PBlob.load(store, entry.root());
        } catch (NodeFormatException e) {
            return null;
        }
    }

    /** The map key: the path relative to the root with forward slashes. */
    static String key(Path root, Path file) {
        Path relative = root.relativize(file.toAbsolutePath().normalize());
        StringBuilder key = new StringBuilder();
        for (Path part : relative) {
            if (!key.isEmpty()) {
                key.append('/');
            }
            key.append(part);
        }
        return key.toString();
    }

    private static Path readLink(Path file) {
        try {
            return Files.readSymbolicLink(file);
        } catch (IOException e) {
            throw new UncheckedIOException("reading link " + file, e);
        }
    }

    /** Refuses a link whose target lies outside the root (PD-B-05). */
    private static void checkInside(Path realRoot, Path link, Path target) {
        Path resolved = link.toAbsolutePath().getParent().resolve(target).normalize();
        Path candidate;
        try {
            candidate = resolved.toRealPath();
        } catch (NoSuchFileException e) {
            candidate = realPath(nearestExisting(resolved)).resolve(remainder(resolved)).normalize();
        } catch (IOException e) {
            throw new UncheckedIOException("resolving link " + link, e);
        }
        if (!candidate.startsWith(realRoot)) {
            throw new IllegalArgumentException("symbolic link " + link + " leaves the root: " + target);
        }
    }

    private static Path nearestExisting(Path path) {
        Path p = path;
        while (p != null && !Files.exists(p)) {
            p = p.getParent();
        }
        return p == null ? path.getRoot() : p;
    }

    private static Path remainder(Path path) {
        Path existing = nearestExisting(path);
        return existing.equals(path) ? path.getFileSystem().getPath("") : existing.relativize(path);
    }

    private static Path realPath(Path path) {
        try {
            return path.toRealPath();
        } catch (IOException e) {
            throw new UncheckedIOException("resolving " + path, e);
        }
    }

    /** The Git mode of a regular file: executable when the owner may execute it. */
    static String mode(Path file) {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
            return permissions.contains(PosixFilePermission.OWNER_EXECUTE) ? FileEntry.EXECUTABLE : FileEntry.REGULAR;
        } catch (UnsupportedOperationException | IOException e) {
            return FileEntry.REGULAR;
        }
    }

    /** The media type from the file name's extension. */
    static String mediaType(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        if (dot < 0 || dot == name.length() - 1) {
            return FileEntry.OCTET_STREAM;
        }
        return MEDIA_TYPES.getOrDefault(name.substring(dot + 1).toLowerCase(Locale.ROOT), FileEntry.OCTET_STREAM);
    }
}
