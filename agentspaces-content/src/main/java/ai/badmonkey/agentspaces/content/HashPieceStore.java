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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The hash-keyed piece store (PDT 2 phase 1, P2-H-01 to P2-H-04; srad/agentspaces-pdt2.md §3.3).
 * Every piece lives once, in {@code pieces/<2 hex>/<64 hex>} named by its sha2-256, whatever root
 * names it. Each root keeps {@code roots/<root>/manifest.cbor} and {@code index.cbor}, the
 * bitfield of the pieces it holds and the hash of each, and the store counts, per hash, the
 * {@code (root, index)} pairs that name it.
 *
 * <ul>
 *   <li>{@link #write} hashes the piece, refuses it when the root's table names another hash, and
 *       links the file a held hash already has (P2-H-01).</li>
 *   <li>{@link #open(Manifest, byte[])} marks every piece whose table hash the store already holds,
 *       so a fetch of a second artifact asks only for the pieces it does not share (P2-H-02).</li>
 *   <li>{@link #forget} unlinks a root's pieces and deletes a file once no root names it.</li>
 *   <li>{@link #importFrom} converts a {@link FilePieceStore}, hashing each held piece once (P2-H-04).</li>
 * </ul>
 *
 * <p>The index of a root reaches disk every {@value #PERSIST_EVERY} pieces, at completion, and on
 * {@link #flush}, after the piece files it names were forced, so a crash loses at most that many
 * pieces of a fetch. A restart reads every index back and rebuilds the counts; a piece whose file
 * is gone reads as not held. Pieces are stored as the swarm verified them, in plaintext, because
 * their names are their plaintext hashes; {@link EncryptedPieceStore} therefore refuses to wrap
 * this store.
 */
public final class HashPieceStore implements PieceStore {

    /** The directory of the piece files. */
    public static final String PIECES = "pieces";

    /** The directory of the per-root manifests and indexes. */
    public static final String ROOTS = "roots";

    /** The per-root index file. */
    public static final String INDEX = "index.cbor";

    private static final String MANIFEST = "manifest.cbor";
    private static final String PIN = "pin";

    /** Pieces between forced writes of a root's index. */
    static final int PERSIST_EVERY = 8;

    /**
     * The index of a root on disk: the pieces held and, at 32 bytes each in piece order, their
     * hashes; a piece not held has 32 zero bytes.
     *
     * @param v      the format version, 1
     * @param have   the bitfield of the pieces held
     * @param hashes the hashes
     */
    record Index(int v, byte[] have, byte[] hashes) {
    }

    /**
     * What {@link #importFrom} did.
     *
     * @param roots        the roots imported
     * @param piecesHashed the pieces read and hashed, once each
     * @param piecesStored the piece files written; a piece another root already held is linked instead
     */
    public record Import(int roots, int piecesHashed, int piecesStored) {
    }

    private static final class Held {
        final Manifest manifest;
        final Path dir;
        final BitSet have;
        final byte[] hashes;
        /** The table, when the manifest carries it or the opener gave it; null otherwise. */
        byte[] table;
        /** Pieces whose files were written since the index last reached disk. */
        final List<Path> unforced = new ArrayList<>();
        volatile long lastServedMillis;

        Held(Manifest manifest, Path dir, BitSet have, byte[] hashes, long now) {
            this.manifest = manifest;
            this.dir = dir;
            this.have = have;
            this.hashes = hashes;
            this.table = manifest.table();
            this.lastServedMillis = now;
        }

        byte[] hash(int index) {
            return Arrays.copyOfRange(hashes, index * Manifest.HASH_BYTES, (index + 1) * Manifest.HASH_BYTES);
        }
    }

    private final Path path;
    private final CborCodec codec;
    private final InstantSource clock;
    private final long quota;
    private final Map<String, Held> held = new LinkedHashMap<>();
    /** The (root, index) pairs naming each hash, by hex. */
    private final Map<String, Integer> references = new HashMap<>();
    /** The bytes of each held hash, by hex. */
    private final Map<String, Integer> sizes = new HashMap<>();
    private long bytesUsed;

    /**
     * Opens or creates a store, reading back every root it holds.
     *
     * @param path  the store directory
     * @param codec the codec of manifests and indexes
     * @param clock the clock of eviction order
     * @param quota the byte quota, counted once per hash; zero or less is unbounded
     * @throws IOException when the directory cannot be created or read
     */
    public HashPieceStore(Path path, CborCodec codec, InstantSource clock, long quota) throws IOException {
        this.path = Objects.requireNonNull(path, "path");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.quota = quota;
        Files.createDirectories(path.resolve(PIECES));
        Files.createDirectories(path.resolve(ROOTS));
        try (Stream<Path> dirs = Files.list(path.resolve(ROOTS))) {
            for (Path dir : (Iterable<Path>) dirs::iterator) {
                Path manifestFile = dir.resolve(MANIFEST);
                if (Files.isRegularFile(manifestFile)) {
                    load(dir, codec.fromBytes(Files.readAllBytes(manifestFile), Manifest.class));
                }
            }
        }
    }

    private void load(Path dir, Manifest manifest) throws IOException {
        int pieces = manifest.pieces();
        BitSet have = new BitSet();
        byte[] hashes = new byte[pieces * Manifest.HASH_BYTES];
        Path indexFile = dir.resolve(INDEX);
        if (Files.isRegularFile(indexFile)) {
            Index index = codec.fromBytes(Files.readAllBytes(indexFile), Index.class);
            if (index.hashes().length == hashes.length) {
                BitSet stored = BitSet.valueOf(index.have());
                for (int i = stored.nextSetBit(0); i >= 0 && i < pieces; i = stored.nextSetBit(i + 1)) {
                    String hex = HexFormat.of().formatHex(index.hashes(), i * Manifest.HASH_BYTES, (i + 1) * Manifest.HASH_BYTES);
                    Path file = pieceFile(hex);
                    if (Files.isRegularFile(file)) {
                        have.set(i);
                        System.arraycopy(index.hashes(), i * Manifest.HASH_BYTES, hashes, i * Manifest.HASH_BYTES, Manifest.HASH_BYTES);
                        link(hex, (int) Files.size(file));
                    }
                }
            }
        }
        held.put(manifest.root(), new Held(manifest, dir, have, hashes, clock.millis()));
    }

    /** The store directory. */
    public Path path() {
        return path;
    }

    /** The directory name of a root under {@value #ROOTS}. */
    String dirName(String root) {
        return root.replaceAll("[^A-Za-z0-9]", "_");
    }

    /** How many {@code (root, index)} pairs name a hash, given in lower-case hex. */
    public synchronized int references(String hex) {
        return references.getOrDefault(hex, 0);
    }

    @Override
    public String kind() {
        return "hash";
    }

    @Override
    public boolean writable() {
        return true;
    }

    @Override
    public synchronized Set<String> roots() {
        return Set.copyOf(held.keySet());
    }

    @Override
    public synchronized Optional<Manifest> manifest(String root) {
        Held h = held.get(root);
        return h == null ? Optional.empty() : Optional.of(h.manifest);
    }

    @Override
    public void open(Manifest manifest) {
        open(manifest, manifest.table());
    }

    /**
     * Opens a root and marks every piece whose table hash this store already holds (P2-H-02).
     *
     * @param manifest the manifest
     * @param table    the verified piece table, or null when not known yet
     */
    @Override
    public synchronized void open(Manifest manifest, byte[] table) {
        Held h = held.get(manifest.root());
        if (h == null) {
            Path dir = path.resolve(ROOTS).resolve(dirName(manifest.root()));
            try {
                Files.createDirectories(dir);
                Files.write(dir.resolve(MANIFEST), codec.toBytes(manifest));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            h = new Held(manifest, dir, new BitSet(), new byte[manifest.pieces() * Manifest.HASH_BYTES], clock.millis());
            held.put(manifest.root(), h);
        }
        if (table == null || table.length != manifest.pieces() * Manifest.HASH_BYTES) {
            return;
        }
        h.table = table;
        boolean seeded = false;
        for (int i = h.have.nextClearBit(0); i < manifest.pieces(); i = h.have.nextClearBit(i + 1)) {
            String hex = HexFormat.of().formatHex(table, i * Manifest.HASH_BYTES, (i + 1) * Manifest.HASH_BYTES);
            Integer size = sizes.get(hex);
            if (size != null && size == manifest.pieceLength(i)) {
                record(h, i, table, i * Manifest.HASH_BYTES, hex, size);
                seeded = true;
            }
        }
        if (seeded) {
            persist(h);
        }
    }

    @Override
    public synchronized BitSet have(String root) {
        Held h = held.get(root);
        return h == null ? new BitSet() : (BitSet) h.have.clone();
    }

    @Override
    public Optional<byte[]> read(String root, int index, int offset, int length) {
        Path file;
        int pieceLength;
        synchronized (this) {
            Held h = held.get(root);
            if (h == null || index < 0 || index >= h.manifest.pieces() || !h.have.get(index)) {
                return Optional.empty();
            }
            pieceLength = h.manifest.pieceLength(index);
            file = pieceFile(HexFormat.of().formatHex(h.hash(index)));
            h.lastServedMillis = clock.millis();
        }
        if (offset < 0 || offset > pieceLength || length < 0) {
            return Optional.empty();
        }
        int n = Math.min(length, pieceLength - offset);
        ByteBuffer buffer = ByteBuffer.allocate(n);
        try (FileChannel data = FileChannel.open(file, StandardOpenOption.READ)) {
            while (buffer.hasRemaining() && data.read(buffer, offset + buffer.position()) >= 0) {
                // fill
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Optional.of(Arrays.copyOf(buffer.array(), buffer.position()));
    }

    /**
     * Stores a piece under its hash, or links the file a held hash already has.
     *
     * @throws IllegalArgumentException when the root's table names another hash or length for the piece
     */
    @Override
    public void write(String root, int index, byte[] piece) {
        byte[] digest = Digests.sha256(piece);
        String hex = HexFormat.of().formatHex(digest);
        synchronized (this) {
            Held h = held.get(root);
            if (h == null) {
                throw new IllegalStateException("root " + root + " is not open in this store");
            }
            if (index < 0 || index >= h.manifest.pieces() || piece.length != h.manifest.pieceLength(index)) {
                throw new IllegalArgumentException("piece " + index + " of " + root + " has the wrong index or length");
            }
            if (h.table != null && !Arrays.equals(digest, 0, Manifest.HASH_BYTES, h.table,
                    index * Manifest.HASH_BYTES, (index + 1) * Manifest.HASH_BYTES)) {
                throw new IllegalArgumentException("piece " + index + " of " + root + " does not hash to its table entry");
            }
            if (h.have.get(index)) {
                return;
            }
            Path file = pieceFile(hex);
            if (!sizes.containsKey(hex)) {
                try {
                    writeFile(file, piece);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                h.unforced.add(file);
            }
            record(h, index, digest, 0, hex, piece.length);
            if (h.have.cardinality() >= h.manifest.pieces() || h.unforced.size() >= PERSIST_EVERY) {
                persist(h);
            }
        }
        evict();
    }

    /** Marks a piece held under the hash at {@code from[at]}. */
    private void record(Held h, int index, byte[] from, int at, String hex, int size) {
        System.arraycopy(from, at, h.hashes, index * Manifest.HASH_BYTES, Manifest.HASH_BYTES);
        h.have.set(index);
        link(hex, size);
    }

    private void link(String hex, int size) {
        if (references.merge(hex, 1, Integer::sum) == 1) {
            sizes.put(hex, size);
            bytesUsed += size;
        }
    }

    private static void writeFile(Path file, byte[] piece) throws IOException {
        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(tmp, piece);
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private Path pieceFile(String hex) {
        return path.resolve(PIECES).resolve(hex.substring(0, 2)).resolve(hex);
    }

    /** Forces the root's new piece files, then writes its index atomically. */
    private void persist(Held h) {
        try {
            for (Path file : h.unforced) {
                if (Files.exists(file)) {
                    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                        channel.force(false);
                    }
                }
            }
            h.unforced.clear();
            Path tmp = h.dir.resolve(INDEX + ".tmp");
            Files.write(tmp, codec.toBytes(new Index(1, h.have.toByteArray(), h.hashes)), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.SYNC);
            Files.move(tmp, h.dir.resolve(INDEX), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public SeekableByteChannel openArtifact(String root) throws IOException {
        List<Path> files = new ArrayList<>();
        Manifest manifest;
        synchronized (this) {
            Held h = held.get(root);
            if (h == null) {
                throw new IOException("root " + root + " is not held here");
            }
            if (h.have.cardinality() < h.manifest.pieces()) {
                throw new IOException("root " + root + " is incomplete: " + h.have.cardinality() + " of " + h.manifest.pieces());
            }
            for (int i = 0; i < h.manifest.pieces(); i++) {
                files.add(pieceFile(HexFormat.of().formatHex(h.hash(i))));
            }
            manifest = h.manifest;
            h.lastServedMillis = clock.millis();
        }
        return new PiecesChannel(files, manifest.pieceSize(), manifest.size());
    }

    @Override
    public synchronized void pin(String root) {
        Held h = held.get(root);
        if (h != null) {
            try {
                Files.write(h.dir.resolve(PIN), new byte[0]);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public synchronized void unpin(String root) {
        Held h = held.get(root);
        if (h != null) {
            try {
                Files.deleteIfExists(h.dir.resolve(PIN));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public synchronized boolean pinned(String root) {
        Held h = held.get(root);
        return h != null && Files.exists(h.dir.resolve(PIN));
    }

    @Override
    public synchronized long bytesUsed() {
        return bytesUsed;
    }

    @Override
    public long quota() {
        return quota;
    }

    @Override
    public synchronized void evict() {
        if (quota <= 0) {
            return;
        }
        while (bytesUsed > quota) {
            String victim = null;
            Held oldest = null;
            for (Map.Entry<String, Held> e : held.entrySet()) {
                Held h = e.getValue();
                if (pinned(e.getKey())) {
                    continue;
                }
                if (oldest == null || h.lastServedMillis < oldest.lastServedMillis
                        || (h.lastServedMillis == oldest.lastServedMillis && h.manifest.created() < oldest.manifest.created())) {
                    oldest = h;
                    victim = e.getKey();
                }
            }
            if (victim == null) {
                return;
            }
            forget(victim);
        }
    }

    /** Drops a root: unlinks each of its pieces and deletes a file once no root names it (P2-H-01). */
    @Override
    public synchronized void forget(String root) {
        Held h = held.remove(root);
        if (h == null) {
            return;
        }
        try {
            for (int i = h.have.nextSetBit(0); i >= 0; i = h.have.nextSetBit(i + 1)) {
                String hex = HexFormat.of().formatHex(h.hash(i));
                if (references.merge(hex, -1, Integer::sum) <= 0) {
                    references.remove(hex);
                    bytesUsed -= sizes.remove(hex);
                    Files.deleteIfExists(pieceFile(hex));
                }
            }
            try (Stream<Path> files = Files.list(h.dir)) {
                for (Path file : (Iterable<Path>) files::iterator) {
                    Files.deleteIfExists(file);
                }
            }
            Files.deleteIfExists(h.dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void flush() {
        for (Held h : held.values()) {
            if (!h.unforced.isEmpty()) {
                persist(h);
            }
        }
    }

    /**
     * Converts a file store (P2-H-04): opens each of its roots here with its manifest and pin, and
     * writes every piece it holds, hashing each once; a piece this store already holds under any
     * root is linked. The file store is left as it was.
     *
     * @param source the file store
     * @return what the import did
     */
    public Import importFrom(FilePieceStore source) {
        int roots = 0;
        int hashed = 0;
        int stored = 0;
        for (String root : source.roots()) {
            Optional<Manifest> manifest = source.manifest(root);
            if (manifest.isEmpty()) {
                continue;
            }
            Manifest m = manifest.get();
            open(m);
            BitSet have = source.have(root);
            for (int i = have.nextSetBit(0); i >= 0 && i < m.pieces(); i = have.nextSetBit(i + 1)) {
                byte[] piece = source.read(root, i, 0, m.pieceLength(i))
                        .orElseThrow(() -> new IllegalStateException("the file store lost a piece of " + root));
                int filesBefore;
                synchronized (this) {
                    filesBefore = sizes.size();
                }
                write(root, i, piece);
                hashed++;
                synchronized (this) {
                    stored += sizes.size() - filesBefore;
                }
            }
            if (source.pinned(root)) {
                pin(root);
            }
            roots++;
        }
        flush();
        return new Import(roots, hashed, stored);
    }

    /** A read-only channel over the piece files of a complete artifact, in order. */
    private static final class PiecesChannel implements SeekableByteChannel {
        private final List<Path> files;
        private final int pieceSize;
        private final long size;
        private long position;
        private boolean open = true;

        PiecesChannel(List<Path> files, int pieceSize, long size) {
            this.files = files;
            this.pieceSize = pieceSize;
            this.size = size;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!open) {
                throw new IOException("closed");
            }
            if (position >= size) {
                return -1;
            }
            int total = 0;
            while (dst.hasRemaining() && position < size) {
                int index = (int) (position / pieceSize);
                long within = position - (long) index * pieceSize;
                try (FileChannel piece = FileChannel.open(files.get(index), StandardOpenOption.READ)) {
                    int limit = dst.limit();
                    long left = Math.min(pieceSize - within, size - position);
                    if (dst.remaining() > left) {
                        dst.limit(dst.position() + (int) left);
                    }
                    int n = piece.read(dst, within);
                    dst.limit(limit);
                    if (n <= 0) {
                        throw new IOException("piece file " + files.get(index) + " is short");
                    }
                    position += n;
                    total += n;
                }
            }
            return total;
        }

        @Override
        public int write(ByteBuffer src) {
            throw new NonWritableChannelException();
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public SeekableByteChannel position(long newPosition) {
            if (newPosition < 0) {
                throw new IllegalArgumentException("negative position");
            }
            position = newPosition;
            return this;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public SeekableByteChannel truncate(long newSize) {
            throw new NonWritableChannelException();
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
