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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.InstantSource;
import java.util.BitSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * The file-backed piece store (LB-T-01, LB-T-02): one directory per root under
 * the store path holding {@code data} (pieces written at their offsets),
 * {@code have} (the verified bitfield, fsynced after each piece), and
 * {@code manifest.cbor}. A restart reads the directories back, so a fetch resumes
 * from its bitfield and a holder republishes what it has.
 */
public final class FilePieceStore implements PieceStore {

    /** Default quota, 10 GiB (decision 14). */
    public static final long DEFAULT_QUOTA = 10L * 1024 * 1024 * 1024;

    private static final String DATA = "data";
    private static final String HAVE = "have";
    private static final String MANIFEST = "manifest.cbor";
    private static final String PIN = "pin";

    /** Pieces between forced syncs of the data file and the sidecar (LB-T-01). */
    static final int PERSIST_EVERY = 8;

    private static final class Held {
        final Manifest manifest;
        final Path dir;
        final BitSet have;
        /** Pieces whose bytes were forced to disk and recorded in the sidecar. */
        int persisted;
        volatile long lastServedMillis;

        Held(Manifest manifest, Path dir, BitSet have, long now) {
            this.manifest = manifest;
            this.dir = dir;
            this.have = have;
            this.lastServedMillis = now;
        }
    }

    private final Path path;
    private final CborCodec codec;
    private final InstantSource clock;
    private final long quota;
    private final Map<String, Held> held = new ConcurrentHashMap<>();

    /**
     * Opens or creates a store, reading back every root it already holds.
     *
     * @param path  the store directory
     * @param codec the codec for manifests
     * @param clock the clock for eviction order
     * @param quota the byte quota; zero or less means unbounded
     * @throws IOException when the directory cannot be created or read
     */
    public FilePieceStore(Path path, CborCodec codec, InstantSource clock, long quota) throws IOException {
        this.path = Objects.requireNonNull(path, "path");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.quota = quota;
        Files.createDirectories(path);
        try (Stream<Path> dirs = Files.list(path)) {
            for (Path dir : (Iterable<Path>) dirs::iterator) {
                Path manifestFile = dir.resolve(MANIFEST);
                if (!Files.isRegularFile(manifestFile)) {
                    continue;
                }
                Manifest manifest = codec.fromBytes(Files.readAllBytes(manifestFile), Manifest.class);
                BitSet have = Files.isRegularFile(dir.resolve(HAVE))
                        ? BitSet.valueOf(Files.readAllBytes(dir.resolve(HAVE))) : new BitSet();
                Held h = new Held(manifest, dir, have, clock.millis());
                h.persisted = have.cardinality();
                held.put(manifest.root(), h);
            }
        }
    }

    /** The store directory. */
    public Path path() {
        return path;
    }

    @Override
    public String kind() {
        return "file";
    }

    @Override
    public boolean writable() {
        return true;
    }

    @Override
    public Set<String> roots() {
        return Set.copyOf(held.keySet());
    }

    @Override
    public Optional<Manifest> manifest(String root) {
        Held h = held.get(root);
        return h == null ? Optional.empty() : Optional.of(h.manifest);
    }

    @Override
    public void open(Manifest manifest) {
        held.computeIfAbsent(manifest.root(), root -> {
            Path dir = path.resolve(safeName(root));
            try {
                Files.createDirectories(dir);
                Files.write(dir.resolve(MANIFEST), codec.toBytes(manifest));
                try (FileChannel data = FileChannel.open(dir.resolve(DATA),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    if (data.size() < manifest.size()) {
                        data.position(Math.max(0, manifest.size() - 1));
                        if (manifest.size() > 0) {
                            data.write(ByteBuffer.wrap(new byte[]{0}));
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new Held(manifest, dir, new BitSet(), clock.millis());
        });
    }

    @Override
    public BitSet have(String root) {
        Held h = held.get(root);
        if (h == null) {
            return new BitSet();
        }
        synchronized (h) {
            return (BitSet) h.have.clone();
        }
    }

    @Override
    public Optional<byte[]> read(String root, int index, int offset, int length) {
        Held h = held.get(root);
        if (h == null || index < 0 || index >= h.manifest.pieces()) {
            return Optional.empty();
        }
        synchronized (h) {
            if (!h.have.get(index)) {
                return Optional.empty();
            }
        }
        int pieceLength = h.manifest.pieceLength(index);
        if (offset < 0 || offset > pieceLength) {
            return Optional.empty();
        }
        int n = Math.min(length, pieceLength - offset);
        ByteBuffer buffer = ByteBuffer.allocate(n);
        try (FileChannel data = FileChannel.open(h.dir.resolve(DATA), StandardOpenOption.READ)) {
            long at = (long) index * h.manifest.pieceSize() + offset;
            while (buffer.hasRemaining()) {
                int read = data.read(buffer, at + buffer.position());
                if (read < 0) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        h.lastServedMillis = clock.millis();
        return Optional.of(java.util.Arrays.copyOf(buffer.array(), buffer.position()));
    }

    @Override
    public void write(String root, int index, byte[] piece) {
        Held h = held.get(root);
        if (h == null) {
            throw new IllegalStateException("root " + root + " is not open in this store");
        }
        synchronized (h) {
            if (h.have.get(index)) {
                return;
            }
            try (FileChannel data = FileChannel.open(h.dir.resolve(DATA), StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(piece);
                long at = (long) index * h.manifest.pieceSize();
                while (buffer.hasRemaining()) {
                    at += data.write(buffer, at);
                }
                h.have.set(index);
                // A sync per piece held the session lock for every fsync and queued inbound
                // pieces behind it; the sidecar records only what was forced, every
                // PERSIST_EVERY pieces or at completion, so a crash loses at most that many.
                boolean complete = h.have.cardinality() >= h.manifest.pieces();
                if (complete || h.have.cardinality() - h.persisted >= PERSIST_EVERY) {
                    data.force(false);
                    persistHave(h);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        evict();
    }

    @Override
    public SeekableByteChannel openArtifact(String root) throws IOException {
        Held h = held.get(root);
        if (h == null) {
            throw new IOException("root " + root + " is not held here");
        }
        synchronized (h) {
            if (h.have.cardinality() < h.manifest.pieces()) {
                throw new IOException("root " + root + " is incomplete: " + h.have.cardinality()
                        + " of " + h.manifest.pieces());
            }
        }
        h.lastServedMillis = clock.millis();
        return FileChannel.open(h.dir.resolve(DATA), StandardOpenOption.READ);
    }

    @Override
    public void pin(String root) {
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
    public void unpin(String root) {
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
    public boolean pinned(String root) {
        Held h = held.get(root);
        return h != null && Files.exists(h.dir.resolve(PIN));
    }

    @Override
    public long bytesUsed() {
        long total = 0;
        for (Held h : held.values()) {
            synchronized (h) {
                for (int i = h.have.nextSetBit(0); i >= 0; i = h.have.nextSetBit(i + 1)) {
                    total += h.manifest.pieceLength(i);
                }
            }
        }
        return total;
    }

    @Override
    public long quota() {
        return quota;
    }

    @Override
    public void evict() {
        if (quota <= 0) {
            return;
        }
        while (bytesUsed() > quota) {
            Held victim = null;
            String victimRoot = null;
            for (Map.Entry<String, Held> e : held.entrySet()) {
                Held h = e.getValue();
                if (pinned(e.getKey())) {
                    continue;
                }
                if (victim == null || h.lastServedMillis < victim.lastServedMillis
                        || (h.lastServedMillis == victim.lastServedMillis
                        && h.manifest.created() < victim.manifest.created())) {
                    victim = h;
                    victimRoot = e.getKey();
                }
            }
            if (victim == null) {
                return;
            }
            forget(victimRoot);
        }
    }

    @Override
    public void forget(String root) {
        Held h = held.remove(root);
        if (h == null) {
            return;
        }
        try (Stream<Path> files = Files.list(h.dir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                Files.deleteIfExists(file);
            }
            Files.deleteIfExists(h.dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Writes the bitfield sidecar atomically; call after forcing the data file. */
    private static void persistHave(Held h) throws IOException {
        Path have = h.dir.resolve(HAVE);
        Path tmp = h.dir.resolve(HAVE + ".tmp");
        Files.write(tmp, h.have.toByteArray(), StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.SYNC);
        Files.move(tmp, have, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        h.persisted = h.have.cardinality();
    }

    /** Forces unpersisted pieces of every root to disk; the capability calls it on close. */
    @Override
    public void flush() {
        for (Held h : held.values()) {
            synchronized (h) {
                if (h.have.cardinality() > h.persisted) {
                    try (FileChannel data = FileChannel.open(h.dir.resolve(DATA), StandardOpenOption.WRITE)) {
                        data.force(false);
                        persistHave(h);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }
    }

    private static String safeName(String root) {
        return root.replaceAll("[^A-Za-z0-9]", "_");
    }
}
