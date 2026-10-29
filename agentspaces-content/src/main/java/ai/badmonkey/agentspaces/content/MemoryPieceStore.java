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

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.InstantSource;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A heap-backed piece store for tests, for browsers without persistent storage,
 * and for small artifacts. Same quota and eviction rules as the file store
 * (LB-T-04): least recently served, unpinned roots go first, oldest manifest
 * first among equals.
 */
public final class MemoryPieceStore implements PieceStore {

    private static final class Held {
        final Manifest manifest;
        final byte[][] pieces;
        final BitSet have = new BitSet();
        volatile long lastServedMillis;
        volatile boolean pinned;

        Held(Manifest manifest, long now) {
            this.manifest = manifest;
            this.pieces = new byte[manifest.pieces()][];
            this.lastServedMillis = now;
        }

        long bytes() {
            long total = 0;
            for (byte[] piece : pieces) {
                if (piece != null) {
                    total += piece.length;
                }
            }
            return total;
        }
    }

    private final Map<String, Held> held = new ConcurrentHashMap<>();
    private final InstantSource clock;
    private final long quota;

    /** An unbounded memory store. */
    public MemoryPieceStore() {
        this(InstantSource.system(), 0);
    }

    /**
     * A memory store.
     *
     * @param clock the clock for eviction order
     * @param quota the byte quota; zero or less means unbounded
     */
    public MemoryPieceStore(InstantSource clock, long quota) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.quota = quota;
    }

    @Override
    public String kind() {
        return "memory";
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
        held.computeIfAbsent(manifest.root(), r -> new Held(manifest, clock.millis()));
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
        if (h == null || index < 0 || index >= h.pieces.length) {
            return Optional.empty();
        }
        byte[] piece;
        synchronized (h) {
            piece = h.pieces[index];
        }
        if (piece == null || offset < 0 || offset > piece.length) {
            return Optional.empty();
        }
        h.lastServedMillis = clock.millis();
        int end = Math.min(piece.length, offset + length);
        return Optional.of(Arrays.copyOfRange(piece, offset, end));
    }

    @Override
    public void write(String root, int index, byte[] piece) {
        Held h = held.get(root);
        if (h == null) {
            throw new IllegalStateException("root " + root + " is not open in this store");
        }
        synchronized (h) {
            if (h.pieces[index] == null) {
                h.pieces[index] = piece.clone();
                h.have.set(index);
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
                throw new IOException("root " + root + " is incomplete: " + h.have.cardinality() + " of " + h.manifest.pieces());
            }
            byte[] all = new byte[(int) Math.min(Integer.MAX_VALUE, h.manifest.size())];
            int at = 0;
            for (byte[] piece : h.pieces) {
                System.arraycopy(piece, 0, all, at, piece.length);
                at += piece.length;
            }
            h.lastServedMillis = clock.millis();
            return new ByteArrayChannel(all);
        }
    }

    @Override
    public void pin(String root) {
        Held h = held.get(root);
        if (h != null) {
            h.pinned = true;
        }
    }

    @Override
    public void unpin(String root) {
        Held h = held.get(root);
        if (h != null) {
            h.pinned = false;
        }
    }

    @Override
    public boolean pinned(String root) {
        Held h = held.get(root);
        return h != null && h.pinned;
    }

    @Override
    public long bytesUsed() {
        long total = 0;
        for (Held h : held.values()) {
            total += h.bytes();
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
                if (h.pinned) {
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
            held.remove(victimRoot, victim);
        }
    }

    @Override
    public void forget(String root) {
        held.remove(root);
    }
}
