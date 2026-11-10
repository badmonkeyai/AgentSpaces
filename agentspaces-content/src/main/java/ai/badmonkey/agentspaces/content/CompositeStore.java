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
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The peer's view over its backends (LB-T-07): reads consult backends in order,
 * writes go to the first writable one, {@code have} is the union, and (when
 * enabled, LB-T-08) a range served by a read-only backend is cached into the
 * writable one so an external registry is read once per range per peer.
 */
public final class CompositeStore implements PieceStore {

    private final List<PieceStore> backends;
    private final PieceStore writable;
    private final boolean cacheExternal;

    /**
     * Composes backends.
     *
     * @param backends      the backends in read order; the first writable one takes writes
     * @param cacheExternal whether ranges read from non-writable backends are cached into the writable one
     */
    public CompositeStore(List<PieceStore> backends, boolean cacheExternal) {
        if (backends.isEmpty()) {
            throw new IllegalArgumentException("at least one backend");
        }
        this.backends = List.copyOf(backends);
        this.writable = backends.stream().filter(PieceStore::writable).findFirst().orElse(null);
        this.cacheExternal = cacheExternal;
    }

    /** The composed backends. */
    public List<PieceStore> backends() {
        return backends;
    }

    @Override
    public String kind() {
        return "composite";
    }

    @Override
    public boolean writable() {
        return writable != null;
    }

    @Override
    public Set<String> roots() {
        Set<String> all = new HashSet<>();
        backends.forEach(b -> all.addAll(b.roots()));
        return all;
    }

    @Override
    public Optional<Manifest> manifest(String root) {
        for (PieceStore b : backends) {
            Optional<Manifest> m = b.manifest(root);
            if (m.isPresent()) {
                return m;
            }
        }
        return Optional.empty();
    }

    @Override
    public void open(Manifest manifest) {
        if (writable == null) {
            throw new IllegalStateException("no writable backend");
        }
        writable.open(manifest);
    }

    @Override
    public void open(Manifest manifest, byte[] table) {
        if (writable == null) {
            throw new IllegalStateException("no writable backend");
        }
        writable.open(manifest, table);
    }

    @Override
    public BitSet have(String root) {
        BitSet union = new BitSet();
        backends.forEach(b -> union.or(b.have(root)));
        return union;
    }

    @Override
    public Optional<byte[]> read(String root, int index, int offset, int length) {
        for (PieceStore b : backends) {
            Optional<byte[]> bytes = b.read(root, index, offset, length);
            if (bytes.isPresent()) {
                if (cacheExternal && !b.writable() && writable != null && offset == 0
                        && !writable.have(root).get(index)) {
                    b.manifest(root).ifPresent(m -> {
                        if (bytes.get().length == m.pieceLength(index)) {
                            writable.open(m);
                            writable.write(root, index, bytes.get());
                        }
                    });
                }
                return bytes;
            }
        }
        return Optional.empty();
    }

    @Override
    public void write(String root, int index, byte[] piece) {
        if (writable == null) {
            throw new IllegalStateException("no writable backend");
        }
        writable.write(root, index, piece);
    }

    @Override
    public SeekableByteChannel openArtifact(String root) throws IOException {
        IOException last = null;
        for (PieceStore b : backends) {
            if (b.have(root).cardinality() > 0) {
                try {
                    return b.openArtifact(root);
                } catch (IOException e) {
                    last = e;
                }
            }
        }
        throw last == null ? new IOException("root " + root + " is not held here") : last;
    }

    @Override
    public void pin(String root) {
        backends.forEach(b -> b.pin(root));
    }

    @Override
    public void unpin(String root) {
        backends.forEach(b -> b.unpin(root));
    }

    @Override
    public boolean pinned(String root) {
        return backends.stream().anyMatch(b -> b.pinned(root));
    }

    @Override
    public long bytesUsed() {
        return backends.stream().mapToLong(PieceStore::bytesUsed).sum();
    }

    @Override
    public long quota() {
        return writable == null ? 0 : writable.quota();
    }

    @Override
    public void evict() {
        backends.forEach(PieceStore::evict);
    }

    @Override
    public void forget(String root) {
        backends.forEach(b -> b.forget(root));
    }

    @Override
    public void flush() {
        backends.forEach(PieceStore::flush);
    }
}
