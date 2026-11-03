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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serves artifacts from files the peer already has, where they lie (LB-T-06):
 * nothing is copied, every registered root is complete and external, and
 * eviction never touches it. A registry with two hundred checkpoints on disk
 * becomes a seeder of all of them by registering each file.
 */
public final class InPlaceStore implements ExternalStore {

    private record Held(Manifest manifest, Path file) {
    }

    private final Map<String, Held> held = new ConcurrentHashMap<>();

    @Override
    public String kind() {
        return "in-place";
    }

    @Override
    public boolean writable() {
        return false;
    }

    /**
     * Registers a file as the complete artifact of a root. The caller has hashed
     * the file and holds its manifest ({@code Content.publish(path, inPlace)}).
     *
     * @param manifest the manifest
     * @param file     the file
     */
    public void register(Manifest manifest, Path file) {
        held.put(manifest.root(), new Held(manifest, file));
    }

    /**
     * Registers the file named by a path string; see {@link #register(Manifest, Path)}.
     *
     * @param manifest the manifest
     * @param key      the file's path
     */
    @Override
    public void register(Manifest manifest, String key) {
        register(manifest, Path.of(key));
    }

    @Override
    public Set<String> roots() {
        return Set.copyOf(held.keySet());
    }

    @Override
    public Optional<Manifest> manifest(String root) {
        Held h = held.get(root);
        return h == null ? Optional.empty() : Optional.of(h.manifest());
    }

    @Override
    public void open(Manifest manifest) {
        // registration is explicit; an in-place root is never opened for writes
    }

    @Override
    public BitSet have(String root) {
        Held h = held.get(root);
        BitSet bits = new BitSet();
        if (h != null) {
            bits.set(0, h.manifest().pieces());
        }
        return bits;
    }

    @Override
    public Optional<byte[]> read(String root, int index, int offset, int length) {
        Held h = held.get(root);
        if (h == null || index < 0 || index >= h.manifest().pieces()) {
            return Optional.empty();
        }
        int pieceLength = h.manifest().pieceLength(index);
        if (offset < 0 || offset > pieceLength) {
            return Optional.empty();
        }
        int n = Math.min(length, pieceLength - offset);
        ByteBuffer buffer = ByteBuffer.allocate(n);
        try (FileChannel data = FileChannel.open(h.file(), StandardOpenOption.READ)) {
            long at = (long) index * h.manifest().pieceSize() + offset;
            while (buffer.hasRemaining()) {
                int read = data.read(buffer, at + buffer.position());
                if (read < 0) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Optional.of(java.util.Arrays.copyOf(buffer.array(), buffer.position()));
    }

    @Override
    public void write(String root, int index, byte[] piece) {
        throw new IllegalStateException("the in-place store is read-only");
    }

    @Override
    public SeekableByteChannel openArtifact(String root) throws IOException {
        Held h = held.get(root);
        if (h == null) {
            throw new IOException("root " + root + " is not registered here");
        }
        return FileChannel.open(h.file(), StandardOpenOption.READ);
    }

    @Override
    public void pin(String root) {
    }

    @Override
    public void unpin(String root) {
    }

    @Override
    public boolean pinned(String root) {
        return held.containsKey(root);
    }

    @Override
    public long bytesUsed() {
        return 0;
    }

    @Override
    public long quota() {
        return 0;
    }

    @Override
    public void evict() {
    }

    @Override
    public void forget(String root) {
        held.remove(root);
    }
}
