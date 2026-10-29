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
package ai.badmonkey.agentspaces.peering.blocks;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * A file-backed block store (LB-B-05): one file per CID under a directory, written
 * through a temporary name and an atomic move, read back on start so a restart serves
 * what it held and a live entry's block survives the process. The last-served time is
 * the file's modification time, which a read refreshes, so eviction order survives too.
 */
public final class FileBlockStore implements BlockStore {

    private final Path dir;
    private final Map<String, Long> sizes = new ConcurrentHashMap<>();
    private final Map<String, Long> lastAccess = new ConcurrentHashMap<>();
    private final AtomicLong bytesUsed = new AtomicLong();

    /**
     * Opens or creates a store, reading back every block it holds.
     *
     * @param dir the directory
     * @throws IOException when the directory cannot be created or listed
     */
    public FileBlockStore(Path dir) throws IOException {
        this.dir = Objects.requireNonNull(dir, "dir");
        Files.createDirectories(dir);
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                String name = file.getFileName().toString();
                if (!Files.isRegularFile(file) || !name.startsWith("z") || name.endsWith(".tmp")) {
                    continue;
                }
                long size = Files.size(file);
                sizes.put(name, size);
                lastAccess.put(name, Files.getLastModifiedTime(file).toMillis());
                bytesUsed.addAndGet(size);
            }
        }
    }

    /** The directory. */
    public Path dir() {
        return dir;
    }

    @Override
    public Optional<byte[]> get(String cid, long nowMillis) {
        if (!sizes.containsKey(cid)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = Files.readAllBytes(dir.resolve(cid));
            lastAccess.put(cid, nowMillis);
            try {
                Files.setLastModifiedTime(dir.resolve(cid), FileTime.fromMillis(nowMillis));
            } catch (IOException ignored) {
                // the in-memory order still holds for this process
            }
            return Optional.of(bytes);
        } catch (IOException e) {
            sizes.remove(cid);
            lastAccess.remove(cid);
            return Optional.empty();
        }
    }

    @Override
    public boolean put(String cid, byte[] bytes, long nowMillis) {
        if (sizes.containsKey(cid)) {
            lastAccess.put(cid, nowMillis);
            return false;
        }
        Path tmp = dir.resolve(cid + ".tmp");
        try {
            Files.write(tmp, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.SYNC);
            Files.move(tmp, dir.resolve(cid), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (sizes.putIfAbsent(cid, (long) bytes.length) != null) {
            return false; // a concurrent put won; the file is the same bytes
        }
        lastAccess.put(cid, nowMillis);
        bytesUsed.addAndGet(bytes.length);
        return true;
    }

    @Override
    public long remove(String cid) {
        Long size = sizes.remove(cid);
        if (size == null) {
            return -1;
        }
        lastAccess.remove(cid);
        bytesUsed.addAndGet(-size);
        try {
            Files.deleteIfExists(dir.resolve(cid));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return size;
    }

    @Override
    public Set<String> cids() {
        return Set.copyOf(sizes.keySet());
    }

    @Override
    public long lastAccessMillis(String cid) {
        return lastAccess.getOrDefault(cid, -1L);
    }

    @Override
    public int size() {
        return sizes.size();
    }

    @Override
    public long bytesUsed() {
        return bytesUsed.get();
    }

    @Override
    public String kind() {
        return "file";
    }
}
