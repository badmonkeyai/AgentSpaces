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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.peering.blocks.BlockStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import java.util.zip.CRC32;

/**
 * A {@link BlockStore} over pack files (srad/agentspaces-pdt.md §3.4; PD-S-02): nodes
 * append to the current pack under a directory, a pack closes at 256 MiB by default
 * and a new one opens, and an index in memory maps each CID to its pack, offset, and
 * length. The one-file-per-CID store of the peering module suits a few thousand
 * blocks; a 1 GiB blob holds about 16,000 chunks, so this store keeps many small
 * nodes in a few large files.
 *
 * <p><b>Record framing.</b> Every record starts with a four-byte length prefix and
 * carries the CID, a one-byte liveness flag, the last-served time, the payload, and a
 * CRC-32 trailer over the CID, time, and payload. A rebuild reads each pack from the
 * start and stops at the first record whose length runs past the end of the file or
 * whose CRC fails, which is how a torn tail from a crash is detected; it truncates the
 * file at that point so later appends frame correctly. A record the rebuild reaches
 * intact is held; a record it does not reach is absent, and a fetch brings the node
 * back by its CID.
 *
 * <p><b>Removal and compaction.</b> A removal flips the record's liveness byte to
 * dead in place, which is the one write the store makes outside an append, and
 * counts the record's bytes as dead in its pack. When more than half of a pack's
 * bytes are dead the store compacts it: it rolls to a fresh pack when the pack is
 * the current one, copies every live record into the current pack, forces the
 * current pack to disk, and only then deletes the old file. A crash before the
 * delete leaves both copies, and the rebuild keeps the later one and counts the
 * earlier as dead, so a live node survives a crash at any step (PD-Q-REL-05;
 * {@code PackNodeStoreCrashTest}). Re-opening a directory finishes any compaction
 * the crash interrupted.
 *
 * <p><b>Index file.</b> {@link #close} forces every pack and writes {@code index.pdti}
 * through a temporary file and an atomic move; {@link #open} loads it when its pack
 * list and lengths match the files on disk and otherwise rebuilds the index from the
 * packs. The index file is deleted after a load, so a process that dies without
 * closing always rebuilds on the next open ({@link #rebuiltOnOpen()} says which path
 * ran). Last-served times live in the record at write time and in the index file at
 * close; a rebuild after a crash restores the write-time value.
 *
 * <p><b>fsync policy.</b> The store writes through a {@link FileChannel} and does not
 * force each put; it forces the current pack before rolling to a new one, the
 * destination pack before a compaction deletes its source, every pack and the index
 * at close, and on {@link #sync()}. {@link Options#syncEveryPut} forces after every
 * append for callers that want each node durable before {@code put} returns. A crash
 * under the default policy loses at most the unforced tail of the current pack,
 * which the frame check ignores, and any dead marks not yet on disk, which make a
 * removed node reappear until the next collection removes it again. No crash makes
 * a held CID return other bytes: the CRC refuses a damaged record, and
 * {@code NodeStore} recomputes the CID of every read (PD-N-06).
 *
 * <p><b>Meta records.</b> The store also keeps small named records of its owner's state
 * beside the nodes (PDT 2 phase 1, P2-V-03): the pins of {@code NodeStore} and the retained
 * versions of {@link Retention}. A meta record uses the node framing with the key
 * {@code meta:<name>} in the CID field, which no CID can be, since a CID's text is base32
 * without a colon. The latest record of a name wins: {@link #putMeta} appends the new record
 * and then marks the old one dead, so a crash between the two leaves both live and the rebuild
 * keeps the later. Meta records are no nodes: {@link #cids}, {@link #size}, and
 * {@link #bytesUsed} leave them out, so a collector never sweeps one, and compaction carries
 * the live ones like any record.
 *
 * <p>Thread safety: reads share a read lock and use positional channel reads; puts,
 * removals, and compaction take the write lock.
 */
public final class PackNodeStore implements BlockStore, MetaStore, AutoCloseable {

    /** The default pack size: a pack closes once an append would carry it past 256 MiB. */
    public static final long DEFAULT_MAX_PACK_BYTES = 256L * 1024 * 1024;

    /** The index file name under the directory. */
    public static final String INDEX_FILE = "index.pdti";

    private static final String PACK_PREFIX = "pack-";
    private static final String PACK_SUFFIX = ".pack";
    private static final int INDEX_MAGIC = 0x50445458; // "PDTX"
    private static final int INDEX_VERSION = 1;
    private static final byte LIVE = 1;
    private static final byte DEAD = 0;
    /** The key prefix of a meta record, which no CID's base32 text contains. */
    private static final String META_PREFIX = "meta:";
    /** The names a meta record may take. */
    private static final java.util.regex.Pattern META_NAME = java.util.regex.Pattern.compile("[A-Za-z0-9._:-]{1,200}");
    /** Length prefix (4), flag (1), CID length (2), last-served (8), CRC (4). */
    private static final int FIXED_OVERHEAD = 4 + 1 + 2 + 8 + 4;

    /**
     * Tuning for a store (PD-S-02).
     *
     * @param maxPackBytes the size at which a pack closes, {@link #DEFAULT_MAX_PACK_BYTES} in production
     * @param syncEveryPut whether to force the pack after every append
     * @param failurePoint a hook the crash test uses to stop a compaction mid-way; {@link FailurePoint#NONE} otherwise
     */
    public record Options(long maxPackBytes, boolean syncEveryPut, FailurePoint failurePoint) {
        public Options {
            if (maxPackBytes <= 0) {
                throw new IllegalArgumentException("maxPackBytes must be positive");
            }
            Objects.requireNonNull(failurePoint, "failurePoint");
        }

        /** The production options. */
        public static Options defaults() {
            return new Options(DEFAULT_MAX_PACK_BYTES, false, FailurePoint.NONE);
        }

        /** These options with another pack size. */
        public Options withMaxPackBytes(long bytes) {
            return new Options(bytes, syncEveryPut, failurePoint);
        }

        /** These options with another failure hook. */
        public Options withFailurePoint(FailurePoint point) {
            return new Options(maxPackBytes, syncEveryPut, point);
        }

        /** These options forcing after every put. */
        public Options withSyncEveryPut(boolean sync) {
            return new Options(maxPackBytes, sync, failurePoint);
        }
    }

    /**
     * A hook the crash test uses to simulate a process death at a named step of
     * compaction (PD-Q-REL-05). The steps are {@code compact.start}, {@code compact.copied}
     * after each record copied, {@code compact.forced} after the destination is forced,
     * {@code compact.deleted} after the source file is gone, and {@code compact.done}.
     * An implementation that throws stops the store where it is; the caller then
     * abandons the instance and opens the directory again.
     */
    @FunctionalInterface
    public interface FailurePoint {
        /** The hook that never fails. */
        FailurePoint NONE = step -> { };

        /**
         * Called at a step.
         *
         * @param step the step name
         */
        void reached(String step);
    }

    /** One live record in the index: where it is and when it was last served. */
    private static final class Entry {
        final long pack;
        final long offset;
        final int cidLength;
        final int length;
        volatile long lastAccess;

        Entry(long pack, long offset, int cidLength, int length, long lastAccess) {
            this.pack = pack;
            this.offset = offset;
            this.cidLength = cidLength;
            this.length = length;
            this.lastAccess = lastAccess;
        }

        long payloadPosition() {
            return offset + 4 + 1 + 2 + cidLength + 8;
        }

        long recordBytes() {
            return recordBytes(cidLength, length);
        }

        static long recordBytes(int cidLength, int length) {
            return FIXED_OVERHEAD + (long) cidLength + length;
        }
    }

    /** One pack file. */
    private static final class Pack {
        final long id;
        final Path path;
        FileChannel channel;
        long length;
        long dead;

        Pack(long id, Path path, FileChannel channel, long length) {
            this.id = id;
            this.path = path;
            this.channel = channel;
            this.length = length;
        }
    }

    private final Path dir;
    private final Options options;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<Long, Pack> packs = new TreeMap<>();
    private final Map<String, Entry> index = new HashMap<>();
    /** The live meta records by name (P2-V-03). */
    private final Map<String, Entry> metaIndex = new HashMap<>();
    private long bytesUsed;
    private Pack current;
    private boolean rebuiltOnOpen;
    private boolean closed;

    private PackNodeStore(Path dir, Options options) {
        this.dir = dir;
        this.options = options;
    }

    /**
     * Opens or creates a store with the default options.
     *
     * @param dir the directory
     * @return the store
     * @throws IOException when the directory cannot be created or a pack cannot be opened
     */
    public static PackNodeStore open(Path dir) throws IOException {
        return open(dir, Options.defaults());
    }

    /**
     * Opens or creates a store.
     *
     * @param dir     the directory
     * @param options the options
     * @return the store
     * @throws IOException when the directory cannot be created or a pack cannot be opened
     */
    public static PackNodeStore open(Path dir, Options options) throws IOException {
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(options, "options");
        Files.createDirectories(dir);
        PackNodeStore store = new PackNodeStore(dir, options);
        store.start();
        return store;
    }

    /** The directory. */
    public Path dir() {
        return dir;
    }

    /** The options. */
    public Options options() {
        return options;
    }

    /** Whether the last {@link #open} rebuilt the index from the packs instead of loading the index file. */
    public boolean rebuiltOnOpen() {
        return rebuiltOnOpen;
    }

    /** The ids of the packs on disk, ascending; the last is the current pack. */
    public List<Long> packIds() {
        lock.readLock().lock();
        try {
            return List.copyOf(packs.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /** The bytes of a pack file as the store counts them, or -1 when no such pack exists. */
    public long packLength(long packId) {
        lock.readLock().lock();
        try {
            Pack pack = packs.get(packId);
            return pack == null ? -1 : pack.length;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** The dead bytes of a pack, or -1 when no such pack exists. */
    public long packDeadBytes(long packId) {
        lock.readLock().lock();
        try {
            Pack pack = packs.get(packId);
            return pack == null ? -1 : pack.dead;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** The pack that holds a CID, or -1 when the CID is absent. */
    public long packOf(String cid) {
        lock.readLock().lock();
        try {
            Entry entry = index.get(cid);
            return entry == null ? -1 : entry.pack;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<byte[]> get(String cid, long nowMillis) {
        Objects.requireNonNull(cid, "cid");
        lock.readLock().lock();
        try {
            checkOpen();
            Entry entry = index.get(cid);
            if (entry == null) {
                return Optional.empty();
            }
            Pack pack = packs.get(entry.pack);
            byte[] bytes = new byte[entry.length];
            readFully(pack.channel, ByteBuffer.wrap(bytes), entry.payloadPosition());
            entry.lastAccess = nowMillis;
            return Optional.of(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("reading " + cid, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean put(String cid, byte[] bytes, long nowMillis) {
        Objects.requireNonNull(cid, "cid");
        Objects.requireNonNull(bytes, "bytes");
        lock.writeLock().lock();
        try {
            checkOpen();
            Entry held = index.get(cid);
            if (held != null) {
                held.lastAccess = nowMillis;
                return false;
            }
            Entry entry = append(cid, bytes, nowMillis);
            index.put(cid, entry);
            bytesUsed += bytes.length;
            if (options.syncEveryPut) {
                current.channel.force(false);
            }
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException("writing " + cid, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long remove(String cid) {
        Objects.requireNonNull(cid, "cid");
        lock.writeLock().lock();
        try {
            checkOpen();
            Entry entry = index.remove(cid);
            if (entry == null) {
                return -1;
            }
            Pack pack = packs.get(entry.pack);
            markDead(pack, entry.offset);
            pack.dead += entry.recordBytes();
            bytesUsed -= entry.length;
            compactIfNeeded(pack);
            return entry.length;
        } catch (IOException e) {
            throw new UncheckedIOException("removing " + cid, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Set<String> cids() {
        lock.readLock().lock();
        try {
            return Set.copyOf(index.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<byte[]> meta(String name) {
        Objects.requireNonNull(name, "name");
        lock.readLock().lock();
        try {
            checkOpen();
            Entry entry = metaIndex.get(name);
            if (entry == null) {
                return Optional.empty();
            }
            byte[] bytes = new byte[entry.length];
            readFully(packs.get(entry.pack).channel, ByteBuffer.wrap(bytes), entry.payloadPosition());
            return Optional.of(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("reading meta " + name, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void putMeta(String name, byte[] value) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(value, "value");
        if (!META_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("a meta name is 1 to 200 of [A-Za-z0-9._:-], got " + name);
        }
        lock.writeLock().lock();
        try {
            checkOpen();
            Entry entry = append(META_PREFIX + name, value, 0);
            if (options.syncEveryPut) {
                current.channel.force(false);
            }
            Entry previous = metaIndex.put(name, entry);
            if (previous != null) {
                Pack pack = packs.get(previous.pack);
                markDead(pack, previous.offset);
                pack.dead += previous.recordBytes();
                compactIfNeeded(pack);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("writing meta " + name, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Set<String> metaNames() {
        lock.readLock().lock();
        try {
            return Set.copyOf(metaIndex.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long lastAccessMillis(String cid) {
        lock.readLock().lock();
        try {
            Entry entry = index.get(cid);
            return entry == null ? -1 : entry.lastAccess;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return index.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long bytesUsed() {
        lock.readLock().lock();
        try {
            return bytesUsed;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String kind() {
        return "pack";
    }

    /** The bytes of every pack file on disk, live and dead, as the store counts them. */
    public long packBytes() {
        lock.readLock().lock();
        try {
            long total = 0;
            for (Pack pack : packs.values()) {
                total += pack.length;
            }
            return total;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Forces every pack to disk.
     *
     * @throws UncheckedIOException when a force fails
     */
    public void sync() {
        lock.writeLock().lock();
        try {
            checkOpen();
            for (Pack pack : packs.values()) {
                pack.channel.force(true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("sync", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compacts one pack now whether or not it is more than half dead, for an operator or a test.
     *
     * @param packId the pack
     * @return whether such a pack existed
     */
    public boolean compact(long packId) {
        lock.writeLock().lock();
        try {
            checkOpen();
            Pack pack = packs.get(packId);
            if (pack == null) {
                return false;
            }
            compactPack(pack);
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException("compacting pack " + packId, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Forces every pack, writes the index file, and closes the channels. A second call does nothing.
     *
     * @throws UncheckedIOException when a write fails
     */
    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            for (Pack pack : packs.values()) {
                pack.channel.force(true);
            }
            writeIndex();
            closeChannels();
            closed = true;
        } catch (IOException e) {
            throw new UncheckedIOException("closing " + dir, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Drops the channels without forcing or writing the index: what a crashed process leaves behind. */
    void abandon() {
        lock.writeLock().lock();
        try {
            closeChannels();
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---- open and rebuild ----

    private void start() throws IOException {
        List<Path> files = new ArrayList<>();
        try (Stream<Path> listing = Files.list(dir)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                String name = file.getFileName().toString();
                if (Files.isRegularFile(file) && name.startsWith(PACK_PREFIX) && name.endsWith(PACK_SUFFIX)) {
                    files.add(file);
                }
            }
        }
        for (Path file : files) {
            String name = file.getFileName().toString();
            long id;
            try {
                id = Long.parseLong(name.substring(PACK_PREFIX.length(), name.length() - PACK_SUFFIX.length()));
            } catch (NumberFormatException e) {
                continue;
            }
            FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
            packs.put(id, new Pack(id, file, channel, channel.size()));
        }
        Path indexFile = dir.resolve(INDEX_FILE);
        boolean loaded = Files.exists(indexFile) && loadIndex(indexFile);
        Files.deleteIfExists(indexFile);
        if (!loaded) {
            index.clear();
            metaIndex.clear();
            bytesUsed = 0;
            for (Pack pack : packs.values()) {
                pack.dead = 0;
            }
            for (Pack pack : packs.values()) {
                scanPack(pack);
            }
            rebuiltOnOpen = true;
        }
        if (packs.isEmpty()) {
            current = newPack(1);
        } else {
            current = packs.get(packs.lastKey());
        }
        for (Pack pack : List.copyOf(packs.values())) {
            compactIfNeeded(pack);
        }
    }

    /** Reads a pack from the start, indexing live records and stopping at a torn tail. */
    private void scanPack(Pack pack) throws IOException {
        long offset = 0;
        ByteBuffer prefix = ByteBuffer.allocate(4);
        while (offset + 4 <= pack.length) {
            prefix.clear();
            readFully(pack.channel, prefix, offset);
            int recordLength = prefix.getInt(0);
            if (recordLength < FIXED_OVERHEAD - 4 || offset + 4 + recordLength > pack.length) {
                break;
            }
            ByteBuffer body = ByteBuffer.allocate(recordLength);
            readFully(pack.channel, body, offset + 4);
            byte flag = body.get(0);
            int cidLength = body.getShort(1) & 0xFFFF;
            int payloadLength = recordLength - (FIXED_OVERHEAD - 4) - cidLength;
            if (payloadLength < 0 || (flag != LIVE && flag != DEAD)) {
                break;
            }
            CRC32 crc = new CRC32();
            crc.update(body.array(), 1, 2 + cidLength + 8 + payloadLength);
            if ((int) crc.getValue() != body.getInt(recordLength - 4)) {
                break;
            }
            long recordBytes = 4L + recordLength;
            if (flag == DEAD) {
                pack.dead += recordBytes;
            } else {
                String cid = new String(body.array(), 3, cidLength, StandardCharsets.US_ASCII);
                long lastAccess = body.getLong(3 + cidLength);
                Entry entry = new Entry(pack.id, offset, cidLength, payloadLength, lastAccess);
                if (cid.startsWith(META_PREFIX)) {
                    // the later record of a name wins; a crash may have left the earlier one live
                    Entry previous = metaIndex.put(cid.substring(META_PREFIX.length()), entry);
                    if (previous != null) {
                        packs.get(previous.pack).dead += previous.recordBytes();
                    }
                } else {
                    Entry previous = index.put(cid, entry);
                    if (previous != null) {
                        packs.get(previous.pack).dead += previous.recordBytes();
                    } else {
                        bytesUsed += payloadLength;
                    }
                }
            }
            offset += recordBytes;
        }
        if (offset < pack.length) {
            pack.channel.truncate(offset);
            pack.length = offset;
        }
    }

    private boolean loadIndex(Path indexFile) throws IOException {
        byte[] bytes = Files.readAllBytes(indexFile);
        if (bytes.length < 4 + 2 + 4 + 4 + 4) {
            return false;
        }
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if ((int) crc.getValue() != in.getInt(bytes.length - 4)) {
            return false;
        }
        if (in.getInt() != INDEX_MAGIC || (in.getShort() & 0xFFFF) != INDEX_VERSION) {
            return false;
        }
        int packCount = in.getInt();
        if (packCount != packs.size()) {
            return false;
        }
        Map<Long, Long> deadByPack = new HashMap<>();
        for (int i = 0; i < packCount; i++) {
            long id = in.getLong();
            long length = in.getLong();
            long dead = in.getLong();
            Pack pack = packs.get(id);
            if (pack == null || pack.length != length) {
                return false;
            }
            deadByPack.put(id, dead);
        }
        int entryCount = in.getInt();
        Map<String, Entry> loaded = new HashMap<>(entryCount * 2);
        Map<String, Entry> loadedMeta = new HashMap<>();
        long used = 0;
        for (int i = 0; i < entryCount; i++) {
            int cidLength = in.getShort() & 0xFFFF;
            byte[] cidBytes = new byte[cidLength];
            in.get(cidBytes);
            long pack = in.getLong();
            long offset = in.getLong();
            int length = in.getInt();
            long lastAccess = in.getLong();
            if (!packs.containsKey(pack)) {
                return false;
            }
            String key = new String(cidBytes, StandardCharsets.US_ASCII);
            Entry entry = new Entry(pack, offset, cidLength, length, lastAccess);
            if (key.startsWith(META_PREFIX)) {
                loadedMeta.put(key.substring(META_PREFIX.length()), entry);
                continue;
            }
            loaded.put(key, entry);
            used += length;
        }
        index.putAll(loaded);
        metaIndex.putAll(loadedMeta);
        bytesUsed = used;
        for (Pack pack : packs.values()) {
            pack.dead = deadByPack.get(pack.id);
        }
        return true;
    }

    private void writeIndex() throws IOException {
        // the meta records ride as entries under their record keys, which no CID can be
        Map<String, Entry> entries = new LinkedHashMap<>(index);
        metaIndex.forEach((name, entry) -> entries.put(META_PREFIX + name, entry));
        int size = 4 + 2 + 4 + packs.size() * 24 + 4;
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            size += 2 + e.getValue().cidLength + 8 + 8 + 4 + 8;
        }
        size += 4;
        ByteBuffer out = ByteBuffer.allocate(size);
        out.putInt(INDEX_MAGIC).putShort((short) INDEX_VERSION).putInt(packs.size());
        for (Pack pack : packs.values()) {
            out.putLong(pack.id).putLong(pack.length).putLong(pack.dead);
        }
        out.putInt(entries.size());
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            Entry entry = e.getValue();
            out.putShort((short) entry.cidLength);
            out.put(e.getKey().getBytes(StandardCharsets.US_ASCII));
            out.putLong(entry.pack).putLong(entry.offset).putInt(entry.length).putLong(entry.lastAccess);
        }
        CRC32 crc = new CRC32();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        Path tmp = dir.resolve(INDEX_FILE + ".tmp");
        Files.write(tmp, out.array());
        Files.move(tmp, dir.resolve(INDEX_FILE), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void closeChannels() {
        for (Pack pack : packs.values()) {
            try {
                pack.channel.close();
            } catch (IOException ignored) {
                // closing is best effort; the data is on disk or in the torn tail
            }
        }
    }

    // ---- appends, dead marks, packs ----

    private Entry append(String cid, byte[] payload, long lastAccess) throws IOException {
        byte[] cidBytes = cid.getBytes(StandardCharsets.US_ASCII);
        if (cidBytes.length > 0xFFFF) {
            throw new IllegalArgumentException("CID longer than 65535 bytes");
        }
        long recordBytes = Entry.recordBytes(cidBytes.length, payload.length);
        if (current.length > 0 && current.length + recordBytes > options.maxPackBytes) {
            roll();
        }
        int recordLength = (int) (recordBytes - 4);
        ByteBuffer out = ByteBuffer.allocate((int) recordBytes);
        out.putInt(recordLength);
        out.put(LIVE);
        out.putShort((short) cidBytes.length);
        out.put(cidBytes);
        out.putLong(lastAccess);
        out.put(payload);
        CRC32 crc = new CRC32();
        crc.update(out.array(), 5, 2 + cidBytes.length + 8 + payload.length);
        out.putInt((int) crc.getValue());
        out.flip();
        long offset = current.length;
        writeFully(current.channel, out, offset);
        current.length += recordBytes;
        return new Entry(current.id, offset, cidBytes.length, payload.length, lastAccess);
    }

    private void markDead(Pack pack, long offset) throws IOException {
        writeFully(pack.channel, ByteBuffer.wrap(new byte[] {DEAD}), offset + 4);
    }

    private void roll() throws IOException {
        current.channel.force(false);
        current = newPack(packs.isEmpty() ? 1 : packs.lastKey() + 1);
    }

    private Pack newPack(long id) throws IOException {
        Path path = dir.resolve(String.format("%s%08d%s", PACK_PREFIX, id, PACK_SUFFIX));
        FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        Pack pack = new Pack(id, path, channel, 0);
        packs.put(id, pack);
        return pack;
    }

    private void compactIfNeeded(Pack pack) throws IOException {
        if (pack.length > 0 && pack.dead * 2 > pack.length) {
            compactPack(pack);
        }
    }

    /**
     * Copies every live record of a pack into the current pack, forces it, and deletes the
     * source (PD-S-02). Every step leaves the directory in a state a rebuild reads correctly.
     */
    private void compactPack(Pack source) throws IOException {
        FailurePoint hook = options.failurePoint;
        hook.reached("compact.start");
        if (source == current) {
            roll();
        }
        long offset = 0;
        ByteBuffer prefix = ByteBuffer.allocate(4);
        while (offset + 4 <= source.length) {
            prefix.clear();
            readFully(source.channel, prefix, offset);
            int recordLength = prefix.getInt(0);
            if (recordLength < FIXED_OVERHEAD - 4 || offset + 4 + recordLength > source.length) {
                break;
            }
            ByteBuffer body = ByteBuffer.allocate(recordLength);
            readFully(source.channel, body, offset + 4);
            byte flag = body.get(0);
            int cidLength = body.getShort(1) & 0xFFFF;
            int payloadLength = recordLength - (FIXED_OVERHEAD - 4) - cidLength;
            if (payloadLength < 0) {
                break;
            }
            if (flag == LIVE) {
                String cid = new String(body.array(), 3, cidLength, StandardCharsets.US_ASCII);
                boolean meta = cid.startsWith(META_PREFIX);
                Map<String, Entry> map = meta ? metaIndex : index;
                String key = meta ? cid.substring(META_PREFIX.length()) : cid;
                Entry entry = map.get(key);
                if (entry != null && entry.pack == source.id && entry.offset == offset) {
                    byte[] payload = new byte[payloadLength];
                    System.arraycopy(body.array(), 3 + cidLength + 8, payload, 0, payloadLength);
                    Entry moved = append(cid, payload, entry.lastAccess);
                    map.put(key, moved);
                    hook.reached("compact.copied");
                }
            }
            offset += 4L + recordLength;
        }
        current.channel.force(false);
        hook.reached("compact.forced");
        source.channel.close();
        Files.deleteIfExists(source.path);
        hook.reached("compact.deleted");
        packs.remove(source.id);
        hook.reached("compact.done");
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("store closed: " + dir);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long at = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, at);
            if (read < 0) {
                throw new IOException("short read at " + at);
            }
            at += read;
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long at = position;
        while (buffer.hasRemaining()) {
            at += channel.write(buffer, at);
        }
    }
}
