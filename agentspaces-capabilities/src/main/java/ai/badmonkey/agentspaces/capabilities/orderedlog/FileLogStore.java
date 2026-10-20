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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.LogEntry;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.KeyFiles;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * A {@link LogStore} on the local filesystem (srad/agentspaces-js.md JS-N-06):
 * one directory per log holding a small {@code term-vote} file, rewritten
 * atomically (same-directory temp file, fsync, rename, the {@link KeyFiles}
 * recipe), and an append-only {@code entries} file of length-prefixed,
 * CRC-checked records. Every write returns only after {@code fsync}, which is
 * what lets {@link RaftLog} answer a vote or acknowledge an append on the
 * strength of it.
 *
 * <p>Both files are plain bytes, big-endian, no codec: {@code term-vote} is a
 * version byte, the term, and the vote (a 2-byte length and UTF-8, length 0 for
 * no vote); each entries record is the term, the command length, the command,
 * and a CRC32 over the three. A record cut short by a crash mid-append fails
 * its length or its CRC, so {@link #load()} drops it and everything after it
 * and trims the file back to the last whole record: a torn tail costs at most
 * an entry the log never acknowledged.
 *
 * <p>Thread-safety: every method is synchronized; the log calls the store
 * under its own lock anyway.
 */
public final class FileLogStore implements LogStore {

    private static final byte VERSION = 1;
    private static final String TERM_VOTE = "term-vote";
    private static final String ENTRIES = "entries";
    /** term (8) + length (4) + crc (4) around the command. */
    private static final int RECORD_OVERHEAD = 16;
    /** Commands larger than this are refused rather than trusted on read. */
    private static final int MAX_COMMAND = 16 * 1024 * 1024;

    private final Path directory;
    private final Path termVoteFile;
    private final Path entriesFile;
    /** Byte offset where record i (0-based) starts, plus the end offset last; built by {@link #load()}. */
    private final List<Long> offsets = new ArrayList<>();
    private boolean loaded;

    /**
     * A store rooted at {@code directory}, created on first write.
     *
     * @param directory the log's own directory; share it with nothing else
     */
    public FileLogStore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.termVoteFile = directory.resolve(TERM_VOTE);
        this.entriesFile = directory.resolve(ENTRIES);
    }

    /** The directory this store lives in. */
    public Path directory() {
        return directory;
    }

    @Override
    public synchronized State load() throws IOException {
        long term = 0;
        PeerId votedFor = null;
        if (Files.exists(termVoteFile)) {
            ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(termVoteFile));
            if (buffer.remaining() < 1 + 8 + 2 || buffer.get() != VERSION) {
                throw new IOException("unreadable term-vote file " + termVoteFile);
            }
            term = buffer.getLong();
            int length = Short.toUnsignedInt(buffer.getShort());
            if (buffer.remaining() != length) {
                throw new IOException("unreadable term-vote file " + termVoteFile);
            }
            if (length > 0) {
                byte[] vote = new byte[length];
                buffer.get(vote);
                votedFor = PeerId.of(new String(vote, StandardCharsets.UTF_8));
            }
        }
        List<LogEntry> entries = readEntries();
        loaded = true;
        return new State(term, votedFor, entries);
    }

    /** Reads every whole record, rebuilds {@link #offsets}, and trims a torn tail. */
    private List<LogEntry> readEntries() throws IOException {
        offsets.clear();
        offsets.add(0L);
        List<LogEntry> entries = new ArrayList<>();
        if (!Files.exists(entriesFile)) {
            return entries;
        }
        byte[] bytes = Files.readAllBytes(entriesFile);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long good = 0;
        while (buffer.remaining() >= RECORD_OVERHEAD) {
            int start = buffer.position();
            long term = buffer.getLong();
            int length = buffer.getInt();
            if (length < 0 || length > MAX_COMMAND || buffer.remaining() < length + 4) {
                break;
            }
            byte[] command = new byte[length];
            buffer.get(command);
            CRC32 crc = new CRC32();
            crc.update(bytes, start, 12 + length);
            if ((int) crc.getValue() != buffer.getInt()) {
                break;
            }
            entries.add(new LogEntry(term, command));
            good = buffer.position();
            offsets.add(good);
        }
        if (good < bytes.length) {
            try (FileChannel channel = FileChannel.open(entriesFile, StandardOpenOption.WRITE)) {
                channel.truncate(good);
                channel.force(true);
            }
        }
        return entries;
    }

    @Override
    public synchronized void saveTermVote(long term, PeerId votedFor) throws IOException {
        byte[] vote = votedFor == null ? new byte[0] : votedFor.value().getBytes(StandardCharsets.UTF_8);
        if (vote.length > 0xFFFF) {
            throw new IOException("vote too long to store: " + vote.length + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + 2 + vote.length);
        buffer.put(VERSION).putLong(term).putShort((short) vote.length).put(vote);
        KeyFiles.write(termVoteFile, buffer.array(), false);
    }

    @Override
    public synchronized void append(long fromIndex, List<LogEntry> entries) throws IOException {
        ensureLoaded();
        long count = offsets.size() - 1;
        if (fromIndex < 1 || fromIndex > count + 1) {
            throw new IllegalArgumentException("append at " + fromIndex + " would leave a gap in "
                    + count + " entries");
        }
        Files.createDirectories(directory);
        try (FileChannel channel = FileChannel.open(entriesFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long position = cutLocked(channel, fromIndex);
            for (LogEntry entry : entries) {
                byte[] record = encode(entry);
                ByteBuffer buffer = ByteBuffer.wrap(record);
                while (buffer.hasRemaining()) {
                    position += channel.write(buffer, position);
                }
                offsets.add(position);
            }
            channel.force(true);
        }
    }

    @Override
    public synchronized void truncateFrom(long index) throws IOException {
        ensureLoaded();
        if (!Files.exists(entriesFile)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(entriesFile, StandardOpenOption.WRITE)) {
            cutLocked(channel, index);
            channel.force(true);
        }
    }

    /** Drops records at and after 1-based {@code index}; returns the new end offset. */
    private long cutLocked(FileChannel channel, long index) throws IOException {
        int keep = (int) Math.max(0, Math.min(offsets.size() - 1, index - 1));
        long end = offsets.get(keep);
        if (keep < offsets.size() - 1) {
            offsets.subList(keep + 1, offsets.size()).clear();
            channel.truncate(end);
        }
        return end;
    }

    /** A store written before it is read learns its offsets first. */
    private void ensureLoaded() throws IOException {
        if (!loaded) {
            readEntries();
            loaded = true;
        }
    }

    private static byte[] encode(LogEntry entry) throws IOException {
        byte[] command = entry.command() == null ? new byte[0] : entry.command();
        if (command.length > MAX_COMMAND) {
            throw new IOException("command too long to store: " + command.length + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.allocate(RECORD_OVERHEAD + command.length);
        buffer.putLong(entry.term()).putInt(command.length).put(command);
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, 12 + command.length);
        buffer.putInt((int) crc.getValue());
        return buffer.array();
    }
}
