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

import ai.badmonkey.agentspaces.peering.blocks.BlockStore;
import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.io.InputStream;
import java.time.InstantSource;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * The program {@code PBlobTest} forks under {@code -Xmx64m} for PD-B-04: it streams a
 * seeded random stream of the given size in MiB through {@link PBlob#of(InputStream,
 * NodeStore)} into a store that counts what is written and keeps no bytes, and prints
 * one line, {@code root=<cid> chunks=<n> bytes=<written> maxHeap=<bytes>}. The test
 * runs the same method in its own JVM and compares. Not a test class: it holds no
 * {@code @Test} and its name matches no surefire include pattern.
 */
public final class HeapBoundedChunkRun {

    /** What a run produced. */
    public record Result(String root, long chunks, long bytes, long maxHeap) { }

    private HeapBoundedChunkRun() {
    }

    /** A store that counts and discards: every put is new, nothing is ever held. */
    static final class CountingStore implements BlockStore {
        long puts;
        long bytes;

        @Override
        public Optional<byte[]> get(String cid, long nowMillis) {
            return Optional.empty();
        }

        @Override
        public boolean put(String cid, byte[] bytes, long nowMillis) {
            puts++;
            this.bytes += bytes.length;
            return true;
        }

        @Override
        public long remove(String cid) {
            return -1;
        }

        @Override
        public Set<String> cids() {
            return Set.of();
        }

        @Override
        public long lastAccessMillis(String cid) {
            return -1;
        }

        @Override
        public int size() {
            return (int) Math.min(puts, Integer.MAX_VALUE);
        }

        @Override
        public long bytesUsed() {
            return bytes;
        }

        @Override
        public String kind() {
            return "counting";
        }
    }

    /** A seeded random stream of a size, produced as it is read. */
    static InputStream stream(long size) {
        Random random = new Random(42);
        return new InputStream() {
            private long remaining = size;
            private final byte[] block = new byte[64 * 1024];
            private int at = block.length;

            @Override
            public int read() {
                byte[] one = new byte[1];
                return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (remaining <= 0) {
                    return -1;
                }
                if (at == block.length) {
                    random.nextBytes(block);
                    at = 0;
                }
                int n = (int) Math.min(Math.min(len, block.length - at), remaining);
                System.arraycopy(block, at, b, off, n);
                at += n;
                remaining -= n;
                return n;
            }
        };
    }

    /** Streams {@code mib} MiB into a counting store and reports the root and the counts. */
    public static Result run(int mib) {
        CountingStore counting = new CountingStore();
        NodeStore store = new NodeStore(counting, InstantSource.system());
        PBlob blob = PBlob.of(stream((long) mib * 1024 * 1024), store);
        return new Result(blob.root(), blob.chunks().size(), blob.writtenChunkBytes(), Runtime.getRuntime().maxMemory());
    }

    /** Parses the line {@link #main} prints. */
    public static Result parse(String output) {
        String line = output.lines().filter(l -> l.startsWith("root=")).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no result line in: " + output));
        String[] fields = line.trim().split(" ");
        return new Result(fields[0].substring("root=".length()), Long.parseLong(fields[1].substring("chunks=".length())),
                Long.parseLong(fields[2].substring("bytes=".length())), Long.parseLong(fields[3].substring("maxHeap=".length())));
    }

    public static void main(String[] args) {
        Result result = run(Integer.parseInt(args[0]));
        System.out.println("root=" + result.root() + " chunks=" + result.chunks() + " bytes=" + result.bytes()
                + " maxHeap=" + result.maxHeap());
    }
}
