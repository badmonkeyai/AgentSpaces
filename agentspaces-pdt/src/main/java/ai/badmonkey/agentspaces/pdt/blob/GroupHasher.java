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

import ai.badmonkey.agentspaces.pdt.kinds.BoNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The chaining values of a stream's 16 KiB chunk groups, hashed on several threads in pure Java (PDT 2 phase 4
 * measures, P2-B-05, owner ruling of 2026-11-09; srad/agentspaces-pdt2.md §10.14). A group's chaining value depends
 * on its bytes and its place alone, as BLAKE3's tree allows, so the values, and the identity and outboard that
 * {@link BaoOutboard} builds from them, are the same for every thread count.
 *
 * <p>The calling thread reads the stream in batches of {@link #BATCH_GROUPS} groups (8 MiB). While up to
 * {@code threads - 1} shared hashing threads work through one batch, the caller reads the next one and then hashes
 * beside them. Each thread claims {@link #CLAIM_GROUPS} groups at a time, so cores of unequal speed finish a batch
 * together. A batch uses one thread per {@link #TASK_GROUPS} groups at most, so a blob under 512 KiB hashes on the
 * caller alone, and the first batch grows from one group, so a small blob allocates no 8 MiB buffer.
 */
final class GroupHasher {

    /** The groups the caller reads ahead while the batch before is hashing: 8 MiB. */
    static final int BATCH_GROUPS = 512;

    /** The groups a thread claims at a time: 64 KiB. */
    static final int CLAIM_GROUPS = 4;

    /** The fewest groups worth a thread of their own: 256 KiB, about half a millisecond of hashing. */
    static final int TASK_GROUPS = 16;

    private static final int GROUP = BoNode.GROUP_BYTES;
    private static final int BATCH_BYTES = BATCH_GROUPS * GROUP;

    /**
     * A hashed stream.
     *
     * @param cvs  the chaining value of each group, in order
     * @param size the stream's bytes
     * @param only the bytes of the one group when the stream holds one group (an empty stream included), else null
     */
    record Hashed(List<byte[]> cvs, long size, byte[] only) {
    }

    private GroupHasher() {
    }

    /**
     * Reads a stream to its end and hashes its chunk groups.
     *
     * @param in      the bytes; read to the end, not closed
     * @param threads the most threads that hash at once, the caller included; at least 1
     * @return the chaining values
     * @throws IOException when the stream fails, or the caller is interrupted while it waits for a batch
     */
    static Hashed hash(InputStream in, int threads) throws IOException {
        byte[] batch = new byte[GROUP];
        int filled = in.readNBytes(batch, 0, batch.length);
        while (filled == batch.length && batch.length < BATCH_BYTES) {
            batch = Arrays.copyOf(batch, batch.length * 2);
            filled += in.readNBytes(batch, filled, batch.length - filled);
        }
        if (filled <= GROUP) {
            byte[] only = Arrays.copyOf(batch, filled);
            return new Hashed(List.of(Blake3.groupCv(only, 0, filled, 0)), filled, only);
        }
        List<byte[]> cvs = new ArrayList<>();
        long size = 0;
        byte[] spare = null;
        while (true) {
            Batch hashing = new Batch(batch, filled, cvs.size(), threads);
            boolean more = filled == batch.length;
            byte[] next = null;
            int nextFilled = 0;
            if (more) {
                next = spare != null ? spare : new byte[BATCH_BYTES];
                try {
                    nextFilled = in.readNBytes(next, 0, next.length);
                } catch (IOException | RuntimeException e) {
                    hashing.abandon();
                    throw e;
                }
            }
            cvs.addAll(hashing.finish());
            size += filled;
            if (!more || nextFilled == 0) {
                return new Hashed(cvs, size, null);
            }
            spare = batch;
            batch = next;
            filled = nextFilled;
        }
    }

    /** One batch's groups, claimed {@link #CLAIM_GROUPS} at a time by its helpers and then by the caller. */
    private static final class Batch {
        private final byte[] bytes;
        private final int length;
        private final long firstGroup;
        private final byte[][] cvs;
        private final int claims;
        private final AtomicInteger next = new AtomicInteger();
        private final CountDownLatch done;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final List<Future<?>> helpers = new ArrayList<>();

        Batch(byte[] bytes, int length, long firstGroup, int threads) {
            this.bytes = bytes;
            this.length = length;
            this.firstGroup = firstGroup;
            this.cvs = new byte[(length + GROUP - 1) / GROUP][];
            this.claims = (cvs.length + CLAIM_GROUPS - 1) / CLAIM_GROUPS;
            this.done = new CountDownLatch(claims);
            int workers = Math.min(threads, Math.max(1, cvs.length / TASK_GROUPS));
            for (int i = 1; i < workers; i++) {
                helpers.add(Pool.HASHERS.submit(this::work));
            }
        }

        private void work() {
            for (int claim; (claim = next.getAndIncrement()) < claims; ) {
                try {
                    int to = Math.min(cvs.length, (claim + 1) * CLAIM_GROUPS);
                    for (int g = claim * CLAIM_GROUPS; g < to; g++) {
                        int start = g * GROUP;
                        cvs[g] = Blake3.groupCv(bytes, start, Math.min(GROUP, length - start), (firstGroup + g) << BoNode.GROUP_LOG);
                    }
                } catch (RuntimeException | Error e) {
                    failure.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            }
        }

        /** Hashes beside the helpers, waits for every claim, and returns the batch's values in order. */
        List<byte[]> finish() throws IOException {
            work();
            try {
                done.await();
            } catch (InterruptedException e) {
                abandon();
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while hashing the chunk groups of a blob");
            }
            for (Future<?> helper : helpers) {
                helper.cancel(false); // a helper still queued has nothing left to claim
            }
            Throwable failed = failure.get();
            if (failed instanceof RuntimeException e) {
                throw e;
            }
            if (failed instanceof Error e) {
                throw e;
            }
            return Arrays.asList(cvs);
        }

        /** Leaves the rest of the batch unclaimed, when the caller stops before it finishes. */
        void abandon() {
            next.set(claims);
            for (Future<?> helper : helpers) {
                helper.cancel(false);
            }
        }
    }

    /** The hashing threads every outboard shares: at most one per core, made on demand and gone after 30 s idle. */
    private static final class Pool {
        static final ExecutorService HASHERS = create();

        private static ExecutorService create() {
            int cores = Runtime.getRuntime().availableProcessors();
            AtomicInteger made = new AtomicInteger();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(cores, cores, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                    task -> {
                        Thread thread = new Thread(task, "agentspaces-blake3-" + made.incrementAndGet());
                        thread.setDaemon(true);
                        return thread;
                    });
            pool.allowCoreThreadTimeOut(true);
            return pool;
        }
    }
}
