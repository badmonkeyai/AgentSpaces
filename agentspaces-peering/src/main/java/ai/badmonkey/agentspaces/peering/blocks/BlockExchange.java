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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.wire.Envelope;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Content-addressed block storage and exchange (spec §7.1, §9): large payloads
 * are stored under their CID ({@code multibase(sha-256(bytes))}), the space
 * replicates only the reference, and a replica missing a block asks holders with
 * {@code BLOCK_WANT} and verifies the returned {@code BLOCK} against the CID
 * before storing it, which makes integrity and deduplication free.
 *
 * <p>Since the large-data review (srad/large-networks-bulk-data.md, group B):
 * a block that answers a request after its waiter timed out is kept for the
 * next read (LB-B-01); candidates are asked one after another with a stagger,
 * so a hit from the first costs one copy (LB-B-03); the store has a byte quota
 * and evicts unreferenced blocks least recently served, never a block a live
 * entry references (LB-B-04); and a block that could never cross a frame is
 * refused at {@link #put} time rather than churning connections at every
 * reader (LB-B-06).
 */
public final class BlockExchange {

    /** Default byte quota of the in-memory store, 512 MiB. */
    public static final long DEFAULT_QUOTA_BYTES = 512L * 1024 * 1024;

    /**
     * Largest block {@link #put} accepts: the 8 MiB frame cap every binding
     * enforces, less 64 KiB for the envelope, signature, and CBOR framing.
     */
    public static final int DEFAULT_MAX_BLOCK_BYTES = 8 * 1024 * 1024 - 64 * 1024;

    /** How long after a request a late answer is still kept (LB-B-01). */
    static final long LATE_KEEP_WINDOW_MILLIS = 60_000;

    /** Upper bound on the stagger between candidates (LB-B-03). */
    static final long MAX_STAGGER_MILLIS = 2_000;

    private record Want(String cid) {
    }

    private record Block(String cid, byte[] bytes) {
    }

    /** Counters an operator or a test reads. */
    public record Stats(int blocks, long bytesUsed, long quotaBytes, long evicted,
                        long lateKept, long droppedUnsolicited, long refusedOversize) {
    }

    private final GroupRuntime runtime;
    private final CborCodec codec;
    private final InstantSource clock;
    private final long quotaBytes;
    private final int maxBlockBytes;
    private final BlockStore store;
    private final Map<String, AtomicInteger> refs = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<byte[]>> pending = new ConcurrentHashMap<>();
    private final Map<String, Long> requestedAt = new ConcurrentHashMap<>();
    private final AtomicLong bytesUsed = new AtomicLong();
    private final AtomicLong evicted = new AtomicLong();
    private final AtomicLong lateKept = new AtomicLong();
    private final AtomicLong droppedUnsolicited = new AtomicLong();
    private final AtomicLong refusedOversize = new AtomicLong();
    private final Object evictLock = new Object();

    /**
     * Attaches the exchange to a group runtime with the default quota and block
     * cap. One instance per group runtime.
     *
     * @param runtime the group runtime
     * @param codec   the CBOR codec
     */
    public BlockExchange(GroupRuntime runtime, CborCodec codec) {
        this(runtime, codec, InstantSource.system(), DEFAULT_QUOTA_BYTES, DEFAULT_MAX_BLOCK_BYTES);
    }

    /**
     * Attaches the exchange to a group runtime.
     *
     * @param runtime       the group runtime
     * @param codec         the CBOR codec
     * @param clock         the clock for the late-keep window and eviction order
     * @param quotaBytes    the store's byte quota; 0 or less means unbounded
     * @param maxBlockBytes the largest block {@link #put} accepts
     */
    public BlockExchange(GroupRuntime runtime, CborCodec codec, InstantSource clock,
                         long quotaBytes, int maxBlockBytes) {
        this(runtime, codec, clock, quotaBytes, maxBlockBytes, new MemoryBlockStore());
    }

    /**
     * Attaches the exchange to a group runtime over a given store (LB-B-05): a
     * {@link FileBlockStore} keeps blocks across restarts and starts with what it holds
     * counted against the quota.
     *
     * @param runtime       the group runtime
     * @param codec         the CBOR codec
     * @param clock         the clock for the late-keep window and eviction order
     * @param quotaBytes    the store's byte quota; 0 or less means unbounded
     * @param maxBlockBytes the largest block {@link #put} accepts
     * @param store         where the blocks live
     */
    public BlockExchange(GroupRuntime runtime, CborCodec codec, InstantSource clock,
                         long quotaBytes, int maxBlockBytes, BlockStore store) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.store = Objects.requireNonNull(store, "store");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.quotaBytes = quotaBytes;
        if (maxBlockBytes <= 0) {
            throw new IllegalArgumentException("maxBlockBytes must be positive");
        }
        this.maxBlockBytes = maxBlockBytes;
        this.bytesUsed.set(store.bytesUsed());
        runtime.onKind(Envelope.Kind.BLOCK_WANT, this::onWant);
        runtime.onKind(Envelope.Kind.BLOCK, (from, body) -> onBlock(body));
    }

    /**
     * Computes the CID of some bytes.
     *
     * @param bytes the bytes
     * @return the CID
     */
    public static String cidOf(byte[] bytes) {
        return Multibase.base58btc(Digests.sha256(bytes));
    }

    /**
     * Stores bytes locally and returns their CID.
     *
     * @param bytes the payload
     * @return the CID
     * @throws IllegalArgumentException when the block exceeds {@link #maxBlockBytes()},
     *                                  since no binding could ever deliver it (LB-B-06)
     */
    public String put(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length > maxBlockBytes) {
            refusedOversize.incrementAndGet();
            throw new IllegalArgumentException("block of " + bytes.length
                    + " bytes exceeds the largest block a frame carries (" + maxBlockBytes
                    + " bytes); a payload this large needs the content swarm"
                    + " (agentspaces.content.escalate-above)");
        }
        String cid = cidOf(bytes);
        storeBlock(cid, bytes.clone());
        return cid;
    }

    /**
     * Returns a locally held block.
     *
     * @param cid the CID
     * @return the bytes, when held here
     */
    public Optional<byte[]> local(String cid) {
        return store.get(cid, clock.millis());
    }

    /** The store the blocks live in. */
    public BlockStore store() {
        return store;
    }

    /**
     * Fetches a block: local store first, then {@code BLOCK_WANT} to the given
     * candidates one after another, with a stagger of up to two seconds (half
     * the timeout when that is shorter), verifying the delivery against the CID.
     * A delivery that lands after the timeout is still stored for the next read.
     *
     * @param cid           the CID
     * @param candidates    peers likely to hold the block (the writer first)
     * @param timeoutMillis how long to wait for delivery
     * @return the bytes, or empty when nobody delivered in time
     */
    public Optional<byte[]> fetch(String cid, List<PeerId> candidates, long timeoutMillis) {
        Optional<byte[]> held = local(cid);
        if (held.isPresent()) {
            return held;
        }
        CompletableFuture<byte[]> future =
                pending.computeIfAbsent(cid, c -> new CompletableFuture<>());
        requestedAt.put(cid, clock.millis());
        byte[] want = codec.toBytes(new Want(cid));
        long stagger = Math.min(MAX_STAGGER_MILLIS, Math.max(1, timeoutMillis / 2));
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        List<PeerId> targets = new ArrayList<>(candidates);
        try {
            int next = 0;
            while (true) {
                if (next < targets.size()) {
                    runtime.send(targets.get(next++), Envelope.Kind.BLOCK_WANT, want);
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return Optional.empty();
                }
                long wait = next < targets.size()
                        ? Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(stagger))
                        : remaining;
                try {
                    return Optional.of(future.get(wait, TimeUnit.NANOSECONDS));
                } catch (TimeoutException e) {
                    if (next >= targets.size()) {
                        return Optional.empty();
                    }
                    // the next candidate is asked on the next loop
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (java.util.concurrent.ExecutionException e) {
            return Optional.empty();
        } finally {
            pending.remove(cid, future);
        }
    }

    /**
     * Marks a block as referenced by a live entry, so eviction skips it (LB-B-04).
     * Balanced by {@link #release(String)} when the entry is collected.
     *
     * @param cid the CID
     */
    public void retain(String cid) {
        if (cid != null) {
            refs.computeIfAbsent(cid, c -> new AtomicInteger()).incrementAndGet();
        }
    }

    /**
     * Drops one reference taken with {@link #retain(String)}.
     *
     * @param cid the CID
     */
    public void release(String cid) {
        if (cid == null) {
            return;
        }
        AtomicInteger count = refs.get(cid);
        if (count != null && count.decrementAndGet() <= 0) {
            refs.remove(cid, count);
        }
    }

    /**
     * How many live entries reference a block (LB-B-04); zero makes it evictable.
     *
     * @param cid the CID
     * @return the reference count
     */
    public int references(String cid) {
        AtomicInteger count = refs.get(cid);
        return count == null ? 0 : count.get();
    }

    /** Returns how many blocks this node holds. */
    public int size() {
        return store.size();
    }

    /** Returns the bytes the store holds. */
    public long bytesUsed() {
        return bytesUsed.get();
    }

    /** Returns the store's byte quota, 0 or less when unbounded. */
    public long quotaBytes() {
        return quotaBytes;
    }

    /** Returns the largest block {@link #put} accepts. */
    public int maxBlockBytes() {
        return maxBlockBytes;
    }

    /** Returns the counters. */
    public Stats stats() {
        return new Stats(store.size(), bytesUsed.get(), quotaBytes, evicted.get(),
                lateKept.get(), droppedUnsolicited.get(), refusedOversize.get());
    }

    private void storeBlock(String cid, byte[] bytes) {
        if (!store.put(cid, bytes, clock.millis())) {
            return;
        }
        bytesUsed.addAndGet(bytes.length);
        evictIfNeeded(cid);
    }

    /**
     * Evicts least recently served, unreferenced blocks until the store is
     * within quota. The block just stored is never the one evicted, so a put or
     * a solicited delivery always succeeds for its caller.
     */
    private void evictIfNeeded(String justStored) {
        if (quotaBytes <= 0 || bytesUsed.get() <= quotaBytes) {
            return;
        }
        synchronized (evictLock) {
            while (bytesUsed.get() > quotaBytes) {
                String victim = null;
                long oldest = Long.MAX_VALUE;
                for (String cid : store.cids()) {
                    if (cid.equals(justStored) || refs.containsKey(cid) || pending.containsKey(cid)) {
                        continue;
                    }
                    long access = store.lastAccessMillis(cid);
                    if (access < oldest) {
                        oldest = access;
                        victim = cid;
                    }
                }
                if (victim == null) {
                    return; // everything left is referenced or in flight
                }
                long freed = store.remove(victim);
                if (freed >= 0) {
                    bytesUsed.addAndGet(-freed);
                    evicted.incrementAndGet();
                }
            }
        }
    }

    private void onWant(PeerId from, byte[] body) {
        Want want;
        try {
            want = codec.fromWire(body, Want.class);
        } catch (RuntimeException e) {
            return;
        }
        if (want == null || want.cid() == null) {
            return;
        }
        Optional<byte[]> held = store.get(want.cid(), clock.millis());
        if (held.isPresent()) {
            runtime.send(from, Envelope.Kind.BLOCK,
                    codec.toBytes(new Block(want.cid(), held.get())));
        }
    }

    private void onBlock(byte[] body) {
        Block block;
        try {
            block = codec.fromWire(body, Block.class);
        } catch (RuntimeException e) {
            return;
        }
        if (block == null || block.cid() == null || block.bytes() == null
                || !cidOf(block.bytes()).equals(block.cid())) {
            return; // corrupt or forged: the CID is the integrity check
        }
        // Only a block this node asked for is stored (ASF-013): an unsolicited
        // push, however CID-valid, cannot grow the store. Locally-put blocks
        // are the application's own working set and stay its responsibility.
        CompletableFuture<byte[]> waiter = pending.get(block.cid());
        if (waiter != null) {
            storeBlock(block.cid(), block.bytes());
            waiter.complete(block.bytes());
            return;
        }
        // LB-B-01: the waiter timed out but this node did ask, recently. Keep
        // the answer so the next read finds it instead of re-transferring.
        Long asked = requestedAt.get(block.cid());
        if (asked != null && clock.millis() - asked <= LATE_KEEP_WINDOW_MILLIS) {
            storeBlock(block.cid(), block.bytes());
            lateKept.incrementAndGet();
            return;
        }
        droppedUnsolicited.incrementAndGet();
    }
}
