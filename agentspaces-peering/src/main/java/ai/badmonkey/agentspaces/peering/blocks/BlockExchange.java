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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Content-addressed block storage and exchange (spec §7.1, §9): large payloads
 * are stored under their CID ({@code multibase(sha-256(bytes))}), the space
 * replicates only the reference, and a replica missing a block asks holders with
 * {@code BLOCK_WANT} and verifies the returned {@code BLOCK} against the CID
 * before storing it, which makes integrity and deduplication free.
 */
public final class BlockExchange {

    private record Want(String cid) {
    }

    private record Block(String cid, byte[] bytes) {
    }

    private final GroupRuntime runtime;
    private final CborCodec codec;
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<byte[]>> pending = new ConcurrentHashMap<>();

    /**
     * Attaches the exchange to a group runtime. One instance per group runtime.
     *
     * @param runtime the group runtime
     * @param codec   the CBOR codec
     */
    public BlockExchange(GroupRuntime runtime, CborCodec codec) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.codec = Objects.requireNonNull(codec, "codec");
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
     */
    public String put(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        String cid = cidOf(bytes);
        store.putIfAbsent(cid, bytes.clone());
        return cid;
    }

    /**
     * Returns a locally held block.
     *
     * @param cid the CID
     * @return the bytes, when held here
     */
    public Optional<byte[]> local(String cid) {
        byte[] bytes = store.get(cid);
        return bytes == null ? Optional.empty() : Optional.of(bytes.clone());
    }

    /**
     * Fetches a block: local store first, then {@code BLOCK_WANT} to the given
     * candidates, verifying the delivery against the CID.
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
        byte[] want = codec.toBytes(new Want(cid));
        for (PeerId candidate : candidates) {
            runtime.send(candidate, Envelope.Kind.BLOCK_WANT, want);
        }
        try {
            return Optional.of(future.get(timeoutMillis, TimeUnit.MILLISECONDS));
        } catch (TimeoutException e) {
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (java.util.concurrent.ExecutionException e) {
            return Optional.empty();
        } finally {
            pending.remove(cid, future);
        }
    }

    /** Returns how many blocks this node holds. */
    public int size() {
        return store.size();
    }

    private void onWant(PeerId from, byte[] body) {
        Want want;
        try {
            want = codec.fromBytes(body, Want.class);
        } catch (RuntimeException e) {
            return;
        }
        if (want == null || want.cid() == null) {
            return;
        }
        byte[] bytes = store.get(want.cid());
        if (bytes != null) {
            runtime.send(from, Envelope.Kind.BLOCK,
                    codec.toBytes(new Block(want.cid(), bytes)));
        }
    }

    private void onBlock(byte[] body) {
        Block block;
        try {
            block = codec.fromBytes(body, Block.class);
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
        if (waiter == null) {
            return;
        }
        store.putIfAbsent(block.cid(), block.bytes());
        waiter.complete(block.bytes());
    }
}
