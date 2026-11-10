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
package ai.badmonkey.agentspaces.pdt;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.store.MetaStore;
import ai.badmonkey.agentspaces.peering.blocks.BlockStore;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;

import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The node store (srad/agentspaces-pdt.md §3.4; PD-S-01, PD-S-06, PD-N-06): nodes
 * by CID over the shipped {@link BlockStore} SPI, which already keys blocks by a CID
 * string and has memory and file implementations. This class adds what a tree
 * needs: {@link #has} without a read, batch {@link #putAll}, root pins with a
 * reference count, and completeness and reachability per root, walked iteratively
 * through {@link NodeCodec#links} and bounded by the tree depth of PD-N-04. Every
 * node a caller hands in with a CID is recomputed and refused on a mismatch, and
 * every read recomputes the CID of the bytes it returns, so a corrupt block never
 * reaches a value (PD-N-06).
 *
 * <p>Two views of a node. {@link #stored} returns the bytes as the backend holds
 * them and the CID names them: what a holder serves and a walk follows. {@link #get}
 * returns the bytes a value reads. In this class the two are the same; the
 * sealed-nodes store of §3.8 ({@code pdt.seal.SealedNodeStore}, PD-P-05) overrides
 * {@link #get}, {@link #put}, {@link #putVerified}, and {@link #prepare} so that the
 * stored bytes are a sealed envelope ({@code sn}) whose links stay in the clear,
 * which is why the walks of this class read through {@link #stored}. Writers that
 * compute a CID before they write go through {@link #prepare}, so the CID always
 * names what the backend will hold.
 *
 * <p>Retention of delta bases and collection (PD-S-03 to PD-S-05) live in
 * {@code pdt.store} and build on this class; nothing here removes a node.
 *
 * <p>Pins persist when the backend is a {@link MetaStore} (PDT 2 phase 1, P2-V-03): the
 * store reads the meta record {@value #PINS_META} when it is built and writes it on
 * {@link #flushMeta} after a pin or a release, so a restart over the same packs starts with
 * the counts of the last flush. {@code Pdt} flushes on its tick and on close.
 */
public class NodeStore {

    /** The meta record of the pins: a CBOR map from root to reference count (P2-V-03). */
    public static final String PINS_META = "pins";

    /** The deepest tree a walk follows: a node at this depth may not link further (PD-N-04). */
    public static final int MAX_DEPTH = NodeCodec.MAX_DEPTH;

    /**
     * Bytes ready for the backend: the CID and the bytes it names, which this store
     * computed from a plaintext node (see {@link #prepare}).
     *
     * @param cid   the CID
     * @param bytes the bytes the backend will hold under it
     */
    public record Stored(String cid, byte[] bytes) {
    }

    private static final ai.badmonkey.agentspaces.common.codec.CborCodec CODEC =
            ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();

    private final BlockStore backend;
    private final InstantSource clock;
    /** The intra-chunk deltas edits over this store keep (PDT 2 phase 4, P2-Z-02); a sibling shares them. */
    private final ai.badmonkey.agentspaces.pdt.blob.ChunkDeltas deltas;
    /** Reference counts by root; every change is one atomic map operation, so pins and unpins never race. */
    private final Map<String, Integer> pins = new ConcurrentHashMap<>();
    /** Advances on every pin, so a collector can tell cheaply whether a root was pinned since it last read the roots (PD-S-04). */
    private final AtomicLong pinGeneration = new AtomicLong();
    /** Pins changed since the last {@link #flushMeta}. */
    private final java.util.concurrent.atomic.AtomicBoolean pinsDirty = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * A store over a backend.
     *
     * @param backend where the bytes live
     * @param clock   the clock the backend wants for its last-served marks
     */
    public NodeStore(BlockStore backend, InstantSource clock) {
        this(backend, clock, new ai.badmonkey.agentspaces.pdt.blob.ChunkDeltas(clock, true));
    }

    /**
     * A store over a backend that keeps its intra-chunk deltas in a given table (PDT 2 phase 4, P2-Z-02), so a
     * sibling store's edits land where this store's holder serves them.
     *
     * @param backend where the bytes live
     * @param clock   the clock the backend wants for its last-served marks
     * @param deltas  the table of intra-chunk deltas
     */
    protected NodeStore(BlockStore backend, InstantSource clock, ai.badmonkey.agentspaces.pdt.blob.ChunkDeltas deltas) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.deltas = Objects.requireNonNull(deltas, "deltas");
        if (backend instanceof MetaStore meta) {
            meta.meta(PINS_META).ifPresent(bytes -> {
                Map<?, ?> stored = CODEC.fromBytes(bytes, Map.class);
                stored.forEach((root, count) -> pins.put((String) root, ((Number) count).intValue()));
            });
        }
    }

    /**
     * Writes the pins to the backend's meta record when they changed since the last call; a
     * backend that is no {@link MetaStore} keeps them in memory only (P2-V-03).
     */
    public void flushMeta() {
        if (backend instanceof MetaStore meta && pinsDirty.getAndSet(false)) {
            meta.putMeta(PINS_META, CODEC.toBytes(new java.util.TreeMap<>(pins)));
        }
    }

    /** A store on the heap over a {@link MemoryBlockStore} with the system clock (PD-S-07). */
    public static NodeStore inMemory() {
        return new NodeStore(new MemoryBlockStore(), InstantSource.system());
    }

    /**
     * A store of the same kind as this one over another backend: the same sealing,
     * the same clock. A recording wrapper builds a sibling over a backend that
     * captures writes (the inline delta of PD-X-07) and must write what this store
     * would write.
     *
     * @param other the backend
     * @return the sibling store
     */
    public NodeStore over(BlockStore other) {
        return new NodeStore(other, clock, deltas);
    }

    /** The intra-chunk deltas edits over this store keep (PDT 2 phase 4, P2-Z-02). */
    public ai.badmonkey.agentspaces.pdt.blob.ChunkDeltas deltas() {
        return deltas;
    }

    /** The backend. */
    public BlockStore backend() {
        return backend;
    }

    /** The clock. */
    public InstantSource clock() {
        return clock;
    }

    /** Whether the backend holds a node, without reading it or marking it served. */
    public boolean has(String cid) {
        Objects.requireNonNull(cid, "cid");
        return backend.lastAccessMillis(cid) >= 0;
    }

    /**
     * Reads the bytes a value reads: in this class the stored bytes, verified
     * against the CID.
     *
     * @param cid the CID
     * @return the bytes, when held
     * @throws NodeFormatException when the held bytes do not hash to the CID (PD-N-06)
     */
    public Optional<byte[]> get(String cid) {
        return stored(cid);
    }

    /**
     * Reads the bytes as the backend holds them and the CID names them, verified
     * against the CID: what a holder serves over the tree exchange and what a walk
     * follows links through (PD-X-03). Equal to {@link #get} unless the store seals.
     *
     * @param cid the CID
     * @return the bytes, when held
     * @throws NodeFormatException when the held bytes do not hash to the CID (PD-N-06)
     */
    public Optional<byte[]> stored(String cid) {
        Objects.requireNonNull(cid, "cid");
        Optional<byte[]> held = backend.get(cid, clock.millis());
        held.ifPresent(bytes -> verify(cid, bytes));
        return held;
    }

    /**
     * Computes the CID of a plaintext node and the bytes the backend will hold under
     * it, without writing. In this class the bytes are the node itself.
     *
     * @param bytes the node or chunk
     * @param codec {@link Cids#DAG_CBOR} for a structured node, {@link Cids#RAW} for a chunk
     * @return the CID and the bytes it names
     * @throws NodeFormatException when a structured node exceeds {@link NodeCodec#MAX_NODE_BYTES}
     */
    public Stored prepare(byte[] bytes, int codec) {
        Objects.requireNonNull(bytes, "bytes");
        String cid = Cids.of(bytes, codec);
        checkSize(cid, bytes, codec);
        return new Stored(cid, bytes);
    }

    /**
     * Stores bytes under the CID it computes for them.
     *
     * @param bytes the node or chunk
     * @param codec {@link Cids#DAG_CBOR} for a structured node, {@link Cids#RAW} for a chunk
     * @return the CID
     * @throws NodeFormatException when a structured node exceeds {@link NodeCodec#MAX_NODE_BYTES}
     */
    public String put(byte[] bytes, int codec) {
        Stored stored = prepare(bytes, codec);
        backend.put(stored.cid(), stored.bytes(), clock.millis());
        return stored.cid();
    }

    /**
     * Stores bytes that arrived with a CID, after recomputing it (PD-N-06). The bytes
     * are stored as they are: what the CID names is what the backend holds.
     *
     * @param cid   the claimed CID
     * @param bytes the bytes
     * @throws NodeFormatException when the bytes do not hash to the CID or a structured node is over its bound
     */
    public void putVerified(String cid, byte[] bytes) {
        Objects.requireNonNull(cid, "cid");
        Objects.requireNonNull(bytes, "bytes");
        int codec = verify(cid, bytes);
        checkSize(cid, bytes, codec);
        backend.put(cid, bytes, clock.millis());
    }

    /**
     * Stores a batch, each verified against its CID; a refusal stops the batch where it is.
     *
     * @param nodes CID to bytes
     * @throws NodeFormatException on the first mismatch
     */
    public void putAll(Map<String, byte[]> nodes) {
        Objects.requireNonNull(nodes, "nodes");
        for (Map.Entry<String, byte[]> entry : nodes.entrySet()) {
            putVerified(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Pins a root, counting references.
     *
     * @param root the CID
     * @return the reference count after the pin
     */
    public int pin(String root) {
        Objects.requireNonNull(root, "root");
        int count = pins.merge(root, 1, Integer::sum);
        pinGeneration.incrementAndGet();
        pinsDirty.set(true);
        return count;
    }

    /**
     * Releases one reference on a root. The decrement and the removal of the last
     * reference are one atomic map operation, so a concurrent {@link #pin} is never lost.
     *
     * @param root the CID
     * @return the reference count after the release, or -1 when the root was not pinned
     */
    public int unpin(String root) {
        Objects.requireNonNull(root, "root");
        int[] left = {-1};
        pins.computeIfPresent(root, (r, count) -> {
            left[0] = count - 1;
            return count <= 1 ? null : count - 1;
        });
        if (left[0] >= 0) {
            pinsDirty.set(true);
        }
        return left[0];
    }

    /** The pinned roots. */
    public Set<String> pinned() {
        return Set.copyOf(pins.keySet());
    }

    /** The reference count of a root, zero when unpinned. */
    public int pinCount(String root) {
        return pins.getOrDefault(root, 0);
    }

    /**
     * A counter that advances on every {@link #pin}. A collector compares it with the
     * value it saw when it last read the roots and re-marks before it removes anything
     * when the two differ, so a root pinned during a sweep is honoured within the same
     * tick (PD-S-04).
     */
    public long pinGeneration() {
        return pinGeneration.get();
    }

    /**
     * Whether the backend holds every node reachable from a root (PD-S-06).
     *
     * @param root the CID
     * @return true when the root and every node under it are present
     * @throws NodeFormatException on a malformed node or a tree deeper than 64
     */
    public boolean complete(String root) {
        return missing(root).isEmpty();
    }

    /**
     * The CIDs reachable from a root that are not held: the frontier a fetch continues from.
     *
     * @param root the CID
     * @return the missing CIDs in breadth-first order, empty when complete
     * @throws NodeFormatException on a malformed node or a tree deeper than 64
     */
    public Set<String> missing(String root) {
        Set<String> missing = new LinkedHashSet<>();
        walk(root, missing);
        return Collections.unmodifiableSet(missing);
    }

    /**
     * Every CID reachable from a root, breadth first, including the root and any CID
     * a held node links to that is itself absent. A walk cannot follow the links of
     * an absent node, so an incomplete tree yields a partial set.
     *
     * @param root the CID
     * @return the reachable CIDs in breadth-first order
     * @throws NodeFormatException on a malformed node or a tree deeper than 64
     */
    public Set<String> reachable(String root) {
        return walk(root, null);
    }

    /** The bytes the backend holds. */
    public long bytesUsed() {
        return backend.bytesUsed();
    }

    /** How many nodes the backend holds. */
    public int size() {
        return backend.size();
    }

    /** The walk follows the stored bytes, so a store that holds sealed envelopes walks their links in the clear. */
    private Set<String> walk(String root, Set<String> missing) {
        Objects.requireNonNull(root, "root");
        Cids.parse(root);
        Set<String> seen = new LinkedHashSet<>();
        Set<String> queued = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        Deque<Integer> depths = new ArrayDeque<>();
        queue.add(root);
        depths.add(1);
        queued.add(root);
        while (!queue.isEmpty()) {
            String cid = queue.poll();
            int depth = depths.poll();
            seen.add(cid);
            Optional<byte[]> bytes = stored(cid);
            if (bytes.isEmpty()) {
                if (missing != null) {
                    missing.add(cid);
                }
                continue;
            }
            List<String> links = NodeCodec.links(cid, bytes.get());
            if (links.isEmpty()) {
                continue;
            }
            if (depth >= MAX_DEPTH) {
                throw new NodeFormatException(cid, "tree deeper than " + MAX_DEPTH + " levels");
            }
            for (String link : links) {
                if (queued.add(link)) {
                    queue.add(link);
                    depths.add(depth + 1);
                }
            }
        }
        return Collections.unmodifiableSet(seen);
    }

    /** Recomputes the CID of bytes and returns the codec the CID names. */
    protected static int verify(String cid, byte[] bytes) {
        Cids.Parsed parsed;
        try {
            parsed = Cids.parse(cid);
        } catch (IllegalArgumentException e) {
            throw new NodeFormatException(cid, "CID refused: " + e.getMessage(), e);
        }
        String actual = Cids.of(bytes, parsed.codec());
        if (!actual.equals(cid)) {
            throw new NodeFormatException(cid, "bytes hash to " + actual + ", which does not match the claimed CID");
        }
        return parsed.codec();
    }

    /**
     * The size rule of PD-N-04 on stored bytes: a structured node is at most 1 MiB,
     * and a sealed envelope ({@code sn}, whose kind the bytes name) at most
     * {@link NodeCodec#MAX_SEALED_NODE_BYTES}.
     */
    protected static void checkSize(String cid, byte[] bytes, int codec) {
        if (codec != Cids.DAG_CBOR || bytes.length <= NodeCodec.MAX_NODE_BYTES) {
            return;
        }
        if (bytes.length <= NodeCodec.MAX_SEALED_NODE_BYTES) {
            String kind;
            try {
                kind = NodeCodec.kindOf(bytes);
            } catch (NodeFormatException e) {
                kind = null;
            }
            if (Kind.SN.equals(kind)) {
                return;
            }
        }
        throw new NodeFormatException(cid, "node of " + bytes.length + " bytes exceeds "
                + NodeCodec.MAX_NODE_BYTES + " bytes (1 MiB)");
    }
}
