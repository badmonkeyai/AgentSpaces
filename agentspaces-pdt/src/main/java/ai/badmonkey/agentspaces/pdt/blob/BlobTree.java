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

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The prolly tree over a blob's chunks (srad/agentspaces-pdt.md §3.3, PD-B-02). Level 0
 * is the list of chunks, each a {@code raw} node. Each level above groups the entries
 * of the level below into {@code b} nodes {@code {k:"b", h: height, c: [links],
 * z: [bytes under each child]}}: a node closes after an entry whose CID digest's first
 * four bytes, read as an unsigned big-endian integer, are below {@code 2^32 / 32}
 * (average fanout 32), with at least {@link #MIN_FANOUT} and at most
 * {@link #MAX_FANOUT} entries per node; the last node of a level closes at the end of
 * the level with whatever remains. Levels repeat until one node remains, the root. A
 * blob of zero or one chunk is that single chunk and its root is the chunk's CID
 * (zero chunks: the empty chunk).
 *
 * <p>Because a boundary depends only on the entry that closes it, an edit that keeps
 * most chunks keeps most interior nodes too. {@link #build} writes the interior nodes
 * absent from the store and counts their bytes; {@link #open} reads a tree back and
 * {@link #seek} descends the {@code z} sums to the chunk holding an offset.
 *
 * <p>The reader trusts no byte count it has not checked (PD-N-04): a node whose
 * {@code z} values overflow is refused, a child {@code b} node whose {@code z} sum
 * differs from what its parent claims for it is refused, and a chunk whose length
 * differs from the entry that names it is refused by {@link #chunk(Entry)}, each
 * with a {@link NodeFormatException} naming the CID.
 */
public final class BlobTree {

    /** The boundary threshold: a digest prefix below this closes a node (2^32 / 32). */
    public static final long BOUNDARY = 0x08000000L;

    /** The fewest entries a node holds, except the last node of a level. */
    public static final int MIN_FANOUT = 4;

    /** The most entries a node holds. */
    public static final int MAX_FANOUT = 256;

    /** The CID of the empty chunk, the root of an empty blob. */
    public static final String EMPTY_ROOT = Cids.of(new byte[0], Cids.RAW);

    /**
     * One entry of a level: a node and the bytes under it.
     *
     * @param cid    the node's CID
     * @param length the bytes of blob content under the node
     */
    public record Entry(String cid, long length) {

        /** Validates the parts. */
        public Entry {
            Objects.requireNonNull(cid, "cid");
            if (length < 0) {
                throw new IllegalArgumentException("negative length " + length);
            }
        }

        /** Whether this entry closes the node it sits in once the node has enough entries. */
        public boolean boundary() {
            byte[] digest = Cids.parse(cid).digest();
            long prefix = ((digest[0] & 0xFFL) << 24) | ((digest[1] & 0xFFL) << 16)
                    | ((digest[2] & 0xFFL) << 8) | (digest[3] & 0xFFL);
            return prefix < BOUNDARY;
        }
    }

    /**
     * The chunk that holds an offset.
     *
     * @param cid    the chunk's CID
     * @param start  the blob offset of the chunk's first byte
     * @param length the chunk's length
     */
    public record Located(String cid, long start, long length) {
    }

    private final NodeStore store;
    private final String root;
    private final long size;
    private final int height;
    private final long writtenBytes;
    private final int writtenNodes;
    private List<Entry> chunks;

    private BlobTree(NodeStore store, String root, long size, int height, long writtenBytes, int writtenNodes,
                     List<Entry> chunks) {
        this.store = store;
        this.root = root;
        this.size = size;
        this.height = height;
        this.writtenBytes = writtenBytes;
        this.writtenNodes = writtenNodes;
        this.chunks = chunks;
    }

    /**
     * Groups one level's entries into the nodes of the level above, by the prolly rule.
     *
     * @param level the entries, in order
     * @return the entry lists of each node, in order
     */
    public static List<List<Entry>> group(List<Entry> level) {
        List<List<Entry>> nodes = new ArrayList<>();
        List<Entry> current = new ArrayList<>();
        for (Entry entry : level) {
            current.add(entry);
            if (current.size() >= MAX_FANOUT || (current.size() >= MIN_FANOUT && entry.boundary())) {
                nodes.add(current);
                current = new ArrayList<>();
            }
        }
        if (!current.isEmpty()) {
            nodes.add(current);
        }
        return nodes;
    }

    /**
     * The node a group of entries encodes to.
     *
     * @param height  the node's height, 1 for the level above the chunks
     * @param entries the entries, at least one
     * @return the canonical node bytes
     */
    public static byte[] encodeNode(int height, List<Entry> entries) {
        List<Object> links = new ArrayList<>(entries.size());
        List<Object> sizes = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            links.add(new Link(entry.cid()));
            sizes.add(entry.length());
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.B);
        node.put("h", height);
        node.put("c", links);
        node.put("z", sizes);
        return NodeCodec.encode(node);
    }

    /**
     * Builds the tree over a list of chunks and writes the interior nodes the store
     * lacks. The chunks themselves must already be in the store.
     *
     * @param chunks the level-0 entries, in order
     * @param store  the store
     * @return the tree, with the count of interior bytes and nodes it wrote
     */
    public static BlobTree build(List<Entry> chunks, NodeStore store) {
        Objects.requireNonNull(chunks, "chunks");
        Objects.requireNonNull(store, "store");
        List<Entry> level0 = List.copyOf(chunks);
        long size = 0;
        for (Entry chunk : level0) {
            size += chunk.length();
        }
        if (level0.isEmpty()) {
            String root = store.put(new byte[0], Cids.RAW);
            return new BlobTree(store, root, 0, 0, 0, 0, List.of());
        }
        long written = 0;
        int writtenNodes = 0;
        List<Entry> level = level0;
        int height = 0;
        while (level.size() > 1) {
            height++;
            List<Entry> next = new ArrayList<>();
            for (List<Entry> entries : group(level)) {
                byte[] bytes = encodeNode(height, entries);
                NodeStore.Stored stored = store.prepare(bytes, Cids.DAG_CBOR);
                String cid = stored.cid();
                if (!store.has(cid)) {
                    store.putVerified(cid, stored.bytes());
                    written += bytes.length;
                    writtenNodes++;
                }
                long under = 0;
                for (Entry entry : entries) {
                    under += entry.length();
                }
                next.add(new Entry(cid, under));
            }
            level = next;
        }
        return new BlobTree(store, level.get(0).cid(), size, height, written, writtenNodes, level0);
    }

    /**
     * Opens a tree held in a store by its root. The root node must be present; chunks
     * and lower nodes load on demand.
     *
     * @param store the store
     * @param root  the root CID: a {@code b} node or a single raw chunk
     * @return the tree
     * @throws NodeFormatException when the root is absent or is a node of another kind
     */
    public static BlobTree open(NodeStore store, String root) {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(root, "root");
        Cids.Parsed parsed = Cids.parse(root);
        byte[] bytes = store.get(root)
                .orElseThrow(() -> new NodeFormatException(root, "blob root is not in the store"));
        if (parsed.codec() == Cids.RAW) {
            return new BlobTree(store, root, bytes.length, 0, 0, 0,
                    bytes.length == 0 ? List.of() : List.of(new Entry(root, bytes.length)));
        }
        Map<String, Object> node = decodeB(root, bytes);
        long size = sum(root, sizes(node));
        return new BlobTree(store, root, size, (int) (long) (Long) node.get("h"), 0, 0, null);
    }

    /** The store the tree reads from. */
    public NodeStore store() {
        return store;
    }

    /** The root CID. */
    public String root() {
        return root;
    }

    /** The blob's size in bytes. */
    public long size() {
        return size;
    }

    /** The root's height: 0 for a single chunk, 1 for one level of interior nodes. */
    public int height() {
        return height;
    }

    /** The interior bytes {@link #build} wrote because the store lacked them. */
    public long writtenBytes() {
        return writtenBytes;
    }

    /** The interior nodes {@link #build} wrote because the store lacked them. */
    public int writtenNodes() {
        return writtenNodes;
    }

    /**
     * The level-0 entries in order, read from the store on first use.
     *
     * @return the chunks
     * @throws NodeFormatException when a node on the way is absent or malformed
     */
    public synchronized List<Entry> chunks() {
        if (chunks == null) {
            List<Entry> leaves = new ArrayList<>();
            collectLeaves(root, size, leaves, 1);
            chunks = Collections.unmodifiableList(leaves);
        }
        return chunks;
    }

    private void collectLeaves(String cid, long under, List<Entry> leaves, int depth) {
        if (Cids.parse(cid).codec() == Cids.RAW) {
            leaves.add(new Entry(cid, under));
            return;
        }
        if (depth > NodeStore.MAX_DEPTH) {
            throw new NodeFormatException(cid, "tree deeper than " + NodeStore.MAX_DEPTH + " levels");
        }
        Map<String, Object> node = load(cid, under);
        List<Link> links = links(node);
        List<Long> sizes = sizes(node);
        for (int i = 0; i < links.size(); i++) {
            collectLeaves(links.get(i).cid(), sizes.get(i), leaves, depth + 1);
        }
    }

    /**
     * The chunk holding an offset, found by descending the {@code z} sums from the
     * root, so only the nodes on one path load.
     *
     * @param offset a blob offset below {@link #size()}
     * @return the chunk and its first byte's offset
     * @throws IndexOutOfBoundsException when the offset is outside the blob
     */
    public Located seek(long offset) {
        if (offset < 0 || offset >= size) {
            throw new IndexOutOfBoundsException("offset " + offset + " outside a blob of " + size + " bytes");
        }
        String cid = root;
        long start = 0;
        long under = size;
        int depth = 1;
        while (Cids.parse(cid).codec() != Cids.RAW) {
            if (depth > NodeStore.MAX_DEPTH) {
                throw new NodeFormatException(cid, "tree deeper than " + NodeStore.MAX_DEPTH + " levels");
            }
            Map<String, Object> node = load(cid, under);
            List<Link> links = links(node);
            List<Long> sizes = sizes(node);
            int i = 0;
            while (i < sizes.size() - 1 && offset >= start + sizes.get(i)) {
                start += sizes.get(i);
                i++;
            }
            cid = links.get(i).cid();
            under = sizes.get(i);
            depth++;
        }
        return new Located(cid, start, under);
    }

    /**
     * The bytes of one chunk, verified against its CID.
     *
     * @param cid the chunk's CID
     * @return the bytes
     * @throws NodeFormatException when the chunk is absent
     */
    public byte[] chunk(String cid) {
        Optional<byte[]> bytes = store.get(cid);
        return bytes.orElseThrow(() -> new NodeFormatException(cid, "chunk is not in the store"));
    }

    /**
     * The bytes of one chunk, verified against its CID and against the length the
     * tree claims for it.
     *
     * @param entry the chunk's entry
     * @return the bytes
     * @throws NodeFormatException when the chunk is absent or its length differs from the entry's
     */
    public byte[] chunk(Entry entry) {
        byte[] bytes = chunk(entry.cid());
        if (bytes.length != entry.length()) {
            throw new NodeFormatException(entry.cid(), "chunk of " + bytes.length + " bytes where the tree claims " + entry.length());
        }
        return bytes;
    }

    /** Loads a {@code b} node whose parent claims {@code under} bytes beneath it. */
    private Map<String, Object> load(String cid, long under) {
        byte[] bytes = store.get(cid).orElseThrow(() -> new NodeFormatException(cid, "blob node is not in the store"));
        Map<String, Object> node = decodeB(cid, bytes);
        long held = sum(cid, sizes(node));
        if (held != under) {
            throw new NodeFormatException(cid, "blob node holds " + held + " bytes where its parent claims " + under);
        }
        return node;
    }

    private static Map<String, Object> decodeB(String cid, byte[] bytes) {
        Map<String, Object> node = NodeCodec.decode(cid, bytes);
        if (!Kind.B.equals(node.get(NodeCodec.KIND_KEY))) {
            throw new NodeFormatException(cid, "kind '" + node.get(NodeCodec.KIND_KEY) + "' where a blob node was expected");
        }
        List<Link> links = links(node);
        List<Long> sizes = sizes(node);
        if (links.isEmpty() || links.size() != sizes.size()) {
            throw new NodeFormatException(cid, "blob node with " + links.size() + " links and " + sizes.size() + " sizes");
        }
        if (!(node.get("h") instanceof Long h) || h < 1) {
            throw new NodeFormatException(cid, "blob node without a height");
        }
        sum(cid, sizes);
        return node;
    }

    /** The sum of a node's byte counts, refused when it overflows a long. */
    private static long sum(String cid, List<Long> sizes) {
        long total = 0;
        try {
            for (long z : sizes) {
                total = Math.addExact(total, z);
            }
        } catch (ArithmeticException e) {
            throw new NodeFormatException(cid, "blob node byte counts overflow", e);
        }
        return total;
    }

    @SuppressWarnings("unchecked")
    private static List<Link> links(Map<String, Object> node) {
        Object c = node.get("c");
        return c == null ? List.of() : (List<Link>) c;
    }

    @SuppressWarnings("unchecked")
    private static List<Long> sizes(Map<String, Object> node) {
        Object z = node.get("z");
        return z == null ? List.of() : (List<Long>) z;
    }
}
