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

import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import ai.badmonkey.agentspaces.pdt.kinds.BoNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A window of a blob read through its Bao outboard (PDT 2 phase 4, P2-B-03, P2-Q-REL-05; srad/agentspaces-pdt2.md
 * §3.5, S6 of §6.4).
 *
 * <p>At open the window walks the {@code bo} nodes that cover its chunk groups, from the root that {@code pdt.bao}
 * names: each parent pair on the way must hash to the value above it, the top pair with BLAKE3's root flag to the
 * identity, and each child node must cover the bytes its parent claims, so the walk ends with the expected chaining
 * value of every group in the window. A node that breaks the chain fails the open. Then each group loads as the stream
 * reaches it, from the chunks of the blob tree that cover it ({@link BlobTree#chunksIn}), and the stream delivers its
 * bytes only when the group's chaining value equals the expected one; a group that does not verify fails the read
 * with an {@link IOException} naming it, after the groups before it arrived, and every later read with the same
 * exception; so does a node or chunk the loader cannot have. A blob of one group verifies that group hashed as the
 * root against the identity.
 */
final class VerifiedWindow extends InputStream {

    private static final int GROUP = BoNode.GROUP_BYTES;

    private final BlobTree tree;
    private final String identity;
    private final long size;
    private final long end;
    private final boolean singleGroup;
    /** The expected chaining value of each group of the window, from the walk of the outboard. */
    private final Map<Long, byte[]> expected = new HashMap<>();
    private final List<BlobTree.Located> chunks;
    private int chunkIndex;
    private byte[] chunk;
    private long position;
    private long group = -1;
    private byte[] current;
    /** The refusal of a group, which every later read throws again (PDT 2 phase 4, P2-B-03). */
    private IOException failure;

    VerifiedWindow(BlobTree tree, BlobIdentity claim, long offset, long length) throws IOException {
        this.tree = Objects.requireNonNull(tree, "tree");
        this.identity = claim.identity();
        this.size = tree.size();
        this.position = offset;
        this.end = offset + length;
        this.singleGroup = BoNode.groups(size) == 1;
        if (length == 0) {
            this.chunks = List.of();
            return;
        }
        long first = offset / GROUP;
        long last = (end - 1) / GROUP;
        BoNode root = node(claim.bao());
        if (root.size() != size) {
            throw new IOException("the outboard " + claim.bao() + " covers " + root.size() + " bytes where the blob "
                    + tree.root() + " holds " + size);
        }
        walk(claim.bao(), root, 0, claim.digest(), true, first, last);
        long from = first * GROUP;
        this.chunks = tree.chunksIn(from, Math.min(size, (last + 1) * GROUP) - from);
    }

    // ------------------------------------------------------------------ the outboard

    private BoNode node(String cid) throws IOException {
        try {
            return BoNode.decode(cid, tree.fetch(cid, "outboard node"));
        } catch (NodeFormatException | NodeUnavailableException e) {
            throw new IOException("the outboard node " + cid + " does not read: " + e.getMessage(), e);
        }
    }

    /** Walks one {@code bo} node whose subtree starts at group {@code firstGroup} and must chain to {@code cv}. */
    private void walk(String cid, BoNode node, long firstGroup, byte[] cv, boolean root, long first, long last)
            throws IOException {
        List<Long> blocks = BoNode.childSizes(node.size());
        long perLeaf = blocks.isEmpty() ? 1 : BoNode.groups(blocks.get(0));
        int leaves = blocks.isEmpty() ? (int) BoNode.groups(node.size()) : blocks.size();
        descend(new Walk(cid, node, node.parents(), blocks, firstGroup, perLeaf, first, last), 0, 0, leaves, cv, root);
    }

    /** One node's walk: its leaves are groups (a leaf node) or child blocks of {@code perLeaf} groups (an upper node). */
    private record Walk(String cid, BoNode node, byte[] parents, List<Long> blocks, long firstGroup, long perLeaf,
                        long first, long last) {
        boolean overlaps(int from, int to) {
            return firstGroup + from * perLeaf <= last && firstGroup + to * perLeaf > first;
        }
    }

    /**
     * Descends the pre-order parents of leaves {@code [from, to)}, whose subtree value is {@code cv}, starting at parent
     * pair {@code pair}: BLAKE3 puts the largest power of two strictly below the count on the left.
     */
    private void descend(Walk w, int pair, int from, int to, byte[] cv, boolean root) throws IOException {
        if (to - from == 1) {
            long groupStart = w.firstGroup + from * w.perLeaf;
            if (w.blocks.isEmpty()) {
                expected.put(groupStart, cv);
                return;
            }
            String child = w.node.children().get(from).cid();
            BoNode node = node(child);
            if (node.size() != w.blocks.get(from)) {
                throw new IOException("the outboard node " + child + " covers " + node.size() + " bytes where its parent "
                        + w.cid + " claims " + w.blocks.get(from));
            }
            walk(child, node, groupStart, cv, false, w.first, w.last);
            return;
        }
        int at = pair * BoNode.PARENT_BYTES;
        byte[] left = Arrays.copyOfRange(w.parents, at, at + Blake3.DIGEST_BYTES);
        byte[] right = Arrays.copyOfRange(w.parents, at + Blake3.DIGEST_BYTES, at + BoNode.PARENT_BYTES);
        if (!Arrays.equals(Blake3.parent(left, right, root), cv)) {
            throw new IOException("the outboard node " + w.cid + " does not chain to the identity " + identity
                    + " at parent " + pair);
        }
        int mid = from + Integer.highestOneBit(to - from - 1);
        if (w.overlaps(from, mid)) {
            descend(w, pair + 1, from, mid, left, false);
        }
        if (w.overlaps(mid, to)) {
            descend(w, pair + 1 + (mid - from - 1), mid, to, right, false);
        }
    }

    // ------------------------------------------------------------------ the groups

    /** Loads group {@code g} from the chunks that cover it and verifies it before it is delivered. */
    private void load(long g) throws IOException {
        long start = g * GROUP;
        long stop = Math.min(size, start + GROUP);
        byte[] bytes = new byte[(int) (stop - start)];
        long filled = start;
        while (filled < stop) {
            BlobTree.Located located = chunks.get(chunkIndex);
            long chunkEnd = located.start() + located.length();
            if (chunkEnd <= filled) {
                chunkIndex++;
                chunk = null;
                continue;
            }
            if (chunk == null) {
                try {
                    chunk = tree.chunk(new BlobTree.Entry(located.cid(), located.length()));
                } catch (NodeFormatException | NodeUnavailableException e) {
                    // PDT 2 phase 4 gate, P2-B-03: a chunk the loader cannot have fails the group as one that does not read
                    throw new IOException("chunk group " + g + ": chunk " + located.cid() + " does not read: " + e.getMessage(), e);
                }
            }
            int n = (int) (Math.min(chunkEnd, stop) - filled);
            System.arraycopy(chunk, (int) (filled - located.start()), bytes, (int) (filled - start), n);
            filled += n;
        }
        byte[] cv = singleGroup ? Blake3.treeRoot(bytes) : Blake3.groupCv(bytes, 0, bytes.length, g << BoNode.GROUP_LOG);
        if (!Arrays.equals(cv, expected.get(g))) {
            throw new IOException("chunk group " + g + " (bytes " + start + " to " + stop + ") does not verify against the identity "
                    + identity);
        }
        current = bytes;
        group = g;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        if (failure != null) {
            throw failure;
        }
        if (len == 0) {
            return 0;
        }
        if (position >= end) {
            return -1;
        }
        long g = position / GROUP;
        if (g != group) {
            try {
                load(g);
            } catch (IOException e) {
                failure = e; // the chunk cursor has moved past the group's start, so the group is not loaded again
                throw e;
            }
        }
        int at = (int) (position - g * GROUP);
        int n = (int) Math.min(len, Math.min(current.length - at, end - position));
        System.arraycopy(current, at, b, off, n);
        position += n;
        return n;
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : one[0] & 0xff;
    }
}
