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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.blob.BlobTree;
import ai.badmonkey.agentspaces.pdt.blob.FastCdc;

import java.util.ArrayList;
import java.util.List;

/**
 * A spilled byte string (srad/agentspaces-pdt.md §3.1, PD-N-05): a byte string whose
 * value exceeds 4 KiB leaves its node as a blob sub-tree of §3.3, chunked by
 * {@link FastCdc} into {@code raw} nodes under the {@code b} interior nodes of
 * {@link BlobTree}, and the value slot links the blob root (the single chunk for a
 * blob of one chunk). The node holds the bytes in memory until a {@link Flusher}
 * writes them; {@link Spill#resolve} reads a stored blob back into a byte string.
 */
final class BlobNode extends TreeNode {

    /** The byte string's bytes, without the CBOR header. */
    final byte[] content;

    BlobNode(byte[] content) {
        this.content = content;
    }

    /** Writes the chunks and the interior nodes the store lacks and returns the root CID. */
    String write(Flusher flusher) {
        List<BlobTree.Entry> level = new ArrayList<>();
        for (byte[] chunk : FastCdc.chunksOf(content)) {
            level.add(new BlobTree.Entry(flusher.put(chunk, Cids.RAW), chunk.length));
        }
        if (level.isEmpty()) {
            return flusher.put(new byte[0], Cids.RAW);
        }
        int height = 0;
        while (level.size() > 1) {
            height++;
            List<BlobTree.Entry> next = new ArrayList<>();
            for (List<BlobTree.Entry> entries : BlobTree.group(level)) {
                long under = 0;
                for (BlobTree.Entry entry : entries) {
                    under += entry.length();
                }
                next.add(new BlobTree.Entry(flusher.put(BlobTree.encodeNode(height, entries), Cids.DAG_CBOR), under));
            }
            level = next;
        }
        return level.get(0).cid();
    }

    @Override
    byte[] encode(Flusher flusher) {
        throw new IllegalStateException("a blob node is written through Flusher.put, chunk by chunk");
    }
}
