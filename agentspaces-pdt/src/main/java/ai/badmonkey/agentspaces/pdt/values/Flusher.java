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
import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Hashes the new nodes under a root and writes them to the store in one batch
 * (srad/agentspaces-pdt.md PD-V-08). The walk is post-order: a child's CID is
 * needed to encode its parent, so children hash first. A node the store already
 * holds is neither written nor reported, so the result is exactly the delta of
 * the batch against the store. A spilled byte string ({@link BlobNode}, PD-N-05)
 * writes its chunks and interior nodes through {@link #put} and reports the root.
 */
final class Flusher {

    private final NodeStore store;
    private final Map<String, byte[]> fresh = new LinkedHashMap<>();

    private Flusher(NodeStore store) {
        this.store = store;
    }

    /**
     * Hashes every unhashed node under {@code root} and writes the ones the store lacks.
     *
     * @return the nodes written, CID to bytes, in post-order
     */
    static Map<String, byte[]> flush(TreeNode root, NodeStore store) {
        Flusher flusher = new Flusher(store);
        flusher.cidOf(root);
        if (!flusher.fresh.isEmpty()) {
            store.putAll(flusher.fresh);
        }
        return Collections.unmodifiableMap(flusher.fresh);
    }

    /** The CID of a child reference: the string itself, or the node's CID after hashing it. */
    String cidOf(Object ref) {
        if (ref instanceof String s) {
            return s;
        }
        TreeNode node = (TreeNode) ref;
        String known = node.cid;
        if (known != null) {
            return known;
        }
        String cid = node instanceof BlobNode blob ? blob.write(this) : put(node.encode(this), Cids.DAG_CBOR);
        node.cid = cid;
        return cid;
    }

    /**
     * Hashes bytes under a codec and queues them for the store when it lacks them.
     * The store prepares them ({@link NodeStore#prepare}), so in the sealed-nodes
     * mode of §3.8 the CID names the sealed envelope and the queued bytes are it.
     *
     * @param bytes the node or chunk
     * @param codec {@link Cids#DAG_CBOR} or {@link Cids#RAW}
     * @return the CID
     */
    String put(byte[] bytes, int codec) {
        NodeStore.Stored stored = store.prepare(bytes, codec);
        String cid = stored.cid();
        if (!fresh.containsKey(cid) && !store.has(cid)) {
            fresh.put(cid, stored.bytes());
        }
        return cid;
    }
}
