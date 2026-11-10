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
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One whole CBOR payload as a tree (srad/agentspaces-pdt.md §3.7, PD-E-02,
 * PD-E-03): the door through which a space's escalated payload enters the spill
 * rules of {@link Spill} and comes back byte for byte. A payload is one CBOR
 * item, the bytes {@code CborCodec.toBytes} produced for an entry.
 *
 * <p>The root is always a dag-cbor node, so a payload root is told from a flat
 * content-swarm artifact root (codec {@code raw}) by its codec alone:
 *
 * <ul>
 *   <li>an indefinite-length map above 4 KiB with keys in canonical order (what the codec
 *       gives a map) becomes a CHAMP sub-tree and the root is its {@code m} node;</li>
 *   <li>an indefinite-length map above 4 KiB whose keys are in another order (a record's
 *       declared field order) becomes a {@code vr} of its keys and values in order under an
 *       {@code x} envelope whose {@code v} links it, so the fields come back as written;</li>
 *   <li>a definite array above 4 KiB becomes a vector and the root is its {@code vr} node;</li>
 *   <li>a byte string above 4 KiB becomes the blob tree of §3.3; when that tree is one chunk
 *       (a raw node) the root is an {@code x} envelope whose {@code v} links it, else the
 *       root is the blob's {@code b} node;</li>
 *   <li>anything else stays whole in an {@code x} node, which holds at most 1 MiB.</li>
 * </ul>
 *
 * <p>{@link #read} reverses each case by the root's kind, through {@link Spill}. The
 * forms are disjoint: an {@code x} root whose value is a bare link item is an envelope,
 * since an application value is never a bare link item (the total rule of 10.2), and the
 * linked kind says which ({@code vr}: an ordered map; {@code raw} or {@code b}: a byte string).
 */
public final class PayloadTree {

    private PayloadTree() {
    }

    /**
     * What a write produced.
     *
     * @param root  the root CID (dag-cbor)
     * @param nodes the nodes the store lacked, CID to bytes, in post-order
     */
    public record Written(String root, Map<String, byte[]> nodes) {

        /** The bytes of the nodes written. */
        public long bytes() {
            long total = 0;
            for (byte[] node : nodes.values()) {
                total += node.length;
            }
            return total;
        }
    }

    /**
     * Spills one payload into the store and returns its root.
     *
     * @param cbor  the payload, one CBOR item
     * @param store the store new nodes go to
     * @return the root and the nodes written
     * @throws IllegalArgumentException when the payload is a bare link item or a value an {@code x} node cannot hold
     */
    public static Written write(byte[] cbor, NodeStore store) {
        Objects.requireNonNull(cbor, "cbor");
        Objects.requireNonNull(store, "store");
        if (cbor.length == 0) {
            throw new IllegalArgumentException("an empty payload is not a CBOR item");
        }
        // PDT 2 phase 6 (rule 7): every member of an escalating space reads the payload, so it holds level-1 kinds only
        Ctx ctx = new Ctx(NodeLoader.of(store), KeyHasher.SHA256, null, true);
        Object spilled = Spill.spill(cbor, ctx);
        TreeNode root;
        Map<String, byte[]> nodes;
        if (spilled instanceof byte[] inline) {
            root = new XNode(inline);
            nodes = Flusher.flush(root, store);
        } else if (spilled instanceof BlobNode blob) {
            nodes = new LinkedHashMap<>(Flusher.flush(blob, store));
            if (Cids.parse(blob.cid).codec() == Cids.RAW) {
                // one chunk: its raw node cannot be the root, so an x envelope links it
                root = new XNode(Spill.linkItem(blob.cid));
                nodes.putAll(Flusher.flush(root, store));
            } else {
                root = blob;
            }
        } else {
            root = (TreeNode) spilled;
            nodes = Flusher.flush(root, store);
        }
        return new Written(root.cid, Collections.unmodifiableMap(nodes));
    }

    /**
     * Reassembles a payload from its root: the bytes {@link #write} was given.
     *
     * @param root   the root CID
     * @param store  the store
     * @param loader the loader cold nodes come from ({@code NodeLoader.of(store)} for a local read)
     * @return the payload
     * @throws NodeFormatException when the root or a node under it is malformed or of a kind no payload produces
     * @throws ai.badmonkey.agentspaces.pdt.NodeUnavailableException when a node is absent and the loader cannot fetch it
     */
    public static byte[] read(String root, NodeStore store, NodeLoader loader) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(loader, "loader");
        if (Cids.parse(root).codec() != Cids.DAG_CBOR) {
            throw new NodeFormatException(root, "a payload root is a dag-cbor node");
        }
        Ctx ctx = new Ctx(loader, KeyHasher.SHA256, null, true);
        String kind = NodeCodec.kindOf(loader.load(root));
        if (Kind.X.equals(kind) || Kind.M.equals(kind) || Kind.VR.equals(kind) || Kind.B.equals(kind)) {
            return Spill.resolve(root, ctx);
        }
        throw new NodeFormatException(root, "a payload root is an x, m, vr, or b node, not '" + kind + "'");
    }
}
