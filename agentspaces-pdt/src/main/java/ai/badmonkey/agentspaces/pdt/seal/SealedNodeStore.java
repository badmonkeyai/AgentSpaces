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
package ai.badmonkey.agentspaces.pdt.seal;

import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.peering.blocks.BlockStore;

import java.time.InstantSource;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link NodeStore} in the sealed-nodes mode of srad/agentspaces-pdt.md §3.8
 * (PD-P-05): it seals every node it is given in plaintext under the ring's write
 * epoch before the backend sees it, names the node by the CID of the sealed bytes,
 * and opens an envelope on {@link #get} with the epoch's opening keys. The stored
 * bytes ({@link #stored}) are the envelope, which is what the tree store serves
 * and what a walk follows: the envelope carries the plaintext's links in the
 * clear, so completeness, the missing frontier, reachability, the collector's
 * mark, and the tree exchange work on a member that holds no key at all.
 *
 * <p>A keyless member builds this store over {@link GroupKeyRing#empty()}: it
 * stores and serves what arrives verified ({@link #putVerified}), walks it, and
 * throws {@link SealedNodeException} from {@link #get} and from {@link #put}. Bytes
 * that arrive from the wire are stored as they are, so a plaintext node written by
 * a member in another mode is held and read as plaintext: the mode governs what
 * this peer writes, and the head's {@code sealing} says what a reader should
 * expect.
 */
public final class SealedNodeStore extends NodeStore {

    private final NodeSealer sealer;

    /**
     * A sealed store.
     *
     * @param backend where the envelopes live
     * @param clock   the clock of last-served marks and of the write epoch
     * @param sealer  the sealer over the group's key ring
     */
    public SealedNodeStore(BlockStore backend, InstantSource clock, NodeSealer sealer) {
        // PDT 2 phase 4, P2-Z-02: a sealed chunk's CID names its envelope, which no frame over plaintext rebuilds
        super(backend, clock, new ai.badmonkey.agentspaces.pdt.blob.ChunkDeltas(clock, false));
        this.sealer = Objects.requireNonNull(sealer, "sealer");
    }

    /**
     * A sealed store over a ring.
     *
     * @param backend where the envelopes live
     * @param clock   the clock
     * @param ring    the group's content key ring; {@link GroupKeyRing#empty()} for a keyless member
     */
    public SealedNodeStore(BlockStore backend, InstantSource clock, GroupKeyRing ring) {
        this(backend, clock, new NodeSealer(ring, clock));
    }

    /** The sealer. */
    public NodeSealer sealer() {
        return sealer;
    }

    @Override
    public NodeStore over(BlockStore other) {
        return new SealedNodeStore(other, clock(), sealer);
    }

    /**
     * The plaintext of a held node: the envelope opened with the epoch's opening keys.
     *
     * @throws SealedNodeException when no held key opens it
     * @throws NodeFormatException when the envelope is malformed, lies about its links, or fails its CID
     */
    @Override
    public Optional<byte[]> get(String cid) {
        Optional<byte[]> held = stored(cid);
        return held.map(bytes -> sealer.open(cid, bytes));
    }

    /**
     * Seals a plaintext node under the write epoch and names it by the envelope.
     *
     * @throws SealedNodeException when the ring holds no key to seal under
     */
    @Override
    public Stored prepare(byte[] bytes, int codec) {
        Objects.requireNonNull(bytes, "bytes");
        if (codec == Cids.DAG_CBOR && bytes.length > NodeCodec.MAX_NODE_BYTES) {
            throw new NodeFormatException("node of " + bytes.length + " bytes exceeds " + NodeCodec.MAX_NODE_BYTES + " bytes (1 MiB)");
        }
        byte[] sealed = sealer.seal(bytes, codec);
        return new Stored(Cids.of(sealed, codec), sealed);
    }

    @Override
    public String toString() {
        return "SealedNodeStore(" + size() + " nodes, epochs " + sealer.ring().epochs() + ")";
    }
}
