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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.WireRecord;

import java.util.List;
import java.util.Optional;

/**
 * One part of a pushed delta (PDT 2 phase 5, P2-P-01; srad/agentspaces-pdt2.md §3.4, §6.10.4): the payload of a
 * rumour on a ref's stream {@code pdt:<space>:<name>}. A writer splits the nodes of a version's delta, parents first,
 * into parts of at most {@link #MAX_BYTES} bytes of nodes. The bundle carries no signature: a receiver stores a node
 * only when it hashes to a CID that the root of a live, signed head reaches (P2-P-02). The record is read strictly
 * ({@code CborCodec.fromWire}), and it changes shape only under a new stream prefix (rule 16, P2-F-08); a level-1
 * member relays the payload by item id without decoding it.
 *
 * @param v     {@link #VERSION}
 * @param root  the version's root
 * @param seq   the head's seq
 * @param part  this part, from 0
 * @param parts the parts of the delta, 1 to {@link #MAX_PARTS}
 * @param nodes the node bytes of this part
 */
@WireRecord
public record PushBundle(int v, String root, long seq, int part, int parts, List<byte[]> nodes) {

    /** The version of the record. */
    public static final int VERSION = 1;
    /** The most bytes of nodes one bundle carries: 1 MiB (§3.4). */
    public static final int MAX_BYTES = 1 << 20;
    /** The most parts of one delta: a delta of 64 MiB at most, past every {@code push.max-bytes} a host may set. */
    public static final int MAX_PARTS = 64;

    /**
     * Checks the version, the root, the seq, the part, and the node bytes.
     *
     * @return the first violated rule, or empty when the bundle is well formed
     */
    public Optional<String> violation() {
        if (v != VERSION) {
            return Optional.of("unsupported PushBundle version " + v);
        }
        if (!PRef.isCid(root)) {
            return Optional.of("root is not a CID: " + root);
        }
        if (seq < 0 || seq > PRef.MAX_SEQ) {
            return Optional.of("seq " + seq + " is outside 0 to " + PRef.MAX_SEQ);
        }
        if (parts < 1 || parts > MAX_PARTS || part < 0 || part >= parts) {
            return Optional.of("part " + part + " of " + parts + " is outside 1 to " + MAX_PARTS + " parts");
        }
        if (nodes == null || nodes.isEmpty()) {
            return Optional.of("a bundle carries at least one node");
        }
        long bytes = 0;
        for (byte[] node : nodes) {
            if (node == null || node.length == 0) {
                return Optional.of("a bundle carries an empty node");
            }
            bytes += node.length;
        }
        if (bytes > MAX_BYTES) {
            return Optional.of("a bundle carries at most " + MAX_BYTES + " bytes of nodes, got " + bytes);
        }
        return Optional.empty();
    }
}
