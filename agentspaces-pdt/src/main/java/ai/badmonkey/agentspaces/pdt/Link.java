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
import java.util.Objects;

/**
 * A link from one node to another (srad/agentspaces-pdt.md §3.1, PD-N-03): on the
 * wire it is CBOR tag 42 over a byte string of {@code 0x00} followed by the binary
 * CID; in a decoded node it is this value holding the CID string.
 *
 * @param cid the CIDv1 string the link names, validated by {@link Cids#parse}
 */
public record Link(String cid) {

    /** Validates the CID. */
    public Link {
        Objects.requireNonNull(cid, "cid");
        Cids.parse(cid);
    }

    /** The multicodec of the linked node: {@link Cids#DAG_CBOR} or {@link Cids#RAW}. */
    public int codec() {
        return Cids.parse(cid).codec();
    }

    /** Whether the link names a structured node ({@code dag-cbor}). */
    public boolean structured() {
        return codec() == Cids.DAG_CBOR;
    }

    /** The binary CID the tag carries after its leading zero byte. */
    public byte[] binary() {
        return Cids.binary(cid);
    }

    @Override
    public String toString() {
        return "Link[" + cid + "]";
    }
}
