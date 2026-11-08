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

import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A value spill node, kind {@code x} (srad/agentspaces-pdt.md §3.1, PD-N-05): one
 * embedded CBOR item held out of line because it exceeds 1 KiB.
 */
final class XNode extends TreeNode {

    final byte[] value;

    XNode(byte[] value) {
        this.value = value;
    }

    XNode(String cid, byte[] value) {
        this.value = value;
        this.cid = cid;
    }

    static XNode decode(String cid, Map<String, Object> node) {
        Object v = node.get("v");
        if (!(v instanceof Embedded e)) {
            throw new ai.badmonkey.agentspaces.pdt.NodeFormatException(cid, "spill node has no 'v'");
        }
        return new XNode(cid, e.bytes());
    }

    @Override
    byte[] encode(Flusher flusher) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.X);
        node.put("v", new Embedded(value));
        return NodeCodec.encode(node);
    }
}
