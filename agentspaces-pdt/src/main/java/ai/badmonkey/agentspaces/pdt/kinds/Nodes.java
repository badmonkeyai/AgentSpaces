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
package ai.badmonkey.agentspaces.pdt.kinds;

import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** What the typed readers of the level-2 kinds share (PDT 2 phase 0). */
final class Nodes {

    private Nodes() {
    }

    /**
     * Decodes a node of one kind and checks that every key the kind requires is present.
     *
     * @param cid      the CID, named in a refusal; may be null
     * @param bytes    the node bytes
     * @param kind     the kind the caller reads
     * @param required the keys the kind requires besides {@code k}
     * @return the decoded node
     * @throws NodeFormatException on any refusal
     */
    static Map<String, Object> decode(String cid, byte[] bytes, String kind, List<String> required) {
        Map<String, Object> node = NodeCodec.decode(cid, bytes);
        Object found = node.get(NodeCodec.KIND_KEY);
        if (!kind.equals(found)) {
            throw new NodeFormatException(cid, "kind '" + found + "' where a '" + kind + "' node belongs");
        }
        for (String key : required) {
            if (!node.containsKey(key)) {
                throw new NodeFormatException(cid, "a '" + kind + "' node lacks '" + key + "'");
            }
        }
        return node;
    }

    /** Builds a typed value, turning the constructor's refusal into a refusal of the node. */
    static <T> T build(String cid, Supplier<T> constructor) {
        try {
            return constructor.get();
        } catch (IllegalArgumentException refused) {
            throw new NodeFormatException(cid, refused.getMessage(), refused);
        }
    }

    /** A list field cast to the element type {@code NodeCodec} guarantees for it. */
    @SuppressWarnings("unchecked")
    static <T> List<T> list(Map<String, Object> node, String key) {
        return (List<T>) node.get(key);
    }
}
