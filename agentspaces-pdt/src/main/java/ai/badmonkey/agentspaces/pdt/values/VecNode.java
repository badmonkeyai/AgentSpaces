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
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A node of the vector trie, kind {@code v} (srad/agentspaces-pdt.md §3.2, PD-V-03):
 * an interior node holds up to 32 child references ({@code c}), a leaf holds up to
 * 32 element references ({@code e}). Interior slots are exactly sized, so a
 * right-edge interior node is shorter than 32; a leaf in the trie is always full,
 * because only a full tail is pushed into the trie and the tail holds the rest. A
 * transient marks the nodes it creates with its edit token and changes them in
 * place (PD-V-05).
 *
 * <p>A stored node is checked on decode (a leaf of exactly 32 items, 1 to 32
 * children) and on every slot access (PD-N-04), so a trie narrower than its root's
 * count claims is refused by name before any index fails.
 */
final class VecNode extends TreeNode {

    static final int WIDTH = 32;
    static final int BITS = 5;
    static final int MASK = WIDTH - 1;

    final boolean leaf;
    Object[] slots;
    AtomicReference<Thread> edit;

    VecNode(boolean leaf, Object[] slots, AtomicReference<Thread> edit) {
        this.leaf = leaf;
        this.slots = slots;
        this.edit = edit;
    }

    VecNode editable(Ctx ctx) {
        if (ctx.owns(edit)) {
            return this;
        }
        return new VecNode(leaf, slots.clone(), ctx.edit);
    }

    /** The slot at an index, refused by name when the node is narrower than the index expects. */
    Object item(int i) {
        if (i < 0 || i >= slots.length) {
            throw new NodeFormatException(cid, "vector node has " + slots.length + " slots where index " + i + " was expected");
        }
        return slots[i];
    }

    VecNode child(int i, Ctx ctx) {
        Object ref = item(i);
        if (ref instanceof VecNode node) {
            return node;
        }
        String cid = (String) ref;
        VecNode loaded = decode(cid, ctx.loader.load(cid));
        slots[i] = loaded;
        return loaded;
    }

    /** A copy with one more slot at the end. */
    VecNode appended(Object slot, Ctx ctx) {
        VecNode n = editable(ctx);
        Object[] grown = new Object[slots.length + 1];
        System.arraycopy(slots, 0, grown, 0, slots.length);
        grown[slots.length] = slot;
        n.slots = grown;
        return n;
    }

    /** A copy without its last slot. */
    VecNode shrunk(Ctx ctx) {
        VecNode n = editable(ctx);
        Object[] cut = new Object[slots.length - 1];
        System.arraycopy(slots, 0, cut, 0, cut.length);
        n.slots = cut;
        return n;
    }

    VecNode withSlot(int i, Object slot, Ctx ctx) {
        VecNode n = editable(ctx);
        n.slots[i] = slot;
        return n;
    }

    @Override
    byte[] encode(Flusher flusher) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.V);
        List<Object> list = new ArrayList<>(slots.length);
        if (leaf) {
            for (Object item : slots) {
                list.add(Spill.embed(item, flusher));
            }
            node.put("e", list);
        } else {
            for (Object child : slots) {
                list.add(new Link(flusher.cidOf(child)));
            }
            node.put("c", list);
        }
        return NodeCodec.encode(node);
    }

    static VecNode decode(String cid, byte[] bytes) {
        Map<String, Object> node = NodeCodec.decode(cid, bytes);
        if (!Kind.V.equals(node.get(NodeCodec.KIND_KEY))) {
            throw new NodeFormatException(cid, "expected a vector node, found kind '" + node.get(NodeCodec.KIND_KEY) + "'");
        }
        boolean hasC = node.get("c") instanceof List<?>;
        boolean hasE = node.get("e") instanceof List<?>;
        if (hasC == hasE) {
            throw new NodeFormatException(cid, "a vector node holds either 'c' or 'e'");
        }
        List<?> list = (List<?>) node.get(hasC ? "c" : "e");
        if (list.isEmpty() || list.size() > WIDTH) {
            throw new NodeFormatException(cid, "vector node with " + list.size() + " slots; 1 to 32 expected");
        }
        if (hasE && list.size() != WIDTH) {
            throw new NodeFormatException(cid, "vector leaf with " + list.size() + " items; a leaf in the trie holds exactly 32");
        }
        Object[] slots = new Object[list.size()];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = hasC ? ((Link) list.get(i)).cid() : Spill.refOf(((Embedded) list.get(i)).bytes());
        }
        VecNode n = new VecNode(hasE, slots, null);
        n.cid = cid;
        return n;
    }
}
