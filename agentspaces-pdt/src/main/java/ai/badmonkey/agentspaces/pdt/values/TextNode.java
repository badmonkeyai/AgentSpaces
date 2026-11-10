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

import ai.badmonkey.agentspaces.pdt.NodeFormatException;

/**
 * A text value held out of line, kind {@code t} (PDT 2 phase 6, P2-T-02; srad/agentspaces-pdt2.md
 * §3.9): a value in the text form of {@link PText} above 1 KiB spills to a node holding the
 * canonical bytes of the same leaf, and reads back as the value it was.
 *
 * <p>The PDT 2 phase 6 review made the node bytes eager, so a text whose node would pass the
 * 1 MiB node bound of PD-N-04 is refused when it enters a map or a vector, as an {@code x} value
 * of that size is, and never at the flush that follows.
 */
final class TextNode extends TreeNode {

    /** The value form: the bytes the slot held before the spill. */
    final byte[] value;
    private final byte[] node;

    TextNode(byte[] value) {
        this.value = value;
        try {
            this.node = PText.fromWire(value).encode();
        } catch (NodeFormatException tooLarge) {
            throw new IllegalArgumentException("a text value whose t node exceeds the 1 MiB node bound (PD-N-04): "
                    + tooLarge.getMessage(), tooLarge);
        }
    }

    @Override
    byte[] encode(Flusher flusher) {
        return node;
    }
}
