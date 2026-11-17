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
package ai.badmonkey.agentspaces.test;

import java.util.Map;
import java.util.Objects;

/**
 * One item the level-2 matcher decodes inside a frame (srad/spoiler-peers.md §3.3): a space state, claim,
 * completion or cancel, a PDT node in a content answer, a request for nodes. One frame may carry many (a
 * {@code PULL_RESP}, a {@code BATCH}, a {@code NODES} answer). The vocabulary belongs to the
 * {@link ItemDecoder} that produced it; {@code agentspaces-spoilers} names its types by an enum.
 *
 * @param type       the item's type, such as {@code STATE}, {@code CLAIM} or {@code NODE}
 * @param stream     the stream or capability it travelled on
 * @param id         the entry id, CID or root the item is about; may be null
 * @param attributes further decoded fields, such as a node's kind or a claim's holder
 */
public record FrameItem(String type, String stream, String id, Map<String, String> attributes) {

    /** Copies the attributes. */
    public FrameItem {
        Objects.requireNonNull(type, "type");
        attributes = Map.copyOf(attributes);
    }

    /** One attribute, or null. */
    public String attribute(String name) {
        return attributes.get(name);
    }
}
