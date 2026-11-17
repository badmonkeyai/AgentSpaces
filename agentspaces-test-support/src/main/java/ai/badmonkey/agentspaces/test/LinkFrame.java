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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One frame on one link of a {@link SimNetwork}, as a {@link LinkFilter} sees it: the two node addresses,
 * the bytes, a fabric-wide send sequence, and the level-1 header and level-2 items, each decoded once and
 * only when a filter asks (srad/spoiler-peers.md §3.3).
 */
public final class LinkFrame {

    private final String from;
    private final String to;
    private final byte[] bytes;
    private final long sequence;
    private final FrameDecoder headers;
    private final ItemDecoder itemDecoder;
    private Optional<FrameHeader> header;
    private List<FrameItem> items;

    LinkFrame(String from, String to, byte[] bytes, long sequence, FrameDecoder headers, ItemDecoder itemDecoder) {
        this.from = Objects.requireNonNull(from, "from");
        this.to = Objects.requireNonNull(to, "to");
        this.bytes = Objects.requireNonNull(bytes, "bytes");
        this.sequence = sequence;
        this.headers = headers;
        this.itemDecoder = itemDecoder;
    }

    /** The sending node's address on the fabric. */
    public String from() {
        return from;
    }

    /** The receiving node's address on the fabric. */
    public String to() {
        return to;
    }

    /** A copy of the bytes on the wire. */
    public byte[] bytes() {
        return bytes.clone();
    }

    /** The order the fabric saw this frame sent in, across every link. */
    public long sequence() {
        return sequence;
    }

    /** The level-1 header, decoded once; empty without a decoder or when the bytes are no frame. */
    public synchronized Optional<FrameHeader> header() {
        if (header == null) {
            header = headers == null ? Optional.empty() : headers.header(bytes);
        }
        return header;
    }

    /** The level-2 items, decoded once; empty when no item decoder is plugged. */
    public synchronized List<FrameItem> items() {
        if (items == null) {
            items = itemDecoder == null ? List.of() : List.copyOf(itemDecoder.items(bytes));
        }
        return items;
    }

    /** The envelope kind's name, or null when the header does not decode. */
    public String kind() {
        return header().map(FrameHeader::kind).orElse(null);
    }

    @Override
    public String toString() {
        return "#" + sequence + " " + from + ">" + to + " " + header().map(h -> h.kind() + h.streams()).orElse("?");
    }
}
