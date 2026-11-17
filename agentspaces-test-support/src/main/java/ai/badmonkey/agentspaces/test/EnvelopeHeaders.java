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

import ai.badmonkey.agentspaces.common.codec.CborCodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The level-1 {@link FrameDecoder} for the SPEC §9 envelope: the kind, sender, recipient, stamp and
 * streams of a frame, and of the frames a {@code BATCH} or {@code RELAY_FRAME} carries. It verifies
 * nothing and reads the CBOR by the field names §9 fixes, because this module cannot see the
 * peering classes; so every module's own tests can filter on kind and stream (srad/spoiler-peers.md
 * SP-N-01, §7.2) without a test-jar, and {@code agentspaces-spoilers} reuses it rather than keeping a
 * second level-1 reader.
 */
public final class EnvelopeHeaders implements FrameDecoder {

    /** The deepest a relay may nest frames before this reader stops: one relay of a batch is the honest most. */
    private static final int MAX_DEPTH = 4;

    private final CborCodec codec = CborCodec.defaultCodec();

    @Override
    public Optional<FrameHeader> header(byte[] frame) {
        return Optional.ofNullable(signed(frame, 0));
    }

    private FrameHeader signed(byte[] frame, int depth) {
        Map<?, ?> outer = map(frame);
        return outer == null || !(outer.get("envelope") instanceof Map<?, ?> envelope) ? null : envelope(envelope, depth);
    }

    private FrameHeader envelope(Map<?, ?> envelope, int depth) {
        Object kind = envelope.get("kind");
        if (kind == null) {
            return null;
        }
        List<String> streams = new ArrayList<>();
        List<FrameHeader> inner = new ArrayList<>();
        Map<?, ?> body = envelope.get("body") instanceof byte[] bytes ? map(bytes) : null;
        if (body != null) {
            switch (kind.toString()) {
                case "RUMOR", "GRAFT", "PRUNE" -> addText(streams, body.get("streamId"));
                case "PULL_RESP" -> addKeys(streams, body.get("deltas"));
                case "DIGEST" -> {
                    for (String field : List.of("digests", "hashes", "tree", "ids")) {
                        addKeys(streams, body.get(field));
                    }
                    if (body.get("want") instanceof Collection<?> want) {
                        want.forEach(s -> addText(streams, s));
                    }
                }
                case "HAVE" -> {
                    if (body.get("items") instanceof Collection<?> items) {
                        for (Object item : items) {
                            if (item instanceof Map<?, ?> m) {
                                addText(streams, m.get("streamId"));
                            }
                        }
                    }
                }
                case "PIPE_DATA" -> addText(streams, body.get("capability"));
                case "BATCH" -> {
                    // inner frames of a batch are bare envelopes that inherit the outer signature (PERF2 §3.3)
                    if (depth < MAX_DEPTH && body.get("frames") instanceof Collection<?> frames) {
                        for (Object f : frames) {
                            Map<?, ?> m = f instanceof byte[] b ? map(b) : null;
                            FrameHeader h = m == null ? null : envelope(m, depth + 1);
                            if (h != null) {
                                inner.add(h);
                            }
                        }
                    }
                }
                case "RELAY_FRAME" -> {
                    // a relay carries the origin's complete signed frame (spec §5.4)
                    if (depth < MAX_DEPTH && body.get("frame") instanceof byte[] b) {
                        FrameHeader h = signed(b, depth + 1);
                        if (h != null) {
                            inner.add(h);
                        }
                    }
                }
                default -> { }
            }
        }
        return new FrameHeader(kind.toString(), text(envelope.get("from")), text(envelope.get("to")),
                stampMillis(envelope.get("stamp")), streams, inner);
    }

    private Map<?, ?> map(byte[] bytes) {
        try {
            return codec.fromBytes(bytes, Map.class);
        } catch (RuntimeException notCbor) {
            return null;
        }
    }

    private static String text(Object value) {
        return value == null ? null : value.toString();
    }

    private static void addText(List<String> out, Object value) {
        if (value != null && !out.contains(value.toString())) {
            out.add(value.toString());
        }
    }

    private static void addKeys(List<String> out, Object value) {
        if (value instanceof Map<?, ?> m) {
            m.keySet().forEach(k -> addText(out, k));
        }
    }

    /** The physical milliseconds of a canonical {@code physical:logical:node} stamp, or -1. */
    static long stampMillis(Object stamp) {
        if (stamp == null) {
            return -1;
        }
        String text = stamp.toString();
        int colon = text.indexOf(':');
        try {
            return Long.parseLong(colon < 0 ? text : text.substring(0, colon));
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
