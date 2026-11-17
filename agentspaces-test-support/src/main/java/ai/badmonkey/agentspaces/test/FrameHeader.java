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

/**
 * The level-1 view of one wire frame (srad/spoiler-peers.md §3.3): what a {@link LinkFilter} matches on
 * before any body is decoded. Kinds are the names of the shipped {@code Envelope.Kind} constants; this
 * module depends on api and common only, so it carries them as text.
 *
 * @param kind        the envelope kind's name, such as {@code RUMOR} or {@code PIPE_DATA}
 * @param sender      the sending peer's id as text
 * @param recipient   the addressed peer's id as text, or null on an unaddressed frame
 * @param stampMillis the physical part of the sender's HLC stamp, or -1 when unreadable
 * @param streams     the streams the frame carries: a rumor's stream, a pull answer's streams, a pipe's
 *                    capability; empty for other kinds
 * @param inner       the headers of the frames a {@code BATCH} or {@code RELAY_FRAME} carries, in order
 */
public record FrameHeader(String kind, String sender, String recipient, long stampMillis, List<String> streams,
                          List<FrameHeader> inner) {

    /** Copies the lists. */
    public FrameHeader {
        Objects.requireNonNull(kind, "kind");
        streams = List.copyOf(streams);
        inner = List.copyOf(inner);
    }

    /** Whether this frame, or a frame it carries, is of the kind. */
    public boolean carriesKind(String wanted) {
        if (kind.equals(wanted)) {
            return true;
        }
        for (FrameHeader h : inner) {
            if (h.carriesKind(wanted)) {
                return true;
            }
        }
        return false;
    }

    /** Whether this frame, or a frame it carries, names the stream. */
    public boolean carriesStream(String wanted) {
        if (streams.contains(wanted)) {
            return true;
        }
        for (FrameHeader h : inner) {
            if (h.carriesStream(wanted)) {
                return true;
            }
        }
        return false;
    }
}
