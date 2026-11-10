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

import java.util.Objects;

/**
 * One run of a text leaf, kind {@code t} (PDT 2 phase 0, P2-T-01; srad/agentspaces-pdt2.md §3.9):
 * a maximal sequence of characters from one replica with consecutive counters, where every
 * character after the first is the right child of the one before it. On the wire the definite
 * array {@code [replica, counter, parent, side, length, deleted, text]}.
 *
 * <p>The build pins three points §3.9 leaves open, so every port writes the same bytes: the
 * {@code parent} is the Fugue parent of the run's first character as a character id
 * {@code [replica, counter]}, or null for a child of the document root; {@code length} counts
 * Unicode code points; and a deleted run keeps its ids and drops its text, so {@code text} is
 * empty on a deleted run and holds exactly {@code length} code points on a live one, which keeps
 * a leaf near one byte per live ASCII character (P2-T-04).
 *
 * @param replica the replica that inserted the characters, non-empty
 * @param counter the counter of the first character
 * @param parent  the Fugue parent of the first character, or null for the root
 * @param side    {@link #LEFT} or {@link #RIGHT}: which side of its parent the first character hangs on;
 *                always {@link #RIGHT} under the document root
 * @param length  the characters in the run, at least one
 * @param deleted whether every character of the run is deleted
 * @param text    the characters, empty when deleted
 */
public record TextRun(String replica, long counter, CharId parent, String side, long length, boolean deleted,
                      String text) {

    /** The first character is a left child of its parent. */
    public static final String LEFT = "L";

    /** The first character is a right child of its parent. */
    public static final String RIGHT = "R";

    /**
     * A character id: the replica and the counter.
     *
     * @param replica the replica, non-empty
     * @param counter the counter, non-negative
     */
    public record CharId(String replica, long counter) {

        /** Checks the parts. */
        public CharId {
            Objects.requireNonNull(replica, "replica");
            if (replica.isEmpty()) {
                throw new IllegalArgumentException("a character id names a non-empty replica");
            }
            if (counter < 0) {
                throw new IllegalArgumentException("a character counter is non-negative, found " + counter);
            }
        }
    }

    /** Checks the run's own rules. */
    public TextRun {
        Objects.requireNonNull(replica, "replica");
        Objects.requireNonNull(side, "side");
        Objects.requireNonNull(text, "text");
        if (replica.isEmpty()) {
            throw new IllegalArgumentException("a run names a non-empty replica");
        }
        if (counter < 0) {
            throw new IllegalArgumentException("a run counter is non-negative, found " + counter);
        }
        if (!LEFT.equals(side) && !RIGHT.equals(side)) {
            throw new IllegalArgumentException("a run side is L or R, found '" + side + "'");
        }
        if (parent == null && LEFT.equals(side)) {
            // PDT 2 phase 0 (P2-T-01): the document root is the leftmost neighbour of every insert, so
            // Fugue gives it right children only, and every port reads a root child in one place
            throw new IllegalArgumentException("a child of the document root hangs on its right, found side L");
        }
        if (length < 1) {
            throw new IllegalArgumentException("a run length is at least 1, found " + length);
        }
        if (counter > Long.MAX_VALUE - length) {
            throw new IllegalArgumentException("a run's counters pass 2^63 - 1");
        }
        if (text.codePoints().anyMatch(cp -> Character.isSurrogate((char) cp) && cp <= Character.MAX_VALUE)) {
            throw new IllegalArgumentException("a run's text holds a lone surrogate");
        }
        if (deleted && !text.isEmpty()) {
            throw new IllegalArgumentException("a deleted run carries no text");
        }
        if (!deleted && text.codePointCount(0, text.length()) != length) {
            throw new IllegalArgumentException("a live run of length " + length + " holds "
                    + text.codePointCount(0, text.length()) + " code points");
        }
    }

    /** The id of the run's first character. */
    public CharId first() {
        return new CharId(replica, counter);
    }
}
