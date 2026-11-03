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
package ai.badmonkey.agentspaces.connect;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The artifact row: how a connector answers with a result too large to travel as rows. The row
 * names an artifact of the content swarm by its root; the agent fetches it with
 * {@code Content.fetch(root)}. The Workers connector cell writes {@code root} and {@code pieces};
 * the celld connector adds {@code size}, {@code mediaType}, {@code name}, and {@code taken}.
 * Every value is a string, as in any {@code DataResult} row.
 */
public final class ArtifactRows {

    /** The content root, multibase {@code b}. */
    public static final String ROOT = "root";
    /** The piece count. */
    public static final String PIECES = "pieces";
    /** The byte size. */
    public static final String SIZE = "size";
    /** The media type. */
    public static final String MEDIA_TYPE = "mediaType";
    /** The artifact name. */
    public static final String NAME = "name";
    /** The ISO-8601 instant the connector finished reading the source. */
    public static final String TAKEN = "taken";

    private ArtifactRows() {
    }

    /** One artifact row, parsed. */
    public record Artifact(String root, int pieces, long size, String mediaType, String name, String taken) {

        public Artifact {
            Objects.requireNonNull(root, "root");
        }

        /**
         * This artifact as a row.
         *
         * @return the row, with absent fields left out
         */
        public Map<String, String> toRow() {
            Map<String, String> row = new HashMap<>();
            row.put(ROOT, root);
            row.put(PIECES, Integer.toString(pieces));
            if (size >= 0) {
                row.put(SIZE, Long.toString(size));
            }
            if (mediaType != null) {
                row.put(MEDIA_TYPE, mediaType);
            }
            if (name != null) {
                row.put(NAME, name);
            }
            if (taken != null) {
                row.put(TAKEN, taken);
            }
            return Map.copyOf(row);
        }
    }

    /**
     * Whether a row names an artifact: it carries a {@code root} in the content-root form
     * (multibase {@code b}) and a {@code pieces} count.
     *
     * @param row a result row
     * @return whether the row is an artifact row
     */
    public static boolean isArtifact(Map<String, String> row) {
        return parse(row).isPresent();
    }

    /**
     * Parses an artifact row.
     *
     * @param row a result row
     * @return the artifact, or empty when the row is not an artifact row
     */
    public static Optional<Artifact> parse(Map<String, String> row) {
        if (row == null) {
            return Optional.empty();
        }
        String root = row.get(ROOT);
        String pieces = row.get(PIECES);
        if (root == null || root.length() < 2 || root.charAt(0) != 'b' || pieces == null) {
            return Optional.empty();
        }
        try {
            int count = Integer.parseInt(pieces);
            long size = row.containsKey(SIZE) ? Long.parseLong(row.get(SIZE)) : -1;
            if (count < 0) {
                return Optional.empty();
            }
            return Optional.of(new Artifact(root, count, size, row.get(MEDIA_TYPE), row.get(NAME), row.get(TAKEN)));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
