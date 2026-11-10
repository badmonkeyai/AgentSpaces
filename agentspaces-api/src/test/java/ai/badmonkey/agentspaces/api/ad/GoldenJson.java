/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.api.ad;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The shared {@code golden.json} as the api module's pin tests read it (neutral vectors, 2026-11-09): the vendored copy
 * beside the generator, else the specification's. With {@code -Dgolden.required=true} a missing file or entry fails
 * the test; otherwise the comparison is skipped.
 */
final class GoldenJson {

    private static final List<Path> CANDIDATES = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean REQUIRED = Boolean.getBoolean("golden.required");

    private GoldenJson() {
    }

    /**
     * The text of one entry, or a skipped test when the file or the entry is absent and the vectors are not required.
     *
     * @param key the entry's key
     * @return the entry's text
     */
    static String entry(String key) {
        Optional<Path> file = CANDIDATES.stream().filter(Files::isRegularFile).findFirst();
        if (REQUIRED) {
            assertThat(file).as("golden.json is required (golden.required=true) but none of " + CANDIDATES + " exists").isPresent();
        }
        assumeTrue(file.isPresent(), "golden.json not present");
        JsonNode node;
        try {
            node = new ObjectMapper().readTree(file.get().toFile()).get(key);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (REQUIRED) {
            assertThat(node).as("golden.json lacks " + key).isNotNull();
        }
        assumeTrue(node != null, "golden.json lacks " + key);
        return node.isTextual() ? node.asText() : node.toString();
    }
}
