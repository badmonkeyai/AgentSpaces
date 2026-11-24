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
package ai.badmonkey.agentspaces.it.policy;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-X-07's source half for EL-T-13 (srad/agentspaces-event-loop.md): no core main class schedules at a fixed rate or a
 * fixed delay. Periodic work is a one-shot re-armed from its own completion or next due, so a late run never runs
 * twice back to back to catch up. {@code ThreadPolicyTest} fails on these calls in the core's bytecode (EL-X-07) and
 * only reports them in the other reactor modules; this source scan keeps failing them in every {@code agentspaces-*}
 * module, and in the examples' main code since phase 6: examples 29, 33 and 34 ran their nodes from a fixed-rate
 * ticker until then and now start their node's own loop instead (EL-X-07; 33 last, once its DTLS bindings flake on the
 * loop was root-caused to the ref engine's moving in-flight step, srad/agentspaces-event-loop.md §10.47).
 */
class NoFixedRateTest {

    @Test
    void noCoreMainClassSchedulesAtAFixedRateOrDelay() throws IOException {
        Path root = Path.of(System.getProperty("user.dir")).resolve("../..").normalize();
        assertThat(root.resolve("pom.xml")).as("the core reactor root").exists();
        List<String> found = new ArrayList<>();
        List<Path> scanned = new ArrayList<>();
        try (Stream<Path> modules = Files.list(root)) {
            modules.filter(p -> p.getFileName().toString().startsWith("agentspaces-")).forEach(scanned::add);
        }
        try (Stream<Path> examples = Files.list(root.resolve("examples"))) {
            // every example, 33 included since its flake on the loop was root-caused (§10.47, EL-X-07)
            examples.filter(p -> p.getFileName().toString().startsWith("example-")).forEach(scanned::add);
        }
        {
            for (Path module : scanned) {
                Path main = module.resolve("src/main/java");
                if (!Files.isDirectory(main)) {
                    continue;
                }
                try (Stream<Path> files = Files.walk(main)) {
                    files.filter(p -> p.toString().endsWith(".java")).forEach(file -> {
                        try {
                            List<String> lines = Files.readAllLines(file);
                            for (int i = 0; i < lines.size(); i++) {
                                String line = lines.get(i);
                                if (line.contains(".scheduleAtFixedRate(") || line.contains(".scheduleWithFixedDelay(")) {
                                    found.add(root.relativize(file) + ":" + (i + 1));
                                }
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                }
            }
        }
        assertThat(scanned).as("the core modules and the examples").hasSizeGreaterThan(40);
        assertThat(found).as("fixed-rate or fixed-delay schedules in core or example main code (EL-T-13, EL-X-07)")
                .isEmpty();
    }
}
