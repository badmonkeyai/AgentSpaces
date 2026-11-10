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
package ai.badmonkey.agentspaces.store;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The drift guard of srad/agentspaces-pdt2.md §6.9 for the store module (PDT 2 phase 3): a store opens no socket,
 * starts no gossip, and adds no peer role. It scans every source file under {@code src/main/java} and fails on an
 * import of {@code peering.transport}, {@code GossipBus}, {@code PeerNode}, or a JDK socket.
 */
class ModuleBoundaryTest {

    private static final Path SOURCES = Path.of("src", "main", "java");

    private static final List<Pattern> FORBIDDEN = List.of(
            Pattern.compile("^\\s*import\\s+(static\\s+)?ai\\.badmonkey\\.agentspaces\\.peering\\.transport\\b"),
            Pattern.compile("^\\s*import\\s+(static\\s+)?[\\w.]*\\bGossipBus\\b"),
            Pattern.compile("^\\s*import\\s+(static\\s+)?[\\w.]*\\bPeerNode\\b"),
            Pattern.compile("^\\s*import\\s+(static\\s+)?java\\.net\\.(Socket|ServerSocket)\\b"));

    @Test
    void mainSourcesImportNoTransportGossipOrSocket() throws IOException {
        assertThat(SOURCES).as("run from the module directory").isDirectory();
        List<String> offences = new ArrayList<>();
        int scanned = 0;
        try (Stream<Path> files = Files.walk(SOURCES)) {
            for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                scanned++;
                List<String> lines = Files.readAllLines(file);
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    for (Pattern pattern : FORBIDDEN) {
                        if (pattern.matcher(line).find()) {
                            offences.add(SOURCES.relativize(file) + ":" + (i + 1) + ": " + line.trim());
                        }
                    }
                }
            }
        }
        assertThat(scanned).as("source files scanned").isGreaterThan(3);
        assertThat(offences).as("imports that breach the §6.9 boundary").isEmpty();
    }

    @Test
    void theGuardRecognisesEachForbiddenImport() {
        List<String> samples = List.of(
                "import ai.badmonkey.agentspaces.peering.transport.tcp.TcpTransport;",
                "import ai.badmonkey.agentspaces.peering.gossip.GossipBus;",
                "import static ai.badmonkey.agentspaces.peering.gossip.GossipBus.of;",
                "import ai.badmonkey.agentspaces.peering.PeerNode;",
                "import java.net.Socket;",
                "import java.net.ServerSocket;");
        for (String sample : samples) {
            assertThat(FORBIDDEN.stream().anyMatch(p -> p.matcher(sample).find())).as(sample).isTrue();
        }
        List<String> allowed = List.of(
                "import ai.badmonkey.agentspaces.peering.blocks.BlockStore;",
                "import java.net.URI;",
                "import ai.badmonkey.agentspaces.content.Messages;");
        for (String sample : allowed) {
            assertThat(FORBIDDEN.stream().anyMatch(p -> p.matcher(sample).find())).as(sample).isFalse();
        }
    }
}
