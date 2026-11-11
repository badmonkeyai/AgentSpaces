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
package ai.badmonkey.agentspaces.content;

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
 * A user reads the messages of this module: exceptions, log lines, and fetch traces. A
 * message says what went wrong and what to do, in plain words, and names no internal
 * design id such as {@code PD-N-04} or {@code P2-X-06}. This guard scans every string
 * literal under {@code src/main/java}, comments aside, and fails on one that names an id.
 */
class MessagesNameNoDesignIdsTest {

    private static final Path SOURCES = Path.of("src", "main", "java");

    /** A design id: a prefix, one or two letter groups, and a number, as in PD-N-04 or P2-Q-SEC-05; or an ISSUE- name. */
    static final Pattern DESIGN_ID = Pattern.compile(
            "\\b[A-Z][A-Z0-9]{1,3}-[A-Z](?:-[A-Z]{1,4})?-\\d{1,3}\\b|\\bISSUE-[A-Z][A-Za-z]");

    @Test
    void noMessageInThisModuleNamesADesignId() throws IOException {
        assertThat(SOURCES).as("run from the module directory").isDirectory();
        List<String> offences = new ArrayList<>();
        int scanned = 0;
        try (Stream<Path> files = Files.walk(SOURCES)) {
            for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                scanned++;
                for (String[] literal : literals(Files.readString(file))) {
                    if (DESIGN_ID.matcher(literal[1]).find()) {
                        offences.add(SOURCES.relativize(file) + ":" + literal[0] + ": " + literal[1]);
                    }
                }
            }
        }
        assertThat(scanned).as("source files scanned").isGreaterThan(20);
        assertThat(offences).as("string literals that name a design id; say what to do instead").isEmpty();
    }

    @Test
    void theGuardFindsAnIdInALiteralAndSkipsOneInAComment() {
        String source = "class A {\n"
                + "  // PD-N-04 in a comment\n"
                + "  /* P2-X-06 in a block */\n"
                + "  String a = \"bound (PD-N-04)\";\n"
                + "  String b = \"SHA-256 and UTF-8 \\\" (P2-Q-SEC-05)\";\n"
                + "  char c = '\"';\n"
                + "  String d = \"\"\"\n      see ISSUE-Motion\n      \"\"\";\n"
                + "  String e = \"HKDF-SHA-256, PDT 2\";\n"
                + "}\n";
        List<String> hits = new ArrayList<>();
        for (String[] literal : literals(source)) {
            if (DESIGN_ID.matcher(literal[1]).find()) {
                hits.add(literal[0]);
            }
        }
        assertThat(hits).containsExactly("4", "5", "7");
    }

    /** The string literals and text blocks of a Java source, each with its line, outside comments and char literals. */
    static List<String[]> literals(String src) {
        List<String[]> out = new ArrayList<>();
        int line = 1;
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '\n') {
                line++;
                i++;
            } else if (src.startsWith("//", i)) {
                int end = src.indexOf('\n', i);
                i = end < 0 ? n : end;
            } else if (src.startsWith("/*", i)) {
                int end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                line += count(src, i, end);
                i = end;
            } else if (c == '\'') {
                int j = i + 1;
                while (j < n && src.charAt(j) != '\'') {
                    j += src.charAt(j) == '\\' ? 2 : 1;
                }
                i = j + 1;
            } else if (src.startsWith("\"\"\"", i)) {
                int end = src.indexOf("\"\"\"", i + 3);
                end = end < 0 ? n : end + 3;
                out.add(new String[] {Integer.toString(line), src.substring(i, end)});
                line += count(src, i, end);
                i = end;
            } else if (c == '"') {
                int j = i + 1;
                while (j < n && src.charAt(j) != '"' && src.charAt(j) != '\n') {
                    j += src.charAt(j) == '\\' ? 2 : 1;
                }
                out.add(new String[] {Integer.toString(line), src.substring(i, Math.min(n, j + 1))});
                i = j + 1;
            } else {
                i++;
            }
        }
        return out;
    }

    private static int count(String src, int from, int to) {
        int lines = 0;
        for (int k = from; k < to; k++) {
            if (src.charAt(k) == '\n') {
                lines++;
            }
        }
        return lines;
    }
}
