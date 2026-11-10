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

import ai.badmonkey.agentspaces.pdt.golden.PdtVectors;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Adds the PDT 2 phase 0 golden vectors (srad/agentspaces-pdt2.md §8: P2-D-01, P2-S-02, P2-B-02,
 * P2-R-01, P2-T-01, P2-V-01) to {@code golden.json} files and changes nothing else. The owner allowed
 * these vectors to be added beside the existing entries, never changing or removing one, so this
 * program does not regenerate the file as {@code GoldenVectors} does: it computes
 * {@code PdtVectors.pdt2Vectors()}, refuses to run when a file already holds one of those names with
 * another value, inserts each missing entry after the last {@code pdt_*} line in emission order, and
 * checks that removing the inserted lines gives back the original bytes before it writes.
 * {@code GoldenVectors} emits the same entries in the same place on a full regeneration, because
 * {@code PdtVectors.vectors()} ends with them.
 *
 * <p>From the {@code agentspaces} directory, after {@code mvn -o -q install -DskipTests} in
 * {@code agentspaces-agent} and {@code agentspaces-pdt}:
 * <pre>
 * mvn -o -q -f agentspaces-pdt/pom.xml dependency:build-classpath -Dmdep.outputFile=/tmp/pdt2-cp.txt
 * CP="agentspaces-pdt/target/classes:$(cat /tmp/pdt2-cp.txt)"
 * java -cp "$CP" tools/golden/Pdt2GoldenVectors.java ../agentspaces-spec/golden.json tools/golden/golden.json
 * </pre>
 * With {@code --check} first it writes nothing and exits 1 when a file lacks a vector or holds
 * another value. Then {@code ../agentspaces-spec/sync-golden.sh} refreshes the client copies.
 */
public final class Pdt2GoldenVectors {

    private Pdt2GoldenVectors() {
    }

    public static void main(String[] args) throws Exception {
        boolean check = args.length > 0 && args[0].equals("--check");
        List<String> files = List.of(args).subList(check ? 1 : 0, args.length);
        if (files.isEmpty()) {
            System.err.println("usage: Pdt2GoldenVectors [--check] golden.json...");
            System.exit(2);
        }
        Map<String, String> vectors = PdtVectors.pdt2Vectors();
        boolean stale = false;
        for (String file : files) {
            stale |= add(Path.of(file), vectors, check);
        }
        System.exit(check && stale ? 1 : 0);
    }

    /** Adds the missing vectors to one file; returns whether it lacked any. */
    @SuppressWarnings("unchecked")
    private static boolean add(Path file, Map<String, String> vectors, boolean check) throws Exception {
        String original = Files.readString(file, StandardCharsets.UTF_8);
        Map<String, Object> existing = new ObjectMapper().readValue(original, LinkedHashMap.class);
        Map<String, String> missing = new LinkedHashMap<>();
        for (Map.Entry<String, String> vector : vectors.entrySet()) {
            Object held = existing.get(vector.getKey());
            if (held == null) {
                missing.put(vector.getKey(), vector.getValue());
            } else if (!vector.getValue().equals(held)) {
                throw new IllegalStateException(file + " holds " + vector.getKey() + " with another value; this program "
                        + "never changes an entry");
            }
        }
        if (missing.isEmpty()) {
            System.out.println(file + ": every PDT 2 vector present and equal");
            return false;
        }
        if (check) {
            System.out.println(file + ": lacks " + missing.keySet());
            return true;
        }
        List<String> lines = new ArrayList<>(List.of(original.split("\n", -1)));
        int last = -1;
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).startsWith("  \"pdt_")) {
                last = i;
            }
        }
        if (last < 0 || !lines.get(last).endsWith(",")) {
            throw new IllegalStateException(file + " has no pdt_* entry followed by another to insert after");
        }
        List<String> inserted = new ArrayList<>();
        missing.forEach((name, value) -> inserted.add("  \"" + name + "\": \"" + value + "\","));
        lines.addAll(last + 1, inserted);
        String updated = String.join("\n", lines);
        List<String> back = new ArrayList<>(List.of(updated.split("\n", -1)));
        back.subList(last + 1, last + 1 + inserted.size()).clear();
        if (!String.join("\n", back).equals(original)) {
            throw new IllegalStateException("removing the inserted lines does not give back " + file);
        }
        Map<String, Object> reread = new ObjectMapper().readValue(updated, LinkedHashMap.class);
        for (Map.Entry<String, Object> entry : existing.entrySet()) {
            if (!entry.getValue().equals(reread.get(entry.getKey()))) {
                throw new IllegalStateException(file + " would change " + entry.getKey());
            }
        }
        Files.writeString(file, updated, StandardCharsets.UTF_8);
        System.out.println(file + ": added " + missing.keySet());
        return true;
    }
}
