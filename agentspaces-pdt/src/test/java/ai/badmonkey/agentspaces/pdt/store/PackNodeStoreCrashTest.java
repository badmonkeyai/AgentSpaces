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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PD-Q-REL-05 for {@link PackNodeStore}: a simulated crash at every step of a
 * compaction, followed by a reopen, leaves every reachable node of a pinned tree in
 * place and readable.
 */
class PackNodeStoreCrashTest {

    /** Throws at the n-th occurrence of a step. */
    private static final class CrashAt implements PackNodeStore.FailurePoint {
        final String step;
        final int occurrence;
        int seen;
        boolean fired;

        CrashAt(String step, int occurrence) {
            this.step = step;
            this.occurrence = occurrence;
        }

        @Override
        public void reached(String reached) {
            if (reached.equals(step) && ++seen == occurrence) {
                fired = true;
                throw new IllegalStateException("simulated crash at " + step + " #" + occurrence);
            }
        }
    }

    /** What a scenario built before the crash. */
    private record Setup(String root, Map<String, byte[]> live, List<String> garbage) { }

    private static final PackNodeStore.Options SMALL = PackNodeStore.Options.defaults().withMaxPackBytes(6000);

    /** A pinned tree spread across several packs, interleaved with garbage that a removal pass will delete. */
    private static Setup build(PackNodeStore backend, Random random) {
        NodeStore store = new NodeStore(backend, InstantSource.system());
        List<String> leaves = new ArrayList<>();
        List<String> garbage = new ArrayList<>();
        for (int i = 0; i < 48; i++) {
            leaves.add(TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 120)));
            garbage.add(TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 120)));
            garbage.add(TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 120)));
        }
        List<String> parents = new ArrayList<>();
        for (int i = 0; i < leaves.size(); i += 8) {
            parents.add(TreeFixtures.putParent(store, 1, leaves.subList(i, i + 8)));
        }
        String root = TreeFixtures.putParent(store, 2, parents);
        Map<String, byte[]> live = new LinkedHashMap<>();
        for (String cid : store.reachable(root)) {
            live.put(cid, store.get(cid).orElseThrow());
        }
        return new Setup(root, live, garbage);
    }

    /** Removes garbage until the hook fires, or all of it. */
    private static boolean removeGarbage(PackNodeStore backend, List<String> garbage, CrashAt crash) {
        try {
            for (String cid : garbage) {
                backend.remove(cid);
            }
            return false;
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("simulated crash");
            return true;
        }
    }

    private static void assertSurvived(Path dir, Setup setup, String scenario) throws IOException {
        try (PackNodeStore reopened = PackNodeStore.open(dir, SMALL)) {
            assertThat(reopened.rebuiltOnOpen()).as(scenario + ": rebuilt").isTrue();
            NodeStore store = new NodeStore(reopened, InstantSource.system());
            assertThat(store.complete(setup.root())).as(scenario + ": root complete").isTrue();
            for (Map.Entry<String, byte[]> entry : setup.live().entrySet()) {
                assertThat(store.get(entry.getKey())).as(scenario + ": " + entry.getKey()).contains(entry.getValue());
            }
            assertThat(store.reachable(setup.root())).hasSize(setup.live().size());
            try (Stream<Path> files = Files.list(dir)) {
                assertThat(files.filter(f -> f.getFileName().toString().endsWith(".tmp")).count()).isZero();
            }
            reopened.abandon();
        }
    }

    @Test
    void aCrashAtEachCompactionStepLeavesEveryReachableNode(@TempDir Path dir) throws IOException {
        Random random = new Random(42);
        List<String> steps = List.of("compact.start", "compact.copied", "compact.forced", "compact.deleted", "compact.done");
        int scenarios = 0;
        for (String step : steps) {
            int occurrences = step.equals("compact.copied") ? 12 : 3;
            for (int occurrence = 1; occurrence <= occurrences; occurrence++) {
                Path home = dir.resolve(step + "-" + occurrence);
                CrashAt crash = new CrashAt(step, occurrence);
                Setup setup;
                boolean fired;
                PackNodeStore backend = PackNodeStore.open(home, SMALL.withFailurePoint(crash));
                try {
                    setup = build(backend, random);
                    assertThat(backend.packIds().size()).isGreaterThan(3);
                    fired = removeGarbage(backend, setup.garbage(), crash);
                } finally {
                    backend.abandon();
                }
                String scenario = step + " #" + occurrence + (fired ? " fired" : " never reached");
                assertSurvived(home, setup, scenario);
                if (fired) {
                    scenarios++;
                }
            }
        }
        assertThat(scenarios).as("crash scenarios that fired").isGreaterThanOrEqualTo(20);
    }

    @Test
    void aCrashBeforeTheDeleteLeavesTwoCopiesAndTheReopenFinishesTheCompaction(@TempDir Path dir) throws IOException {
        Random random = new Random(7);
        CrashAt crash = new CrashAt("compact.forced", 1);
        PackNodeStore backend = PackNodeStore.open(dir, SMALL.withFailurePoint(crash));
        Setup setup = build(backend, random);
        long packCount = backend.packIds().size();
        assertThat(removeGarbage(backend, setup.garbage(), crash)).isTrue();
        backend.abandon();
        try (Stream<Path> files = Files.list(dir)) {
            assertThat(files.filter(f -> f.getFileName().toString().endsWith(".pack")).count())
                    .as("source still on disk after the crash").isGreaterThanOrEqualTo(packCount);
        }
        try (PackNodeStore reopened = PackNodeStore.open(dir, SMALL)) {
            NodeStore store = new NodeStore(reopened, InstantSource.system());
            assertThat(store.complete(setup.root())).isTrue();
            Set<String> cids = reopened.cids();
            assertThat(cids).containsAll(setup.live().keySet());
            assertThat(reopened.bytesUsed()).as("each live node counted once")
                    .isEqualTo(setup.live().values().stream().mapToLong(b -> b.length).sum()
                            + cids.stream().filter(c -> !setup.live().containsKey(c)).count() * 120L);
            reopened.abandon();
        }
    }

    @Test
    void theHookItselfDoesNotFireOutsideCompaction(@TempDir Path dir) throws IOException {
        CrashAt crash = new CrashAt("compact.start", 1);
        try (PackNodeStore backend = PackNodeStore.open(dir, PackNodeStore.Options.defaults().withFailurePoint(crash))) {
            byte[] bytes = TreeFixtures.chunk(new Random(1), 10);
            String cid = Cids.of(bytes, Cids.RAW);
            backend.put(cid, bytes, 1);
            assertThat(backend.get(cid, 2)).contains(bytes);
            assertThat(crash.fired).isFalse();
            assertThatThrownBy(() -> backend.compact(1L)).isInstanceOf(IllegalStateException.class);
            assertThat(crash.fired).isTrue();
        }
    }
}
