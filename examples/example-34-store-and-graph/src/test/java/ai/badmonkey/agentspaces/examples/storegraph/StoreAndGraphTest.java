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
package ai.badmonkey.agentspaces.examples.storegraph;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The acceptance test of example 34 (srad/agentspaces-pdt2.md §7.3, P2-O-03) over loopback tcp: the superfriends
 * graph on a {@code db} ref that two linear writers take turns on and a Python reader answers lazily, then a blob
 * that two stores, one blind, hold after its writer leaves, prove under challenge, and reconcile after one store
 * restarts from a truncated pack.
 */
class StoreAndGraphTest {

    static final IntSupplier FREE_TCP = () -> {
        try (ServerSocket socket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    };

    @Test
    @Timeout(600)
    void theGraphOnADbRefTakesTurnsAndThePythonReaderAnswersFromTheIndexItLoads(@TempDir Path dir) throws Exception {
        // P2-D-09 and PD-Q-INT-01: the Python read is part of the row, so a missing interpreter skips the test
        Assumptions.assumeTrue(PythonDbReader.available(), "python reader: skipped (" + PythonDbReader.whyUnavailable() + ")");
        GraphHalf.Report report = GraphHalf.run(dir, FREE_TCP, GraphHalf.Sizes.quick(), true, System.out);
        System.out.print(report.summary());

        assertThat(report.dbNodes()).as("the graph spans many dn nodes").isGreaterThan(100);
        // linear mode: every swap took the head the one before it wrote, so the bases form one chain from v0
        assertThat(report.turns()).hasSize(2 * GraphHalf.Sizes.quick().findingsPerAnalyst());
        Set<String> bases = new HashSet<>();
        Set<String> roots = new HashSet<>();
        for (GraphHalf.Turn turn : report.turns()) {
            assertThat(bases.add(turn.base())).as("no two swaps over one base: " + report.turns()).isTrue();
            roots.add(turn.root());
        }
        roots.add(report.v0());
        roots.remove(report.finalRoot());
        assertThat(bases).as("the bases are v0 and every root but the last").isEqualTo(roots);
        assertThat(report.turns()).extracting(GraphHalf.Turn::analyst).contains("analyst-1", "analyst-2");
        // P2-Q-PRF-01 at example scale: the first transaction over an empty tail ships its root and tail inline
        assertThat(report.turns().get(0).deltaBytes()).as("P2-Q-PRF-01: " + report.turns().get(0)).isLessThan(2_000);
        assertThat(new HashSet<>(report.javaRoots().values())).as("every Java peer derefs one root: " + report.javaRoots())
                .containsExactly(report.finalRoot());

        GraphHalf.Lookup python = report.python();
        assertThat(python).as(report.pythonNote()).isNotNull();
        assertThat(python.root()).as("PD-Q-INT-01: the Python reader derefs the Java root").isEqualTo(report.finalRoot());
        assertThat(python.e()).as("lookup " + SuperfriendsGraph.ACME_KEY).isEqualTo(report.acme());
        assertThat(python.name()).isEqualTo(SuperfriendsGraph.ACME);
        assertThat(python.about()).as("the findings about Acme, the tail's included").isEqualTo(report.aboutAcme());
        assertThat(report.aboutAcme()).as("the analysts' findings sit in the tail").containsAll(report.turns().stream()
                .map(GraphHalf.Turn::entity).toList());
        assertThat(python.held()).as("P2-D-04: the reader loads the nodes its reads walk, a few of " + report.dbNodes())
                .isLessThan(report.dbNodes() / 4);
    }

    @Test
    @Timeout(600)
    void twoStoresOneBlindHoldTheBlobAfterItsWriterLeavesAndReconcileAfterARestart(@TempDir Path dir) throws Exception {
        StoreHalf.Report report = StoreHalf.run(dir, "tcp", FREE_TCP, StoreHalf.Sizes.quick(), System.out);
        System.out.print(report.summary());
        assertStoreHalf(report, StoreHalf.Sizes.quick());
    }

    /** The store half's rows, on any binding (PdtBindingsTest repeats them on dtls). */
    static void assertStoreHalf(StoreHalf.Report report, StoreHalf.Sizes sizes) {
        assertThat(report.blobBytes()).isEqualTo(sizes.blobBytes()).isLessThanOrEqualTo(64L << 20);
        // P2-S-03 and P2-S-08: two receipts, one from the blind store
        assertThat(report.receipts()).extracting(StoreHalf.Receipt::store).containsExactlyInAnyOrder("store-a", "store-b");
        assertThat(report.receipts()).filteredOn(StoreHalf.Receipt::blind).extracting(StoreHalf.Receipt::store)
                .containsExactly("store-b");
        // P2-Q-REL-02: the late reader fetched every byte from the stores after the writer left
        StoreHalf.Cold cold = report.cold();
        assertThat(cold.complete()).as("the late reader's fetch: " + cold).isTrue();
        assertThat(cold.bytesBySource().keySet()).as("every byte came from the stores").isNotEmpty()
                .isSubsetOf(Set.of("store-a", "store-b"));
        assertThat(cold.identity()).as("P2-B-01: the bytes read have the writer's BLAKE3 identity").isEqualTo(report.identity());
        // P2-S-07 and P2-Q-REL-03: both stores prove they hold the root, the blind one without the key
        assertThat(report.challenges()).extracting(StoreHalf.Challenge::store).containsExactly("store-a", "store-b");
        assertThat(report.challenges()).allMatch(StoreHalf.Challenge::passed, "every challenge passed");
        // P2-K-03 and P2-Q-REL-06: store-a lost a pack in a restart and fetched the root again from store-b
        StoreHalf.Restart restart = report.restart();
        assertThat(restart.lostNodes()).as("the truncated pack lost nodes").isPositive();
        assertThat(restart.completeAfterRestart()).as("store-a no longer holds the root whole").isFalse();
        assertThat(restart.repairing()).as("the reconcile names the pinned root").isEqualTo(List.of(report.root()));
        assertThat(restart.repaired()).as("store-a holds the root again: " + restart).isTrue();
        assertThat(restart.equalSets()).as("P2-Q-REL-06: equal held sets").isTrue();
        assertThat(restart.challenge().passed()).as("store-a passes a challenge after the repair").isTrue();
    }
}
