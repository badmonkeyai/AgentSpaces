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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P28 of reviews/RBE-REGRESSION-MATRIX.md (srad/replication-by-entry.md RE-I-04, RE-I-05): the store half of example 34
 * with every peer's anti-entropy answers cut to 4 KiB, the smallest budget a node takes. The late reader's content and
 * ref spaces then arrive in many answers, a holding or a receipt possibly before the manifest or pin it names. The
 * reader still joins the stores, fetches the table from them alone, and every row of the store half holds as with the
 * default budget. The run prints how long the join and the fetch took.
 */
class StoreJoinBudgetTest {

    private static final long BUDGET = 4_096;

    @Test
    @Timeout(600)
    void theStoreHalfHoldsWhenTheReadersSpacesArriveInBudgetedAnswers(@TempDir Path dir) throws Exception {
        StoreHalf.Report report = StoreHalf.run(dir, "tcp", StoreAndGraphTest.FREE_TCP, StoreHalf.Sizes.bindings(), BUDGET,
                System.out);
        System.out.printf("store join under a %,d-byte budget (P28): %s%n", BUDGET, report.join());
        System.out.print(report.summary());
        assertThat(report.join().remainder()).as("the reader's spaces came in answers cut to the budget: " + report.join())
                .isPositive();
        StoreAndGraphTest.assertStoreHalf(report, StoreHalf.Sizes.bindings());
    }
}
