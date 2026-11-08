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

import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link QuotaNodeStore} (PD-S-05): refuses past the quota with the over-budget reason, warns, evicts nothing. */
class QuotaTest {

    private static final Random RANDOM = new Random(42);

    @Test
    void unboundedAdmitsEverything() {
        NodeStore store = NodeStore.inMemory();
        QuotaNodeStore quota = QuotaNodeStore.unbounded(store);
        assertThat(quota.quota()).isEqualTo(QuotaNodeStore.UNBOUNDED);
        assertThat(quota.headroom()).isEqualTo(Long.MAX_VALUE);
        for (int i = 0; i < 100; i++) {
            quota.put(TreeFixtures.chunk(RANDOM, 1000), Cids.RAW);
        }
        assertThat(store.size()).isEqualTo(100);
        assertThat(quota.store()).isSameAs(store);
    }

    @Test
    void refusesANewNodePastTheQuotaWithTheOverBudgetReasonAndWarns() {
        NodeStore store = NodeStore.inMemory();
        List<QuotaExceededException> warnings = new ArrayList<>();
        QuotaNodeStore quota = new QuotaNodeStore(store, 2500, warnings::add);
        quota.put(TreeFixtures.chunk(RANDOM, 1000), Cids.RAW);
        quota.put(TreeFixtures.chunk(RANDOM, 1000), Cids.RAW);
        assertThat(quota.headroom()).isEqualTo(500);
        byte[] big = TreeFixtures.chunk(RANDOM, 600);
        String bigCid = Cids.of(big, Cids.RAW);
        assertThatThrownBy(() -> quota.put(big, Cids.RAW))
                .isInstanceOf(QuotaExceededException.class)
                .hasMessageContaining(bigCid)
                .hasMessageContaining("2500");
        assertThat(warnings).hasSize(1);
        QuotaExceededException refusal = warnings.get(0);
        assertThat(refusal.reason()).isEqualTo(Messages.REASON_OVER_BUDGET);
        assertThat(refusal.cid()).isEqualTo(bigCid);
        assertThat(refusal.requested()).isEqualTo(600);
        assertThat(refusal.bytesUsed()).isEqualTo(2000);
        assertThat(refusal.quota()).isEqualTo(2500);
        assertThat(store.has(bigCid)).isFalse();
        assertThat(store.size()).as("nothing evicted").isEqualTo(2);
        quota.put(TreeFixtures.chunk(RANDOM, 500), Cids.RAW);
        assertThat(quota.headroom()).isZero();
        assertThatThrownBy(() -> quota.putVerified(bigCid, big)).isInstanceOf(QuotaExceededException.class);
        assertThat(warnings).hasSize(2);
    }

    @Test
    void aHeldNodeAndAPinnedWriteAreAdmittedPastTheQuota() {
        NodeStore store = NodeStore.inMemory();
        QuotaNodeStore quota = new QuotaNodeStore(store, 1000, refusal -> { });
        byte[] held = TreeFixtures.chunk(RANDOM, 900);
        String heldCid = quota.put(held, Cids.RAW);
        assertThat(quota.put(held, Cids.RAW)).as("already held costs nothing").isEqualTo(heldCid);
        quota.putVerified(heldCid, held);
        byte[] pinned = TreeFixtures.chunk(RANDOM, 900);
        String pinnedCid = Cids.of(pinned, Cids.RAW);
        assertThatThrownBy(() -> quota.putVerified(pinnedCid, pinned)).isInstanceOf(QuotaExceededException.class);
        quota.putVerified(pinnedCid, pinned, true);
        assertThat(store.has(pinnedCid)).isTrue();
        assertThat(quota.put(TreeFixtures.chunk(RANDOM, 10), Cids.RAW, true)).isNotNull();
        assertThat(store.size()).isEqualTo(3);
        assertThat(quota.headroom()).isZero();
    }

    @Test
    void aBatchIsJudgedWholeAndStoresNothingWhenRefused() {
        NodeStore store = NodeStore.inMemory();
        QuotaNodeStore quota = new QuotaNodeStore(store, 1000, refusal -> { });
        byte[] already = TreeFixtures.chunk(RANDOM, 400);
        String alreadyCid = quota.put(already, Cids.RAW);
        Map<String, byte[]> batch = new LinkedHashMap<>();
        batch.put(alreadyCid, already);
        byte[] a = TreeFixtures.chunk(RANDOM, 300);
        byte[] b = TreeFixtures.chunk(RANDOM, 400);
        batch.put(Cids.of(a, Cids.RAW), a);
        batch.put(Cids.of(b, Cids.RAW), b);
        assertThatThrownBy(() -> quota.putAll(batch, false))
                .isInstanceOf(QuotaExceededException.class)
                .satisfies(e -> assertThat(((QuotaExceededException) e).requested()).isEqualTo(700));
        assertThat(store.size()).isEqualTo(1);
        batch.remove(Cids.of(b, Cids.RAW));
        quota.putAll(batch, false);
        assertThat(store.size()).isEqualTo(2);
        quota.putAll(Map.of(Cids.of(b, Cids.RAW), b), true);
        assertThat(store.size()).isEqualTo(3);
    }

    @Test
    void loweringTheQuotaBelowUseEvictsNothingReachable() {
        NodeStore store = NodeStore.inMemory();
        QuotaNodeStore quota = QuotaNodeStore.unbounded(store);
        String root = TreeFixtures.putTree(store, RANDOM, 20, 5, 100);
        store.pin(root);
        Set<String> reachable = store.reachable(root);
        quota.quota(100);
        assertThat(quota.headroom()).isZero();
        assertThatThrownBy(() -> quota.put(TreeFixtures.chunk(RANDOM, 1), Cids.RAW)).isInstanceOf(QuotaExceededException.class);
        assertThat(store.complete(root)).isTrue();
        assertThat(store.backend().cids()).containsAll(reachable);
        NodeCollector collector = new NodeCollector(store, Set::of, null);
        collector.collect();
        assertThat(store.complete(root)).as("collection keeps the pinned tree too").isTrue();
        assertThatThrownBy(() -> quota.quota(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
