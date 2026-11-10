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

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.pdt.store.MetaStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The store's policy and ledger (PDT 2 phase 3, P2-S-05, P2-S-06; srad/agentspaces-pdt2.md §3.2.3, appendix C):
 * sizes in binary units, the defaults of appendix C, refusals by name, bytes and roots per requester and per
 * group with shared roots charged in full and reported as dedup, and a ledger that survives a restart over a
 * meta store.
 */
class StoreLedgerTest {

    private static final PeerId ALICE = PeerId.of("zAlice");
    private static final PeerId BOB = PeerId.of("zBob");

    private static final class Meta implements MetaStore {
        final Map<String, byte[]> records = new HashMap<>();

        @Override
        public Optional<byte[]> meta(String name) {
            return Optional.ofNullable(records.get(name));
        }

        @Override
        public void putMeta(String name, byte[] value) {
            records.put(name, value);
        }

        @Override
        public Set<String> metaNames() {
            return records.keySet();
        }
    }

    @Test
    void thePolicyReadsBinarySizesAndTheAppendixDefaults() {
        assertThat(StorePolicy.bytes("2TiB")).isEqualTo(2L << 40);
        assertThat(StorePolicy.bytes("50GiB")).isEqualTo(50L << 30);
        assertThat(StorePolicy.bytes("64 GB")).isEqualTo(64L << 30);
        assertThat(StorePolicy.bytes("1024")).isEqualTo(1024);
        assertThat(StorePolicy.reserveOf("5%", 2000)).isEqualTo(100);
        assertThat(StorePolicy.reserveOf("1KiB", 2000)).isEqualTo(1024);
        assertThatThrownBy(() -> StorePolicy.bytes("lots")).hasMessageContaining("lots");

        StorePolicy policy = StorePolicy.of(1L << 30);
        assertThat(policy.reserveBytes()).isEqualTo((1L << 30) / 20);
        assertThat(policy.maxObjectBytes()).isEqualTo(64L << 30);
        assertThat(policy.leaseDefault()).isEqualTo(Duration.ofDays(30));
        assertThat(policy.leaseMax()).isEqualTo(Duration.ofDays(365));
        assertThat(policy.copiesMax()).isEqualTo(3);
        assertThat(policy.acceptKinds()).containsExactlyInAnyOrder("tree", "artifact");
        assertThat(policy.acceptRequesters()).isEqualTo(StorePolicy.Accept.GRANT);
        assertThat(policy.blind()).isFalse();
        assertThat(policy.tier()).isEqualTo("hot");
        assertThat(policy.usableBytes()).isEqualTo((1L << 30) - (1L << 30) / 20);
    }

    @Test
    void aPolicyOutOfRangeIsRefusedByName() {
        assertThatThrownBy(() -> StorePolicy.of(0)).hasMessageContaining("capacity");
        assertThatThrownBy(() -> StorePolicy.of(100).reserve(100)).hasMessageContaining("reserve");
        assertThatThrownBy(() -> StorePolicy.of(100).leases(Duration.ofDays(2), Duration.ofDays(1))).hasMessageContaining("lease");
        assertThatThrownBy(() -> StorePolicy.of(100).copiesMax(0)).hasMessageContaining("copies");
        assertThatThrownBy(() -> StorePolicy.of(100).acceptKinds(Set.of("blob"))).hasMessageContaining("kinds");
        assertThatThrownBy(() -> StorePolicy.of(100).tier("warm")).hasMessageContaining("tier");
    }

    @Test
    void theLedgerCountsPerRequesterAndGroupAndChargesSharedRootsInFull() {
        StoreLedger ledger = new StoreLedger("zGroup", null, "site-a");
        ledger.hold(new StoreLedger.Held("bafyA", "tree", 1000, ALICE, 5_000, 2));
        ledger.hold(new StoreLedger.Held("bafyA", "tree", 1000, BOB, 9_000, 1));
        ledger.hold(new StoreLedger.Held("bafkB", "artifact", 300, BOB, 7_000, 1));
        assertThat(ledger.usedBytes()).as("a root shared by two requesters is held once").isEqualTo(1300);
        assertThat(ledger.dedupBytes()).isEqualTo(1000);
        assertThat(ledger.byRequester()).containsEntry(ALICE, new StoreLedger.Totals(1000, 1))
                .containsEntry(BOB, new StoreLedger.Totals(1300, 2));
        assertThat(ledger.byGroup()).containsEntry("zGroup", new StoreLedger.Totals(1300, 2));
        assertThat(ledger.lapsed(6_000)).extracting(StoreLedger.Held::requester).containsExactly(ALICE);
        assertThat(ledger.release("bafyA", ALICE)).isPresent();
        assertThat(ledger.holds("bafyA")).as("bob still holds it").isTrue();
        assertThat(ledger.release("bafyA", BOB)).isPresent();
        assertThat(ledger.holds("bafyA")).isFalse();
    }

    @Test
    void theLedgerSurvivesARestartOverAMetaStore() {
        Meta meta = new Meta();
        StoreLedger first = new StoreLedger("zGroup", meta, "site-a");
        first.hold(new StoreLedger.Held("bafyA", "tree", 1000, ALICE, 5_000, 2));
        StoreLedger restarted = new StoreLedger("zGroup", meta, "site-a");
        assertThat(restarted.holding("bafyA", ALICE)).contains(new StoreLedger.Held("bafyA", "tree", 1000, ALICE, 5_000, 2));
        assertThat(new StoreLedger("zGroup", meta, "site-b").holdings()).as("each store name keeps its own record").isEmpty();
    }
}
