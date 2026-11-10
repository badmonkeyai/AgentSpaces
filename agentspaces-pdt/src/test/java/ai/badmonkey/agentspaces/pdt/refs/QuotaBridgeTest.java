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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.StoreRefusedException;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.store.QuotaExceededException;
import ai.badmonkey.agentspaces.pdt.swarm.NodeStoreTreeStore;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code agentspaces.pdt.store.quota} behind the bridge (PDT 2 phase 7, P2-X-08, PD-S-05): the tree store a facade
 * hands the content swarm admits every received node through a {@code QuotaNodeStore}, so a fetch that would carry
 * the store past its quota fails with the refusal, and the store keeps what it held.
 */
class QuotaBridgeTest {

    private static final long QUOTA = 4096;

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @Test
    void theBridgeAdmitsReceivedNodesThroughTheQuota() {
        NodeStore store = new NodeStore(new MemoryBlockStore(), fleet.clock);
        NodeStoreTreeStore bridge = new NodeStoreTreeStore(store, QUOTA);
        byte[] small = new byte[1000];
        byte[] large = new byte[4000];
        String smallCid = ai.badmonkey.agentspaces.content.tree.Cids.of(small, ai.badmonkey.agentspaces.content.tree.Cids.RAW);
        String largeCid = ai.badmonkey.agentspaces.content.tree.Cids.of(large, ai.badmonkey.agentspaces.content.tree.Cids.RAW);

        bridge.putVerified(smallCid, small);
        bridge.putVerified(smallCid, small);

        assertThatThrownBy(() -> bridge.putVerified(largeCid, large))
                .isInstanceOf(QuotaExceededException.class)
                .isInstanceOf(StoreRefusedException.class)
                .satisfies(e -> assertThat(((StoreRefusedException) e).reason()).isEqualTo(Messages.REASON_OVER_BUDGET));
        assertThat(store.has(largeCid)).isFalse();
        assertThat(store.bytesUsed()).isEqualTo(1000);
    }

    @Test
    void aFetchPastTheReadersQuotaFailsWithTheRefusalInsteadOfWaiting() throws Exception {
        RefFleet.Peer writer = fleet.newPeer("a", 1);
        RefFleet.Peer reader = fleet.newPeer("b", 2, RefFleet.Spec.defaults().pdt(p -> p.quota(QUOTA)), "a");
        fleet.settle(6);
        PMap<String, String> value = writer.pdt().mapRef("facts", RefFleet.STR, RefFleet.STR).swap(m -> {
            Random random = new Random(5);
            Editor<String, String> editor = m.editor();
            for (int i = 0; i < 500; i++) {
                editor.assoc(RefFleet.key(i), RefFleet.value(100, random));
            }
            return editor.done().value();
        });
        writer.pdt().swarm().hold(value.root());
        fleet.settle(6);

        TreeFetch fetch = reader.pdt().swarm().fetchTree(value.root());

        assertThat(fleet.runUntil(Duration.ofSeconds(20), fetch::isDoneOrFailed)).as("the fetch ends").isTrue();
        assertThat(fetch.complete()).isFalse();
        assertThat(fetch.failure()).containsInstanceOf(QuotaExceededException.class);
        assertThat(reader.store().bytesUsed()).as("the store stays within its quota").isLessThanOrEqualTo(QUOTA);
    }
}
