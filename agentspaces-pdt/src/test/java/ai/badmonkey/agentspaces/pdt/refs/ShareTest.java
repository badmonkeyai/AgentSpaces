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

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 1, P2-X-02 (PD-R-10): a bridge that is a member of two groups keeps one node store
 * under both, shares a ref's version into the second group as a head with the same root, and its
 * second swarm serves the nodes there; {@code Content.shareTree} holds a bare tree root in the
 * second group for a lease.
 */
class ShareTest {

    private static final String NAME = "facts";

    @Test
    void aBridgeSharesARefIntoASecondGroupWithTheSameRoot() throws Exception {
        RefFleet first = new RefFleet();
        RefFleet second = new RefFleet();
        try {
            NodeStore shared = new NodeStore(new MemoryBlockStore(), first.clock);
            RefFleet.Peer writer = first.newPeer("w", 1);
            RefFleet.Peer bridgeInFirst = first.newPeer("b", 2, RefFleet.Spec.defaults().store(shared), "w");
            RefFleet.Peer bridgeInSecond = second.newPeer("b", 3, RefFleet.Spec.defaults().store(shared));
            RefFleet.Peer reader = second.newPeer("r", 4, "b");
            first.settle(12);
            second.settle(12);
            Ref<PMap<String, String>> facts = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            for (int i = 0; i < 60; i++) {
                int n = i;
                facts.swap(m -> m.assoc("k" + n, "value-" + n));
            }
            String root = facts.deref().root();
            Ref<PMap<String, String>> onBridge = bridgeInFirst.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(first.runUntil(Duration.ofSeconds(20), () -> onBridge.heads().roots().contains(root)
                    && shared.complete(root))).isTrue();
            java.util.Set<String> before = shared.backend().cids();

            Ref<PMap<String, String>> inSecond = bridgeInFirst.pdt().share(onBridge, bridgeInSecond.pdt());
            assertThat(inSecond.heads().roots()).containsExactly(root);
            java.util.Set<String> added = new java.util.HashSet<>(shared.backend().cids());
            added.removeAll(before);
            assertThat(added).as("one store under both groups: no value node moved, only the new head's vg node")
                    .allMatch(cid -> ai.badmonkey.agentspaces.pdt.kinds.VgNode.decode(cid, shared.get(cid).orElseThrow()) != null);
            assertThat(RefFleet.headEntries(bridgeInSecond, NAME).get(0).value().nodes()).as("no inline delta").isNull();
            Ref<PMap<String, String>> onReader = reader.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(second.runUntil(Duration.ofSeconds(20), () -> onReader.heads().roots().equals(java.util.List.of(root))
                    && reader.store().complete(root))).as("the second group's reader fetches from the bridge").isTrue();
            assertThat(onReader.deref().toMap()).isEqualTo(facts.deref().toMap());
            assertThat(writer.pdt().swarm().content().spaces().holdings(root)).as("the second group's holding stays there")
                    .noneMatch(h -> h.holder().equals(bridgeInSecond.id()));
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void shareTreeHoldsABareRootForItsLease() throws Exception {
        RefFleet first = new RefFleet();
        RefFleet second = new RefFleet();
        try {
            NodeStore shared = new NodeStore(new MemoryBlockStore(), first.clock);
            RefFleet.Peer bridgeInFirst = first.newPeer("b", 1, RefFleet.Spec.defaults().store(shared));
            RefFleet.Peer bridgeInSecond = second.newPeer("b", 2, RefFleet.Spec.defaults().store(shared));
            RefFleet.Peer reader = second.newPeer("r", 3, "b");
            first.settle(4);
            second.settle(12);
            Ref<PMap<String, String>> local = bridgeInFirst.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            String root = local.swap(m -> m.assoc("a", "1").assoc("b", "2")).root();
            assertThatThrownBy(() -> bridgeInSecond.content().shareTree(
                    ai.badmonkey.agentspaces.content.tree.Cids.of(new byte[]{9}, ai.badmonkey.agentspaces.content.tree.Cids.DAG_CBOR),
                    Lease.of(Duration.ofMinutes(1)))).isInstanceOf(IllegalStateException.class).hasMessageContaining("not complete");
            Holding holding = bridgeInSecond.content().shareTree(root, Lease.of(Duration.ofSeconds(30)));
            assertThat(holding.complete()).isTrue();
            assertThat(holding.count()).isEqualTo(shared.reachable(root).size());
            assertThat(second.runUntil(Duration.ofSeconds(10), () -> reader.content().spaces().holdings(root).stream()
                    .anyMatch(h -> h.holder().equals(bridgeInSecond.id()) && h.complete()))).isTrue();
            var fetch = reader.pdt().swarm().fetchTree(root);
            assertThat(second.runUntil(Duration.ofSeconds(10), fetch::complete)).isTrue();
            assertThat(reader.store().complete(root)).isTrue();
            second.clock.advance(Duration.ofSeconds(31));
            assertThat(second.runUntil(Duration.ofSeconds(10), () -> reader.content().spaces().holdings(root).stream()
                    .noneMatch(h -> h.holder().equals(bridgeInSecond.id())))).as("the holding lapses with its lease").isTrue();
        } finally {
            first.close();
            second.close();
        }
    }
}
