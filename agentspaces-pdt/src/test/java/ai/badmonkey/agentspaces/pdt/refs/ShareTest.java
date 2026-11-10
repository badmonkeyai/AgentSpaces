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
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 1, P2-X-02 (PD-R-10): a bridge that is a member of two groups keeps one node store
 * under both, shares a ref's version into the second group as a head with the same root, and its
 * second swarm serves the nodes there; over two stores a bridge that holds the tree in part fetches
 * the rest before it copies the tree; {@code Content.shareTree} holds a bare tree root in the
 * second group for a lease.
 */
class ShareTest {

    private static final String NAME = "facts";
    /** The ticks in a row over which a quiet store holds the same nodes: a second of fleet time. */
    private static final int QUIET_TICKS = 4;

    /**
     * Ticks the fleet until the bridge's store is quiet, within a bound (swarm retries, sliding window, the ShareTest
     * bridge race, ROBUSTNESS.md §10.11). The bridge fetches the writer's superseded versions too, and those fetches
     * run on after the newest root is complete, storing nodes that no later version links to. A check of what the
     * share added therefore reads the store once the superseded heads have left the bridge's space, the bridge runs
     * no tree fetch, and the store has held the same nodes over {@link #QUIET_TICKS} ticks in a row.
     */
    private static boolean storeQuiet(RefFleet fleet, RefFleet.Peer bridge, NodeStore store, String root)
            throws InterruptedException {
        java.util.concurrent.atomic.AtomicReference<java.util.Set<String>> last = new java.util.concurrent.atomic.AtomicReference<>();
        int[] same = {0};
        return fleet.runUntil(Duration.ofSeconds(20), () -> {
            java.util.Set<String> now = store.backend().cids();
            boolean idle = RefFleet.headEntries(bridge, NAME).stream().allMatch(e -> e.value().root().equals(root))
                    && bridge.content().activeTreeFetches().isEmpty();
            same[0] = idle && now.equals(last.getAndSet(now)) ? same[0] + 1 : 0;
            return same[0] >= QUIET_TICKS;
        });
    }

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
            assertThat(storeQuiet(first, bridgeInFirst, shared, root)).as("the bridge's store goes quiet").isTrue();
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

    /**
     * A bridge with two stores that follows the ref lazily holds the writer's tree in part, and the share fetches the
     * rest before it copies the tree into the second store (pre-publish 0.6.0, ROBUSTNESS.md §10.11 item 3): the
     * target's head carries no inline delta, so the bridge serves the whole tree in the second group. Before, the copy
     * threw {@code NoSuchElementException} on the first node the first store lacked.
     */
    @Test
    void aLazyBridgeWithTwoStoresFetchesTheRestOfTheTreeBeforeItShares() throws Exception {
        RefFleet first = new RefFleet();
        RefFleet second = new RefFleet();
        try {
            RefFleet.Peer writer = first.newPeer("w", 1);
            RefFleet.Peer bridgeInFirst = first.newPeer("b", 2, RefFleet.Spec.defaults().pdt(p -> p.follow(Pdt.FOLLOW_LAZY)), "w");
            RefFleet.Peer bridgeInSecond = second.newPeer("b", 3);
            RefFleet.Peer reader = second.newPeer("r", 4, "b");
            first.settle(12);
            second.settle(12);
            PMap<String, String> written = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).swap(m -> {
                Random random = new Random(3);
                Editor<String, String> editor = m.editor();
                for (int i = 0; i < 2_000; i++) {
                    editor.assoc(RefFleet.key(i), RefFleet.value(100, random));
                }
                return editor.done().value();
            });
            String root = written.root();
            Ref<PMap<String, String>> onBridge = bridgeInFirst.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(first.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(bridgeInFirst, NAME).size() == 1)).isTrue();
            CompletableFuture<Optional<String>> read = CompletableFuture.supplyAsync(
                    () -> onBridge.deref().get(RefFleet.key(1234)), r -> Thread.ofVirtual().start(r));
            assertThat(first.runUntil(Duration.ofSeconds(30), read::isDone)).isTrue();
            assertThat(read.get()).isEqualTo(written.get(RefFleet.key(1234)));
            assertThat(bridgeInFirst.store().has(root)).isTrue();
            assertThat(bridgeInFirst.store().complete(root)).as("the lazy bridge holds the tree in part").isFalse();

            CompletableFuture<Ref<PMap<String, String>>> share = CompletableFuture.supplyAsync(
                    () -> bridgeInFirst.pdt().share(onBridge, bridgeInSecond.pdt()), r -> Thread.ofVirtual().start(r));
            assertThat(first.runUntil(Duration.ofSeconds(30), () -> {
                second.tickAll();
                return share.isDone();
            })).isTrue();
            Ref<PMap<String, String>> inSecond = share.get();

            assertThat(inSecond.heads().roots()).containsExactly(root);
            assertThat(bridgeInSecond.store().complete(root)).as("the second store holds the whole tree").isTrue();
            Ref<PMap<String, String>> onReader = reader.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(second.runUntil(Duration.ofSeconds(20), () -> onReader.heads().roots().equals(java.util.List.of(root))
                    && reader.store().complete(root))).as("the second group's reader fetches from the bridge").isTrue();
            assertThat(onReader.deref().toMap()).isEqualTo(written.toMap());
        } finally {
            first.close();
            second.close();
        }
    }

    /**
     * When nobody serves the rest of a tree the lazy bridge holds in part, the share over two stores refuses within
     * the load timeout with {@link NodeUnavailableException}, names the root, and writes no head in the second group
     * (pre-publish 0.6.0, ROBUSTNESS.md §10.11 item 3). Before, it threw {@code NoSuchElementException}.
     */
    @Test
    void aLazyBridgeWhoseWriterIsGoneRefusesTheShareByNamingTheRoot() throws Exception {
        Duration loadTimeout = Duration.ofSeconds(2);
        RefFleet first = new RefFleet();
        RefFleet second = new RefFleet();
        try {
            RefFleet.Peer writer = first.newPeer("w", 1);
            RefFleet.Peer bridgeInFirst = first.newPeer("b", 2,
                    RefFleet.Spec.defaults().pdt(p -> p.follow(Pdt.FOLLOW_LAZY).loadTimeout(loadTimeout)), "w");
            RefFleet.Peer bridgeInSecond = second.newPeer("b", 3);
            first.settle(12);
            second.settle(12);
            PMap<String, String> written = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).swap(m -> {
                Random random = new Random(5);
                Editor<String, String> editor = m.editor();
                for (int i = 0; i < 2_000; i++) {
                    editor.assoc(RefFleet.key(i), RefFleet.value(100, random));
                }
                return editor.done().value();
            });
            String root = written.root();
            Ref<PMap<String, String>> onBridge = bridgeInFirst.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(first.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(bridgeInFirst, NAME).size() == 1)).isTrue();
            CompletableFuture<Optional<String>> read = CompletableFuture.supplyAsync(
                    () -> onBridge.deref().get(RefFleet.key(7)), r -> Thread.ofVirtual().start(r));
            assertThat(first.runUntil(Duration.ofSeconds(30), read::isDone)).isTrue();
            assertThat(bridgeInFirst.store().complete(root)).as("the lazy bridge holds the tree in part").isFalse();
            first.kill(writer);

            long started = System.nanoTime();
            CompletableFuture<Ref<PMap<String, String>>> share = CompletableFuture.supplyAsync(
                    () -> bridgeInFirst.pdt().share(onBridge, bridgeInSecond.pdt()), r -> Thread.ofVirtual().start(r));
            assertThat(first.runUntil(Duration.ofSeconds(30), share::isDone)).isTrue();
            long waited = (System.nanoTime() - started) / 1_000_000;

            assertThatThrownBy(share::get).hasCauseInstanceOf(NodeUnavailableException.class)
                    .hasMessageContaining(root);
            assertThat(waited).as("the refusal comes within about one load timeout").isLessThan(loadTimeout.toMillis() * 3);
            assertThat(bridgeInSecond.pdt().refs().heads(NAME).isEmpty()).as("no head in the second group").isTrue();
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
