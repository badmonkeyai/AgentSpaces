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

import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.pdt.stores.Stores;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Store reconciliation (PDT 2 phase 5, P2-K-03, P2-Q-REL-06; S4 of §6.4): two stores that hold the same pins
 * reconcile their held roots by negentropy over the content swarm, and the one that lost nodes since (a truncated
 * pack) fetches its pinned roots again from the other, so the two end with equal held sets. A root the other store
 * holds for a pin this store never took stays there, since no requester signed for it here (F1). The store's tick
 * reconciles with one other store per {@code reconcile-every}.
 */
class ReconcileTest {

    private final StoreFleet fleet = new StoreFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private <T> T await(CompletableFuture<T> future) throws Exception {
        assertThat(fleet.runUntil(Duration.ofSeconds(30), future::isDone)).as("the reconcile settles").isTrue();
        return future.get();
    }

    private record Fleet(StoreFleet.Peer a, StoreFleet.Peer b, StoreFleet.Peer writer, List<String> shared, String onlyB) {
    }

    /** Stores a and b both hold three roots for the writer, and b alone a fourth. */
    private Fleet twoStoresHoldingTheSamePins(StoreFleet.Spec storeSpec) throws Exception {
        StoreFleet.Peer a = fleet.newPeer("store-a", 91, storeSpec);
        StoreFleet.Peer b = fleet.newPeer("store-b", 92, storeSpec, "store-a");
        StoreFleet.Peer writer = fleet.newPeer("writer", 93, StoreFleet.Spec.member(), "store-a");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).size() == 2)).isTrue();
        List<String> shared = List.of(StoreFleet.facts(writer, "one", 300, 21).root(),
                StoreFleet.facts(writer, "two", 300, 22).root(), StoreFleet.facts(writer, "three", 300, 23).root());
        for (String root : shared) {
            writer.pdt().storeFor(root, StoreChooser.Need.of(0, 2), Duration.ofDays(1));
        }
        String onlyB = StoreFleet.facts(writer, "four", 300, 24).root();
        writer.content().holdTree(onlyB);
        fleet.writePin(writer, new StorePin(StorePin.VERSION, onlyB, StorePin.KIND_TREE, 86400, 1, writer.id(), List.of(),
                Map.of(Stores.STORES_ATTR, b.id().value())), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> shared.stream().allMatch(r -> StoreFleet.receiptHolders(writer, r).size() == 2)
                && StoreFleet.receiptHolders(writer, onlyB).size() == 1)).isTrue();
        return new Fleet(a, b, writer, shared, onlyB);
    }

    /**
     * Store a loses the nodes below the root that two of the shared roots hold alone, as a truncated pack would, and
     * its holdings show it at the next walk: both at once when {@code atOnce}, else each at some walk, since the
     * tick's own reconcile may repair the first root before the second one's walk comes round.
     */
    private void truncate(Fleet f, boolean atOnce) throws Exception {
        Set<String> keep = new HashSet<>(f.a().store().reachable(f.shared().get(2)));
        for (String root : f.shared().subList(0, 2)) {
            for (String cid : f.a().store().reachable(root)) {
                if (!keep.contains(cid) && !cid.equals(root)) {
                    f.a().store().backend().remove(cid);
                }
            }
        }
        Set<String> shown = new HashSet<>();
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> {
            Set<String> held = f.a().content().heldRoots();
            List<String> lost = f.shared().subList(0, 2).stream().filter(r -> !held.contains(r)).toList();
            shown.addAll(lost);
            return atOnce ? lost.size() == 2 : shown.size() == 2;
        })).as("a's holdings show the loss").isTrue();
    }

    @Test
    void twoStoresReconcileToEqualHeldSetsAndTheOneThatLostNodesFetchesItsPinnedRootsAgain() throws Exception {
        Fleet f = twoStoresHoldingTheSamePins(StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        truncate(f, true);

        ContentStore.Reconciled outcome = await(f.a().contentStore().reconcile(f.b().id()));
        assertThat(outcome.store()).isEqualTo(f.b().id());
        assertThat(outcome.repairing()).containsExactlyInAnyOrderElementsOf(f.shared().subList(0, 2));
        assertThat(outcome.unpinned()).as("b's own pin is no requester's pin here (F1)").isEqualTo(1);
        assertThat(outcome.need()).isEqualTo(3);
        assertThat(outcome.have()).isZero();

        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> f.shared().stream().allMatch(r -> f.a().store().complete(r))))
                .as("a fetches its pinned roots again").isTrue();
        for (String root : f.shared()) {
            assertThat(f.a().store().reachable(root)).as("P2-Q-REL-06: equal held sets").isEqualTo(f.b().store().reachable(root));
        }
        assertThat(f.a().store().has(f.onlyB())).as("a takes no root that no requester pinned on it").isFalse();
        assertThat(f.a().contentStore().ledger().holds(f.onlyB())).isFalse();
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> f.a().content().heldRoots().containsAll(f.shared()))).isTrue();
        ContentStore.Reconciled again = await(f.a().contentStore().reconcile(f.b().id()));
        assertThat(again.repairing()).isEmpty();
        assertThat(again.need()).as("only b's own pin differs now").isEqualTo(1);
    }

    /**
     * PDT 2 phase 5 gate, P2-Q-REL-06: a store that lost a pinned root's own node, as a pack cut before its last
     * record would, no longer holds the root. Its last holding said complete, and {@code Content.heldRoots} read that
     * holding, so the store went on counting the root as held and a reconcile never fetched it again.
     */
    @Test
    void aStoreThatLostAPinnedRootsOwnNodeFetchesTheRootAgain() throws Exception {
        Fleet f = twoStoresHoldingTheSamePins(StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        String lost = f.shared().get(0);
        Set<String> keep = new HashSet<>(f.a().store().reachable(f.shared().get(1)));
        keep.addAll(f.a().store().reachable(f.shared().get(2)));
        for (String cid : f.a().store().reachable(lost)) {
            if (!keep.contains(cid)) {
                f.a().store().backend().remove(cid);
            }
        }
        assertThat(f.a().store().has(lost)).as("the root's own node is gone").isFalse();
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> !f.a().content().heldRoots().contains(lost)))
                .as("a's held roots show the loss of the root").isTrue();

        ContentStore.Reconciled outcome = await(f.a().contentStore().reconcile(f.b().id()));
        assertThat(outcome.repairing()).containsExactly(lost);
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> f.a().store().complete(lost)))
                .as("a fetches the root again").isTrue();
        assertThat(f.a().store().reachable(lost)).as("P2-Q-REL-06: equal held sets").isEqualTo(f.b().store().reachable(lost));
    }

    @Test
    void theTickReconcilesWithOneOtherStoreEveryInterval() throws Exception {
        Fleet f = twoStoresHoldingTheSamePins(StoreFleet.Spec.store(StorePolicy.of(1L << 30))
                .storeBuilder(b -> b.reconcileEvery(Duration.ofSeconds(20))));
        truncate(f, false);

        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> f.shared().stream().allMatch(r -> f.a().store().complete(r))))
                .as("the tick reconciles and repairs without a call").isTrue();
        assertThat(f.a().contentStore().lastReconcile()).hasValueSatisfying(r -> assertThat(r.store()).isEqualTo(f.b().id()));
        assertThat(f.b().contentStore().lastReconcile()).hasValueSatisfying(r -> assertThat(r.store()).isEqualTo(f.a().id()));
    }

    /**
     * PDT 2 phase 5 adversary, P2-K-03: the tick's turn passed on only after a reconcile that succeeded, so a member
     * that publishes a store card and never answers (here its answers to store a are lost) held store a on itself
     * for every interval, and a reconciled with no other store again. The turn now passes on whatever the outcome.
     */
    @Test
    void aStoreThatNeverAnswersHoldsNoOtherStoreBackFromTheTicksTurn() throws Exception {
        StoreFleet.Spec spec = StoreFleet.Spec.store(StorePolicy.of(1L << 30)).storeBuilder(b -> b.reconcileEvery(Duration.ofSeconds(20)));
        StoreFleet.Peer a = fleet.newPeer("store-a", 94, spec);
        StoreFleet.Peer b = fleet.newPeer("store-b", 95, spec, "store-a");
        fleet.newPeer("store-d", 96, spec, "store-a");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> a.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).size() == 3)).isTrue();
        fleet.network.oneWay("store-d", "store-a");

        Set<ContentStore.Reconciled> done = Collections.newSetFromMap(new IdentityHashMap<>());
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> {
            a.contentStore().lastReconcile().ifPresent(done::add);
            return done.size() >= 2;
        })).as("store a reconciles with store b turn after turn").isTrue();
        assertThat(done).allSatisfy(r -> assertThat(r.store()).isEqualTo(b.id()));
    }
}
