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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.agent.content.SignedPin;
import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.PublishOptions;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a store refuses (PDT 2 phase 3, P2-S-05, P2-Q-SEC-06, P2-Q-AVL-02): a requester the authorizer does not
 * grant {@code CONTENT_FETCH} on the root gets no pin and no challenge answer; a kind the store does not accept
 * stays untaken; a forged pin naming another requester is not taken; a store at capacity refuses new pins with a
 * reason and keeps serving what it holds.
 */
class StorePolicyTest {

    private final StoreFleet fleet = new StoreFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private boolean cardsSeen(StoreFleet.Peer peer, int n) throws InterruptedException {
        return fleet.runUntil(Duration.ofSeconds(20), () -> peer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).size() == n);
    }

    @Test
    void aRequesterWithoutTheGrantGetsNoPinAndNoChallengeAnswer() throws Exception {
        StoreFleet.Peer[] mallory = new StoreFleet.Peer[1];
        java.util.Set<ai.badmonkey.agentspaces.common.id.PeerId> refused = java.util.concurrent.ConcurrentHashMap.newKeySet();
        StoreFleet.Peer store = fleet.newPeer("store", 31, StoreFleet.Spec.store(StorePolicy.of(1L << 30))
                .storeBuilder(b -> b.authorizer(StoreFleet.refusing(refused)))
                .contentBuilder(b -> b.authorizer(StoreFleet.refusing(refused))));
        mallory[0] = fleet.newPeer("mallory", 32, StoreFleet.Spec.member(), "store");
        refused.add(mallory[0].id());
        StoreFleet.Peer alice = fleet.newPeer("alice", 33, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(mallory[0], 1)).isTrue();
        String root = StoreFleet.facts(mallory[0], "m", 200, 1).root();
        mallory[0].pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofHours(1));
        fleet.settle(40);
        assertThat(store.contentStore().ledger().holds(root)).as("no CONTENT_FETCH, no pin").isFalse();
        assertThat(store.contentStore().status().refused()).isZero(); // refused by the scan, never taken
        assertThat(mallory[0].contentSpace().readAll(Template.of(SignedPin.class), 8)).as("the pin stays for a store that grants it")
                .hasSize(1);

        // a forged pin: mallory signs terms that name alice as the requester; alice did not sign them (F1)
        String aliceRoot = StoreFleet.facts(alice, "a", 200, 2).root();
        alice.content().holdTree(aliceRoot);
        StorePin terms = new StorePin(StorePin.VERSION, aliceRoot, StorePin.KIND_TREE, 3600, 1, alice.id(), List.of(), null);
        SignedPin bySelf = SignedPin.sign(new StorePin(StorePin.VERSION, aliceRoot, StorePin.KIND_TREE, 3600, 1,
                mallory[0].id(), List.of(), null), fleet.groupId, fleet.clock.millis() + 600_000, mallory[0].identity());
        SignedPin forged = new SignedPin(SignedPin.VERSION, terms, 1, bySelf.notAfter(), bySelf.key(), bySelf.sig());
        mallory[0].contentSpace().write(forged, Lease.of(Duration.ofMinutes(10)), forged.tags());
        fleet.runFor(Duration.ofSeconds(3));
        assertThat(store.contentStore().ledger().holds(aliceRoot)).isFalse();
        assertThat(store.contentStore().status().unsigned()).as("the forgery is counted once").isEqualTo(1);

        // the grant gates challenges too: alice's pin is held, and mallory's challenge of it is refused
        alice.pdt().storeFor(aliceRoot, StoreChooser.Need.of(0, 1), Duration.ofHours(1));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> store.contentStore().ledger().holds(aliceRoot))).isTrue();
        StoreFleet.facts(mallory[0], "a", 200, 2); // the same value: mallory holds the nodes to pick from
        var answer = mallory[0].pdt().challenge(aliceRoot, store.id());
        assertThat(fleet.runUntil(Duration.ofSeconds(20), answer::isDone)).isTrue();
        assertThat(answer.get().passed()).isFalse();
        assertThat(answer.get().reason()).contains("REJECT reason 3");
    }

    @Test
    void aChallengeInvokeOnARootTheRequesterMayFetchOpensNoNodeOfAnotherRoot() throws Exception {
        // mallory may fetch her own tree only; the store holds hers and alice's, and her invoke names alice's nodes
        Set<String> malloryScopes = java.util.concurrent.ConcurrentHashMap.newKeySet();
        java.util.concurrent.atomic.AtomicReference<ai.badmonkey.agentspaces.common.id.PeerId> malloryId =
                new java.util.concurrent.atomic.AtomicReference<>();
        ai.badmonkey.agentspaces.api.spi.Authorizer scoped = (peer, operation, scope) -> operation
                != ai.badmonkey.agentspaces.api.spi.Authorizer.Operation.CONTENT_FETCH || !peer.equals(malloryId.get())
                || malloryScopes.contains(scope);
        StoreFleet.Peer store = fleet.newPeer("store", 34, StoreFleet.Spec.store(StorePolicy.of(1L << 30))
                .storeBuilder(b -> b.authorizer(scoped)).contentBuilder(b -> b.authorizer(scoped)));
        StoreFleet.Peer mallory = fleet.newPeer("mallory", 35, StoreFleet.Spec.member(), "store");
        malloryId.set(mallory.id());
        StoreFleet.Peer alice = fleet.newPeer("alice", 36, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(alice, 1)).isTrue();
        String malloryRoot = StoreFleet.facts(mallory, "m", 200, 3).root();
        malloryScopes.addAll(mallory.store().reachable(malloryRoot));
        String aliceRoot = StoreFleet.facts(alice, "a", 200, 4).root();
        mallory.pdt().storeFor(malloryRoot, StoreChooser.Need.of(0, 1), Duration.ofHours(1));
        alice.pdt().storeFor(aliceRoot, StoreChooser.Need.of(0, 1), Duration.ofHours(1));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> store.contentStore().ledger().holds(malloryRoot)
                && store.contentStore().ledger().holds(aliceRoot))).isTrue();

        String own = mallory.store().reachable(malloryRoot).iterator().next();
        var allowed = store.contentStore().invoke(store.contentStore().assetName(), ContentStore.OP_CHALLENGE,
                Map.of("root", malloryRoot, "picks", own), "inv-1", mallory.id());
        assertThat(allowed.ok()).as(String.valueOf(allowed)).isTrue();
        assertThat(allowed.result().get(own)).isNotEmpty();
        String foreign = alice.store().reachable(aliceRoot).stream().filter(c -> !malloryScopes.contains(c)).findFirst().orElseThrow();
        var refusedInvoke = store.contentStore().invoke(store.contentStore().assetName(), ContentStore.OP_CHALLENGE,
                Map.of("root", malloryRoot, "picks", foreign), "inv-2", mallory.id());
        assertThat(refusedInvoke.ok()).as("alice's node through mallory's root: " + refusedInvoke).isFalse();
        assertThat(refusedInvoke.error()).isEqualTo("refused-scope");
    }

    @Test
    void aKindTheStoreRefusesStaysUntakenAndAFullStoreRefusesWithAReasonAndKeepsServing() throws Exception {
        StoreFleet.Peer trees = fleet.newPeer("trees", 41, StoreFleet.Spec.store(StorePolicy.of(1L << 30)
                .acceptKinds(Set.of(StorePin.KIND_TREE)).acceptRequesters(StorePolicy.Accept.OPEN)));
        StoreFleet.Peer writer = fleet.newPeer("writer", 42, StoreFleet.Spec.member(), "trees");
        fleet.settle(6);
        assertThat(cardsSeen(writer, 1)).isTrue();
        byte[] bytes = new byte[200_000];
        new Random(4).nextBytes(bytes);
        Manifest artifact = writer.content().publish(bytes, PublishOptions.defaults().withPieceSize(65_536));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> trees.content().spaces().manifest(artifact.root()).isPresent())).isTrue();
        writer.pdt().storeFor(artifact.root(), StoreChooser.Need.of(0, 1), Duration.ofHours(1));
        fleet.settle(40);
        assertThat(trees.contentStore().ledger().holds(artifact.root())).as("an artifact pin at a trees-only store").isFalse();

        // a store with room for one small tree takes it, then refuses the next and says it is full
        StoreFleet.Peer small = fleet.newPeer("small", 43, StoreFleet.Spec.store(StorePolicy.of(40_000).reserve(0)
                .acceptRequesters(StorePolicy.Accept.OPEN)), "trees");
        assertThat(cardsSeen(writer, 2)).isTrue();
        String first = StoreFleet.facts(writer, "one", 300, 1).root();
        writer.pdt().storeFor(first, StoreChooser.Need.of(0, 1).avoid(Set.of(trees.id())), Duration.ofHours(1));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> small.contentStore().ledger().holds(first))).isTrue();
        String second = StoreFleet.facts(writer, "two", 2_000, 2).root();
        StorePin pin = new StorePin(StorePin.VERSION, second, StorePin.KIND_TREE, 3600, 1, writer.id(), List.of(),
                Map.of(ai.badmonkey.agentspaces.pdt.stores.Stores.STORES_ATTR, small.id().value()));
        writer.content().holdTree(second);
        fleet.writePin(writer, pin, Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> small.contentStore().status().full()))
                .as(small.contentStore().status().toString()).isTrue();
        assertThat(small.contentStore().ledger().holds(second)).isFalse();
        assertThat(small.contentStore().status().abandoned() + small.contentStore().status().refused()).isPositive();
        var held = writer.pdt().challenge(first, small.id());
        assertThat(fleet.runUntil(Duration.ofSeconds(20), held::isDone)).isTrue();
        assertThat(held.get().passed()).as("a full store keeps serving what it holds").isTrue();
        assertThat(small.content().pdtLevel()).isEqualTo(Content.PDT_LEVEL);
    }
}
