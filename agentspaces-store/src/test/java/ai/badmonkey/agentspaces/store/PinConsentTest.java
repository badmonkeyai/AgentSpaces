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

import ai.badmonkey.agentspaces.agent.content.SignedPin;
import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.agent.content.StoreReceipt;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.pdt.stores.Stores;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 follow-ups, 2026-11-09, F1 (pin consent; adversary finding 7 of srad/agentspaces-pdt2.md §10.8): a store
 * charges only the member who signed the pin. A forged receipt gives its writer no right to forward a pin for
 * another member, a pin whose requester did not sign it is refused, counted once, and strikes nobody, a signed
 * pin is taken whoever relays it, and a signature past its deadline buys nothing.
 */
class PinConsentTest {

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
    void aForgedReceiptAndAForwardedPinNamingAnotherRequesterChargeNobody() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 71, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer alice = fleet.newPeer("alice", 72, StoreFleet.Spec.member(), "store");
        StoreFleet.Peer mallory = fleet.newPeer("mallory", 73, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(mallory, 1)).isTrue();
        String root = StoreFleet.facts(alice, "alice", 300, 5).root();
        alice.content().holdTree(root);

        // finding 7: mallory claims to hold alice's root, then forwards "a copy" of a pin that names alice
        StoreReceipt claim = new StoreReceipt(StoreReceipt.VERSION, root, mallory.id(), 1, fleet.clock.millis() + 3_600_000, false);
        mallory.contentSpace().write(claim, Lease.of(Duration.ofHours(1)), claim.tags());
        StorePin terms = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 86400, 2, alice.id(), List.of(),
                Map.of(Stores.STORES_ATTR, store.id().value()));
        SignedPin mallorys = SignedPin.sign(new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 86400, 2, mallory.id(),
                List.of(), terms.attrs()), fleet.groupId, fleet.clock.millis() + 600_000, mallory.identity());
        SignedPin forwarded = new SignedPin(SignedPin.VERSION, terms, 1, mallorys.notAfter(), mallorys.key(), mallorys.sig());
        mallory.contentSpace().write(forwarded, Lease.of(Duration.ofMinutes(10)), forwarded.tags());
        fleet.runFor(Duration.ofSeconds(3));

        assertThat(store.contentStore().ledger().holds(root)).as("alice signed nothing").isFalse();
        assertThat(store.contentStore().status().byRequester()).doesNotContainKey(alice.id());
        assertThat(store.contentStore().status().unsigned()).as("refused once, over many scans").isEqualTo(1);
        assertThat(store.contentStore().status().taken()).isZero();
        assertThat(mallory.contentSpace().readAll(Template.of(SignedPin.class), 8)).as("the forgery stays untaken").hasSize(1);
        assertThat(store.content().stats().piecesStruck()).as("the store strikes nobody").isZero();
    }

    @Test
    void aSignedPinIsTakenWhoeverRelaysItAndChargedToItsSigner() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 74, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer alice = fleet.newPeer("alice", 75, StoreFleet.Spec.member(), "store");
        StoreFleet.Peer relay = fleet.newPeer("relay", 76, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(relay, 1)).isTrue();
        String root = StoreFleet.facts(alice, "relayed", 300, 6).root();
        alice.content().holdTree(root);
        StorePin terms = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 86400, 1, alice.id(), List.of(), null);
        SignedPin signed = SignedPin.sign(terms, fleet.groupId, fleet.clock.millis() + 600_000, alice.identity());
        relay.contentSpace().write(signed, Lease.of(Duration.ofMinutes(10)), signed.tags());

        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> store.contentStore().ledger().holds(root))).isTrue();
        assertThat(store.contentStore().ledger().holding(root, alice.id())).as("charged to alice, who signed").isPresent();
        assertThat(store.contentStore().status().byRequester()).containsOnlyKeys(alice.id());
        assertThat(store.contentStore().status().unsigned()).isZero();
    }

    @Test
    void aPinPastItsSignaturesDeadlineIsNotTaken() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 77, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer alice = fleet.newPeer("alice", 78, StoreFleet.Spec.member(), "store");
        StoreFleet.Peer mallory = fleet.newPeer("mallory", 79, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(mallory, 1)).isTrue();
        String root = StoreFleet.facts(alice, "lapsed", 300, 7).root();
        alice.content().holdTree(root);
        // alice signed for one minute; mallory replays the pin an hour later under a fresh entry lease
        StorePin terms = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 86400, 1, alice.id(), List.of(), null);
        SignedPin signed = SignedPin.sign(terms, fleet.groupId, fleet.clock.millis() + 60_000, alice.identity());
        fleet.clock.advance(Duration.ofHours(1));
        mallory.contentSpace().write(signed, Lease.of(Duration.ofMinutes(10)), signed.tags());
        fleet.runFor(Duration.ofSeconds(3));

        assertThat(store.contentStore().ledger().holds(root)).isFalse();
        assertThat(store.contentStore().status().unsigned()).as("alice signed it; it lapsed").isZero();
    }

    @Test
    void aSignedDeadlineBeyondTheEntryLeaseAndTheSkewIsRefusedAndCountedOnce() throws Exception {
        // PDT 2 phase 5, owner ruling R2 of §10.11: a far deadline would let any member replay copies of a pin to new
        // stores until then, so a store takes a signed pin only while its notAfter lies within PIN_ENTRY_LEASE and the
        // clock skew of now
        StoreFleet.Peer store = fleet.newPeer("store", 84, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer alice = fleet.newPeer("alice", 85, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(alice, 1)).isTrue();
        String far = StoreFleet.facts(alice, "far", 300, 14).root();
        String near = StoreFleet.facts(alice, "near", 300, 15).root();
        alice.content().holdTree(far);
        alice.content().holdTree(near);
        long bound = fleet.clock.millis() + Stores.PIN_ENTRY_LEASE.plus(ContentStore.DEADLINE_SKEW).toMillis();
        StorePin farTerms = new StorePin(StorePin.VERSION, far, StorePin.KIND_TREE, 86400, 1, alice.id(), List.of(), null);
        // an hour past the bound, so the ticks of the test clock never bring it within the bound
        SignedPin farPin = SignedPin.sign(farTerms, fleet.groupId, bound + 3_600_000, alice.identity());
        alice.contentSpace().write(farPin, Lease.of(Duration.ofMinutes(10)), farPin.tags());
        StorePin nearTerms = new StorePin(StorePin.VERSION, near, StorePin.KIND_TREE, 86400, 1, alice.id(), List.of(), null);
        SignedPin nearPin = SignedPin.sign(nearTerms, fleet.groupId, bound, alice.identity());
        alice.contentSpace().write(nearPin, Lease.of(Duration.ofMinutes(10)), nearPin.tags());

        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> store.contentStore().ledger().holds(near)))
                .as("a deadline at the bound is taken").isTrue();
        fleet.runFor(Duration.ofSeconds(2));
        assertThat(store.contentStore().ledger().holds(far)).as("a deadline past the bound is not").isFalse();
        assertThat(store.contentStore().status().refused()).as("refused once, over many scans").isEqualTo(1);
        assertThat(store.contentStore().status().unsigned()).as("alice signed it").isZero();
        assertThat(alice.contentSpace().readAll(Template.of(SignedPin.class), 4)).as("the far pin stays untaken").hasSize(1);
    }

    @Test
    void aForgersOwnSignatureOverTermsNamingAnotherRequesterAndARewrittenSignedPinChargeNobody() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 61, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer alice = fleet.newPeer("alice", 62, StoreFleet.Spec.member(), "store");
        StoreFleet.Peer mallory = fleet.newPeer("mallory", 63, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(mallory, 1)).isTrue();
        String root = StoreFleet.facts(alice, "own-key", 300, 9).root();
        alice.content().holdTree(root);
        long notAfter = fleet.clock.millis() + 600_000;

        // mallory signs the consent bytes of terms that name alice under mallory's own key: the signature verifies
        StorePin named = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 86400, 1, alice.id(), List.of(),
                Map.of(Stores.STORES_ATTR, store.id().value()));
        SignedPin ownKey = new SignedPin(SignedPin.VERSION, named, 1, notAfter, mallory.identity().rawPublicKey(),
                mallory.identity().sign(SignedPin.consentBytes(named, fleet.groupId, notAfter)));
        mallory.contentSpace().write(ownKey, Lease.of(Duration.ofMinutes(10)), ownKey.tags());
        // alice signed a pin for another store and a day; mallory points it at this store for a year
        StorePin elsewhere = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 86400, 1, alice.id(), List.of(),
                Map.of(Stores.STORES_ATTR, mallory.id().value()));
        SignedPin signed = SignedPin.sign(elsewhere, fleet.groupId, notAfter, alice.identity());
        StorePin redirected = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 365L * 86400, 1, alice.id(), List.of(),
                named.attrs());
        SignedPin rewritten = new SignedPin(SignedPin.VERSION, redirected, 1, notAfter, signed.key(), signed.sig());
        mallory.contentSpace().write(rewritten, Lease.of(Duration.ofMinutes(10)), rewritten.tags());
        fleet.runFor(Duration.ofSeconds(3));

        assertThat(store.contentStore().ledger().holds(root)).isFalse();
        assertThat(store.contentStore().status().byRequester()).doesNotContainKey(alice.id());
        assertThat(store.contentStore().status().unsigned()).as("both refused, each once").isEqualTo(2);
        assertThat(store.contentStore().status().taken()).isZero();
    }

    @Test
    void aReplayedSignedPinIsTakenOncePerStoreSoItExtendsNoLease() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 64, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer alice = fleet.newPeer("alice", 65, StoreFleet.Spec.member(), "store");
        StoreFleet.Peer mallory = fleet.newPeer("mallory", 66, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(mallory, 1)).isTrue();
        String root = StoreFleet.facts(alice, "replayed", 300, 10).root();
        alice.content().holdTree(root);
        // alice asks for one minute of holding, good for one minute
        StorePin terms = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 60, 1, alice.id(), List.of(),
                Map.of(Stores.STORES_ATTR, store.id().value()));
        SignedPin signed = SignedPin.sign(terms, fleet.groupId, fleet.clock.millis() + 60_000, alice.identity());
        alice.contentSpace().write(signed, Lease.of(Duration.ofSeconds(60)), signed.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> store.contentStore().ledger().holds(root))).isTrue();
        long until = store.contentStore().ledger().holding(root, alice.id()).orElseThrow().until();

        // mallory copies the signed pin a third of a minute later, inside its deadline, more than a quarter lease on
        fleet.clock.advance(Duration.ofSeconds(20));
        mallory.contentSpace().write(signed, Lease.of(Duration.ofSeconds(30)), signed.tags());
        long stop = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < stop) {
            fleet.peers.forEach(p -> p.node().tick()); // frames move, the test clock stands
            Thread.sleep(20);
        }

        assertThat(store.contentStore().ledger().holding(root, alice.id()).orElseThrow().until())
                .as("alice signed one minute once").isEqualTo(until);
        assertThat(store.contentStore().status().taken()).isEqualTo(1);
        assertThat(mallory.contentSpace().readAll(Template.of(SignedPin.class), 4)).as("the copy stays untaken").hasSize(1);
    }

    @Test
    void aStoreThatFinishesAFetchAfterTheSignaturesDeadlineHoldsTheRootAndForwardsNoCopy() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 67, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer alice = fleet.newPeer("alice", 68, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(cardsSeen(alice, 1)).isTrue();
        // alice asks two copies, good for ten seconds, and holds the tree only after the store took the pin
        String root = StoreFleet.facts(alice, "late", 300, 12).root();
        StorePin terms = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 3600, 2, alice.id(), List.of(),
                Map.of(Stores.STORES_ATTR, store.id().value()));
        SignedPin signed = SignedPin.sign(terms, fleet.groupId, fleet.clock.millis() + 10_000, alice.identity());
        alice.contentSpace().write(signed, Lease.of(Duration.ofSeconds(10)), signed.tags());
        long stop = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!alice.contentSpace().readAll(Template.of(SignedPin.class), 4).isEmpty() && System.nanoTime() < stop) {
            fleet.peers.forEach(p -> p.node().tick()); // frames move, the test clock stands
            Thread.sleep(20);
        }
        assertThat(alice.contentSpace().readAll(Template.of(SignedPin.class), 4)).as("the store took the pin").isEmpty();
        fleet.clock.advance(Duration.ofSeconds(15));
        alice.content().holdTree(root);

        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> store.contentStore().status().taken() == 1)).isTrue();
        assertThat(store.contentStore().ledger().holding(root, alice.id())).isPresent();
        assertThat(store.contentStore().status().abandoned()).isZero();
        assertThat(alice.contentSpace().readAll(Template.of(SignedPin.class), 4)).as("no copy past the deadline").isEmpty();
    }

    @Test
    void aPinThatAsksForeverPlacesItsSecondCopyOnTheNextStore() throws Exception {
        StoreFleet.Peer a = fleet.newPeer("store-a", 81, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer b = fleet.newPeer("store-b", 82, StoreFleet.Spec.store(StorePolicy.of(1L << 30)), "store-a");
        StoreFleet.Peer alice = fleet.newPeer("alice", 83, StoreFleet.Spec.member(), "store-a");
        fleet.settle(6);
        assertThat(cardsSeen(alice, 2)).isTrue();
        String root = StoreFleet.facts(alice, "forever", 300, 13).root();
        // a lease of Long.MAX_VALUE seconds, which each store holds for its lease-max; the copy a store forwards
        // lapses with the signature, at most PIN_ENTRY_LEASE on
        alice.pdt().storeFor(root, StoreChooser.Need.of(0, 2), ChronoUnit.FOREVER.getDuration());

        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> StoreFleet.receiptHolders(alice, root).size() == 2))
                .as(a.contentStore().status() + " / " + b.contentStore().status()).isTrue();
        assertThat(a.contentStore().status().abandoned() + b.contentStore().status().abandoned()).isZero();
        assertThat(a.contentStore().status().taken() + b.contentStore().status().taken()).isEqualTo(2);
    }
}
