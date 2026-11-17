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
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.pdt.stores.Stores;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The content store peer end to end on the sim network (PDT 2 phase 3, P2-S-01, P2-S-03, P2-S-04,
 * P2-Q-REL-02; S2 of §6.4): stores advertise their cards, a writer pins a tree for two copies through the
 * planner's choice, the first store forwards the second copy, the writer leaves and a store restarts over its
 * pack, and a late reader fetches every node from the stores. A renewal extends the lease; a lease that lapses
 * releases the root and withdraws the receipt.
 *
 * <p>Every wait is a measured wait on the fleet ({@link StoreFleet#runUntil(String, Duration, java.util.function.BooleanSupplier)}),
 * which records the ticks, the fake time and the wall time of each. fm: (a) ceilings of 10 and 20 s rose to 30 s by the
 * ceiling policy (at least 30 s and ten times typical; each wait here is typically under a second); the 30 and 60 s
 * ceilings are unchanged, and no condition changed. The races' orders are made by held links, not by time.
 */
class StorePeerTest {

    private final StoreFleet fleet = new StoreFleet();

    @TempDir
    Path dir;

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static List<AssetCard> storeCards(StoreFleet.Peer peer) {
        return peer.discovery().find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind"))
                && c.expiresAt().isAfter(java.time.Instant.EPOCH.plusMillis(0)));
    }

    @Test
    void aPinnedTreeOutlivesItsWriterAndAStoreRestart() throws Exception {
        PackNodeStore pack = PackNodeStore.open(dir.resolve("site-a"));
        NodeStore durable = new NodeStore(pack, fleet.clock);
        PeerIdentity siteA = PeerIdentity.generate();
        StoreFleet.Peer a = fleet.newPeer("site-a", 1, StoreFleet.Spec.store(StorePolicy.of(1L << 30)).nodes(durable).identity(siteA));
        StoreFleet.Peer b = fleet.newPeer("site-b", 2, StoreFleet.Spec.store(StorePolicy.of(1L << 30)), "site-a");
        StoreFleet.Peer writer = fleet.newPeer("writer", 3, StoreFleet.Spec.member(), "site-a");
        fleet.settle(6);
        assertThat(fleet.runUntil("store.card.two-seen", Duration.ofSeconds(30), () -> storeCards(writer).size() == 2))
                .as("both cards reach the writer").isTrue();
        assertThat(a.content().describe(fleet.groupId).parameters()).containsEntry(Content.PDT_PARAMETER, "2");

        PMap<String, String> value = StoreFleet.facts(writer, "facts", 2_000, 7);
        String root = value.root();
        Stores.Pinning pinning = writer.pdt().storeFor(root, StoreChooser.Need.of(0, 2), Duration.ofDays(1));
        assertThat(pinning.chosen()).hasSize(2);
        assertThat(pinning.pin().copies()).isEqualTo(2);
        assertThat(pinning.pin().attrs()).containsKey(Stores.STORES_ATTR);
        assertThat(fleet.runUntil("store.pin.two-receipts", Duration.ofSeconds(60), () -> StoreFleet.receiptHolders(writer, root).size() == 2))
                .as("two receipts: " + a.contentStore().status() + " / " + b.contentStore().status()).isTrue();
        assertThat(StoreFleet.receiptHolders(writer, root)).containsExactlyInAnyOrder(a.id(), b.id());
        assertThat(a.store().complete(root)).isTrue();
        assertThat(b.store().complete(root)).isTrue();
        assertThat(a.contentSpace().readAll(Template.of(SignedPin.class), 16)).as("every copy was taken").isEmpty();
        long bytes = a.contentStore().ledger().holding(root, writer.id()).orElseThrow().bytes();
        assertThat(bytes).isPositive();
        assertThat(a.contentStore().status().byRequester()).containsEntry(writer.id(), new StoreLedger.Totals(bytes, 1));

        // the writer leaves; store a restarts over its pack under the same identity
        fleet.kill(writer);
        fleet.kill(a);
        pack.close();
        PackNodeStore reopened = PackNodeStore.open(dir.resolve("site-a"));
        StoreFleet.Peer a2 = fleet.newPeer("site-a2", 4, StoreFleet.Spec.store(StorePolicy.of(1L << 30))
                .nodes(new NodeStore(reopened, fleet.clock)).identity(siteA).storeBuilder(sb -> sb.name("site-a")), "site-b");
        assertThat(a2.contentStore().ledger().holding(root, writer.id())).as("the ledger survived the restart").isPresent();
        assertThat(a2.store().complete(root)).isTrue();

        StoreFleet.Peer reader = fleet.newPeer("reader", 5, StoreFleet.Spec.member(), "site-b");
        fleet.settle(8);
        TreeFetch fetch = reader.content().fetchTree(root, List.of(), FetchOptions.defaults());
        assertThat(fleet.runUntil("store.fetch.after-restart", Duration.ofSeconds(60), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(fetch.progress().bytesBySource().entrySet().stream().filter(e -> e.getValue() > 0)
                .map(java.util.Map.Entry::getKey).toList()).as("every byte came from the stores")
                .isNotEmpty().allMatch(id -> id.equals(a2.id()) || id.equals(b.id()));
        assertThat(PMap.load(reader.store(), root, StoreFleet.STR, StoreFleet.STR).get("key-1234")).isPresent();
        reopened.close();
    }

    @Test
    void aPinTwoStoresTakeAcrossAPartitionIsHeldAndChargedByTheWinnerAlone() throws Exception {
        // a take lease longer than the test, so the loser learns it lost from the claims and not at a renewal
        StoreFleet.Spec racer = StoreFleet.Spec.store(StorePolicy.of(1L << 30)).storeBuilder(sb -> sb.takeLease(Duration.ofDays(1)));
        StoreFleet.Peer a = fleet.newPeer("race-a", 13, racer);
        StoreFleet.Peer b = fleet.newPeer("race-b", 14, racer, "race-a");
        StoreFleet.Peer writer = fleet.newPeer("writer", 15, StoreFleet.Spec.member(), "race-a");
        fleet.settle(6);
        assertThat(fleet.runUntil("store.card.two-seen", Duration.ofSeconds(30), () -> storeCards(writer).size() == 2)).isTrue();
        String root = StoreFleet.facts(writer, "race", 300, 8).root();

        // nothing a store sends leaves it, so neither sees the other's claim; no holder is known yet, so both
        // take and both wait
        List<String[]> links = List.of(new String[]{"race-a", "race-b"}, new String[]{"race-b", "race-a"},
                new String[]{"race-a", "writer"}, new String[]{"race-b", "writer"});
        links.forEach(l -> fleet.network.hold(l[0], l[1]));
        var handle = fleet.writePin(writer, racePin(writer, root), Duration.ofMinutes(10));
        assertThat(fleet.runUntil("store.race.both-claimed", Duration.ofSeconds(30), () -> a.contentSpace().currentClaim(handle.entryId()).isPresent()
                && b.contentSpace().currentClaim(handle.entryId()).isPresent())).as("both stores took the pin").isTrue();
        assertThat(a.contentSpace().currentClaim(handle.entryId()).orElseThrow().holder())
                .isNotEqualTo(b.contentSpace().currentClaim(handle.entryId()).orElseThrow().holder());
        links.forEach(l -> fleet.network.release(l[0], l[1], ai.badmonkey.agentspaces.test.SimNetwork.Order.FIFO, 1));
        fleet.settle(8);
        writer.content().holdTree(root);
        assertOneHolder(writer, root, a, b);
    }

    @Test
    void aRaceLoserThatCompletesBeforeTheWinnersClaimArrivesKeepsTheHoldingTheWinnerAcknowledges() throws Exception {
        // store-race, 2026-11-15: the race above failed once in a loaded reactor, both stores at taken=1. A claim
        // can reach the other store after that store's fetch ended (a rumor sent while its sender's view was empty
        // reaches nobody, and a held frame older than five minutes is dropped), and the loser then completes on its
        // own claim. Here that order is made on purpose: the loser's links open first, it fetches and completes,
        // and only then does the winner's claim arrive. Since the settle guarantee (SPEC 7.4, rule two) the winner
        // that learns of that authentic completion before its own acknowledges it and writes no result, so the
        // loser's receipt is the pin's one receipt and the loser must keep the holding. Before the store followed
        // that rule both stores gave the pin up and nobody held it (final gate, 2026-11-15).
        StoreFleet.Spec racer = StoreFleet.Spec.store(StorePolicy.of(1L << 30)).storeBuilder(sb -> sb.takeLease(Duration.ofDays(1)));
        StoreFleet.Peer a = fleet.newPeer("race-a", 16, racer);
        StoreFleet.Peer b = fleet.newPeer("race-b", 17, racer, "race-a");
        StoreFleet.Peer writer = fleet.newPeer("writer", 18, StoreFleet.Spec.member(), "race-a");
        fleet.settle(6);
        assertThat(fleet.runUntil("store.card.two-seen", Duration.ofSeconds(30), () -> storeCards(writer).size() == 2)).isTrue();
        String root = StoreFleet.facts(writer, "late", 300, 11).root();

        List<String[]> links = List.of(new String[]{"race-a", "race-b"}, new String[]{"race-b", "race-a"},
                new String[]{"race-a", "writer"}, new String[]{"race-b", "writer"});
        links.forEach(l -> fleet.network.hold(l[0], l[1]));
        var handle = fleet.writePin(writer, racePin(writer, root), Duration.ofMinutes(10));
        assertThat(fleet.runUntil("store.race.both-claimed", Duration.ofSeconds(30), () -> a.contentSpace().currentClaim(handle.entryId()).isPresent()
                && b.contentSpace().currentClaim(handle.entryId()).isPresent())).as("both stores took the pin").isTrue();
        // the claim lattice picks the winner the same way on every replica, so the test can name it now
        ai.badmonkey.agentspaces.space.replicated.TakeClaim claimA = a.contentSpace().currentClaim(handle.entryId()).orElseThrow();
        ai.badmonkey.agentspaces.space.replicated.TakeClaim claimB = b.contentSpace().currentClaim(handle.entryId()).orElseThrow();
        boolean aWins = ai.badmonkey.agentspaces.space.replicated.TakeClaim.merge(claimA, claimB).equals(claimA);
        StoreFleet.Peer winner = aWins ? a : b;
        StoreFleet.Peer loser = aWins ? b : a;

        // the loser's links open: its claim reaches the others, the winner's stays held, and the loser completes
        fleet.network.release(loser.name(), winner.name(), ai.badmonkey.agentspaces.test.SimNetwork.Order.FIFO, 1);
        fleet.network.release(loser.name(), "writer", ai.badmonkey.agentspaces.test.SimNetwork.Order.FIFO, 1);
        writer.content().holdTree(root);
        long started = System.nanoTime();
        // typically well under a second of real time; the ceiling only means clearly broken
        assertThat(fleet.runUntil("store.race.loser-completes", Duration.ofSeconds(60), () -> loser.contentStore().status().taken() == 1))
                .as("the loser completes on its own claim: " + loser.contentStore().status()).isTrue();
        System.out.printf("store race: the loser completed after %d ms%n", (System.nanoTime() - started) / 1_000_000);
        assertThat(loser.contentStore().ledger().holds(root)).isTrue();

        // the winner's claim arrives: the winner acknowledges the loser's completion and writes no receipt, and
        // the loser keeps the holding it completed
        fleet.network.release(winner.name(), loser.name(), ai.badmonkey.agentspaces.test.SimNetwork.Order.FIFO, 1);
        fleet.network.release(winner.name(), "writer", ai.badmonkey.agentspaces.test.SimNetwork.Order.FIFO, 1);
        assertThat(fleet.runUntil("store.race.claim-reaches-loser", Duration.ofSeconds(30), () -> loser.contentSpace().currentClaim(handle.entryId())
                .map(c -> c.holder().peer().equals(winner.id())).orElse(false)))
                .as("the winner's claim reached the loser, so the loser's watch saw it").isTrue();
        assertOneHolder(writer, root, a, b);
        // two more holding intervals, so the loser's race watch has run with the winner's claim in view
        fleet.settle(2 * (int) (StoreFleet.HOLDING.toMillis() / 250));
        assertThat(loser.contentStore().ledger().holds(root)).as("the loser's watch kept the holding").isTrue();
        assertThat(StoreFleet.receiptHolders(writer, root)).containsExactly(loser.id());
        assertThat(loser.contentStore().status().taken()).as("the loser's completion stands").isEqualTo(1);
        assertThat(loser.contentStore().status().abandoned()).isZero();
        assertThat(winner.contentStore().status().taken()).as("the winner acknowledged and wrote no receipt").isZero();
        assertThat(winner.contentStore().status().abandoned()).isEqualTo(1);
    }

    @Test
    void whenBothStoresCompleteBeforeEitherHearsOfTheOtherTheLoserGivesTheHoldingUp() throws Exception {
        // Rule three of the settle guarantee (SPEC 7.4): both holders complete before either learns of the other,
        // so two receipts exist and one must go. The loser gives its holding up only because the winner also wrote
        // a receipt (ContentStore.driveRaces). Made deterministic here: each store already has the tree's nodes, so
        // its fetch completes locally with no frame on the wire, and every link out of a store stays held until
        // both have completed; neither can hear of the other's claim or receipt before its own completion.
        StoreFleet.Spec racer = StoreFleet.Spec.store(StorePolicy.of(1L << 30)).storeBuilder(sb -> sb.takeLease(Duration.ofDays(1)));
        StoreFleet.Peer a = fleet.newPeer("race-a", 19, racer);
        StoreFleet.Peer b = fleet.newPeer("race-b", 20, racer, "race-a");
        StoreFleet.Peer writer = fleet.newPeer("writer", 21, StoreFleet.Spec.member(), "race-a");
        fleet.settle(6);
        assertThat(fleet.runUntil("store.card.two-seen", Duration.ofSeconds(30), () -> storeCards(writer).size() == 2)).isTrue();
        // the same entries and seed make the same tree on each peer, under refs of their own
        String root = StoreFleet.facts(writer, "both", 300, 12).root();
        assertThat(StoreFleet.facts(a, "both-a", 300, 12).root()).isEqualTo(root);
        assertThat(StoreFleet.facts(b, "both-b", 300, 12).root()).isEqualTo(root);
        assertThat(a.store().complete(root) && b.store().complete(root)).as("each store has every node").isTrue();

        // the writer's pin reaches both stores; nothing a store sends leaves it
        List<String[]> links = List.of(new String[]{"race-a", "race-b"}, new String[]{"race-b", "race-a"},
                new String[]{"race-a", "writer"}, new String[]{"race-b", "writer"});
        links.forEach(l -> fleet.network.hold(l[0], l[1]));
        var handle = fleet.writePin(writer, racePin(writer, root), Duration.ofMinutes(10));
        long started = System.nanoTime();
        // typically well under a second of real time; the ceiling only means clearly broken
        assertThat(fleet.runUntil("store.race.both-complete", Duration.ofSeconds(60), () -> a.contentStore().status().taken() == 1
                && b.contentStore().status().taken() == 1))
                .as("both stores completed on their own claims: " + a.contentStore().status() + " / "
                        + b.contentStore().status()).isTrue();
        System.out.printf("store race (rule three): both completed after %d ms%n", (System.nanoTime() - started) / 1_000_000);
        assertThat(a.contentStore().ledger().holds(root) && b.contentStore().ledger().holds(root)).isTrue();
        // the claim lattice picks the winner the same way on every replica, so the test can name it now
        ai.badmonkey.agentspaces.space.replicated.TakeClaim claimA = a.contentSpace().currentClaim(handle.entryId()).orElseThrow();
        ai.badmonkey.agentspaces.space.replicated.TakeClaim claimB = b.contentSpace().currentClaim(handle.entryId()).orElseThrow();
        assertThat(claimA.holder()).isNotEqualTo(claimB.holder());
        boolean aWins = ai.badmonkey.agentspaces.space.replicated.TakeClaim.merge(claimA, claimB).equals(claimA);
        StoreFleet.Peer winner = aWins ? a : b;
        StoreFleet.Peer loser = aWins ? b : a;

        // the held claims, completions and receipts arrive: the loser sees the winner's claim and receipt and gives up
        links.forEach(l -> fleet.network.release(l[0], l[1], ai.badmonkey.agentspaces.test.SimNetwork.Order.FIFO, 1));
        started = System.nanoTime();
        assertThat(fleet.runUntil("store.race.one-holder", Duration.ofSeconds(60), () -> !loser.contentStore().ledger().holds(root)
                && StoreFleet.receiptHolders(writer, root).equals(List.of(winner.id()))))
                .as("the loser gave the holding up: " + loser.contentStore().status() + ", receipts "
                        + StoreFleet.receiptHolders(writer, root)).isTrue();
        System.out.printf("store race (rule three): one holder after %d ms%n", (System.nanoTime() - started) / 1_000_000);
        // two more holding intervals, so the winner's watch has run with both receipts gone by, and it kept its own
        fleet.settle(2 * (int) (StoreFleet.HOLDING.toMillis() / 250));
        assertThat(winner.contentStore().ledger().holds(root)).as("the winner keeps the holding").isTrue();
        assertThat(StoreFleet.receiptHolders(writer, root)).containsExactly(winner.id());
        assertThat(winner.contentStore().status().taken()).isEqualTo(1);
        assertThat(winner.contentStore().status().abandoned()).isZero();
        assertThat(loser.contentStore().status().taken()).as("the loser's count moved from taken").isZero();
        assertThat(loser.contentStore().status().abandoned()).as("to abandoned").isEqualTo(1);
        assertThat(loser.contentSpace().duplicateCompletions())
                .as("the displaced holder cannot tell an acknowledgement from a second completion").isZero();
        // The winner's duplicateCompletions is not asserted: it counts only when the loser's completion reaches it
        // still carrying the loser's claim. The links were held for many ticks, so the receivers closed them and
        // the held frames went to closed connections (measured: 76 of 89 on one run); anti-entropy repairs, and
        // whether the loser offers its own completion before it adopts the winner's is an ordering this test does
        // not fix. LeaseRaceLateClaimTest scripts that order and asserts the count.
    }

    private static StorePin racePin(StoreFleet.Peer writer, String root) {
        return new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 3600, 1, writer.id(), List.of(), null);
    }

    /** Waits for one receipt and both stores done with the pin, then checks that only the store with the receipt holds and charges. */
    private void assertOneHolder(StoreFleet.Peer writer, String root, StoreFleet.Peer a, StoreFleet.Peer b) throws Exception {
        long started = System.nanoTime();
        // typically under a second of real time; the ceiling only means clearly broken
        assertThat(fleet.runUntil("store.race.one-receipt", Duration.ofSeconds(60), () -> StoreFleet.receiptHolders(writer, root).size() == 1
                && a.contentStore().status().taken() + a.contentStore().status().abandoned() + b.contentStore().status().taken()
                + b.contentStore().status().abandoned() >= 2)).as(a.contentStore().status() + " / " + b.contentStore().status()).isTrue();
        System.out.printf("store race: one holder after %d ms%n", (System.nanoTime() - started) / 1_000_000);
        fleet.settle(8);

        assertThat(StoreFleet.receiptHolders(writer, root)).as("one receipt").hasSize(1);
        assertThat(a.contentStore().status().taken() + b.contentStore().status().taken()).isEqualTo(1);
        assertThat(java.util.stream.Stream.of(a, b).filter(s -> s.contentStore().ledger().holds(root)).map(StoreFleet.Peer::id))
                .as("only the store with the receipt holds and charges the requester").containsExactlyElementsOf(StoreFleet.receiptHolders(writer, root));
    }

    @Test
    void aRenewalExtendsTheLeaseAndALapsedLeaseReleasesTheRoot() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 11, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer writer = fleet.newPeer("writer", 12, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(fleet.runUntil("store.card.one-seen", Duration.ofSeconds(30), () -> storeCards(writer).size() == 1)).isTrue();
        String root = StoreFleet.facts(writer, "notes", 300, 3).root();
        writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofSeconds(60));
        assertThat(fleet.runUntil("store.pin.one-receipt", Duration.ofSeconds(30), () -> StoreFleet.receiptHolders(writer, root).size() == 1)).isTrue();
        long first = store.contentStore().ledger().holding(root, writer.id()).orElseThrow().until();

        // half the lease later the writer renews: the store extends the lease and replaces its receipt
        fleet.clock.advance(Duration.ofSeconds(30));
        writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofSeconds(60));
        assertThat(fleet.runUntil("store.lease.renewed", Duration.ofSeconds(30), () -> store.contentStore().ledger().holding(root, writer.id())
                .map(h -> h.until() > first).orElse(false))).isTrue();
        assertThat(fleet.runUntil("store.lease.receipt-renewed", Duration.ofSeconds(30), () -> writer.pdt().stores().receipts(root).size() == 1
                && writer.pdt().stores().receipts(root).get(0).until() > first)).as("one receipt, the renewed one").isTrue();

        // no renewal: after the lease and one holding interval the store releases the root
        fleet.clock.advance(Duration.ofSeconds(65));
        assertThat(fleet.runUntil("store.lease.lapse-release", Duration.ofSeconds(30), () -> !store.contentStore().ledger().holds(root))).isTrue();
        assertThat(store.store().pinned()).doesNotContain(root);
        assertThat(fleet.runUntil("store.lease.receipt-withdrawn", Duration.ofSeconds(30), () -> writer.pdt().stores().receipts(root).isEmpty())).isTrue();
        assertThat(store.content().treeRoots()).doesNotContain(root);
        assertThat(store.contentStore().status().released()).isEqualTo(1);
    }

    @Test
    void aRenewalGoesToTheStoreThatHoldsTheRootThoughAnotherHasMoreRoom() throws Exception {
        // PDT 2 phase 3, P2-S-04: a renewal extends the holder's lease; choosing by free bytes alone moved the
        // root to the emptier store on every renewal
        StoreFleet.Peer first = fleet.newPeer("renew-a", 31, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer second = fleet.newPeer("renew-b", 32, StoreFleet.Spec.store(StorePolicy.of(1L << 30)), "renew-a");
        StoreFleet.Peer writer = fleet.newPeer("writer", 33, StoreFleet.Spec.member(), "renew-a");
        fleet.settle(6);
        assertThat(fleet.runUntil("store.card.two-seen", Duration.ofSeconds(30), () -> storeCards(writer).size() == 2)).isTrue();
        String root = StoreFleet.facts(writer, "renewed", 300, 9).root();
        Stores.Pinning pinning = writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofSeconds(60));
        assertThat(fleet.runUntil("store.pin.one-receipt", Duration.ofSeconds(30), () -> StoreFleet.receiptHolders(writer, root).size() == 1)).isTrue();
        StoreFleet.Peer holder = pinning.chosen().get(0).issuer().equals(first.id()) ? first : second;
        StoreFleet.Peer other = holder == first ? second : first;
        long until = holder.contentStore().ledger().holding(root, writer.id()).orElseThrow().until();
        // the holder's card now shows fewer free bytes than the other store's
        assertThat(fleet.runUntil("store.card.room-shown", Duration.ofSeconds(30), () -> storeCards(writer).stream()
                .anyMatch(c -> c.issuer().equals(holder.id()) && !c.access().get("free").equals(Long.toString(StorePolicy.of(1L << 30).usableBytes())))))
                .isTrue();

        fleet.clock.advance(Duration.ofSeconds(30));
        Stores.Pinning renewal = writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofSeconds(60));
        assertThat(renewal.chosen()).extracting(AssetCard::issuer).containsExactly(holder.id());
        assertThat(fleet.runUntil("store.lease.renewed-at-holder", Duration.ofSeconds(30), () -> holder.contentStore().ledger().holding(root, writer.id())
                .map(h -> h.until() > until).orElse(false))).isTrue();
        fleet.settle(8);
        assertThat(other.contentStore().ledger().holds(root)).as("the other store fetched nothing").isFalse();
        assertThat(StoreFleet.receiptHolders(writer, root)).containsExactly(holder.id());
    }

    @Test
    void anUnpinAfterARestartWithdrawsTheReceiptTheStoreWroteBeforeIt() throws Exception {
        // PDT 2 phase 3, P2-S-04 and P2-Q-REL-02: the receipt handles lived in memory only, so a restarted store
        // released the root and left its receipt counting as a copy for the rest of the lease
        PackNodeStore pack = PackNodeStore.open(dir.resolve("site-u"));
        PeerIdentity siteU = PeerIdentity.generate();
        StoreFleet.Peer a = fleet.newPeer("site-u", 34, StoreFleet.Spec.store(StorePolicy.of(1L << 30))
                .nodes(new NodeStore(pack, fleet.clock)).identity(siteU));
        StoreFleet.Peer writer = fleet.newPeer("writer", 35, StoreFleet.Spec.member(), "site-u");
        fleet.settle(6);
        assertThat(fleet.runUntil("store.card.one-seen", Duration.ofSeconds(30), () -> storeCards(writer).size() == 1)).isTrue();
        String root = StoreFleet.facts(writer, "kept", 300, 10).root();
        writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofDays(30));
        assertThat(fleet.runUntil("store.pin.one-receipt", Duration.ofSeconds(30), () -> StoreFleet.receiptHolders(writer, root).size() == 1)).isTrue();

        fleet.kill(a);
        pack.close();
        PackNodeStore reopened = PackNodeStore.open(dir.resolve("site-u"));
        StoreFleet.Peer a2 = fleet.newPeer("site-u2", 36, StoreFleet.Spec.store(StorePolicy.of(1L << 30))
                .nodes(new NodeStore(reopened, fleet.clock)).identity(siteU).storeBuilder(sb -> sb.name("site-u")), "writer");
        fleet.settle(8);
        assertThat(fleet.runUntil("store.unpin.receipt-read", Duration.ofSeconds(30), () -> !a2.contentSpace().readAll(
                Template.of(ai.badmonkey.agentspaces.agent.content.StoreReceipt.class), 4).isEmpty())).isTrue();
        assertThat(a2.contentStore().invoke(a2.contentStore().assetName(), ContentStore.OP_UNPIN,
                java.util.Map.of("root", root), "unpin-1", writer.id()).ok()).isTrue();
        assertThat(fleet.runUntil("store.unpin.receipt-withdrawn", Duration.ofSeconds(30), () -> StoreFleet.receiptHolders(writer, root).isEmpty()))
                .as("the receipt from before the restart is withdrawn").isTrue();
        fleet.kill(a2);
        reopened.close();
    }
}
