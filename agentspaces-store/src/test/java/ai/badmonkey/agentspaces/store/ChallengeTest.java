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
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Possession challenges against content stores (PDT 2 phase 3, P2-S-07, P2-Q-REL-03; S3 of §6.4): a store
 * that holds a root passes; once its node store loses the root's nodes the next two challenges fail, the
 * requester strikes it, its receipt stops counting, and the requester pins the root again on a store it has not
 * struck.
 */
class ChallengeTest {

    private final StoreFleet fleet = new StoreFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private Content.ChallengeResult await(CompletableFuture<Content.ChallengeResult> answer) throws Exception {
        assertThat(fleet.runUntil(Duration.ofSeconds(20), answer::isDone)).isTrue();
        return answer.get();
    }

    @Test
    void aStoreThatStopsHoldingFailsTwoChallengesAndItsReceiptStopsCounting() throws Exception {
        StoreFleet.Peer lossy = fleet.newPeer("lossy", 21, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer writer = fleet.newPeer("writer", 22, StoreFleet.Spec.member(), "lossy");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> !writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).isEmpty())).isTrue();
        String root = StoreFleet.facts(writer, "facts", 1_500, 5).root();
        writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofDays(1));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> StoreFleet.receiptHolders(writer, root).size() == 1)).isTrue();

        Content.ChallengeResult passed = await(writer.pdt().challenge(root, lossy.id()));
        assertThat(passed.passed()).as(passed.toString()).isTrue();
        assertThat(passed.proven()).isEqualTo(16);
        assertThat(lossy.contentStore().query(lossy.contentStore().assetName(), java.util.Map.of()).rows())
                .singleElement().satisfies(row -> assertThat(row).containsEntry("challengesPassed", "1"));

        // the store's node store loses the tree's nodes; a second store joins
        for (String cid : lossy.store().reachable(root)) {
            lossy.store().backend().remove(cid);
        }
        StoreFleet.Peer spare = fleet.newPeer("spare", 23, StoreFleet.Spec.store(StorePolicy.of(1L << 30)), "lossy");
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).size() == 2)).isTrue();

        Content.ChallengeResult first = await(writer.pdt().challenge(root, lossy.id()));
        assertThat(first.passed()).isFalse();
        assertThat(writer.pdt().stores().struck(lossy.id())).as("one failure is no strike").isFalse();
        assertThat(StoreFleet.receiptHolders(writer, root)).containsExactly(lossy.id());
        Content.ChallengeResult second = await(writer.pdt().challenge(root, lossy.id()));
        assertThat(second.passed()).isFalse();
        assertThat(writer.pdt().stores().struck(lossy.id())).isTrue();

        // the struck store's receipt stops counting, and the replacement pin lands on the spare store
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> StoreFleet.receiptHolders(writer, root).contains(spare.id())))
                .as("spare: " + spare.contentStore().status()).isTrue();
        assertThat(StoreFleet.receiptHolders(writer, root)).containsExactly(spare.id());
        assertThat(spare.store().complete(root)).isTrue();
        assertThat(await(writer.pdt().challenge(root, spare.id())).passed()).isTrue();
        assertThat(writer.contentSpace().readAll(Template.of(SignedPin.class), 8)).isEmpty();
    }

    @Test
    void aPassOnARootTheStoreKeepsDoesNotHideARootItLost() throws Exception {
        StoreFleet.Peer lossy = fleet.newPeer("lossy", 24, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer writer = fleet.newPeer("writer", 25, StoreFleet.Spec.member(), "lossy");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> !writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).isEmpty())).isTrue();
        String kept = StoreFleet.facts(writer, "kept", 400, 6).root();
        String lost = StoreFleet.facts(writer, "lost", 400, 7).root();
        writer.pdt().storeFor(kept, StoreChooser.Need.of(0, 1), Duration.ofDays(1));
        writer.pdt().storeFor(lost, StoreChooser.Need.of(0, 1), Duration.ofDays(1));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> StoreFleet.receiptHolders(writer, kept).size() == 1
                && StoreFleet.receiptHolders(writer, lost).size() == 1)).isTrue();
        java.util.Set<String> keep = lossy.store().reachable(kept);
        for (String cid : lossy.store().reachable(lost)) {
            if (!keep.contains(cid)) {
                lossy.store().backend().remove(cid);
            }
        }

        // the requester alternates: lost, kept, lost; the two failures on the lost root are in a row for it
        assertThat(await(writer.pdt().challenge(lost, lossy.id())).passed()).isFalse();
        assertThat(await(writer.pdt().challenge(kept, lossy.id())).passed()).isTrue();
        assertThat(writer.pdt().stores().struck(lossy.id())).isFalse();
        assertThat(await(writer.pdt().challenge(lost, lossy.id())).passed()).isFalse();
        assertThat(writer.pdt().stores().struck(lossy.id())).as("a store that lost a root is struck").isTrue();
        assertThat(StoreFleet.receiptHolders(writer, lost)).isEmpty();
    }

    @Test
    void aReplacementThatFindsNoFittingStoreWritesNoPinAndSaysThatNoStoreFits() throws Exception {
        // PDT 2 phase 5, owner ruling R1 of §10.11: a replacement after a strike names its stores or is not written,
        // so every signed pin names its stores and a copy of one charges its signer at most once per named store
        StoreFleet.Peer lossy = fleet.newPeer("lossy", 46, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer writer = fleet.newPeer("writer", 47, StoreFleet.Spec.member(), "lossy");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> !writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).isEmpty())).isTrue();
        String root = StoreFleet.facts(writer, "alone", 400, 14).root();
        writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofDays(1));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> StoreFleet.receiptHolders(writer, root).size() == 1)).isTrue();
        assertThat(writer.pdt().stores().unplaced()).isEmpty();
        for (String cid : lossy.store().reachable(root)) {
            lossy.store().backend().remove(cid);
        }

        assertThat(await(writer.pdt().challenge(root, lossy.id())).passed()).isFalse();
        assertThat(await(writer.pdt().challenge(root, lossy.id())).passed()).isFalse();
        assertThat(writer.pdt().stores().struck(lossy.id())).isTrue();
        fleet.settle(8);

        assertThat(writer.contentSpace().readAll(Template.of(SignedPin.class), 8))
                .as("no store but the struck one exists, so no replacement pin is written").isEmpty();
        assertThat(writer.pdt().stores().unplaced()).as("the requester hears that no store fits").containsExactly(root);
    }

    @Test
    void aStruckStoresRootsMoveWithinTheNeedToAStoreThatHoldsNoneOfThem() throws Exception {
        // PDT 2 phase 3, P2-S-07 and S3 of §6.4: the replacement pin dropped the requester's tier and the stores that
        // already hold the root, and only the challenged root moved, so the struck store's other roots kept one copy
        StoreFleet.Peer lossy = fleet.newPeer("lossy", 41, StoreFleet.Spec.store(StorePolicy.of(1L << 35).tier(StorePolicy.COLD)));
        StoreFleet.Peer big = fleet.newPeer("big", 42, StoreFleet.Spec.store(StorePolicy.of(1L << 40).tier(StorePolicy.COLD)), "lossy");
        StoreFleet.Peer hot = fleet.newPeer("hot", 43, StoreFleet.Spec.store(StorePolicy.of(1L << 42)), "lossy");
        StoreFleet.Peer writer = fleet.newPeer("writer", 44, StoreFleet.Spec.member(), "lossy");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).size() == 3)).isTrue();
        String lost = StoreFleet.facts(writer, "lost", 400, 11).root();
        String other = StoreFleet.facts(writer, "other", 400, 12).root();
        StoreChooser.Need cold = StoreChooser.Need.of(0, 2).tier(StorePolicy.COLD);
        writer.pdt().storeFor(lost, cold, Duration.ofDays(1));
        writer.pdt().storeFor(other, cold, Duration.ofDays(1));
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> StoreFleet.receiptHolders(writer, lost).size() == 2
                && StoreFleet.receiptHolders(writer, other).size() == 2)).isTrue();
        assertThat(StoreFleet.receiptHolders(writer, other)).containsExactlyInAnyOrder(lossy.id(), big.id());
        StoreFleet.Peer spare = fleet.newPeer("spare", 45, StoreFleet.Spec.store(StorePolicy.of(1L << 30).tier(StorePolicy.COLD)), "lossy");
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).size() == 4)).isTrue();
        java.util.Set<String> keep = lossy.store().reachable(other);
        for (String cid : lossy.store().reachable(lost)) {
            if (!keep.contains(cid)) {
                lossy.store().backend().remove(cid);
            }
        }

        assertThat(await(writer.pdt().challenge(lost, lossy.id())).passed()).isFalse();
        assertThat(await(writer.pdt().challenge(lost, lossy.id())).passed()).isFalse();
        assertThat(writer.pdt().stores().struck(lossy.id())).isTrue();
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> StoreFleet.receiptHolders(writer, lost).contains(spare.id())
                && StoreFleet.receiptHolders(writer, other).contains(spare.id())))
                .as("spare: " + spare.contentStore().status() + ", hot: " + hot.contentStore().status()).isTrue();
        assertThat(StoreFleet.receiptHolders(writer, lost)).containsExactlyInAnyOrder(big.id(), spare.id());
        assertThat(StoreFleet.receiptHolders(writer, other)).containsExactlyInAnyOrder(big.id(), spare.id());
        assertThat(hot.contentStore().ledger().holdings()).as("the hot store is outside the need").isEmpty();
    }
}
