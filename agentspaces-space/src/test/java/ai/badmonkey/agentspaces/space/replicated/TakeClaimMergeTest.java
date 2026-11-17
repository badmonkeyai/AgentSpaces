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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The take-claim lattice (spec §7.4, TECH-SPEC §7.6): the total preference order
 * that makes LEASE_RACE and AUCTION arbitration deterministic fleet-wide, and the
 * property that the merge always returns one of its inputs.
 */
class TakeClaimMergeTest {

    private static final EntryId ENTRY = EntryId.of("11111111-2222-3333-4444-555555555555");
    private static final SpaceId SPACE = SpaceId.local("tasks");
    private static final AgentId ALICE = new AgentId(PeerId.of("zAaaaPeer1111"), "worker");
    private static final AgentId BOB = new AgentId(PeerId.of("zBbbbPeer1111"), "worker");

    private static TakeClaim claim(long epoch, HlcTimestamp stamp, AgentId holder,
                                   double bid, long expires) {
        return new TakeClaim(ENTRY, SPACE, epoch, stamp, holder, bid, expires);
    }

    private static HlcTimestamp at(long physical, int logical) {
        return new HlcTimestamp(physical, logical, "n");
    }

    /** Spec §7.4: a re-claim of a lapsed entry (higher epoch) supersedes any older generation. */
    @Test
    void aHigherEpochAlwaysWins() {
        TakeClaim older = claim(1, at(5, 0), ALICE, 0.0, 1_000);
        TakeClaim newer = claim(2, at(1, 0), BOB, 99.0, 500);

        assertThat(TakeClaim.merge(older, newer)).isSameAs(newer);
        assertThat(TakeClaim.merge(newer, older)).isSameAs(newer);
    }

    /** Spec §7.4 AUCTION: within an epoch the lower bid wins regardless of stamp. */
    @Test
    void withinAnEpochTheLowerBidWins() {
        TakeClaim cheap = claim(1, at(9, 0), ALICE, 2.0, 1_000);
        TakeClaim costly = claim(1, at(1, 0), BOB, 10.0, 1_000);

        assertThat(TakeClaim.merge(cheap, costly)).isSameAs(cheap);
        assertThat(TakeClaim.merge(costly, cheap)).isSameAs(cheap);
    }

    /** Spec §7.4 LEASE_RACE: among equal bids the lowest HLC stamp wins. */
    @Test
    void amongEqualBidsTheLowestStampWins() {
        TakeClaim earlier = claim(1, at(5, 0), BOB, 0.0, 1_000);
        TakeClaim laterPhysical = claim(1, at(6, 0), ALICE, 0.0, 1_000);
        TakeClaim laterLogical = claim(1, at(5, 1), ALICE, 0.0, 1_000);

        assertThat(TakeClaim.merge(earlier, laterPhysical)).isSameAs(earlier);
        assertThat(TakeClaim.merge(laterPhysical, earlier)).isSameAs(earlier);
        assertThat(TakeClaim.merge(earlier, laterLogical)).isSameAs(earlier);
        assertThat(TakeClaim.merge(laterLogical, earlier)).isSameAs(earlier);
    }

    /** Spec §7.4: identical stamps break the tie on the peer id, deterministically. */
    @Test
    void identicalStampsBreakTheTieOnTheHolderId() {
        TakeClaim alice = claim(1, at(5, 0), ALICE, 0.0, 1_000);
        TakeClaim bob = claim(1, at(5, 0), BOB, 0.0, 1_000);
        TakeClaim expected = ALICE.encoded().compareTo(BOB.encoded()) <= 0 ? alice : bob;

        assertThat(TakeClaim.merge(alice, bob)).isSameAs(expected);
        assertThat(TakeClaim.merge(bob, alice)).isSameAs(expected);
    }

    /** Spec §7.2 renewal: the same holder's later expiry survives, which is how a take lease renews. */
    @Test
    void theSameHoldersLaterExpiryWins() {
        TakeClaim original = claim(1, at(5, 0), ALICE, 0.0, 1_000);
        TakeClaim renewed = claim(1, at(5, 0), ALICE, 0.0, 5_000);

        assertThat(TakeClaim.merge(original, renewed)).isSameAs(renewed);
        assertThat(TakeClaim.merge(renewed, original)).isSameAs(renewed);
        assertThat(TakeClaim.merge(renewed, renewed)).isSameAs(renewed);
    }

    /** TECH-SPEC §7.6: the merge returns an input, so a claim's signature survives every merge. */
    @Test
    void mergeAlwaysReturnsOneOfItsInputsAndHandlesNulls() {
        TakeClaim a = claim(1, at(5, 0), ALICE, 0.0, 1_000);
        TakeClaim b = claim(1, at(4, 0), BOB, 0.0, 1_000);
        List<TakeClaim> inputs = List.of(a, b);

        assertThat(inputs).contains(TakeClaim.merge(a, b));
        assertThat(TakeClaim.merge(a, b)).isSameAs(TakeClaim.merge(b, a));
        assertThat(TakeClaim.merge(null, a)).isSameAs(a);
        assertThat(TakeClaim.merge(a, null)).isSameAs(a);
        assertThat(TakeClaim.merge(null, null)).isNull();
    }

    /** Spec §7.2: a TAKE hold lapses at exactly its expiry instant (inclusive). */
    @Test
    void expiryIsInclusiveAtTheBoundary() {
        TakeClaim hold = claim(1, at(5, 0), ALICE, 0.0, 10_000);

        assertThat(hold.expired(9_999)).isFalse();
        assertThat(hold.expired(10_000)).isTrue();
    }

    /** Spec §7.4: hostile field values are refused at construction, not at merge. */
    @Test
    void invalidClaimsAreRefusedAtConstruction() {
        assertThatThrownBy(() -> claim(0, at(5, 0), ALICE, 0.0, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> claim(1, at(5, 0), ALICE, Double.NaN, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> claim(1, at(5, 0), ALICE, Double.POSITIVE_INFINITY, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static SpaceWire.SignedClaim signed(TakeClaim claim, int mark) {
        return new SpaceWire.SignedClaim(claim, new byte[] {(byte) mark}, new byte[] {(byte) mark});
    }

    private static void permutations(List<SpaceWire.SignedClaim> rest, List<SpaceWire.SignedClaim> prefix,
                                     List<List<SpaceWire.SignedClaim>> out) {
        if (rest.isEmpty()) {
            out.add(prefix);
            return;
        }
        for (int i = 0; i < rest.size(); i++) {
            List<SpaceWire.SignedClaim> next = new ArrayList<>(rest);
            List<SpaceWire.SignedClaim> grown = new ArrayList<>(prefix);
            grown.add(next.remove(i));
            permutations(next, grown, out);
        }
    }

    /**
     * P3 (reviews/RBE-REGRESSION-MATRIX.md §3): a batch of contending claims for one entry, merged one after another in
     * any order, as a replica merges a PULL_RESP's claim into the one it holds and then the next batch's, ends on the
     * claim the pairwise merges of live gossip pick, under AUCTION bids and under LEASE_RACE stamps (a stamp tie
     * included), and the signed claim kept is the winner's own proof.
     */
    @Test
    void aBatchOfContendingClaimsFoldsToTheSameWinnerInEveryOrder() {
        List<SpaceWire.SignedClaim> auction = List.of(
                signed(claim(1, at(3, 0), ALICE, 9.0, 1_000), 1),
                signed(claim(1, at(8, 0), BOB, 2.0, 1_000), 2),
                signed(claim(1, at(6, 0), ALICE, 2.0, 1_000), 3),
                signed(claim(1, at(1, 0), BOB, 4.0, 1_000), 4));
        List<SpaceWire.SignedClaim> leaseRace = List.of(
                signed(claim(1, at(7, 0), ALICE, 0.0, 1_000), 5),
                signed(claim(1, at(5, 2), BOB, 0.0, 1_000), 6),
                signed(claim(1, at(5, 2), ALICE, 0.0, 1_000), 7),
                signed(claim(1, at(9, 0), BOB, 0.0, 1_000), 8));
        for (List<SpaceWire.SignedClaim> batch : List.of(auction, leaseRace)) {
            TakeClaim live = null;
            for (SpaceWire.SignedClaim signed : batch) {
                live = TakeClaim.merge(live, signed.claim());
            }
            List<List<SpaceWire.SignedClaim>> orders = new ArrayList<>();
            permutations(batch, List.of(), orders);
            assertThat(orders).hasSize(24);
            for (List<SpaceWire.SignedClaim> order : orders) {
                SpaceWire.SignedClaim held = null;
                for (SpaceWire.SignedClaim offered : order) {
                    held = held == null ? offered : SpaceWire.SignedClaim.merge(held, offered);
                }
                assertThat(held.claim()).as("order %s", order).isSameAs(live);
                SpaceWire.SignedClaim winner = held;
                assertThat(batch).as("the proof travels with its claim")
                        .anySatisfy(input -> assertThat(input).isSameAs(winner));
            }
        }
        assertThat(TakeClaim.merge(auction.get(1).claim(), auction.get(2).claim()))
                .as("AUCTION: the lower stamp breaks a tie of bids").isSameAs(auction.get(2).claim());
    }
}
