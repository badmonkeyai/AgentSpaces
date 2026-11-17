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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Convergence adversary of phase 1 (srad/replication-by-entry.md) on whole replicas: three replicas of one space,
 * driven by random writes, takes, completions, cancels, renewals, clock steps and sweeps on each, with partial
 * anti-entropy between random pairs. With no control-character ids in play, the text digest and the audited set hold
 * the same lines once an acknowledged line ({@code a:} in the text) is read as the line itself, as the audited set
 * holds it, and the lines of a present entry whose lease lapsed are left out (RE-T-06, phase 4), so at every step two replicas' digests read that way are equal exactly when their stream hashes are: a
 * root equal while the digests differ (or the reverse) is a slot the trie and the text disagree on. Every replica's
 * live index must match its map after every step.
 *
 * <p>Acknowledgements come into play since phase 2 (RE-H-03): with sweeps and clock steps, a replica that saw a
 * shorter maximum lease collects a tombstone first and acknowledges another replica's re-offer of it. Until then this
 * test asserted that no acknowledgement ever appeared.
 *
 * <p>Full convergence is asserted only for a mix without take races between replicas, clock steps, sweeps, cancels or
 * renewals: with those in play the three replicas do not converge, and they do not with the code before phase 1
 * either (checked 2026-11-14 on a scratch copy of the module holding the pre-1b ReplicatedSpace, SpaceStateCrdt,
 * HashTrie and Slot: 6 of 6 seeds failed to converge), so that is not a phase 1 regression and is left out here.
 */
class AdversaryReplicaConvergenceTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zAdv", PeerIdentity.generate().peerId(), GroupId.of("zAdv"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "adversary",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace replica(String name) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(nodes.size() + 1).build();
        nodes.add(node);
        node.listen(network.register(name), name);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        return ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock).settleWindow(Duration.ZERO).build();
    }

    @ParameterizedTest
    @ValueSource(longs = {11, 12, 13, 14, 15, 16})
    void digestsAndRootsAgreeAtEveryStep(long seed) throws Exception {
        run(seed, 0x1FF, false, false);
    }

    /** Writes, takes and completions on replica a only, and exchanges: a full exchange converges all three. */
    @ParameterizedTest
    @ValueSource(longs = {21, 22, 23, 24})
    void aRaceFreeHistoryConvergesWithEqualRoots(long seed) throws Exception {
        run(seed, 0x18F, true, true);
    }

    private void run(long seed, int mask, boolean takeOne, boolean converge) throws Exception {
        Random random = new Random(seed);
        ReplicatedSpace[] r = {replica("a"), replica("b"), replica("c")};
        List<List<EntryHandle>> handles = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        List<List<TakenEntry<TaskEntry>>> taken = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        for (int step = 0; step < 400; step++) {
            int i = random.nextInt(3);
            int op = random.nextInt(9);
            if ((mask & (1 << op)) == 0) {
                op = 7;
            }
            if (takeOne && (op == 2 || op == 3)) {
                i = 0; // takes and completions on one replica only: no take race between replicas
            }
            ReplicatedSpace s = r[i];
            switch (op) {
                case 0, 1 -> handles.get(i).add(s.write(new TaskEntry("w-" + step, step),
                        Lease.of(Duration.ofSeconds(10 + random.nextInt(300)))));
                case 2 -> s.take(Template.of(TaskEntry.class), Lease.of(Duration.ofSeconds(20 + random.nextInt(60))),
                        Duration.ZERO).ifPresent(taken.get(i)::add);
                case 3 -> {
                    if (!taken.get(i).isEmpty()) {
                        TakenEntry<TaskEntry> t = taken.get(i).remove(random.nextInt(taken.get(i).size()));
                        try {
                            s.complete(t);
                        } catch (RuntimeException lost) {
                            // the take lapsed or lost its race: nothing to complete
                        }
                    }
                }
                case 4 -> {
                    if (!handles.get(i).isEmpty()) {
                        EntryHandle h = handles.get(i).remove(random.nextInt(handles.get(i).size()));
                        try {
                            if (random.nextBoolean()) {
                                h.cancel();
                            } else {
                                h.renew(Duration.ofSeconds(30 + random.nextInt(300)));
                            }
                        } catch (RuntimeException gone) {
                            // taken, completed or lapsed already
                        }
                    }
                }
                case 5 -> clock.advance(Duration.ofSeconds(random.nextInt(40)));
                case 6 -> s.sweepNow();
                default -> {
                    int j = (i + 1 + random.nextInt(2)) % 3;
                    exchange(r[i], r[j]);
                }
            }
            check(r, "seed " + seed + " step " + step);
        }
        // A full exchange: every pair both ways until nothing moves.
        for (int round = 0; round < 6; round++) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    if (i != j) {
                        exchange(r[i], r[j]);
                    }
                }
            }
        }
        check(r, "seed " + seed + " after the full exchange");
        if (!converge) {
            return;
        }
        assertThat(new String(r[1].digest())).as("b converged with a").isEqualTo(new String(r[0].digest()));
        assertThat(new String(r[2].digest())).as("c converged with a").isEqualTo(new String(r[0].digest()));
        assertThat(r[1].streamHash()).isEqualTo(r[0].streamHash());
        assertThat(r[2].streamHash()).isEqualTo(r[0].streamHash());
    }

    /**
     * A replica's text digest as the set of lines the audited set holds: an acknowledged line counts as the line it
     * acknowledges (annex I.6), so a replica that acknowledged a state and one that holds it read alike; and since
     * phase 4 (RE-T-06) a present entry whose lease lapsed by the replica's clock contributes neither its state line
     * nor its claim line, while the lines acknowledged for it stay. The lapsed entries are read from the states the
     * replica would offer, judged here by the shared clock, not from the trie's own mark.
     */
    private java.util.Set<String> audited(ReplicatedSpace replica) {
        java.util.Set<String> lapsed = new java.util.HashSet<>();
        byte[] all = replica.deltaFor(new byte[0]);
        if (all.length > 0) {
            SpaceWire.SyncDelta delta = ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec()
                    .fromBytes(all, SpaceWire.SyncDelta.class);
            for (SpaceWire.EntryStateDto dto : delta.states()) {
                ai.badmonkey.agentspaces.space.crdt.EntryState state = dto.toState();
                if (state.present() && state.lease().value().expiresAtMillis() <= clock.millis()) {
                    lapsed.add(dto.entryIdOf().value());
                }
            }
        }
        java.util.Set<String> lines = new java.util.TreeSet<>();
        for (String line : new String(replica.digest(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("a:")) {
                lines.add(line.substring(2));
            } else if (!lapsed.contains(line.substring(2, line.lastIndexOf('=')))) {
                lines.add(line);
            }
        }
        return lines;
    }

    /** One anti-entropy pull: {@code to} sends its digest and merges what {@code from} offers against it. */
    private static void exchange(ReplicatedSpace from, ReplicatedSpace to) {
        byte[] offer = from.deltaFor(to.digest());
        if (offer.length > 0) {
            to.applySyncDelta(offer);
        }
    }

    private void check(ReplicatedSpace[] r, String at) {
        for (int i = 0; i < r.length; i++) {
            assertThat(r[i].liveIndexMatchesForTest()).as("live index of %d at %s", i, at).isTrue();
        }
        for (int i = 0; i < r.length; i++) {
            for (int j = i + 1; j < r.length; j++) {
                boolean digestsEqual = audited(r[i]).equals(audited(r[j]));
                boolean rootsEqual = Arrays.equals(r[i].streamHash(), r[j].streamHash());
                assertThat(rootsEqual).as("roots of %d and %d equal exactly when their digests are, at %s", i, j, at)
                        .isEqualTo(digestsEqual);
            }
        }
    }
}
