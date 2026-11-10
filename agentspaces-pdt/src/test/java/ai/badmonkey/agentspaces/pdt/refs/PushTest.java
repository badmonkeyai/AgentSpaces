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

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Eager push (PDT 2 phase 5, group P; srad/agentspaces-pdt2.md §3.4, S5 of §6.4): a writer with push on publishes a
 * delta above the inline bound as {@link PushBundle} rumours on the ref's stream {@code pdt:<space>:<name>}, and
 * every full follower with push on verifies the nodes against the head's root, stores them, and sends no request.
 * A follower whose bundles did not complete the root within the grace falls back to {@code TREE_WANT} (P2-P-02,
 * P2-Q-REL-04).
 */
class PushTest {

    private static final String NAME = "plans";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static RefFleet.Spec pushing() {
        return RefFleet.Spec.defaults().pdt(b -> b.push(PushPublisher.Config.on()));
    }

    /** A writer and {@code followers} full followers, every one with push on, in one settled group. */
    private List<RefFleet.Peer> group(int followers) throws Exception {
        List<RefFleet.Peer> peers = new ArrayList<>();
        peers.add(fleet.newPeer("w", 100, pushing()));
        for (int i = 0; i < followers; i++) {
            peers.add(fleet.newPeer("f" + i, 101 + i, pushing(), "w"));
        }
        fleet.settle(10);
        for (RefFleet.Peer p : peers) {
            p.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        }
        return peers;
    }

    /** A value whose delta from {@code base} is about {@code bytes} bytes of fresh entries. */
    private static PMap<String, String> grow(PMap<String, String> base, int bytes, long seed) {
        Random random = new Random(seed);
        PMap<String, String> out = base;
        for (int i = 0; i * 120 < bytes; i++) {
            out = out.assoc("plan-" + seed + "-" + i, RefFleet.value(100, random));
        }
        return out;
    }

    /** The value without the entries {@link #grow} added for {@code bytes} and {@code seed}. */
    private static PMap<String, String> drop(PMap<String, String> base, int bytes, long seed) {
        PMap<String, String> out = base;
        for (int i = 0; i * 120 < bytes; i++) {
            out = out.dissoc("plan-" + seed + "-" + i);
        }
        return out;
    }

    private static long fetched(List<RefFleet.Peer> followers) {
        return followers.stream().mapToLong(p -> p.pdt().stats().deltasFetched()).sum();
    }

    @Test
    void aDeltaAboveTheInlineBoundReachesEveryFullFollowerByPushWithNoTreeWant() throws Exception {
        List<RefFleet.Peer> peers = group(20);
        RefFleet.Peer writer = peers.get(0);
        List<RefFleet.Peer> followers = peers.subList(1, peers.size());
        Ref<PMap<String, String>> ref = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        // small heads from every member settle the Plumtree, as a fleet's own traffic does: a member prunes the edges
        // its duplicates arrive over, and the origin's own edges only when it relays another member's item
        for (int i = 0; i < 3; i++) {
            for (RefFleet.Peer p : peers) {
                int n = i;
                p.pdt().mapRef("warm-" + p.name(), RefFleet.STR, RefFleet.STR).swap(m -> m.assoc("n", Integer.toString(n)));
            }
            fleet.settle(12);
        }
        PMap<String, String> small = ref.swap(m -> m.assoc("warm", "x"));
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> fleet.allHold(small.root()))).isTrue();
        AtomicLong sent = new AtomicLong();
        for (RefFleet.Peer f : followers) {
            fleet.network.tap("w", f.name(), frame -> sent.addAndGet(frame.length));
        }

        PMap<String, String> big = ref.swap(m -> grow(m, 200_000, 7));
        assertThat(writer.pdt().refs().pushStats().bytesPushed()).as("the delta passes the inline bound")
                .isGreaterThan(PRef.INLINE_DELTA_BYTES);
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> fleet.allHold(big.root()))).as("every follower holds the root").isTrue();

        assertThat(fetched(followers)).as("P2-P-03: zero TREE_WANTs").isZero();
        assertThat(writer.content().stats().nodesServed()).as("the writer served no node").isZero();
        PushPublisher.Stats pushed = writer.pdt().refs().pushStats();
        assertThat(pushed.deltasPushed()).isEqualTo(1);
        assertThat(pushed.bundlesSent()).isEqualTo((pushed.bytesPushed() + PushBundle.MAX_BYTES - 1) / PushBundle.MAX_BYTES);
        long received = followers.stream().mapToLong(p -> p.pdt().refs().pushStats().bytesStored()).sum();
        assertThat(received).as("each follower stored the delta once").isEqualTo(20 * pushed.bytesPushed());
        long copies = writer.runtime().gossip().rumorsSentByStream().getOrDefault(PushReceiver.stream(RefFleet.REF_SPACE, NAME), 0L);
        System.out.println("P2-Q-PRF-04 measured: delta " + pushed.bytesPushed() + " bytes in " + pushed.bundlesSent()
                + " bundles, " + copies + " copies sent, writer sent " + sent.get() + " bytes to 20 followers ("
                + String.format("%.2f", sent.get() / (double) pushed.bytesPushed()) + " x)");
        // P2-P-03: the writer sends each bundle to each of its eager peers once, and the tree carries it on. How many
        // eager peers the writer keeps is the Plumtree's choice: 1 to 10 of 20 in the runs of §10.12, so P2-Q-PRF-04
        // (at most 3) is measured there and in PdtStress push, and needs the owner's ruling
        assertThat(copies).as("each bundle at most once to each follower").isLessThanOrEqualTo(20 * pushed.bundlesSent());
        assertThat(sent.get()).as("every frame the writer sent while it pushed: the copies, their envelopes, and the head")
                .isLessThanOrEqualTo((long) (copies * pushed.bytesPushed() * 1.05) + 20 * 16 * 1024);
    }

    @Test
    void aMemberThatDoesNotFollowTheRefRelaysEachBundleItReceivesAndStoresNothing() throws Exception {
        // §10.4, phase 5: GossipBus carries a stream's rumours to every member, so a member that does not follow
        // the ref fully receives bundles it never reads; this measures what that costs it
        List<RefFleet.Peer> peers = group(10);
        List<RefFleet.Peer> idle = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            idle.add(fleet.newPeer("idle" + i, 150 + i, pushing(), "w"));
        }
        fleet.settle(10);
        for (int i = 0; i < 3; i++) {
            for (RefFleet.Peer p : fleet.peers) {
                int n = i;
                p.pdt().mapRef("warm-" + p.name(), RefFleet.STR, RefFleet.STR).swap(m -> m.assoc("n", Integer.toString(n)));
            }
            fleet.settle(12);
        }
        AtomicLong toIdle = new AtomicLong();
        for (RefFleet.Peer from : fleet.peers) {
            for (RefFleet.Peer to : idle) {
                if (from != to) {
                    fleet.network.tap(from.name(), to.name(), frame -> toIdle.addAndGet(frame.length));
                }
            }
        }
        RefFleet.Peer writer = peers.get(0);
        PMap<String, String> big = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 200_000, 9));
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> peers.stream().allMatch(p -> p.store().complete(big.root())))).isTrue();
        fleet.settle(8);
        long delta = writer.pdt().refs().pushStats().bytesPushed();
        for (RefFleet.Peer p : idle) {
            assertThat(p.store().has(big.root())).as("a member that does not follow the ref stores nothing").isFalse();
            assertThat(p.pdt().refs().pushStats().bundlesReceived()).isZero();
        }
        System.out.println("§10.4 relay cost measured: 4 members that do not follow the ref received " + toIdle.get()
                + " bytes for a pushed delta of " + delta + " bytes (" + String.format("%.2f", toIdle.get() / (4.0 * delta))
                + " x the delta each)");
        assertThat(toIdle.get()).as("each relays at most about one copy and a duplicate").isLessThanOrEqualTo(4 * 2 * delta + 4 * 64 * 1024);
    }

    @Test
    void followersThatLoseBundlesFallBackToTreeWantAndEveryFollowerConverges() throws Exception {
        List<RefFleet.Peer> peers = group(20);
        RefFleet.Peer writer = peers.get(0);
        List<RefFleet.Peer> followers = peers.subList(1, peers.size());
        // P2-Q-REL-04: one bundle in ten is lost at its receiver
        AtomicInteger seen = new AtomicInteger();
        for (RefFleet.Peer f : followers) {
            PushReceiver receiver = f.pdt().refs().pushReceiver();
            f.runtime().gossip().replaceHandler(PushReceiver.stream(RefFleet.REF_SPACE, NAME), (from, id, payload) -> {
                if (seen.incrementAndGet() % 10 != 0) {
                    receiver.onBundle(PushReceiver.stream(RefFleet.REF_SPACE, NAME), from, payload);
                }
            });
        }
        Ref<PMap<String, String>> ref = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        List<String> roots = new ArrayList<>();
        // P2-Q-REL-04 asks for 100 deltas (PDT 2 phase 5 gate; the build ran 10). Each round replaces the entries of the
        // round before, so the value stays near one delta and the harness's walks of it stay short
        for (int round = 0; round < 100; round++) {
            int r = round;
            PMap<String, String> next = ref.swap(m -> grow(r == 0 ? m : drop(m, 40_000 + 15_000 * ((r - 1) % 3), 19 + r),
                    40_000 + 15_000 * (r % 3), 20 + r));
            roots.add(next.root());
            assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> fleet.allHold(next.root())))
                    .as("round " + round + ": every follower converges").isTrue();
        }
        assertThat(fetched(followers)).as("the followers that lost a bundle fetched the rest").isPositive();
        long fallbacks = followers.stream().mapToLong(p -> p.pdt().refs().pushStats().fallbacks()).sum();
        assertThat(fetched(followers)).as("every fetch is a fallback from an announced push").isLessThanOrEqualTo(fallbacks);
        for (RefFleet.Peer f : followers) {
            assertThat(f.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).deref().root()).isEqualTo(roots.get(roots.size() - 1));
        }
    }

    @Test
    void aWriterWithoutPushAndADeltaOverTheMaximumArePullOnly() throws Exception {
        RefFleet.Peer writer = fleet.newPeer("w", 200, RefFleet.Spec.defaults()
                .pdt(b -> b.push(new PushPublisher.Config(true, 2L << 20, 64 * 1024, Duration.ofSeconds(2)))));
        RefFleet.Peer follower = fleet.newPeer("f", 201, pushing(), "w");
        fleet.settle(10);
        follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> ref = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> big = ref.swap(m -> grow(m, 150_000, 31));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> fleet.allHold(big.root())
                && follower.pdt().stats().deltasFetched() == 1)).isTrue();
        assertThat(writer.pdt().refs().pushStats().deltasPullOnly()).as("over max-bytes: pull-only").isEqualTo(1);
        assertThat(writer.pdt().refs().pushStats().bundlesSent()).isZero();
        assertThat(follower.pdt().stats().deltasFetched()).as("the follower fetched at once").isEqualTo(1);
        assertThat(follower.pdt().refs().pushStats().fallbacks()).as("no push was announced, so no fallback").isZero();
    }
}
