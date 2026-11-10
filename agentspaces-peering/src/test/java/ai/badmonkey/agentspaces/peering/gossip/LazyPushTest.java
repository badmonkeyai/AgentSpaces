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
package ai.badmonkey.agentspaces.peering.gossip;

import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lazy push (PERF2 §3.4, Plumtree) on one bus with a scripted sampler: eager push to every
 * capable member, the random fan-out to the rest, a duplicate prunes the sender, a HAVE for a
 * missing item turns into a GRAFT, and a GRAFT is answered from the recent cache.
 */
class LazyPushTest {

    private record Sent(PeerId to, String kind, Object body, long at) { }

    private final PeerId p1 = PeerIdentity.generate().peerId();
    private final PeerId p2 = PeerIdentity.generate().peerId();
    private final PeerId p3 = PeerIdentity.generate().peerId();
    private final PeerId p4 = PeerIdentity.generate().peerId();
    private final PeerId legacy1 = PeerIdentity.generate().peerId();
    private final PeerId legacy2 = PeerIdentity.generate().peerId();
    private final Set<PeerId> capable = Set.of(p1, p2, p3, p4);
    private final List<PeerId> members = List.of(p1, legacy1, p2, p3, legacy2, p4);
    private final PeerSampler sampler = n -> members.size() <= n ? members : members.subList(0, n);
    private final List<Sent> sent = new ArrayList<>();
    private final List<String> delivered = new ArrayList<>();
    /** The node's clock: a wait reads it (swarm retries), so a test moves it by hand. */
    private final long[] clock = {0};
    private final GossipBus.FrameSender sender = new GossipBus.FrameSender() {
        @Override public void sendRumor(PeerId to, Bodies.Rumor rumor) { sent.add(new Sent(to, "RUMOR", rumor, clock[0])); }
        @Override public void sendDigest(PeerId to, Bodies.Digest digest) { sent.add(new Sent(to, "DIGEST", digest, clock[0])); }
        @Override public void sendPullResp(PeerId to, Bodies.PullResp resp) { sent.add(new Sent(to, "PULL_RESP", resp, clock[0])); }
        @Override public void sendHave(PeerId to, Bodies.Have have) { sent.add(new Sent(to, "HAVE", have, clock[0])); }
        @Override public void sendGraft(PeerId to, Bodies.Graft graft) { sent.add(new Sent(to, "GRAFT", graft, clock[0])); }
        @Override public void sendPrune(PeerId to, Bodies.Prune prune) { sent.add(new Sent(to, "PRUNE", prune, clock[0])); }
    };
    private final GossipBus bus = new GossipBus(sampler, sender, CborCodec.defaultCodec(), 3, 6, () -> clock[0],
            new java.util.Random(7));

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static Bodies.Rumor rumor(String text) {
        return new Bodies.Rumor("news", text, 6, bytes(text));
    }

    private List<PeerId> sentTo(String kind) {
        return sent.stream().filter(f -> f.kind().equals(kind)).map(Sent::to).toList();
    }

    /** Ticks every 250 ms of the clock until it reads {@code millis}, as the node's scheduler does. */
    private void runUntil(long millis) {
        while (clock[0] + 250 <= millis) {
            clock[0] += 250;
            bus.tick(clock[0]);
        }
    }

    private List<Long> graftTimes() {
        return sent.stream().filter(f -> f.kind().equals("GRAFT")).map(Sent::at).toList();
    }

    private static Bodies.Have have(byte[] hash) {
        return new Bodies.Have(List.of(new Bodies.HaveItem("news", hash)));
    }

    private void enable() {
        bus.lazyPush(capable::contains);
        bus.onStream("news", (from, id, payload) -> delivered.add(new String(payload, StandardCharsets.UTF_8)));
    }

    @Test
    void aPublishPushesToEveryCapableMemberAndFansOutToTheRestThenAnnouncesToLazyMembersNextTick() {
        enable();
        bus.publish("news", "a", bytes("a"));
        assertThat(sentTo("RUMOR")).as("every capable member eagerly, and the fan-out of three covers both legacy members")
                .containsExactlyInAnyOrder(p1, p2, p3, p4, legacy1, legacy2);
        bus.tick(1_000);
        assertThat(sentTo("HAVE")).as("nobody is lazy: everyone got the payload, nothing to announce").isEmpty();
        byte[] any = Digests.sha256(bytes("a"));
        bus.onPrune(p2, new Bodies.Prune("news", any));
        bus.onPrune(p3, new Bodies.Prune("news", any));
        bus.onPrune(p4, new Bodies.Prune("news", any));
        bus.onPrune(p1, new Bodies.Prune("news", any));
        sent.clear();
        bus.publish("news", "b", bytes("b"));
        assertThat(sentTo("RUMOR")).as("only the legacy fan-out: every capable member is lazy now")
                .containsExactlyInAnyOrder(legacy1, legacy2);
        bus.tick(1_250);
        assertThat(sentTo("HAVE")).as("every pruned member hears the announcement on the next tick").hasSize(4)
                .allSatisfy(to -> assertThat(capable).contains(to));
        Bodies.Have have = (Bodies.Have) sent.stream().filter(f -> f.kind().equals("HAVE")).findFirst().orElseThrow().body();
        assertThat(have.items()).hasSize(1);
        assertThat(have.items().get(0).streamId()).isEqualTo("news");
        assertThat(have.items().get(0).hash()).isEqualTo(Digests.sha256(bytes("b")));
        sent.clear();
        bus.tick(1_500);
        assertThat(sentTo("HAVE")).as("nothing new to announce").isEmpty();
    }

    @Test
    void aDuplicateDeliveredPointToPointOrOnADirectStreamNeverPrunes() {
        enable();
        bus.directDelivery("peers");
        bus.onRumor(p1, rumor("x"));
        bus.onRumor(p2, new Bodies.Rumor("news", "x", 1, bytes("x")));
        assertThat(sentTo("PRUNE")).as("a hop-budget-one copy is an introduction or a graft answer, not a tree push").isEmpty();
        bus.onRumor(p3, new Bodies.Rumor("peers", "ad", 6, bytes("ad")));
        bus.onRumor(p4, new Bodies.Rumor("peers", "ad", 5, bytes("ad")));
        assertThat(sentTo("PRUNE")).as("the peers stream is introduced directly and flooded on purpose").isEmpty();
        assertThat(bus.lazyPeers()).isEmpty();
        bus.onRumor(p2, rumor("x"));
        assertThat(sentTo("PRUNE")).as("a forwarded duplicate on an ordinary stream still prunes").containsExactly(p2);
    }

    @Test
    void aDuplicateFromACapableMemberPrunesItAndAFirstCopyFromItRestoresIt() {
        enable();
        bus.onRumor(p1, rumor("x"));
        assertThat(delivered).containsExactly("x");
        assertThat(sentTo("RUMOR")).as("forwarded along the tree only: a hint-less member with an endpoint of its "
                + "own had its chance at the origin and gets the rest by anti-entropy (PERF2 §3 item 4)")
                .containsExactlyInAnyOrder(p2, p3, p4);
        sent.clear();
        bus.onRumor(p2, rumor("x"));
        assertThat(delivered).as("a duplicate is not delivered twice").containsExactly("x");
        assertThat(sentTo("PRUNE")).as("the redundant edge is pruned").containsExactly(p2);
        assertThat(bus.lazyPeers()).containsExactly(p2);
        sent.clear();
        bus.onRumor(legacy1, rumor("x"));
        assertThat(sentTo("PRUNE")).as("a legacy member cannot be pruned; it does not understand the frame").isEmpty();
        bus.publish("news", "y", bytes("y"));
        assertThat(sentTo("RUMOR")).as("the lazy member gets no payload").doesNotContain(p2)
                .contains(p1, p3, p4);
        sent.clear();
        bus.onRumor(p2, new Bodies.Rumor("news", "z1", 1, bytes("z1")));
        assertThat(bus.lazyPeers()).as("a point-to-point copy does not restore the edge").containsExactly(p2);
        bus.onRumor(p2, rumor("z"));
        assertThat(bus.lazyPeers()).as("a first copy pushed over the pruned edge makes it eager again").isEmpty();
    }

    @Test
    void anAnnouncedItemThatNeverArrivesIsGraftedAfterAWaitInTimeAndAnAnnouncementOfASeenItemIsIgnored() {
        enable();
        byte[] hash = Digests.sha256(bytes("missing"));
        bus.onPrune(p3, new Bodies.Prune("news", hash));
        assertThat(bus.lazyPeers()).containsExactly(p3);
        bus.onHave(p3, have(hash));
        runUntil(GossipBus.GRAFT_BASE_MILLIS - 1);
        assertThat(sentTo("GRAFT")).as("not yet: the eager copy may still be on its way").isEmpty();
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).as("the only announcer is asked once the spread wait ends").containsExactly(p3);
        assertThat(bus.lazyPeers()).as("asking makes the announcer eager").isEmpty();
        Bodies.Graft graft = (Bodies.Graft) sent.get(sent.size() - 1).body();
        assertThat(graft.streamId()).isEqualTo("news");
        assertThat(graft.hash()).isEqualTo(hash);
        bus.onRumor(p3, rumor("missing"));
        assertThat(delivered).containsExactly("missing");
        sent.clear();
        bus.onHave(p1, have(hash));
        runUntil(clock[0] + 20_000);
        assertThat(sentTo("GRAFT")).as("an announcement of a seen item is ignored").isEmpty();
    }

    /**
     * Swarm retries, A1 (ROBUSTNESS.md §10.4, test 1): every member hears the origin's announcement first, so the
     * first announcer heard is almost always the origin, and a member with another announcer queued asks that one.
     */
    @Test
    void theFirstAskGoesToAnAnnouncerOtherThanTheFirstHeard() {
        enable();
        for (int i = 0; i < 20; i++) {
            byte[] hash = Digests.sha256(bytes("item-" + i));
            bus.onHave(p3, have(hash));
            bus.onHave(p4, have(hash));
        }
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).as("every item is asked of p4, never of p3, the first heard").hasSize(20)
                .containsOnly(p4);
        assertThat(bus.repairCounters().get("GRAFT_FIRST_HEARD")).isZero();
    }

    /**
     * Swarm retries, the gate's adversarial review (ROBUSTNESS.md §10.6): a member that announces items quickly and
     * answers none of its asks would draw the first ask of every item it announced second. Once its asks went
     * unanswered, a member asks the announcers that answered first, so a refusing announcer costs no wait.
     */
    @Test
    void anAnnouncerThatLeftItsAsksUnansweredIsAskedAfterTheOthers() {
        enable();
        for (int i = 0; i < 4; i++) {
            byte[] hash = Digests.sha256(bytes("refused-" + i));
            bus.onHave(p1, have(hash));
            bus.onHave(p3, have(hash));
        }
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).as("p3 is the only announcer besides the first heard").hasSize(4).containsOnly(p3);
        for (int i = 0; i < 4; i++) {
            bus.onRumor(p4, rumor("refused-" + i)); // p3 never answered; the items came from p4
        }
        sent.clear();
        long now = clock[0];
        for (int i = 0; i < 10; i++) {
            byte[] hash = Digests.sha256(bytes("next-" + i));
            bus.onHave(p1, have(hash));
            bus.onHave(p3, have(hash));
            bus.onHave(p4, have(hash));
        }
        // the first round's copies came 1.75 s after their announcement, which widens the spread (adaptive wait, §10.9)
        runUntil(now + GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_MAX_MILLIS + 250);
        java.util.Map<String, PeerId> firstAsked = new java.util.LinkedHashMap<>();
        sent.stream().filter(f -> f.kind().equals("GRAFT")).forEach(f -> firstAsked.putIfAbsent(
                java.util.HexFormat.of().formatHex(((Bodies.Graft) f.body()).hash()), f.to()));
        assertThat(firstAsked).as("every item asked for").hasSize(10);
        assertThat(firstAsked.values()).as("every first ask goes to p4, which left no ask unanswered").containsOnly(p4);
    }

    /**
     * Swarm retries, sliding window, the gate (ROBUSTNESS.md §10.11): three members that announce every item just after
     * its honest holders and answer no ask cannot take the first ask of every item. The window of the three announcers
     * heard last held only them, so each item's first three asks could go unanswered. A member asks them only after the
     * announcers that answer once their asks went unanswered.
     */
    @Test
    void threeAnnouncersHeardLastThatNeverAnswerAreAskedAfterTheHolders() {
        PeerId origin = PeerIdentity.generate().peerId();
        PeerId holder = PeerIdentity.generate().peerId();
        List<PeerId> silent = List.of(PeerIdentity.generate().peerId(), PeerIdentity.generate().peerId(),
                PeerIdentity.generate().peerId());
        List<PeerId> view = new ArrayList<>(List.of(origin, holder));
        view.addAll(silent);
        GossipBus local = new GossipBus(n -> n >= view.size() ? view : view.subList(0, n), sender,
                CborCodec.defaultCodec(), 3, 6, () -> clock[0], new java.util.Random(7));
        local.lazyPush(view::contains);
        local.onStream("news", (from, id, payload) -> delivered.add(new String(payload, StandardCharsets.UTF_8)));
        for (int round = 0; round < 2; round++) {
            String prefix = round == 0 ? "early-" : "late-";
            for (int i = 0; i < 12; i++) {
                byte[] hash = Digests.sha256(bytes(prefix + i));
                local.onHave(origin, have(hash));
                local.onHave(holder, have(hash));
                silent.forEach(member -> local.onHave(member, have(hash)));
            }
            sent.clear();
            long until = clock[0] + GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_MAX_MILLIS + 250;
            while (clock[0] + 250 <= until) {
                clock[0] += 250;
                local.tick(clock[0]);
            }
            java.util.Map<String, PeerId> firstAsked = new java.util.LinkedHashMap<>();
            sent.stream().filter(f -> f.kind().equals("GRAFT")).forEach(f -> firstAsked.putIfAbsent(
                    java.util.HexFormat.of().formatHex(((Bodies.Graft) f.body()).hash()), f.to()));
            assertThat(firstAsked).as("every item asked for").hasSize(12);
            if (round == 1) {
                assertThat(firstAsked.values()).as("once their asks went unanswered, the silent members are asked last")
                        .containsOnly(holder);
            }
            for (int i = 0; i < 12; i++) {
                local.onRumor(holder, rumor(prefix + i)); // the silent members never answer; the holder's copy comes
            }
        }
    }

    /**
     * Swarm retries, sliding window, the gate (ROBUSTNESS.md §10.11): a member asks for an announced item only members
     * that announced it. The window as built let another member of the group slide in once the only announcer left an
     * ask unanswered, so a member that names the same items it never serves every second drew GRAFTs to members that
     * never held them, and each GRAFT made that member's edge to the asker eager again.
     */
    @Test
    void anItemOnlyOneMemberAnnouncesIsAskedOfThatMemberAlone() {
        enable();
        List<Bodies.HaveItem> items = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            items.add(new Bodies.HaveItem("news", Digests.sha256(bytes("named-" + i))));
        }
        for (int t = 0; t < 60; t++) { // 15 s, the same items named again every second
            if (t % 4 == 0) {
                bus.onHave(p1, new Bodies.Have(items));
            }
            clock[0] += 250;
            bus.tick(clock[0]);
        }
        assertThat(bus.repairCounters().get("GRAFT_RETRY")).as("the items were asked for again").isPositive();
        assertThat(sentTo("GRAFT")).as("every ask goes to the member that named the items").containsOnly(p1);
    }

    /**
     * Swarm retries, A1 (ROBUSTNESS.md §10.4): an eager copy that is merely late, here 600 ms after the announcement,
     * arrives before any ask, so no graft fetches a copy already on its way and no duplicate prunes a sound edge
     * (scenario 7 of §3).
     */
    @Test
    void aCopyMerelyLateArrivesBeforeAnyAsk() {
        enable();
        bus.onHave(p3, have(Digests.sha256(bytes("late"))));
        runUntil(600);
        bus.onRumor(p1, rumor("late"));
        runUntil(10_000);
        assertThat(sentTo("GRAFT")).as("no graft of a copy on its way").isEmpty();
        assertThat(sentTo("PRUNE")).isEmpty();
        assertThat(delivered).containsExactly("late");
    }

    /**
     * Swarm retries, A3 (ROBUSTNESS.md §10.3): after a stall the scheduler runs the missed ticks back to back, and a
     * wait counted in ticks would end within milliseconds; the wait reads the clock, so catch-up ticks change nothing.
     */
    @Test
    void catchUpTicksAfterAStallDoNotShortenTheWait() {
        enable();
        bus.onHave(p3, have(Digests.sha256(bytes("stalled"))));
        for (int i = 0; i < 12; i++) {
            bus.tick(clock[0]);
        }
        assertThat(sentTo("GRAFT")).as("twelve ticks in the same millisecond ask nothing").isEmpty();
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).containsExactly(p3);
    }

    /**
     * Swarm retries, adaptive wait (ROBUSTNESS.md §10.8 decision 1, §10.9): a node whose asks come back at once, and
     * whose tree copies never come after their announcement, asks within a tick of the next announcement. The fixed
     * wait asked 500 ms to 1.5 s after it, which on a fabric that delivers in microseconds delayed every repair by two
     * to six ticks. The repair window shrinks with the wait.
     */
    @Test
    void theFirstAskFollowsTheLatenessThisNodeObservesOnAFastFabric() {
        enable();
        for (int i = 0; i < 10; i++) {
            bus.onHave(p3, have(Digests.sha256(bytes("fast-" + i))));
            int asked = sentTo("GRAFT").size();
            while (sentTo("GRAFT").size() == asked) {
                clock[0] += 250;
                bus.tick(clock[0]);
            }
            bus.onRumor(p3, new Bodies.Rumor("news", "fast-" + i, 1, bytes("fast-" + i))); // answered at once
        }
        long announced = clock[0];
        int before = sentTo("GRAFT").size();
        bus.onHave(p3, have(Digests.sha256(bytes("next"))));
        runUntil(announced + GossipBus.GRAFT_BASE_MILLIS);
        assertThat(graftTimes().subList(before, graftTimes().size())).as("asked within a tick and one spread of the floor")
                .singleElement().satisfies(at -> assertThat(at - announced).isLessThan(GossipBus.GRAFT_BASE_MILLIS));
        assertThat(bus.repairWindowMillis()).as("no lateness observed: no base, two spreads at their floor, one backoff")
                .isEqualTo(2 * GossipBus.GRAFT_SPREAD_MIN_MILLIS + GossipBus.GRAFT_BACKOFF_MILLIS);
    }

    /**
     * Swarm retries, adaptive wait (§10.9): tree copies that come 1.5 s after their announcement are lateness the node
     * observes, though no ask of its own repaired them, so it spreads its asks over twice that. The fixed wait followed
     * the graft round trips alone and kept its 1 s window, so most of its asks fetched a copy already on its way.
     */
    @Test
    void treeCopiesThatComeLateAfterTheirAnnouncementWidenTheSpread() {
        enable();
        for (int i = 0; i < 10; i++) {
            long announced = clock[0];
            bus.onHave(p3, have(Digests.sha256(bytes("slow-tree-" + i))));
            runUntil(announced + 1_500);
            bus.onRumor(p1, rumor("slow-tree-" + i)); // the eager copy, 1.5 s after the announcement
        }
        assertThat(bus.repairWindowMillis()).as("the base at its ceiling, two spreads of twice 1.5 s, one backoff")
                .isEqualTo(GossipBus.GRAFT_BASE_MILLIS + 2 * 3_000 + GossipBus.GRAFT_BACKOFF_MILLIS);
    }

    /**
     * Swarm retries, A2 (ROBUSTNESS.md §10.4, test 3): a member that asked every announcer once asks again, backing off
     * 1, 2, 4, 8 s with a spread and going round the announcers again, while members still announce the item; it
     * stops once the item arrives.
     */
    @Test
    void anItemEveryAnnouncerWasAskedForIsAskedAgainWithBackoffUntilItArrives() {
        enable();
        byte[] hash = Digests.sha256(bytes("slow"));
        bus.onHave(p3, have(hash));
        bus.onHave(p4, have(hash));
        for (long t = 5_000; t <= 40_000; t += 5_000) {
            runUntil(t);
            bus.onHave(p4, have(hash)); // p4's probes keep carrying the item
        }
        List<Long> times = graftTimes();
        assertThat(times).as("asked again after both announcers were asked once").hasSizeGreaterThanOrEqualTo(6);
        assertThat(sentTo("GRAFT").subList(0, 2)).as("one round asks each announcer").containsExactlyInAnyOrder(p3, p4);
        for (int i = 1; i < times.size(); i++) {
            long gap = times.get(i) - times.get(i - 1);
            long backoff = Math.min(GossipBus.GRAFT_BACKOFF_MAX_MILLIS, GossipBus.GRAFT_BACKOFF_MILLIS << Math.min(i - 1, 10));
            assertThat(gap).as("ask %d waits its backoff", i).isGreaterThanOrEqualTo(backoff);
        }
        assertThat(bus.repairCounters().get("GRAFT_RETRY")).isEqualTo(times.size() - 1);
        bus.onRumor(p4, rumor("slow"));
        int asked = times.size();
        runUntil(clock[0] + 30_000);
        assertThat(graftTimes()).as("nothing more once the item arrived").hasSize(asked);
    }

    /**
     * Swarm retries (ROBUSTNESS.md §10.2): an item no member announces any more is given up once its last
     * announcement is older than the horizon, so an announcer that never serves costs a bounded number of asks.
     */
    @Test
    void anItemNoMemberAnnouncesAnyMoreIsDroppedAfterTheHorizon() {
        enable();
        bus.onHave(p3, have(Digests.sha256(bytes("gone"))));
        runUntil(GossipBus.MISSING_HORIZON_MILLIS + GossipBus.GRAFT_BACKOFF_MAX_MILLIS * 3);
        List<Long> times = graftTimes();
        assertThat(times).isNotEmpty();
        assertThat(times.get(times.size() - 1)).as("no ask past the horizon and one backoff")
                .isLessThanOrEqualTo(GossipBus.MISSING_HORIZON_MILLIS + 2 * GossipBus.GRAFT_BACKOFF_MAX_MILLIS);
        assertThat(bus.repairCounters().get("GRAFT_DROPPED")).isEqualTo(1);
    }

    /**
     * Swarm retries (ROBUSTNESS.md §10.2): a graft whose item then arrives from another member anyway counts as
     * premature, once.
     */
    @Test
    void aGraftWhoseItemCameFromAnotherMemberCountsAsPremature() {
        enable();
        bus.onHave(p3, have(Digests.sha256(bytes("raced"))));
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).containsExactly(p3);
        bus.onRumor(p1, rumor("raced"));
        bus.onRumor(p3, new Bodies.Rumor("news", "raced", 5, bytes("raced")));
        assertThat(bus.repairCounters().get("GRAFT_PREMATURE")).isEqualTo(1);
    }

    /**
     * Swarm retries, the gate's adversarial review (ROBUSTNESS.md §10.6): only the copy that repairs an item gives a
     * round trip. A member that answers its asks late on purpose, after the item came from another member, repaired
     * nothing, so its late answers must not widen the spread of every later ask. The eager copies' own lateness after
     * the announcement does count (adaptive wait, §10.9), and the late answers add nothing to it.
     */
    @Test
    void aLateAnswerToAnItemThatArrivedAnywayLeavesTheSpreadAlone() {
        enable();
        for (int i = 0; i < 10; i++) {
            bus.onHave(p3, have(Digests.sha256(bytes("late-" + i))));
        }
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).hasSize(10).containsOnly(p3);
        for (int i = 0; i < 10; i++) {
            bus.onRumor(p1, rumor("late-" + i)); // the eager copy comes from another member first
        }
        long window = bus.repairWindowMillis();
        clock[0] += 3_000; // p3 answers three seconds later
        for (int i = 0; i < 10; i++) {
            bus.onRumor(p3, new Bodies.Rumor("news", "late-" + i, 5, bytes("late-" + i)));
        }
        assertThat(bus.repairWindowMillis()).as("no repair was observed, so the late answers leave the window").isEqualTo(window);
        assertThat(bus.repairCounters().get("GRAFT_PREMATURE")).as("each item's asks count once").isEqualTo(10);
    }

    /**
     * Swarm retries, the gate's adversarial review (ROBUSTNESS.md §10.6): once a member asked answers, a later copy
     * from anyone else is a duplicate of a repaired item, so the asks for it were not premature.
     */
    @Test
    void aDuplicateAfterAnAskedMemberAnsweredIsNotPremature() {
        enable();
        byte[] hash = Digests.sha256(bytes("two-asks"));
        bus.onHave(p3, have(hash));
        bus.onHave(p4, have(hash));
        long firstBy = GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250;
        runUntil(firstBy + GossipBus.GRAFT_BACKOFF_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).as("both announcers asked, the first heard last").containsExactly(p4, p3);
        bus.onRumor(p3, new Bodies.Rumor("news", "two-asks", 5, bytes("two-asks")));
        bus.onRumor(p1, rumor("two-asks"));
        assertThat(bus.repairCounters().get("GRAFT_PREMATURE")).isZero();
    }

    /**
     * Swarm retries, the gate's adversarial review (ROBUSTNESS.md §10.6): a flood of announcements pushes the oldest
     * items this node still wants out of its bounded memory, and each such give-up counts, so a flood shows on the
     * counters before anyone needs a trace.
     */
    @Test
    void anAnnouncedItemPushedOutOfTheBoundedMemoryCountsAsEvicted() {
        enable();
        List<Bodies.HaveItem> items = new ArrayList<>();
        for (int i = 0; i < GossipBus.MISSING_CAPACITY + 10; i++) {
            items.add(new Bodies.HaveItem("news", Digests.sha256(bytes("evicted-" + i))));
        }
        bus.onHave(p1, new Bodies.Have(items.subList(0, GossipBus.HAVE_ITEMS_MAX)));
        bus.onHave(p1, new Bodies.Have(items.subList(GossipBus.HAVE_ITEMS_MAX, items.size())));
        assertThat(bus.repairCounters().get("GRAFT_EVICTED")).isEqualTo(10);
    }

    /** A HAVE from {@code from} of {@code count} new items no member serves, named with {@code prefix}. */
    private void flood(PeerId from, String prefix, int count) {
        List<Bodies.HaveItem> items = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            items.add(new Bodies.HaveItem("news", Digests.sha256(bytes(prefix + i))));
        }
        bus.onHave(from, new Bodies.Have(items));
    }

    private List<byte[]> graftedHashes(PeerId to) {
        return sent.stream().filter(f -> f.kind().equals("GRAFT") && f.to().equals(to))
                .map(f -> ((Bodies.Graft) f.body()).hash()).toList();
    }

    /**
     * Swarm retries, flood shares (ROBUSTNESS.md §10.7): a flood of announcements every 250 ms pushed every item an honest
     * member announced out of the bounded memory before its first ask. Each announcer has its own share now, and a flood
     * pushes out the flooder's own oldest items, so the honest item is still asked for once its wait ends.
     */
    @Test
    void aFloodOfAnnouncementsPushesOutTheFloodersOwnItemsFirst() {
        enable();
        byte[] honest = Digests.sha256(bytes("honest"));
        bus.onHave(p3, have(honest));
        for (int round = 0; round < 3; round++) {
            runUntil(clock[0] + 250);
            flood(p1, "flood-" + round + "-", GossipBus.HAVE_ITEMS_MAX);
        }
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(graftedHashes(p3)).as("the honest item outlived three floods and was asked of its announcer")
                .singleElement().isEqualTo(honest);
        assertThat(bus.repairCounters().get("GRAFT_EVICTED_OWN")).as("each flood pushed out the flooder's own items")
                .isEqualTo(3L * GossipBus.HAVE_ITEMS_MAX - (GossipBus.MISSING_CAPACITY - 1));
        assertThat(bus.repairCounters().get("GRAFT_EVICTED_OTHER")).isZero();
        assertThat(bus.repairCounters().get("GRAFT_EVICTED")).isEqualTo(bus.repairCounters().get("GRAFT_EVICTED_OWN"));
    }

    /**
     * Swarm retries, flood shares (ROBUSTNESS.md §10.7): a flooder that names an honest item first must not carry it out
     * with its own flood. A share gives up an item another member named too only after the items it alone named.
     */
    @Test
    void aShareGivesUpAnItemAnotherMemberNamedTooLast() {
        enable();
        byte[] honest = Digests.sha256(bytes("named-first-by-the-flooder"));
        flood(p1, "before-", 10);
        bus.onHave(p1, have(honest));
        bus.onHave(p3, have(honest));
        runUntil(250);
        flood(p1, "flood-", GossipBus.HAVE_ITEMS_MAX);
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(graftedHashes(p3)).as("the item p3 named too outlived p1's flood and was asked of p3")
                .singleElement().isEqualTo(honest);
    }

    /**
     * Swarm retries, flood shares, the gate (ROBUSTNESS.md §10.7): a flooder that names an honest member's items just
     * before its flood must not carry them out with it. The bus moved half of them into the flooder's smaller share, and
     * the flood then pushed them out with the flooder's own items, though the honest member held ten items, far below an
     * even split.
     */
    @Test
    void aFlooderThatNamesAnHonestMembersItemsBeforeItsFloodCannotCarryThemOut() {
        enable();
        List<Bodies.HaveItem> honest = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            honest.add(new Bodies.HaveItem("news", Digests.sha256(bytes("honest-" + i))));
        }
        bus.onHave(p3, new Bodies.Have(honest));
        bus.onHave(p1, new Bodies.Have(honest));
        flood(p1, "flood-", GossipBus.HAVE_ITEMS_MAX);
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        List<String> asked = sent.stream().filter(f -> f.kind().equals("GRAFT"))
                .map(f -> java.util.HexFormat.of().formatHex(((Bodies.Graft) f.body()).hash())).toList();
        assertThat(asked).as("every item p3 named is asked for after p1's flood")
                .containsAll(honest.stream().map(item -> java.util.HexFormat.of().formatHex(item.hash())).toList());
        assertThat(bus.repairCounters().get("GRAFT_EVICTED_OWN")).as("the flood pushed out ten items of p1's share").isEqualTo(10);
    }

    /**
     * Swarm retries, flood shares (ROBUSTNESS.md §10.7): one announcer alone may fill the whole memory; another announcer
     * below an even split takes its room from the largest share, and the largest share then makes room from its own items.
     */
    @Test
    void anAnnouncerBelowAnEvenSplitTakesTheRoomFromTheLargestShare() {
        enable();
        flood(p1, "busy-", GossipBus.MISSING_CAPACITY);
        assertThat(bus.repairCounters().get("GRAFT_EVICTED")).as("one announcer alone fills the whole memory").isZero();
        byte[] other = Digests.sha256(bytes("other"));
        bus.onHave(p3, have(other));
        assertThat(bus.repairCounters().get("GRAFT_EVICTED_OTHER")).as("p1's oldest item made room for p3's").isEqualTo(1);
        flood(p1, "more-", 1);
        assertThat(bus.repairCounters().get("GRAFT_EVICTED_OWN")).as("p1 holds more than half, so its own item goes").isEqualTo(1);
        runUntil(10_000); // the first asks of p1's 254 older items go first, 16 a tick
        assertThat(graftedHashes(p3)).first().isEqualTo(other);
        assertThat(sent.stream().filter(f -> f.kind().equals("GRAFT") && f.to().equals(p1)).map(f -> ((Bodies.Graft) f.body()).hash()))
                .as("p1's two oldest items left the memory").doesNotContain(Digests.sha256(bytes("busy-0")), Digests.sha256(bytes("busy-1")));
    }

    /**
     * Swarm retries, the gate's adversarial review (ROBUSTNESS.md §10.6): the ring of graft round trips keeps working
     * once its sample count passes the int range, which a node grafting at its whole budget reaches in about a year.
     */
    @Test
    void theRoundTripRingKeepsWorkingPastTheIntRange() throws ReflectiveOperationException {
        enable();
        java.lang.reflect.Field ring = GossipBus.class.getDeclaredField("graftRtts");
        ring.setAccessible(true);
        Object rtts = ring.get(bus);
        java.lang.reflect.Field count = rtts.getClass().getDeclaredField("count");
        count.setAccessible(true);
        count.set(rtts, (long) Integer.MAX_VALUE);
        for (int i = 0; i < 3; i++) {
            bus.onHave(p3, have(Digests.sha256(bytes("wrap-" + i))));
        }
        // the ring now holds 64 round trips of zero, so the asks follow that lateness: within a tick and one spread
        runUntil(GossipBus.GRAFT_SPREAD_MIN_MILLIS + 250);
        assertThat(sentTo("GRAFT")).hasSize(3);
        for (int i = 0; i < 3; i++) {
            bus.onRumor(p3, rumor("wrap-" + i));
        }
        assertThat(delivered).hasSize(3);
        assertThat(bus.repairWindowMillis()).isBetween(2 * GossipBus.GRAFT_SPREAD_MIN_MILLIS
                + GossipBus.GRAFT_BACKOFF_MILLIS, GossipBus.GRAFT_BASE_MILLIS + 2 * GossipBus.GRAFT_SPREAD_MAX_MILLIS
                + GossipBus.GRAFT_BACKOFF_MILLIS);
    }

    /**
     * Asker-only repair (ROBUSTNESS.md §10.9): an answer that comes after the tree copy is a duplicate, and it prunes the
     * edge its GRAFT made though it carries a budget of one, as it did when it kept its rumor's budget. Without that
     * prune, every premature ask would leave one more eager edge behind.
     */
    @Test
    void anAnswerThatTheTreeCopyOvertookPrunesTheEdgeItsGraftMade() {
        enable();
        byte[] hash = Digests.sha256(bytes("overtaken"));
        bus.onPrune(p3, new Bodies.Prune("news", hash));
        bus.onHave(p3, have(hash));
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).containsExactly(p3);
        assertThat(bus.lazyPeers()).as("the ask made p3 eager").isEmpty();
        bus.onRumor(p1, rumor("overtaken")); // the tree copy comes first
        bus.onRumor(p3, new Bodies.Rumor("news", "overtaken", 1, bytes("overtaken"))); // then the answer
        assertThat(sentTo("PRUNE")).as("the edge the graft made goes").containsExactly(p3);
        assertThat(bus.lazyPeers()).containsExactly(p3);
        sent.clear();
        bus.onRumor(p3, new Bodies.Rumor("news", "overtaken", 1, bytes("overtaken")));
        assertThat(sentTo("PRUNE")).as("no answer is due any more, so a copy of one hop prunes nothing").isEmpty();
    }

    /**
     * Asker-only repair, the gate (ROBUSTNESS.md §10.9): the record of the asks in flight is bounded, and a flood's asks
     * push the record of an honest ask out of it before a slow answer comes. The answer ends at the asker, so the asker
     * took it for a point-to-point copy: it forwarded nothing to its eager members, kept no copy to answer a GRAFT, and
     * announced the item to nobody. The item's entry among the wanted items still says this node asked for it, so the
     * asker now forwards the item along its own eager edges as usual.
     */
    @Test
    void anAnswerThatComesAfterAFloodPushedOutTheRecordOfItsAskIsStillForwarded() {
        enable();
        byte[] hash = Digests.sha256(bytes("honest"));
        long honestAsk = -1;
        int floodAsksSince = 0;
        for (int tick = 0; tick < 400 && floodAsksSince <= GossipBus.MISSING_CAPACITY; tick++) {
            if (tick % 4 == 0) {
                bus.onHave(p3, have(hash)); // p3 keeps announcing the item inside the horizon
                flood(p1, "flood-" + tick + "-", GossipBus.HAVE_ITEMS_MAX);
            }
            int before = sent.size();
            clock[0] += 250;
            bus.tick(clock[0]);
            for (Sent frame : sent.subList(before, sent.size())) {
                if (!frame.kind().equals("GRAFT")) {
                    continue;
                }
                if (java.util.Arrays.equals(((Bodies.Graft) frame.body()).hash(), hash)) {
                    honestAsk = frame.at();
                    floodAsksSince = 0;
                } else if (honestAsk >= 0) {
                    floodAsksSince++;
                }
            }
        }
        assertThat(honestAsk).as("p3 was asked").isNotNegative();
        assertThat(floodAsksSince).as("the flood's asks since then fill the record of asks")
                .isGreaterThan(GossipBus.MISSING_CAPACITY);
        sent.clear();
        bus.onRumor(p3, new Bodies.Rumor("news", "honest", 1, bytes("honest"))); // the slow answer
        assertThat(delivered).containsExactly("honest");
        assertThat(sent.stream().filter(f -> f.kind().equals("RUMOR")).toList())
                .as("the asker forwards the item along its eager edges, each copy with a budget of one")
                .extracting(Sent::to).containsExactlyInAnyOrder(p1, p2, p4);
        sent.clear();
        bus.onGraft(p2, new Bodies.Graft("news", hash));
        assertThat(sentTo("RUMOR")).as("the asker keeps the item to answer a GRAFT").containsExactly(p2);
    }

    /**
     * Swarm retries, adaptive wait, the gate (ROBUSTNESS.md §10.9): a member that announces its own items and pushes them
     * at once gives this node tree copies that come no time after their announcement. Its samples filled the ring of
     * copy delays, and the wait fell from what the other members' late copies set to its floor, so the asks for honest
     * items came early again and fetched copies already on their way, the fault of §10.4. Each member now holds a few
     * samples of each ring at most, so one member cannot move the wait that the other members' copies set.
     */
    @Test
    void oneMemberThatPushesRightAfterItsAnnouncementsCannotPullTheWaitDown() {
        enable();
        List<PeerId> honestAnnouncers = List.of(p1, p2, p3);
        for (int i = 0; i < GossipBus.GRAFT_RTT_SAMPLES; i++) {
            long announced = clock[0];
            bus.onHave(honestAnnouncers.get(i % 3), have(Digests.sha256(bytes("honest-" + i))));
            runUntil(announced + 250);
            bus.onRumor(p1, rumor("honest-" + i)); // the tree copy, a tick after the announcement
        }
        long honest = bus.repairWindowMillis();
        assertThat(honest).as("a lateness of one tick: a base of 250 ms, two spreads of 500 ms, one backoff")
                .isEqualTo(250 + 2 * 500 + GossipBus.GRAFT_BACKOFF_MILLIS);
        pushRightAfterAnnouncing(p4, "pushed-");
        assertThat(bus.repairWindowMillis()).as("p4 alone cannot pull the wait down").isEqualTo(honest);
    }

    /** {@code member} announces many items of its own and pushes each right after its announcement. */
    private void pushRightAfterAnnouncing(PeerId member, String prefix) {
        for (int i = 0; i < 4 * GossipBus.GRAFT_RTT_SAMPLES; i++) {
            bus.onHave(member, have(Digests.sha256(bytes(prefix + i))));
            bus.onRumor(member, rumor(prefix + i));
        }
    }

    @Test
    void aGraftIsAnsweredFromTheRecentCacheAndMakesTheAskerEager() {
        enable();
        bus.publish("news", "k", bytes("k"));
        bus.onPrune(p2, new Bodies.Prune("news", Digests.sha256(bytes("k"))));
        sent.clear();
        bus.onGraft(p2, new Bodies.Graft("news", Digests.sha256(bytes("k"))));
        assertThat(sentTo("RUMOR")).containsExactly(p2);
        assertThat(((Bodies.Rumor) sent.get(0).body()).payload()).isEqualTo(bytes("k"));
        assertThat(bus.lazyPeers()).isEmpty();
        sent.clear();
        bus.onGraft(p2, new Bodies.Graft("news", Digests.sha256(bytes("never-published"))));
        assertThat(sent).as("an unknown item gets no answer").isEmpty();
    }

    @Test
    void withoutCapableMembersTheRandomFanoutIsUnchangedAndNothingNewIsSent() {
        bus.lazyPush(p -> false);
        bus.publish("news", "a", bytes("a"));
        assertThat(sentTo("RUMOR")).as("fanout three of the sampler's order").containsExactly(p1, legacy1, p2);
        bus.tick(1_000);
        bus.tick(2_000);
        assertThat(sentTo("HAVE")).isEmpty();
        bus.onRumor(p1, rumor("a"));
        assertThat(sentTo("PRUNE")).isEmpty();
        GossipBus plain = new GossipBus(sampler, sender, CborCodec.defaultCodec(), 3, 6);
        sent.clear();
        plain.publish("news", "b", bytes("b"));
        plain.tick(1_000);
        assertThat(sentTo("RUMOR")).containsExactly(p1, legacy1, p2);
        assertThat(sent).hasSize(3);
    }

    /**
     * Swarm retries (ROBUSTNESS.md §10.2, P4 of §10.3): the repair window covers the first ask and one retry, each
     * with its spread, and widens with the graft round trips the node observes, so a stream's own fallback waits for it.
     */
    @Test
    void theRepairWindowCoversTheFirstAskAndOneRetryAndWidensWithSlowGrafts() {
        enable();
        assertThat(bus.repairWindowMillis()).isEqualTo(GossipBus.GRAFT_BASE_MILLIS + 2 * GossipBus.GRAFT_SPREAD_COLD_MILLIS
                + GossipBus.GRAFT_BACKOFF_MILLIS);
        for (int i = 0; i < 10; i++) {
            bus.onHave(p3, have(Digests.sha256(bytes("slow-" + i))));
        }
        runUntil(GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sentTo("GRAFT")).hasSize(10);
        clock[0] += 1_500; // every answer takes about a second and a half
        for (int i = 0; i < 10; i++) {
            bus.onRumor(p3, rumor("slow-" + i));
        }
        long window = bus.repairWindowMillis();
        assertThat(window).as("twice a p90 of about 1.5 to 2.5 s, below the ceiling")
                .isBetween(GossipBus.GRAFT_BASE_MILLIS + 2 * 3_000 + GossipBus.GRAFT_BACKOFF_MILLIS,
                        GossipBus.GRAFT_BASE_MILLIS + 2 * GossipBus.GRAFT_SPREAD_MAX_MILLIS + GossipBus.GRAFT_BACKOFF_MILLIS);
    }

    /**
     * Swarm retries, the gate's adversarial review (ROBUSTNESS.md §10.6): the retries of items a flood of announcements
     * named, which nobody serves, keep the per-tick budget busy for the whole horizon, so the first ask of an item an
     * honest member announces goes before them and is not delayed past its spread wait.
     */
    @Test
    void theFirstAskOfANewItemGoesBeforeTheRetriesOfAFlood() {
        enable();
        List<Bodies.HaveItem> flood = new ArrayList<>();
        for (int i = 0; i < GossipBus.HAVE_ITEMS_MAX; i++) {
            flood.add(new Bodies.HaveItem("news", Digests.sha256(bytes("never-served-" + i))));
        }
        bus.onHave(p1, new Bodies.Have(flood));
        runUntil(5_000);
        byte[] honest = Digests.sha256(bytes("honest"));
        bus.onHave(p3, have(honest));
        runUntil(5_000 + GossipBus.GRAFT_BASE_MILLIS + GossipBus.GRAFT_SPREAD_COLD_MILLIS + 250);
        assertThat(sent.stream().filter(f -> f.kind().equals("GRAFT") && f.to().equals(p3)).toList())
                .as("the honest item is asked for within its spread wait").hasSize(1)
                .allSatisfy(f -> assertThat(((Bodies.Graft) f.body()).hash()).isEqualTo(honest));
    }

    /**
     * The budgets bound a hostile announcer (swarm retries, ROBUSTNESS.md §10.2): a member that announces items it
     * never serves, again and again, gets at most {@link GossipBus#GRAFTS_PER_TICK} asks a tick, the node tracks at
     * most {@link GossipBus#MISSING_CAPACITY} items, and each item's asks back off.
     */
    @Test
    void graftsPerTickAndAnnouncedItemsAreBounded() {
        enable();
        List<Bodies.HaveItem> items = new ArrayList<>();
        for (int i = 0; i < GossipBus.HAVE_ITEMS_MAX + 50; i++) {
            items.add(new Bodies.HaveItem("news", Digests.sha256(bytes("flood-" + i))));
        }
        int maxPerTick = 0;
        for (int t = 0; t < 240; t++) { // a minute, the flood announced again every second
            if (t % 4 == 0) {
                bus.onHave(p1, new Bodies.Have(items));
            }
            int before = sentTo("GRAFT").size();
            clock[0] += 250;
            bus.tick(clock[0]);
            maxPerTick = Math.max(maxPerTick, sentTo("GRAFT").size() - before);
        }
        assertThat(maxPerTick).as("one tick asks for a bounded number").isEqualTo(GossipBus.GRAFTS_PER_TICK);
        long total = sentTo("GRAFT").size();
        long perItem = 1 + 60_000 / GossipBus.GRAFT_BACKOFF_MAX_MILLIS + 4;
        assertThat(total).as("each tracked item's asks back off: at most about %d each in a minute", perItem)
                .isLessThanOrEqualTo(GossipBus.MISSING_CAPACITY * perItem);
    }

    @Test
    void announcementsStopOnceTheTreeHasSettledAndTheProbeCarriesTheRecentItems() {
        enable();
        bus.onPrune(p2, new Bodies.Prune("news", Digests.sha256(bytes("x")))); // a topology change at tick 0
        int haves = 0;
        List<Integer> haveTicks = new ArrayList<>();
        for (int i = 1; i <= 40; i++) {
            bus.publish("news", "i" + i, bytes("item-" + i));
            bus.tick(i * 250L);
            int now = sentTo("HAVE").size();
            if (now > haves) {
                haveTicks.add(i);
                haves = now;
            }
        }
        assertThat(haveTicks).as("every tick while young, every fourth while settling, none once settled")
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 28);
        List<Bodies.HaveItem> carried = bus.piggyback(p1);
        assertThat(carried).hasSize(GossipBus.PIGGYBACK_ITEMS);
        assertThat(carried.get(carried.size() - 1).hash()).isEqualTo(Digests.sha256(bytes("item-40")));
        assertThat(carried.get(0).hash()).isEqualTo(Digests.sha256(bytes("item-9")));
        sent.clear();
        bus.onGraft(p3, new Bodies.Graft("news", Digests.sha256(bytes("item-40")))); // p3 was never lazy: no change
        bus.publish("news", "after", bytes("after"));
        bus.tick(41 * 250L);
        assertThat(sentTo("HAVE")).as("a graft from an eager member is not a topology change").isEmpty();
        bus.onPrune(p4, new Bodies.Prune("news", Digests.sha256(bytes("after"))));
        bus.publish("news", "young-again", bytes("young-again"));
        bus.tick(42 * 250L);
        assertThat(sentTo("HAVE")).as("a prune makes the tree young again: p2 and p4 hear").hasSize(2);
        // A member admitted after an item was forwarded is not told of it: that is history, and
        // its anti-entropy brings it in registration order.
        bus.memberAdmitted(p3);
        assertThat(bus.piggyback(p3)).as("nothing forwarded since p3 joined").isNull();
        bus.publish("news", "since", bytes("since"));
        bus.tick(43 * 250L);
        assertThat(bus.piggyback(p3)).hasSize(1);
        assertThat(bus.piggyback(p1)).hasSize(GossipBus.PIGGYBACK_ITEMS);
    }

    @Test
    void theProbeHashesFollowTheStatesAndTheRootStandsInAboveSixteen() {
        assertThat(bus.probeHashes()).isNull();
        assertThat(bus.rootDigest()).isNull();
        bus.reconcile("space:a", state("A"));
        bus.reconcile("space:b", state("B"));
        java.util.Map<String, byte[]> digests = bus.probeHashes();
        assertThat(digests).containsOnlyKeys("space:a", "space:b");
        assertThat(digests.get("space:a")).as("a hash of the digest, never the digest").isEqualTo(Digests.sha256(bytes("A")));
        byte[] root = bus.rootDigest();
        assertThat(root).hasSize(32).isEqualTo(bus.rootDigest());
        assertThat(bus.differs(digests)).isFalse();
        assertThat(bus.differs(root)).isFalse();
        assertThat(bus.differs(java.util.Map.of("space:a", Digests.sha256(bytes("A2"))))).isTrue();
        assertThat(bus.differs(java.util.Map.of("space:zzz", bytes("unknown")))).as("a stream only they hold").isFalse();
        assertThat(bus.differs(Digests.sha256(bytes("other root")))).isTrue();
        for (int i = 0; i < GossipBus.PROBE_DIGEST_STREAMS; i++) {
            bus.reconcile("space:" + i, state("" + i));
        }
        assertThat(bus.probeHashes()).as("too many streams for a probe: the root goes instead").isNull();
        assertThat(bus.rootDigest()).isNotEqualTo(root);
    }

    private static ReconcilableState state(String digest) {
        return new ReconcilableState() {
            @Override public byte[] digest() { return bytes(digest); }
            @Override public byte[] deltaFor(byte[] remoteDigest) { return new byte[0]; }
            @Override public void applyDelta(byte[] delta) { }
        };
    }

    @Test
    void aDuplicateReplacesTheFirstEdgeOnlyForAClearlyShorterPath() {
        enable();
        bus.onRumor(p1, new Bodies.Rumor("news", "x", 5, bytes("x")));   // relayed once: five hops left
        bus.onRumor(p2, new Bodies.Rumor("news", "x", 6, bytes("x")));   // the origin's own push, beaten by the relay
        assertThat(sentTo("PRUNE")).as("the origin's edge is kept; the relayer's goes").containsExactly(p1);
        assertThat(bus.lazyPeers()).containsExactly(p1);
        sent.clear();
        bus.onRumor(p2, new Bodies.Rumor("news", "w", 4, bytes("w")));   // relayed twice
        bus.onRumor(p3, new Bodies.Rumor("news", "w", 5, bytes("w")));   // relayed once: one hop fewer, not direct
        assertThat(sentTo("PRUNE")).as("a one-hop difference between two relays keeps the first edge").containsExactly(p3);
        assertThat(bus.lazyPeers()).containsExactlyInAnyOrder(p1, p3);
        sent.clear();
        bus.lazyPeers().forEach(p -> bus.onGraft(p, new Bodies.Graft(null, null))); // restore for the next case
        sent.clear();
        bus.onRumor(p3, new Bodies.Rumor("news", "y", 2, bytes("y")));   // relayed four times
        bus.onRumor(p4, new Bodies.Rumor("news", "y", 4, bytes("y")));   // relayed twice: two hops fewer
        assertThat(sentTo("PRUNE")).as("a clearly shorter relay replaces the first edge").containsExactly(p3);
        assertThat(bus.lazyPeers()).containsExactly(p3);
    }

    @Test
    void aMemberLeavingForgetsEveryPruneSoDeliveryNeverWaitsOnRepair() {
        enable();
        bus.onPrune(p2, new Bodies.Prune("news", Digests.sha256(bytes("x"))));
        bus.onPrune(p3, new Bodies.Prune("news", Digests.sha256(bytes("x"))));
        assertThat(bus.lazyPeers()).containsExactlyInAnyOrder(p2, p3);
        bus.memberLeft(p4);
        assertThat(bus.lazyPeers()).as("the tree may have lost its only path through p4").isEmpty();
        sent.clear();
        bus.publish("news", "after", bytes("after"));
        assertThat(sentTo("RUMOR")).contains(p1, p2, p3);
    }

    @Test
    void anEchoOfTheNodesOwnItemNeverPrunes() {
        enable();
        bus.publish("news", "mine", bytes("mine"));
        sent.clear();
        bus.onRumor(p2, new Bodies.Rumor("news", "mine", 4, bytes("mine"))); // back round a cycle
        assertThat(sentTo("PRUNE")).as("the direct edge to p2 is the shortest path; the cycle is pruned at p2").isEmpty();
        assertThat(bus.lazyPeers()).isEmpty();
    }

    @Test
    void anAlwaysEagerStreamReachesPrunedMembersTooAndIsNeverAnnounced() {
        enable();
        bus.alwaysEager("revocations");
        bus.onPrune(p2, new Bodies.Prune("news", Digests.sha256(bytes("x"))));
        sent.clear();
        bus.publish("revocations", "r1", bytes("revoke"));
        assertThat(sentTo("RUMOR")).as("every capable member, pruned or not, and the legacy fan-out")
                .containsExactlyInAnyOrder(p1, p2, p3, p4, legacy1, legacy2);
        bus.tick(1);
        assertThat(sentTo("HAVE")).as("nothing to announce: everyone got it").isEmpty();
        assertThat(bus.piggyback(p1)).isNull();
    }
}
