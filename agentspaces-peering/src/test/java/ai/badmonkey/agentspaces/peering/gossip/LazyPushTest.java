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

    private record Sent(PeerId to, String kind, Object body) { }

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
    private final GossipBus.FrameSender sender = new GossipBus.FrameSender() {
        @Override public void sendRumor(PeerId to, Bodies.Rumor rumor) { sent.add(new Sent(to, "RUMOR", rumor)); }
        @Override public void sendDigest(PeerId to, Bodies.Digest digest) { sent.add(new Sent(to, "DIGEST", digest)); }
        @Override public void sendPullResp(PeerId to, Bodies.PullResp resp) { sent.add(new Sent(to, "PULL_RESP", resp)); }
        @Override public void sendHave(PeerId to, Bodies.Have have) { sent.add(new Sent(to, "HAVE", have)); }
        @Override public void sendGraft(PeerId to, Bodies.Graft graft) { sent.add(new Sent(to, "GRAFT", graft)); }
        @Override public void sendPrune(PeerId to, Bodies.Prune prune) { sent.add(new Sent(to, "PRUNE", prune)); }
    };
    private final GossipBus bus = new GossipBus(sampler, sender, CborCodec.defaultCodec(), 3, 6);

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static Bodies.Rumor rumor(String text) {
        return new Bodies.Rumor("news", text, 6, bytes(text));
    }

    private List<PeerId> sentTo(String kind) {
        return sent.stream().filter(f -> f.kind().equals(kind)).map(Sent::to).toList();
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
        assertThat(sentTo("RUMOR")).as("forwarded to everyone but the sender; the legacy quota is three, two exist")
                .containsExactlyInAnyOrder(p2, p3, p4, legacy1, legacy2);
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
    void anAnnouncedItemThatNeverArrivesIsGraftedFromTheAnnouncerThenTheNext() {
        enable();
        byte[] hash = Digests.sha256(bytes("missing"));
        bus.onPrune(p3, new Bodies.Prune("news", hash));
        assertThat(bus.lazyPeers()).containsExactly(p3);
        bus.onHave(p3, new Bodies.Have(List.of(new Bodies.HaveItem("news", hash))));
        bus.onHave(p4, new Bodies.Have(List.of(new Bodies.HaveItem("news", hash))));
        bus.tick(1);
        assertThat(sentTo("GRAFT")).as("not yet: the eager copy may still be on its way").isEmpty();
        bus.tick(2);
        assertThat(sentTo("GRAFT")).as("the first announcer is asked after two ticks").containsExactly(p3);
        assertThat(bus.lazyPeers()).as("asking makes the announcer eager").isEmpty();
        Bodies.Graft graft = (Bodies.Graft) sent.get(0).body();
        assertThat(graft.streamId()).isEqualTo("news");
        assertThat(graft.hash()).isEqualTo(hash);
        for (int i = 0; i < GossipBus.GRAFT_RETRY_TICKS - 1; i++) {
            bus.tick(2 + i);
        }
        assertThat(sentTo("GRAFT")).as("the retry waits").hasSize(1);
        bus.tick(10);
        assertThat(sentTo("GRAFT")).as("then the next announcer is asked").containsExactly(p3, p4);
        bus.tick(20);
        bus.tick(30);
        assertThat(sentTo("GRAFT")).as("no announcer left: the item is left to anti-entropy").hasSize(2);
        sent.clear();
        bus.onRumor(p4, rumor("missing"));
        assertThat(delivered).containsExactly("missing");
        bus.onHave(p1, new Bodies.Have(List.of(new Bodies.HaveItem("news", hash))));
        bus.tick(40);
        assertThat(sentTo("GRAFT")).as("an announcement of a seen item is ignored").isEmpty();
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

    @Test
    void graftsPerTickAndAnnouncedItemsAreBounded() {
        enable();
        List<Bodies.HaveItem> items = new ArrayList<>();
        for (int i = 0; i < GossipBus.HAVE_ITEMS_MAX + 50; i++) {
            items.add(new Bodies.HaveItem("news", Digests.sha256(bytes("flood-" + i))));
        }
        bus.onHave(p1, new Bodies.Have(items));
        bus.tick(1);
        bus.tick(2);
        assertThat(sentTo("GRAFT")).as("one tick asks for a bounded number").hasSize(GossipBus.GRAFTS_PER_TICK);
        int total = 0;
        for (int t = 2; t < 200; t++) {
            bus.tick(t);
        }
        total = sentTo("GRAFT").size();
        assertThat(total).as("every announced item is asked for once at most, and the flood was capped")
                .isLessThanOrEqualTo(GossipBus.HAVE_ITEMS_MAX);
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
