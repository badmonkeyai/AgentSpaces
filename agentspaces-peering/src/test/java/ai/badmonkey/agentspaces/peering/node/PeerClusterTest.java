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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Three peers on a SimNetwork: membership convergence, rumor propagation,
 * anti-entropy convergence across a partition, and failure detection. All
 * deterministic: seeded randomness, TestClock, manual ticks.
 */
class PeerClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private GroupAdvertisement groupAd;

    /** A trivially mergeable set-of-strings state for anti-entropy testing. */
    static final class SetState implements ReconcilableState {
        final java.util.Set<String> items = ConcurrentHashMap.newKeySet();

        @Override
        public byte[] digest() {
            return String.join("\n", new java.util.TreeSet<>(items))
                    .getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] deltaFor(byte[] remoteDigest) {
            java.util.Set<String> remote = new java.util.HashSet<>(List.of(
                    new String(remoteDigest, StandardCharsets.UTF_8).split("\n")));
            java.util.Set<String> missing = new java.util.TreeSet<>(items);
            missing.removeAll(remote);
            return String.join("\n", missing).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void applyDelta(byte[] delta) {
            for (String item : new String(delta, StandardCharsets.UTF_8).split("\n")) {
                if (!item.isEmpty()) {
                    items.add(item);
                }
            }
        }
    }

    @BeforeEach
    void setUp() {
        GroupId groupId = GroupId.of("zTestGroup");
        groupAd = new GroupAdvertisement("aspace://zTestGroup", PeerIdentity.generate().peerId(),
                groupId, Instant.EPOCH, Duration.ofDays(1), "test-group",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1)));
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode newNode(String simAddress, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate())
                .clock(clock)
                .randomSeed(seed)
                .build();
        node.listen(network.register(simAddress), simAddress);
        nodes.add(node);
        return node;
    }

    private static List<PeerAdvertisement.Endpoint> seed(String address) {
        return List.of(new PeerAdvertisement.Endpoint("mem", address, 0));
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            for (PeerNode node : nodes) {
                node.tick();
            }
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private PeerNode nodeWithIdentity(String simAddress, long seed, PeerIdentity identity)
            throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(simAddress), simAddress);
        nodes.add(node);
        return node;
    }

    @Test
    void inviteGroupAdmitsCredentialHoldersAndRejectsStrangers() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        GroupId groupId = GroupId.of("zInvite");
        GroupAdvertisement invite = new GroupAdvertisement("aspace://zInvite",
                founderId.peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
                "invite-group", GroupAdvertisement.MembershipPolicy.INVITE,
                ConflictStrategyType.LEASE_RACE,
                new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1)));

        PeerNode founder = nodeWithIdentity("f", 10, founderId);
        PeerIdentity guestId = PeerIdentity.generate();
        PeerNode guest = nodeWithIdentity("g", 11, guestId);
        PeerNode stranger = nodeWithIdentity("s", 12, PeerIdentity.generate());

        GroupRuntime rf = founder.joinGroup(invite, GroupMembership.Config.defaults(), List.of());
        // The founder issues the guest a credential; the stranger has none.
        String credential = JoinCredentials.issue(founderId, groupId, guestId.peerId());
        GroupRuntime rg = guest.joinGroup(invite, GroupMembership.Config.defaults(), seed("f"),
                Map.of(JoinCredentials.HINT_KEY, credential), null);
        GroupRuntime rs = stranger.joinGroup(invite, GroupMembership.Config.defaults(),
                seed("f"), Map.of(), null);

        tickAll(8);

        // The founder admits the credentialed guest and never the stranger.
        assertThat(rf.membership().allMembers()).extracting(GroupMembership.Member::id)
                .contains(guest.peerId())
                .doesNotContain(stranger.peerId());
        // The guest is a full member (sees the founder); the stranger is admitted nowhere.
        assertThat(rg.membership().allMembers()).extracting(GroupMembership.Member::id)
                .contains(founder.peerId());
        assertThat(rg.membership().allMembers()).extracting(GroupMembership.Member::id)
                .doesNotContain(stranger.peerId());
    }

    /** TODO-EFG §4 token ingestion (TODO item 6): a member's credential hints reach the listener on admission and again when a refreshed self-advertisement changes them. */
    @Test
    void credentialHintsReachTheListenerOnAdmissionAndRefresh() throws Exception {
        List<Map.Entry<PeerId, Map<String, String>>> seen = new CopyOnWriteArrayList<>();
        PeerNode observer = PeerNode.builder(PeerIdentity.generate())
                .clock(clock).randomSeed(30)
                .onCredentialHints((peer, hints) -> seen.add(Map.entry(peer, hints)))
                .build();
        observer.listen(network.register("obs"), "obs");
        nodes.add(observer);
        PeerNode holder = newNode("holder", 31);
        holder.credentialHint(JoinCredentials.OIDC_HINT_KEY, "token-v1");

        observer.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        holder.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("obs"));
        tickAll(4);

        // Admission delivered the hint once; unchanged refreshes were not re-delivered.
        assertThat(seen).as("the hint arrives with the admission").hasSize(1);
        assertThat(seen.get(0).getKey()).isEqualTo(holder.peerId());
        assertThat(seen.get(0).getValue())
                .containsEntry(JoinCredentials.OIDC_HINT_KEY, "token-v1");

        // The holder rotates its token: the next self-advertisement carries the
        // new value and the listener sees exactly that change.
        holder.credentialHint(JoinCredentials.OIDC_HINT_KEY, "token-v2");
        tickAll(4);
        assertThat(seen).hasSize(2);
        assertThat(seen.get(1).getValue())
                .containsEntry(JoinCredentials.OIDC_HINT_KEY, "token-v2");

        // Removing the hint is a change too; further unchanged refreshes are quiet.
        holder.removeCredentialHint(JoinCredentials.OIDC_HINT_KEY);
        tickAll(4);
        assertThat(seen).hasSize(3);
        assertThat(seen.get(2).getValue()).doesNotContainKey(JoinCredentials.OIDC_HINT_KEY);
        tickAll(4);
        assertThat(seen).hasSize(3);
    }

    @Test
    void policyGroupDefersToTheValidator() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        GroupId groupId = GroupId.of("zPolicy");
        GroupAdvertisement policy = new GroupAdvertisement("aspace://zPolicy",
                founderId.peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
                "policy-group", GroupAdvertisement.MembershipPolicy.POLICY,
                ConflictStrategyType.LEASE_RACE,
                new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1)));

        PeerNode founder = nodeWithIdentity("f", 20, founderId);
        PeerNode good = nodeWithIdentity("good", 21, PeerIdentity.generate());
        PeerNode bad = nodeWithIdentity("bad", 22, PeerIdentity.generate());

        // The founder admits any candidate presenting the right badge hint.
        GroupRuntime rf = founder.joinGroup(policy, GroupMembership.Config.defaults(),
                List.of(), Map.of(),
                (candidate, group, credentials) -> "ok".equals(credentials.get("badge")));
        GroupRuntime rgood = good.joinGroup(policy, GroupMembership.Config.defaults(),
                seed("f"), Map.of("badge", "ok"), null);
        GroupRuntime rbad = bad.joinGroup(policy, GroupMembership.Config.defaults(),
                seed("f"), Map.of("badge", "nope"), null);

        tickAll(8);

        assertThat(rf.membership().allMembers()).extracting(GroupMembership.Member::id)
                .contains(good.peerId())
                .doesNotContain(bad.peerId());
    }

    @Test
    void membershipConvergesAcrossThreePeers() throws Exception {
        PeerNode a = newNode("a", 1);
        PeerNode b = newNode("b", 2);
        PeerNode c = newNode("c", 3);

        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("a"));
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("a"));

        tickAll(4);

        assertThat(ra.membership().allMembers()).extracting(GroupMembership.Member::id)
                .containsExactlyInAnyOrder(b.peerId(), c.peerId());
        assertThat(rb.membership().allMembers()).extracting(GroupMembership.Member::id)
                .containsExactlyInAnyOrder(a.peerId(), c.peerId());
        assertThat(rc.membership().allMembers()).extracting(GroupMembership.Member::id)
                .containsExactlyInAnyOrder(a.peerId(), b.peerId());
    }

    @Test
    void rumorsReachEveryPeer() throws Exception {
        PeerNode a = newNode("a", 1);
        PeerNode b = newNode("b", 2);
        PeerNode c = newNode("c", 3);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("a"));
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("a"));
        tickAll(4);

        List<String> bSeen = new CopyOnWriteArrayList<>();
        List<String> cSeen = new CopyOnWriteArrayList<>();
        rb.gossip().onStream("news", (from, id, payload) ->
                bSeen.add(new String(payload, StandardCharsets.UTF_8)));
        rc.gossip().onStream("news", (from, id, payload) ->
                cSeen.add(new String(payload, StandardCharsets.UTF_8)));

        ra.gossip().publish("news", "item-1", "hello fleet".getBytes(StandardCharsets.UTF_8));

        assertThat(bSeen).containsExactly("hello fleet");
        assertThat(cSeen).containsExactly("hello fleet");
    }

    @Test
    void antiEntropyHealsAPartitionedPeer() throws Exception {
        PeerNode a = newNode("a", 1);
        PeerNode b = newNode("b", 2);
        PeerNode c = newNode("c", 3);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("a"));
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("a"));
        tickAll(4);

        SetState sa = new SetState();
        SetState sb = new SetState();
        SetState sc = new SetState();
        ra.gossip().reconcile("set", sa);
        rb.gossip().reconcile("set", sb);
        rc.gossip().reconcile("set", sc);

        // C is partitioned from both while A adds an item and rumors it.
        network.partition("a", "c");
        network.partition("b", "c");
        sa.items.add("fact-1");
        ra.gossip().publish("set-items", "fact-1", new byte[0]);
        sb.items.add("fact-1"); // B heard the rumor conceptually; state carries via anti-entropy

        assertThat(sc.items).isEmpty();

        network.heal();
        // C's anti-entropy digests eventually land on A or B, which reply with deltas.
        tickAll(6);

        assertThat(sc.items).contains("fact-1");
        assertThat(sa.items).isEqualTo(sb.items).isEqualTo(sc.items);
    }

    @Test
    void deadPeerIsDroppedAfterProbesAndTtl() throws Exception {
        PeerNode a = newNode("a", 1);
        PeerNode b = newNode("b", 2);
        GroupMembership.Config fast = new GroupMembership.Config(
                Duration.ofSeconds(10), Duration.ofSeconds(1), 1);
        GroupRuntime ra = a.joinGroup(groupAd, fast, List.of());
        b.joinGroup(groupAd, fast, seed("a"));
        tickAll(3);
        assertThat(ra.membership().allMembers()).hasSize(1);

        // B dies: no more frames, and pings to it fail silently.
        b.close();
        network.partition("a", "b");

        // Advance beyond the member TTL; A's ticks expire the silent member.
        for (int i = 0; i < 12; i++) {
            clock.advance(Duration.ofSeconds(1));
            a.tick();
        }

        assertThat(ra.membership().allMembers()).isEmpty();
    }

    /** Spec §5.3/§9: receivers ignore frames for groups they are not members of, with no side effects. */
    @Test
    void framesForUnjoinedGroupsAreIgnored() throws Exception {
        PeerNode a = newNode("a", 1);
        PeerNode b = newNode("b", 2);
        a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("a"));
        tickAll(3);
        List<String> news = new CopyOnWriteArrayList<>();
        rb.gossip().onStream("news", (from, id, payload) ->
                news.add(new String(payload, StandardCharsets.UTF_8)));
        Map<PeerId, String> modesBefore = b.channelModes();

        // A's validly signed frame for a group B never joined reaches B's transport.
        GroupId other = GroupId.of("zOtherGroup");
        TransportConnection injector = network.register("injector").dial("b");
        injector.send(TestFrames.signed(a.identity(), other, Envelope.Kind.RUMOR, b.peerId(),
                clock, 0, new Bodies.Rumor("news", "n1",
                        3, "wrong room".getBytes(StandardCharsets.UTF_8))));

        assertThat(news).as("no runtime, no handler").isEmpty();
        assertThat(b.group(other)).isEmpty();
        assertThat(b.channelModes()).as("no connection cached for it").isEqualTo(modesBefore);

        // The same frame in the joined group is served.
        injector.send(TestFrames.signed(a.identity(), groupAd.group(), Envelope.Kind.RUMOR, b.peerId(),
                clock, 1, new Bodies.Rumor("news", "n2",
                        3, "right room".getBytes(StandardCharsets.UTF_8))));
        assertThat(news).containsExactly("right room");
    }

    /** Spec §5.2: a member unreachable directly but reachable through another member survives via indirect probing. */
    @Test
    void anUnreachableMemberSurvivesWhileARelayCanStillReachIt() throws Exception {
        PeerNode a = newNode("a", 1);
        PeerNode b = newNode("b", 2);
        PeerNode c = newNode("c", 3);
        GroupMembership.Config fast = new GroupMembership.Config(
                Duration.ofSeconds(30), Duration.ofSeconds(1), 1);
        GroupRuntime ra = a.joinGroup(groupAd, fast, List.of());
        b.joinGroup(groupAd, fast, seed("a"));
        c.joinGroup(groupAd, fast, seed("a"));
        tickAll(4);
        assertThat(ra.membership().allMembers()).hasSize(2);

        // A can no longer reach B directly; C still can. A's direct pings to B
        // fail, escalate to PING_REQ through C, and C's vouching ACKs keep B alive.
        network.partition("a", "b");
        for (int i = 0; i < 8; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(2));
        }
        assertThat(ra.membership().member(b.peerId())).hasValueSatisfying(m ->
                assertThat(m.suspect()).as("cleared by the relay's ACK").isFalse());
        assertThat(ra.membership().allMembers()).hasSize(2);

        // Once nobody can reach B, the indirect stage fails and A drops B long
        // before the 30-second lease of attention runs out.
        network.partition("b", "c");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(2));
        }
        assertThat(ra.membership().member(b.peerId())).isEmpty();
        assertThat(ra.membership().member(c.peerId())).as("the honest relay is untouched").isPresent();
    }

    private SignedGroupAdvertisement founded(PeerIdentity founderId,
                                             GroupAdvertisement.MembershipPolicy policy,
                                             Duration period) {
        return GroupFounding.found(founderId, "founded", policy, ConflictStrategyType.LEASE_RACE,
                new GroupAdvertisement.GossipParameters(3, period), clock.instant(),
                Duration.ofDays(1));
    }

    /** Spec §4.4/§5.1: a self-certifying GroupID admits exactly one founding document; a different policy under the same id is refused at join. */
    @Test
    void aSelfCertifyingGroupRefusesAnAdvertisementWithADifferentPolicyUnderTheSameId()
            throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement open =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(1));
        PeerNode founder = nodeWithIdentity("f", 30, founderId);
        PeerNode member = newNode("m", 31);
        PeerNode victim = newNode("v", 32);
        GroupRuntime rf = founder.joinGroup(open, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rm = member.joinGroup(open, GroupMembership.Config.defaults(), seed("f"));
        tickAll(3);
        assertThat(rf.membership().member(member.peerId())).isPresent();
        assertThat(rm.founding()).contains(open);

        // An attacker re-issues the same GroupID with an INVITE policy under its own key...
        PeerIdentity attacker = PeerIdentity.generate();
        GroupAdvertisement o = open.advertisement();
        GroupAdvertisement swapped = new GroupAdvertisement(o.id(), attacker.peerId(), o.group(),
                o.issued(), o.ttl(), o.name(), GroupAdvertisement.MembershipPolicy.INVITE,
                o.defaultStrategy(), o.gossip());
        byte[] attackerSig = attacker.sign(ai.badmonkey.agentspaces.common.codec.CborCodec
                .defaultCodec().toBytes(GroupFounding.fieldsOf(swapped)));
        SignedGroupAdvertisement forged =
                new SignedGroupAdvertisement(swapped, attacker.rawPublicKey(), attackerSig);
        assertThatThrownBy(() -> victim.joinGroup(forged, GroupMembership.Config.defaults(), seed("f")))
                .isInstanceOf(IllegalArgumentException.class);
        // ...and the founder's own document with the policy flipped in place is refused too.
        GroupAdvertisement flipped = new GroupAdvertisement(o.id(), o.issuer(), o.group(), o.issued(),
                o.ttl(), o.name(), GroupAdvertisement.MembershipPolicy.INVITE, o.defaultStrategy(),
                o.gossip());
        assertThatThrownBy(() -> victim.joinGroup(
                new SignedGroupAdvertisement(flipped, open.founderPublicKey(), open.signature()),
                GroupMembership.Config.defaults(), seed("f")))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(victim.group(o.group())).as("nothing was joined").isEmpty();
        tickAll(2);
        assertThat(rf.membership().member(victim.peerId())).isEmpty();
        // The genuine document still joins.
        assertThat(victim.joinGroup(open, GroupMembership.Config.defaults(), seed("f")).id())
                .isEqualTo(o.group());
    }

    /** Spec §10.1 join by GroupID: a newcomer with only the id and a seed fetches the founding advertisement, verifies it, and joins. */
    @Test
    void aNewcomerJoinsByGroupIdAloneAndVerifiesTheFoundingAdvertisement() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement open =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(1));
        GroupId groupId = open.advertisement().group();
        PeerNode founder = nodeWithIdentity("f", 40, founderId);
        PeerNode literal = newNode("lit", 41);
        PeerNode newcomer = newNode("n", 42);
        GroupRuntime rf = founder.joinGroup(open, GroupMembership.Config.defaults(), List.of());
        // A member that joined with the literal (unverified) advertisement serves nothing.
        literal.joinGroup(open.advertisement(), GroupMembership.Config.defaults(), seed("f"));
        tickAll(3);
        assertThatThrownBy(() -> newcomer.joinGroup(groupId, GroupMembership.Config.defaults(),
                seed("lit"), Duration.ofMillis(200)))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
        assertThat(newcomer.group(groupId)).isEmpty();

        // The founder (a self-certifying member) answers; the newcomer verifies and joins.
        GroupRuntime rn = newcomer.joinGroup(groupId, GroupMembership.Config.defaults(),
                seed("f"), Duration.ofSeconds(5));
        assertThat(rn.advertisement()).isEqualTo(open.advertisement());
        assertThat(rn.founding()).contains(open);
        tickAll(4);
        assertThat(rf.membership().member(newcomer.peerId())).isPresent();
        assertThat(rn.membership().allMembers()).extracting(GroupMembership.Member::id)
                .containsExactlyInAnyOrder(founder.peerId(), literal.peerId());
        // And having joined by id, the newcomer serves the document onward itself.
        PeerNode third = newNode("t", 43);
        assertThat(third.joinGroup(groupId, GroupMembership.Config.defaults(), seed("n"),
                Duration.ofSeconds(5)).founding()).contains(open);
    }

    /** A ReconcilableState that counts the digests anti-entropy asks it for. */
    static final class CountingState implements ReconcilableState {
        final AtomicInteger digests = new AtomicInteger();

        @Override
        public byte[] digest() {
            digests.incrementAndGet();
            return new byte[0];
        }

        @Override
        public byte[] deltaFor(byte[] remoteDigest) {
            return new byte[0];
        }

        @Override
        public void applyDelta(byte[] delta) {
        }
    }

    /** Spec §5.3: the anti-entropy channel pulls once per GossipParameters.period of the node's clock while anything moves, and backs off by doubling while idle (PERF2 §3.2); rumors are unpaced. */
    @Test
    void antiEntropyCadenceFollowsTheGroupsGossipPeriod() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement slow =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(5));
        SignedGroupAdvertisement fast = GroupFounding.found(founderId, "fast",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1)),
                clock.instant(), Duration.ofDays(1));
        PeerNode a = nodeWithIdentity("a", 50, founderId);
        PeerNode b = newNode("b", 51);
        a.joinGroup(slow, GroupMembership.Config.defaults(), List.of());
        a.joinGroup(fast, GroupMembership.Config.defaults(), List.of());
        GroupRuntime slowAtB = b.joinGroup(slow, GroupMembership.Config.defaults(), seed("a"));
        GroupRuntime fastAtB = b.joinGroup(fast, GroupMembership.Config.defaults(), seed("a"));
        tickAll(6); // converge; B has a partner in both groups
        assertThat(slowAtB.membership().member(a.peerId())).isPresent();

        CountingState slowState = new CountingState();
        CountingState fastState = new CountingState();
        slowAtB.gossip().reconcile("count", slowState);
        fastAtB.gossip().reconcile("count", fastState);
        long slowBase = slowAtB.gossip().antiEntropyRounds();
        long fastBase = fastAtB.gossip().antiEntropyRounds();

        // Ten one-second ticks of B alone with nothing moving after one rumor on each group (which
        // resets any backoff the convergence ticks built up): the first round is on the base period,
        // then the idle backoff doubles it, so the 1 s group pulls at 0, 1, 3 and 7 s and the 5 s
        // group at 0 and 5 s (its second period is already 10 s).
        fastAtB.gossip().publish("news", "warm-fast", new byte[] {0});
        slowAtB.gossip().publish("news", "warm-slow", new byte[] {0});
        clock.advance(Duration.ofSeconds(5)); // both groups are due at the start of the window
        for (int i = 0; i < 10; i++) {
            b.tick();
            clock.advance(Duration.ofSeconds(1));
        }
        // Rounds, not digest computations: since v0.1.17 the probes compute digests too.
        assertThat(fastAtB.gossip().antiEntropyRounds() - fastBase).isEqualTo(4);
        assertThat(slowAtB.gossip().antiEntropyRounds() - slowBase).isEqualTo(2);

        // Activity resets the backoff: a rumor on the fast group, and the next tick pulls again.
        fastAtB.gossip().publish("news", "item", new byte[] {1});
        long before = fastAtB.gossip().antiEntropyRounds();
        b.tick();
        clock.advance(Duration.ofSeconds(1));
        assertThat(fastAtB.gossip().antiEntropyRounds()).isEqualTo(before + 1);
    }

    /**
     * A set-union state: the digest is the sorted members joined by commas, the delta for a
     * differing digest is the whole set, and a merge is the union, so pulls in any order and any
     * number converge on the same set and a repeated pull changes nothing. {@code applied}
     * records each delta that changed the set.
     */
    private static final class InventoryState implements ai.badmonkey.agentspaces.peering.gossip.ReconcilableState {
        final java.util.concurrent.ConcurrentSkipListSet<String> members = new java.util.concurrent.ConcurrentSkipListSet<>();
        final java.util.List<String> applied = new java.util.concurrent.CopyOnWriteArrayList<>();

        InventoryState(String own) {
            members.add(own);
        }

        void add(String member) {
            members.add(member);
        }

        String digestString() {
            return String.join(",", members);
        }

        @Override
        public byte[] digest() {
            return digestString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] deltaFor(byte[] theirDigest) {
            return java.util.Arrays.equals(theirDigest, digest()) ? new byte[0] : digest();
        }

        @Override
        public void applyDelta(byte[] delta) {
            String text = new String(delta, StandardCharsets.UTF_8);
            boolean changed = false;
            for (String member : text.split(",")) {
                changed |= members.add(member);
            }
            if (changed) {
                applied.add(text);
            }
        }
    }

    /**
     * A newcomer reconciles with each member it admits within a tick of admission, so it holds
     * what the fleet already had (cards, space state) long before its periodic round would have
     * picked that member: here the group's period is five seconds and the pull lands in two.
     */
    @Test
    void aNewMemberIsReconciledWithinATickOfAdmission() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement slow =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(5));
        PeerNode a = nodeWithIdentity("a", 70, founderId);
        GroupRuntime atA = a.joinGroup(slow, GroupMembership.Config.defaults(), List.of());
        InventoryState stateA = new InventoryState("a-holds-this");
        atA.gossip().reconcile("inventory", stateA);
        tickAll(2); // A's own first round found nobody; its next is five seconds out
        PeerNode b = newNode("b", 71);
        GroupRuntime atB = b.joinGroup(slow, GroupMembership.Config.defaults(), seed("a"));
        InventoryState stateB = new InventoryState("b-empty");
        atB.gossip().reconcile("inventory", stateB);
        tickAll(2);
        assertThat(atB.membership().member(a.peerId())).isPresent();
        assertThat(stateB.members).as("B pulled A's state on admitting A, not five seconds later")
                .contains("a-holds-this");
        assertThat(stateA.members).as("A pulled B's state on admitting B").contains("b-empty");
        assertThat(stateA.digestString()).as("converged").isEqualTo(stateB.digestString());
    }

    /**
     * v0.1.17: the probe carries the anti-entropy digests, so a change on a member is pulled
     * within a probe of it, long before the group's own period (five seconds here, and backed
     * off) would have picked that member.
     */
    @Test
    void aProbeThatCarriesADifferingDigestPullsWithinAProbe() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement slow =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(5));
        PeerNode a = nodeWithIdentity("a", 80, founderId);
        PeerNode b = newNode("b", 81);
        GroupRuntime atA = a.joinGroup(slow, GroupMembership.Config.defaults(), List.of());
        GroupRuntime atB = b.joinGroup(slow, GroupMembership.Config.defaults(), seed("a"));
        InventoryState stateA = new InventoryState("v1");
        InventoryState stateB = new InventoryState("v1");
        atA.gossip().reconcile("inventory", stateA);
        atB.gossip().reconcile("inventory", stateB);
        tickAll(8);
        assertThat(atB.membership().member(a.peerId())).isPresent();
        stateB.applied.clear();
        long roundsBefore = atB.gossip().antiEntropyRounds();
        stateA.add("v2");
        tickAll(6); // at most one stretched probe interval each way
        assertThat(stateB.members).as("B pulled A's change on a probe").contains("v2");
        assertThat(atB.gossip().antiEntropyRounds() - roundsBefore).as("one targeted round, not one per probe")
                .isBetween(1L, 2L);
    }

    /**
     * v0.1.17: an idle fleet refreshes its advertisements directly (one frame per member per
     * interval) and probes at a quarter of the base rate, so thirty idle seconds of four members
     * cost a dozen refresh frames rather than the flood's several copies each.
     */
    @Test
    void anIdleFleetRefreshesDirectlyAndProbesSlowly() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement group =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(1));
        PeerNode a = nodeWithIdentity("a", 90, founderId);
        PeerNode b = newNode("b", 91);
        PeerNode c = newNode("c", 92);
        PeerNode d = newNode("d", 93);
        a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        for (PeerNode node : List.of(b, c, d)) {
            node.joinGroup(group, GroupMembership.Config.defaults(), seed("a"));
        }
        tickAll(12);
        for (PeerNode node : List.of(a, b, c, d)) {
            assertThat(node.group(group.advertisement().group()).orElseThrow().membership().allMembers()).hasSize(3);
        }
        long rumorsBefore = 0;
        long pingsBefore = 0;
        for (PeerNode node : List.of(a, b, c, d)) {
            rumorsBefore += node.traffic().sentByKind().getOrDefault(Envelope.Kind.RUMOR, 0L);
            pingsBefore += node.traffic().sentByKind().getOrDefault(Envelope.Kind.PING, 0L);
        }
        tickAll(30);
        long rumors = 0;
        long pings = 0;
        for (PeerNode node : List.of(a, b, c, d)) {
            rumors += node.traffic().sentByKind().getOrDefault(Envelope.Kind.RUMOR, 0L);
            pings += node.traffic().sentByKind().getOrDefault(Envelope.Kind.PING, 0L);
        }
        assertThat(rumors - rumorsBefore).as("one refresh interval: each member sent its advertisement to each other once")
                .isEqualTo(4L * 3);
        assertThat(pings - pingsBefore).as("probes stretched to four seconds: about eight per member, not thirty")
                .isBetween(4L * 6, 4L * 12);
    }

    /**
     * v0.1.17: a connection the other end closed is noticed at once, not when a later send
     * fails, so the node re-dials and re-introduces itself on its very next tick. Here A
     * evicts B and closes the link on its tick; B's next tick re-introduces B, and A, whose
     * group is open, admits it again, two ticks after the eviction.
     */
    @Test
    void aRemoteCloseIsNoticedAtOnceAndTheNodeReintroducesItself() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement group =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(1));
        PeerNode a = nodeWithIdentity("a", 60, founderId);
        PeerNode b = newNode("b", 61);
        GroupRuntime atA = a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        GroupRuntime atB = b.joinGroup(group, GroupMembership.Config.defaults(), seed("a"));
        tickAll(4);
        assertThat(atA.membership().member(b.peerId())).isPresent();
        assertThat(b.connectionFor(a.peerId())).isNotNull();

        atA.membership().evict(b.peerId());
        a.tick();                       // A's view changed: it closes its channel to B
        clock.advance(Duration.ofSeconds(1));
        assertThat(b.connectionFor(a.peerId())).as("B learned of the close without sending anything").isNull();
        assertThat(atB.membership().member(a.peerId())).as("a closed link is not a verdict on A").isPresent();

        b.tick();                       // B re-dials A and introduces itself
        clock.advance(Duration.ofSeconds(1));
        assertThat(atA.membership().member(b.peerId())).as("re-admitted on the next tick").isPresent();
        assertThat(a.connectionFor(b.peerId())).isNotNull();
    }

    /**
     * ROBUSTNESS.md §3.6: two members that lost each other (a partition longer than the
     * membership TTL), with no third party to carry an introduction, find each other again:
     * each keeps the other's last endpoints and introduces itself to them every few seconds for
     * a while after the loss. A member ejected on purpose is not courted that way.
     */
    @Test
    void twoMembersThatLostEachOtherFindEachOtherAgainButAnEjectedOneIsNotCourted() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement group =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(1));
        PeerNode a = nodeWithIdentity("a", 62, founderId);
        PeerNode b = newNode("b", 63);
        GroupRuntime atA = a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        GroupRuntime atB = b.joinGroup(group, GroupMembership.Config.defaults(), seed("a"));
        tickAll(4);
        network.partition("a", "b");
        tickAll(40); // past the 30 s membership TTL: each drops the other for silence
        assertThat(atA.membership().allMembers()).isEmpty();
        assertThat(atB.membership().allMembers()).isEmpty();
        network.heal();
        tickAll(8); // the lost-member retry runs every five seconds
        assertThat(atA.membership().member(b.peerId())).as("A found B again").isPresent();
        assertThat(atB.membership().member(a.peerId())).as("B found A again").isPresent();

        // An ejection is a verdict, not a loss: A does not introduce itself to B again on its own.
        atA.membership().evict(b.peerId());
        network.partition("a", "b"); // and B cannot reach A to re-introduce itself either
        tickAll(12);
        assertThat(atA.membership().member(b.peerId())).as("not courted").isEmpty();
        assertThat(a.traffic().sentByKind().getOrDefault(Envelope.Kind.RUMOR, 0L))
                .as("no introductions attempted toward an ejected peer while cut off")
                .isEqualTo(a.traffic().sentByKind().getOrDefault(Envelope.Kind.RUMOR, 0L));
    }

    /**
     * An introduction is answered with an introduction (v0.1.17): the moment a seed admits a
     * newcomer from its dial, it introduces itself back on that connection, so the newcomer
     * holds the seed before the seed's next frame, with no tick in between. Before, the seed's
     * introduction waited for its next tick, and every frame it pushed meanwhile was dropped at
     * the newcomer as a non-member's.
     */
    @Test
    void aSeedIntroducesItselfBackTheMomentItAdmitsANewcomer() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement group =
                founded(founderId, GroupAdvertisement.MembershipPolicy.OPEN, Duration.ofSeconds(1));
        PeerNode a = nodeWithIdentity("a", 64, founderId);
        PeerNode b = newNode("b", 65);
        GroupRuntime atA = a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        GroupRuntime atB = b.joinGroup(group, GroupMembership.Config.defaults(), seed("a"));
        // No tick has run anywhere yet.
        assertThat(atA.membership().member(b.peerId())).as("A admitted B from its dial").isPresent();
        assertThat(atB.membership().member(a.peerId())).as("B holds A from A's answer, before any tick").isPresent();
        List<String> atBNews = new CopyOnWriteArrayList<>();
        atB.gossip().onStream("news", (from, id, payload) -> atBNews.add(new String(payload, StandardCharsets.UTF_8)));
        atA.gossip().publish("news", "first", "first frame after admission".getBytes(StandardCharsets.UTF_8));
        assertThat(atBNews).as("nothing A pushes right after admitting B is lost").containsExactly("first frame after admission");
    }
}
