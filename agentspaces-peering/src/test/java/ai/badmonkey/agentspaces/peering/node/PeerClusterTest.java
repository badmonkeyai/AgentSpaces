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

    /** Spec §5.3: the anti-entropy channel pulls once per GossipParameters.period of the node's clock; rumors and probes are unpaced. */
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

        // Ten one-second ticks of B alone: the 1 s group pulls every tick,
        // the 5 s group only when five seconds have passed since its last pull.
        for (int i = 0; i < 10; i++) {
            b.tick();
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(fastState.digests.get()).isEqualTo(10);
        assertThat(slowState.digests.get()).isEqualTo(2);
    }
}
