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
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Leaving one group (ISSUE-PeerRobustness-2, R2-2): everything the node holds
 * for the group goes and nothing held for another changes; no frame of the
 * group leaves afterwards; shared links stay; the others evict the leaver by
 * TTL; and a rejoin starts afresh.
 */
class LeaveGroupTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final WireCodec wire = new WireCodec(CborCodec.defaultCodec());
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupId inviteId = GroupId.of("zLeaveInvite");
    /** An INVITE group, so the admission memory has something to hold. */
    private final GroupAdvertisement invite = new GroupAdvertisement("aspace://zLeaveInvite",
            founderId.peerId(), inviteId, Instant.EPOCH, Duration.ofDays(1), "leave-invite",
            GroupAdvertisement.MembershipPolicy.INVITE, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final GroupId openId = GroupId.of("zLeaveOpen");
    private final GroupAdvertisement open = new GroupAdvertisement("aspace://zLeaveOpen",
            PeerIdentity.generate().peerId(), openId, Instant.EPOCH, Duration.ofDays(1), "leave-open",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, PeerIdentity identity) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private static List<PeerAdvertisement.Endpoint> seed(String address) {
        return List.of(new PeerAdvertisement.Endpoint("mem", address, 0));
    }

    private GroupRuntime joinInvite(PeerNode node, List<PeerAdvertisement.Endpoint> seeds, PeerIdentity who) {
        Map<String, String> hints = who.peerId().equals(founderId.peerId()) ? Map.of()
                : Map.of(JoinCredentials.HINT_KEY, JoinCredentials.issue(founderId, inviteId, who.peerId()));
        return node.joinGroup(invite, GroupMembership.Config.defaults(), seeds, hints, null);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private static List<PeerId> view(GroupRuntime runtime) {
        return runtime.membership().allMembers().stream().map(GroupMembership.Member::id).toList();
    }

    /** Counts the frames one node sends in one group, decoded off the fabric. */
    private AtomicInteger countFrames(String from, String to, GroupId groupId) {
        AtomicInteger count = new AtomicInteger();
        network.tap(from, to, frame -> wire.decode(frame)
                .filter(envelope -> groupId.equals(envelope.group()))
                .ifPresent(envelope -> count.incrementAndGet()));
        return count;
    }

    /**
     * R2-2.1 to R2-2.4: a node in two groups leaves one. Nothing of that group leaves it
     * afterwards, the other group's membership and gossip go on, the link shared with a member of
     * both groups stays while the link to a member of the left group alone is closed, the runtime
     * is released and its listener told, and the others evict the leaver within the TTL.
     */
    @Test
    void leavesOneGroupOnly() throws Exception {
        PeerNode founder = node("founder", 1, founderId);
        PeerIdentity nId = PeerIdentity.generate();
        PeerIdentity qId = PeerIdentity.generate();
        PeerNode n = node("n", 2, nId);
        PeerNode q = node("q", 3, qId);
        GroupRuntime founderInvite = joinInvite(founder, List.of(), founderId);
        founder.joinGroup(open, GroupMembership.Config.defaults(), List.of());
        GroupRuntime nInvite = joinInvite(n, seed("founder"), nId);
        GroupRuntime nOpen = n.joinGroup(open, GroupMembership.Config.defaults(), seed("founder"));
        GroupRuntime qInvite = joinInvite(q, seed("founder"), qId);
        tickAll(10);
        assertThat(view(nInvite)).containsExactlyInAnyOrder(founder.peerId(), q.peerId());
        assertThat(view(nOpen)).containsExactly(founder.peerId());
        assertThat(n.connectedTo(founder.peerId())).isTrue();
        assertThat(n.connectedTo(q.peerId())).isTrue();
        assertThat(n.admissionMemory().forGroup(inviteId)).containsKey(q.peerId());

        List<String> closed = new CopyOnWriteArrayList<>();
        nInvite.onClose(() -> closed.add("told"));
        AutoCloseable work = nInvite.onTick(() -> closed.add("ticked"));
        nInvite.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> closed.add("pipe"));
        AtomicInteger toFounder = countFrames("n", "founder", inviteId);
        AtomicInteger toQ = countFrames("n", "q", inviteId);
        long openFramesBefore = n.traffic().framesSent();

        assertThat(n.leaveGroup(inviteId)).isTrue();
        assertThat(n.leaveGroup(inviteId)).as("leaving twice is a no-op").isFalse();
        assertThat(closed).containsExactly("told");
        assertThat(nInvite.closed()).isTrue();
        assertThat(nInvite.tickWorkCount()).isZero();
        assertThat(n.group(inviteId)).isEmpty();
        assertThat(n.groups()).extracting(GroupRuntime::id).containsExactly(openId);
        assertThat(n.admissionMemory().forGroup(inviteId)).isEmpty();
        assertThat(n.heldAdCount(inviteId)).as("nothing of the left group is served to a later partner (SR-1)").isZero();
        assertThat(n.heldAdCount(openId)).isEqualTo(1);
        assertThat(n.connectedTo(founder.peerId())).as("the founder is a member of the open group too: link kept")
                .isTrue();
        assertThat(n.connectedTo(q.peerId())).as("q is a member of no remaining group: link closed").isFalse();
        work.close();

        tickAll(40);
        assertThat(toFounder.get()).as("no frame of the left group to the founder").isZero();
        assertThat(toQ.get()).as("no frame of the left group to q").isZero();
        assertThat(closed).as("no tick work and no kind handler of the left group ran").containsExactly("told");
        nInvite.send(founder.peerId(), Envelope.Kind.PIPE_DATA, new byte[1]); // a late send over the kept link
        assertThat(toFounder.get()).as("goes nowhere").isZero();
        assertThat(view(founderInvite)).as("the founder evicts the leaver by TTL").containsExactly(q.peerId());
        assertThat(view(qInvite)).containsExactly(founder.peerId());
        assertThat(view(nOpen)).as("the open group's membership is untouched").containsExactly(founder.peerId());
        assertThat(n.traffic().framesSent()).as("the open group still sends").isGreaterThan(openFramesBefore);

        // The open group's gossip still carries news both ways.
        List<String> founderNews = new CopyOnWriteArrayList<>();
        founder.group(openId).orElseThrow().gossip().onStream("news",
                (from, id, payload) -> founderNews.add(new String(payload, StandardCharsets.UTF_8)));
        nOpen.gossip().publish("news", "after-leave", "after-leave".getBytes(StandardCharsets.UTF_8));
        tickAll(5);
        assertThat(founderNews).contains("after-leave");
    }

    /** ISSUE-BootstrapCore §14: a late tick after leave is a no-op; work or handlers registered on the left runtime run never. */
    @Test
    void lateTickAfterLeaveIsANoOp() throws Exception {
        PeerNode founder = node("founder", 1, founderId);
        PeerIdentity nId = PeerIdentity.generate();
        PeerNode n = node("n", 2, nId);
        joinInvite(founder, List.of(), founderId);
        GroupRuntime nInvite = joinInvite(n, seed("founder"), nId);
        tickAll(5);
        assertThat(view(nInvite)).containsExactly(founder.peerId());
        AtomicInteger ran = new AtomicInteger();
        nInvite.onTick(ran::incrementAndGet);
        tickAll(1);
        assertThat(ran.get()).isEqualTo(1);

        assertThat(n.leaveGroup(inviteId)).isTrue();
        AtomicInteger framesInGroup = countFrames("n", "founder", inviteId);
        long framesBefore = n.traffic().framesSent();
        nInvite.onTick(ran::incrementAndGet); // registered late: never runs
        nInvite.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> ran.incrementAndGet());
        assertThat(nInvite.tickWorkCount()).isZero();
        tickAll(10);
        assertThat(ran.get()).as("the left runtime's work never ran again").isEqualTo(1);
        assertThat(framesInGroup.get()).isZero();
        assertThat(n.traffic().framesSent()).as("a node in no group sends nothing").isEqualTo(framesBefore);
        assertThat(n.groups()).isEmpty();
    }

    /** Leave then join again works and starts with no admission memory; a listener registered late runs at once. */
    @Test
    void rejoinAfterLeave() throws Exception {
        PeerNode founder = node("founder", 1, founderId);
        PeerIdentity nId = PeerIdentity.generate();
        PeerIdentity qId = PeerIdentity.generate();
        PeerNode n = node("n", 2, nId);
        PeerNode q = node("q", 3, qId);
        GroupRuntime founderInvite = joinInvite(founder, List.of(), founderId);
        GroupRuntime first = joinInvite(n, seed("founder"), nId);
        joinInvite(q, seed("founder"), qId);
        tickAll(10);
        assertThat(view(first)).containsExactlyInAnyOrder(founder.peerId(), q.peerId());
        assertThat(n.admissionMemory().forGroup(inviteId)).containsKey(q.peerId());

        first.close();
        assertThat(first.closed()).isTrue();
        List<String> late = new CopyOnWriteArrayList<>();
        first.onClose(() -> late.add("late"));
        assertThat(late).containsExactly("late");
        assertThat(n.admissionMemory().forGroup(inviteId)).as("leaving forgets the memory; a rejoin starts afresh")
                .isEmpty();
        tickAll(40);
        assertThat(view(founderInvite)).containsExactly(q.peerId());
        assertThat(n.admissionMemory().forGroup(inviteId)).isEmpty();

        GroupRuntime again = joinInvite(n, seed("founder"), nId);
        assertThat(again).isNotSameAs(first);
        assertThat(again.closed()).isFalse();
        assertThat(n.admissionMemory().forGroup(inviteId)).as("the join's own introduction admits nobody by proxy")
                .doesNotContainKey(n.peerId());
        tickAll(10);
        assertThat(view(again)).containsExactlyInAnyOrder(founder.peerId(), q.peerId());
        assertThat(view(founderInvite)).containsExactlyInAnyOrder(n.peerId(), q.peerId());
        assertThat(n.admissionMemory().forGroup(inviteId)).containsKey(q.peerId());
    }
}
