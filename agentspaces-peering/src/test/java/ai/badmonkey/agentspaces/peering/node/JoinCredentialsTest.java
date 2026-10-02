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
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * INVITE admission (spec §5.1): a credential admits exactly the PeerID the
 * founder bound it to, for exactly the group it names, only when the founder
 * signed it; the founder itself needs none; and garbage credentials fail
 * closed and quietly.
 */
class JoinCredentialsTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zInviteOnly");
    private final GroupAdvertisement invite = new GroupAdvertisement("aspace://zInviteOnly",
            founderId.peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "invite-only",
            GroupAdvertisement.MembershipPolicy.INVITE, ConflictStrategyType.LEASE_RACE,
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

    private GroupRuntime join(PeerNode node, List<PeerAdvertisement.Endpoint> seeds,
                              Map<String, String> hints) {
        return node.joinGroup(invite, GroupMembership.Config.defaults(), seeds, hints, null);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** Spec §5.1: the credential is bound to the candidate's PeerID; a stranger replaying it is refused. */
    @Test
    void aCredentialIsBoundToTheCandidatePeerId() throws Exception {
        PeerNode founder = node("f", 1, founderId);
        PeerIdentity guestId = PeerIdentity.generate();
        PeerNode guest = node("g", 2, guestId);
        PeerNode stranger = node("s", 3, PeerIdentity.generate());
        GroupRuntime rf = join(founder, List.of(), Map.of());

        String guestCredential = JoinCredentials.issue(founderId, groupId, guestId.peerId());
        join(guest, seed("f"), Map.of(JoinCredentials.HINT_KEY, guestCredential));
        // The stranger got hold of the guest's credential and presents it as its own.
        join(stranger, seed("f"), Map.of(JoinCredentials.HINT_KEY, guestCredential));
        tickAll(8);

        assertThat(rf.membership().allMembers()).extracting(GroupMembership.Member::id)
                .contains(guest.peerId())
                .doesNotContain(stranger.peerId());
    }

    /** Spec §5.1: only a credential signed by the founder (the group's issuer) admits. */
    @Test
    void aCredentialSignedByANonFounderIsRefused() throws Exception {
        PeerNode founder = node("f", 1, founderId);
        PeerIdentity guestId = PeerIdentity.generate();
        PeerNode guest = node("g", 2, guestId);
        GroupRuntime rf = join(founder, List.of(), Map.of());

        String forged = JoinCredentials.issue(PeerIdentity.generate(), groupId, guestId.peerId());
        join(guest, seed("f"), Map.of(JoinCredentials.HINT_KEY, forged));
        tickAll(8);

        assertThat(rf.membership().allMembers()).isEmpty();
    }

    /** Spec §5.1: a credential is scoped to one group; one issued for another group admits nowhere. */
    @Test
    void aCredentialForAnotherGroupIsRefused() throws Exception {
        PeerNode founder = node("f", 1, founderId);
        PeerIdentity guestId = PeerIdentity.generate();
        PeerNode guest = node("g", 2, guestId);
        GroupRuntime rf = join(founder, List.of(), Map.of());

        String elsewhere = JoinCredentials.issue(founderId, GroupId.of("zElsewhere"), guestId.peerId());
        join(guest, seed("f"), Map.of(JoinCredentials.HINT_KEY, elsewhere));
        tickAll(8);

        assertThat(rf.membership().allMembers()).isEmpty();
    }

    /** Spec §5.1: malformed credentials fail closed without throwing into the frame reader. */
    @Test
    void garbageCredentialsAreRefusedQuietly() throws Exception {
        PeerIdentity guestId = PeerIdentity.generate();
        assertThat(JoinCredentials.verify(null, guestId.peerId(), invite, codec)).isFalse();
        assertThat(JoinCredentials.verify("not base64!", guestId.peerId(), invite, codec)).isFalse();
        assertThat(JoinCredentials.verify(Base64.getEncoder().encodeToString(new byte[]{9, 9, 9}),
                guestId.peerId(), invite, codec)).isFalse();
        assertThat(JoinCredentials.verify(Base64.getEncoder().encodeToString(codec.toBytes(Map.of())),
                guestId.peerId(), invite, codec)).isFalse();

        // And end to end: the candidate presenting garbage is simply not admitted.
        PeerNode founder = node("f", 1, founderId);
        PeerNode guest = node("g", 2, guestId);
        GroupRuntime rf = join(founder, List.of(), Map.of());
        join(guest, seed("f"), Map.of(JoinCredentials.HINT_KEY, "not base64!"));
        tickAll(8);
        assertThat(rf.membership().allMembers()).isEmpty();
        assertThat(JoinCredentials.verify(JoinCredentials.issue(founderId, groupId, guestId.peerId()),
                guestId.peerId(), invite, codec)).as("the genuine article verifies").isTrue();
    }

    /** Spec §5.1: the founder created the group and is admitted by members without a credential. */
    @Test
    void theFounderAdmitsItselfWithoutACredential() throws Exception {
        PeerIdentity guestId = PeerIdentity.generate();
        PeerNode guest = node("g", 2, guestId);
        PeerNode founder = node("f", 1, founderId);
        GroupRuntime rg = join(guest, List.of(),
                Map.of(JoinCredentials.HINT_KEY, JoinCredentials.issue(founderId, groupId, guestId.peerId())));

        // The founder arrives late, seeded by the guest, presenting no credential at all.
        GroupRuntime rf = join(founder, seed("g"), Map.of());
        tickAll(8);

        assertThat(rg.membership().allMembers()).extracting(GroupMembership.Member::id)
                .containsExactly(founder.peerId());
        assertThat(rf.membership().allMembers()).extracting(GroupMembership.Member::id)
                .as("and the guest's credential admits it to the founder").containsExactly(guest.peerId());
    }
}
