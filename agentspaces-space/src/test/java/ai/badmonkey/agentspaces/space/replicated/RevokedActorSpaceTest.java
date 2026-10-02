/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ASF-047: revocation is enforced on what a revoked peer did, not only on the
 * frames it sends. Its entries relayed by honest members are refused by
 * replicas that do not already hold them, replicas that held them keep them
 * (SPEC §7.5: a revocation does not remove work already admitted), an honest
 * holder's completion of its entry still lands, and the refusal is
 * acknowledged so anti-entropy stops re-offering it.
 */
class RevokedActorSpaceTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zRevokedActors");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRevokedActors", founderId.peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "revoked-actors",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                        ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer peer(PeerIdentity identity, String address, long seed, String... seedAddresses)
            throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        return new Peer(identity, node, runtime, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    @Test
    void aLateJoinerRefusesARevokedWritersEntriesWhileHoldersKeepThem() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer honest = peer(PeerIdentity.generate(), "h", 2, "f");
        Peer revoked = peer(PeerIdentity.generate(), "v", 3, "f");
        tickAll(6);
        revoked.space().write(new TaskEntry("written before the revocation", 1), MINUTES_30);
        tickAll(6);
        assertThat(honest.space().knownEntries()).isEqualTo(1);

        assertThat(founder.runtime().revoke(revoked.identity().peerId(), "compromised")).isPresent();
        tickAll(4);
        assertThat(honest.runtime().revocations().revoked(revoked.identity().peerId())).isTrue();

        Peer late = peer(PeerIdentity.generate(), "l", 4, "h");
        tickAll(10);

        assertThat(late.space().knownEntries())
                .as("a replica that did not hold the revoked peer's entry never adopts it").isZero();
        assertThat(honest.space().knownEntries())
                .as("replicas that held it keep it (SPEC §7.5)").isEqualTo(1);
        assertThat(founder.space().read(Template.of(TaskEntry.class))).isPresent();
    }

    @Test
    void anHonestHoldersCompletionOfARevokedPeersEntryStillLands() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer honest = peer(PeerIdentity.generate(), "h", 2, "f");
        Peer revoked = peer(PeerIdentity.generate(), "v", 3, "f");
        tickAll(6);
        revoked.space().write(new TaskEntry("finish me", 2), MINUTES_30);
        tickAll(6);
        assertThat(founder.runtime().revoke(revoked.identity().peerId(), "retired")).isPresent();
        tickAll(4);

        TakenEntry<TaskEntry> taken = honest.space().take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(5)), Duration.ofSeconds(1)).orElseThrow();
        honest.space().complete(taken);
        tickAll(6);

        assertThat(founder.space().read(Template.of(TaskEntry.class)))
                .as("the honest holder's completion is the actor that counts, and it lands")
                .isEmpty();
    }

    @Test
    void aRefusalIsAcknowledgedSoItIsNotReOffered() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer honest = peer(PeerIdentity.generate(), "h", 2, "f");
        Peer revoked = peer(PeerIdentity.generate(), "v", 3, "f");
        tickAll(6);
        revoked.space().write(new TaskEntry("refused downstream", 3), MINUTES_30);
        tickAll(6);
        assertThat(founder.runtime().revoke(revoked.identity().peerId(), "compromised")).isPresent();
        tickAll(4);

        Peer late = peer(PeerIdentity.generate(), "l", 4, "h");
        tickAll(10);
        assertThat(late.space().knownEntries()).isZero();

        byte[] lateDigest = late.space().digest();
        assertThat(new String(lateDigest, StandardCharsets.UTF_8))
                .as("the late joiner acknowledges what it refused").contains("a:e:");
        assertThat(honest.space().deltaFor(lateDigest))
                .as("the holder no longer offers the refused entry").isEmpty();
    }
}
