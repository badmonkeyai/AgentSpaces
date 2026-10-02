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

import ai.badmonkey.agentspaces.api.ad.CredentialRevocation;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SPEC §5.6 v0.1.13 (TODO-9-10-11 C3): agent revocations in a replicated
 * space, under the freeze rule. A retired agent's history still reaches a
 * late joiner while its later writes do not; a compromised key's history
 * reaches no new replica, though holders keep what they hold; a revoked
 * holder cannot complete, its claim lapses, and the entry is taken by
 * another; and a revoked agent cannot write locally.
 */
class RevokedAgentSpaceTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final PeerIdentity hostId = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zRevokedAgents");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRevokedAgents", founderId.peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "revoked-agents",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerIdentity identity, GroupRuntime runtime, ReplicatedSpace space) {
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
        return new Peer(identity, runtime, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private AgentIdentity agent(String name) {
        return hostId.renewingSubordinate(name, Duration.ofDays(1), clock);
    }

    private static List<String> titles(ReplicatedSpace space) {
        return space.readAll(Template.of(TaskEntry.class), 10).stream().map(TaskEntry::topic).toList();
    }

    @Test
    void aRetiredAgentsHistoryReachesALateJoinerButItsLaterWritesDoNot() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer host = peer(hostId, "h", 2, "f");
        tickAll(6);
        AgentIdentity planner = agent("planner");
        Space asPlanner = host.space().as(planner);
        asPlanner.write(new TaskEntry("before", 1), HOUR);
        tickAll(4);
        java.time.Instant retiredFrom = clock.instant();
        clock.advance(Duration.ofSeconds(1));
        asPlanner.write(new TaskEntry("after", 2), HOUR);
        tickAll(4);
        assertThat(titles(founder.space())).containsExactlyInAnyOrder("before", "after");

        // Retired as of a moment between the two writes.
        assertThat(founder.runtime().revoke(CredentialRevocation.Target.agent(planner.id()),
                CredentialRevocation.RETIRED, retiredFrom)).isPresent();
        tickAll(4);
        assertThat(titles(founder.space())).as("what the founder holds stays")
                .containsExactlyInAnyOrder("before", "after");

        Peer late = peer(PeerIdentity.generate(), "l", 3, "h");
        tickAll(10);
        assertThat(titles(late.space())).as("history before the revocation still arrives")
                .containsExactly("before");
        assertThatThrownBy(() -> asPlanner.write(new TaskEntry("refused locally", 3), HOUR))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("revoked");
    }

    @Test
    void aCompromisedKeysHistoryReachesNoNewReplicaWhileHoldersKeepIt() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer host = peer(hostId, "h", 2, "f");
        tickAll(6);
        AgentIdentity planner = agent("planner");
        host.space().as(planner).write(new TaskEntry("held", 1), HOUR);
        tickAll(4);
        assertThat(titles(founder.space())).containsExactly("held");

        assertThat(host.runtime().revokeAgentKey(planner.id(), planner.publicKey(),
                CredentialRevocation.KEY_COMPROMISE)).isPresent();
        tickAll(4);
        Peer late = peer(PeerIdentity.generate(), "l", 3, "f");
        tickAll(10);
        assertThat(titles(late.space())).as("a stolen key could have back-dated it").isEmpty();
        assertThat(titles(founder.space())).as("what a replica holds stays (SPEC §7.5)")
                .containsExactly("held");
        // A sibling agent of the same peer is untouched.
        host.space().as(agent("sibling")).write(new TaskEntry("sibling", 2), HOUR);
        tickAll(6);
        assertThat(titles(late.space())).containsExactly("sibling");
    }

    @Test
    void aRevokedHolderCannotCompleteAndItsTakeLapsesToAnother() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer host = peer(hostId, "h", 2, "f");
        Peer other = peer(PeerIdentity.generate(), "o", 3, "f");
        tickAll(6);
        founder.space().write(new TaskEntry("contested", 1), HOUR);
        tickAll(4);
        AgentIdentity worker = agent("worker");
        Space asWorker = host.space().as(worker);
        TakenEntry<TaskEntry> taken = asWorker.take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(1)).orElseThrow();
        tickAll(4);

        assertThat(founder.runtime().revokeAgent(worker.id(), CredentialRevocation.PRIVILEGE_WITHDRAWN))
                .isPresent();
        tickAll(2);
        asWorker.complete(taken);
        tickAll(6);
        clock.advance(Duration.ofMinutes(2)); // the take lease lapses
        tickAll(4);
        assertThat(titles(founder.space())).as("the revoked holder's completion was refused; the entry is back")
                .containsExactly("contested");
        TakenEntry<TaskEntry> retaken = other.space().take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(1)).orElseThrow();
        other.space().complete(retaken);
        tickAll(6);
        assertThat(titles(founder.space())).as("another agent finished it").isEmpty();
    }

    /**
     * Audit 2026-10-02: a peer-signed state is judged at its lease stamp, so a
     * renewal made after a retirement takes effect does not reach a new replica
     * even though the entry was first written before it.
     */
    @Test
    void aRenewalAfterTheRetirementTookEffectIsRefusedByALateJoiner() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer host = peer(hostId, "h", 2, "f");
        tickAll(6);
        ai.badmonkey.agentspaces.api.space.EntryHandle handle =
                host.space().write(new TaskEntry("written before", 1), HOUR);
        tickAll(2);
        java.time.Instant retiredFrom = clock.instant();
        clock.advance(Duration.ofSeconds(5));
        handle.renew(Duration.ofHours(2)); // peer-signed, after the effective instant
        tickAll(2);
        assertThat(founder.runtime().revoke(ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.agent(
                hostId.agent("worker")), CredentialRevocation.RETIRED, retiredFrom)).isPresent();
        tickAll(2);

        Peer late = peer(PeerIdentity.generate(), "l", 3, "h");
        tickAll(10);
        assertThat(late.space().knownEntries())
                .as("the renewed state carries the renewal's stamp, after the retirement").isZero();
        assertThat(titles(founder.space())).as("the founder keeps what it held").containsExactly("written before");
    }

    /**
     * Audit 2026-10-02: a revoked actor's state is judged only once it verifies,
     * since the judgement reads its signing time. A relayed record carrying a
     * forged, unsigned lease stamp after the retirement is dropped as a forgery,
     * not refused-and-acknowledged, so it adds no ack line to the digest.
     */
    @Test
    void aForgedStateOfARevokedAgentIsDroppedUnacknowledged() throws Exception {
        Peer founder = peer(founderId, "f", 1);
        Peer host = peer(hostId, "h", 2, "f");
        tickAll(6);
        ai.badmonkey.agentspaces.api.space.EntryHandle handle =
                host.space().write(new TaskEntry("held", 1), HOUR);
        tickAll(4);
        SpaceWire.EntryStateDto real = founder.space().signedState(handle.entryId()).orElseThrow();
        assertThat(founder.runtime().revoke(CredentialRevocation.Target.agent(hostId.agent("worker")),
                CredentialRevocation.RETIRED, clock.instant())).isPresent();
        tickAll(2);

        SpaceWire.EntryStateDto forged = new SpaceWire.EntryStateDto(real.record(),
                real.issuerPublicKey(), real.adds(), real.removes(),
                new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(clock.millis() + 5_000, 0,
                        real.leaseStamp().node()),
                real.leaseValue(), false, new byte[64]);
        host.runtime().gossip().publish("space:tasks", "forged-after-retirement",
                CborCodec.defaultCodec().toBytes(new SpaceWire.Delta(forged, null, null)));
        tickAll(4);

        assertThat(new String(founder.space().digest(), java.nio.charset.StandardCharsets.UTF_8))
                .as("an unverified state earns no refused-and-acknowledged line").doesNotContain("a:");
        assertThat(titles(founder.space())).containsExactly("held");
    }

    /** SPEC §6.1 v0.1.13: the ordered-log path refuses a revoked holder's committed claim, as gossip does. */
    @Test
    void theOrderedLogPathRefusesARevokedHoldersClaim() throws Exception {
        Peer host = peer(hostId, "h", 2);
        tickAll(2);
        ai.badmonkey.agentspaces.api.entry.EntryId entryId =
                host.space().write(new TaskEntry("ordered", 1), HOUR).entryId();
        CborCodec codec = CborCodec.defaultCodec();
        java.util.function.Function<String, TakeClaim> claimBy = name -> new TakeClaim(entryId,
                host.space().id(), 1, new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(
                        clock.millis(), 0, hostId.peerId().value()),
                hostId.agent(name), 0.0, clock.millis() + 60_000);

        assertThat(host.runtime().revokeAgent(hostId.agent("clerk"), CredentialRevocation.RETIRED))
                .isPresent();
        TakeClaim revoked = claimBy.apply("clerk");
        assertThatThrownBy(() -> host.space().applyAuthorizedClaim(entryId, revoked,
                hostId.rawPublicKey(), hostId.sign(codec.toBytes(revoked))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("revoked");
        assertThat(host.space().currentClaim(entryId)).as("nothing installed").isEmpty();

        TakeClaim sibling = claimBy.apply("sibling");
        host.space().applyAuthorizedClaim(entryId, sibling, hostId.rawPublicKey(),
                hostId.sign(codec.toBytes(sibling)));
        assertThat(host.space().currentClaim(entryId)).map(TakeClaim::holder)
                .contains(hostId.agent("sibling"));
    }
}
