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
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.SpaceAdmissionException;
import ai.badmonkey.agentspaces.space.SpaceReadOnlyException;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Space admission and the read-only transition (spec §7.5): under an
 * ALLOWLIST only the listed agents may write, take, or complete, locally and
 * through inbound deltas, while everyone in the group may read; and once the
 * founders' lease on the space lapses, writes and takes are refused while
 * reads keep working.
 */
class SpaceAdmissionTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zAdmission");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zAdmission", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "admission",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, AgentId agent, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, PeerIdentity identity,
                         Consumer<ReplicatedSpace.Builder> customize,
                         String... seedAddresses) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace.Builder builder = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock)
                .settleWindow(Duration.ZERO);
        customize.accept(builder);
        nodes.add(node);
        return new Peer(node, identity.agent("worker"), builder.build());
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    private static Consumer<ReplicatedSpace.Builder> allowOnly(AgentId... agents) {
        return builder -> builder.admission(SpaceAdvertisement.Admission.ALLOWLIST, Set.of(agents));
    }

    /** Spec §7.5: under ALLOWLIST a local agent outside the list cannot write or take, but can still read. */
    @Test
    void anUnlistedLocalWriterIsRefused() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        PeerIdentity bId = PeerIdentity.generate();
        AgentId allowed = aId.agent("worker");
        Peer a = newPeer("a", 1, aId, allowOnly(allowed));
        Peer b = newPeer("b", 2, bId, allowOnly(allowed), "a");
        tickAll(4);
        assertThat(b.space().admission()).isEqualTo(SpaceAdvertisement.Admission.ALLOWLIST);
        assertThat(b.space().allowedAgents()).containsExactly(allowed);

        assertThatThrownBy(() -> b.space().write(new TaskEntry("refused", 1), MINUTES_30))
                .isInstanceOf(SpaceAdmissionException.class)
                .hasMessageContaining(b.agent().encoded());
        assertThatThrownBy(() -> b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isInstanceOf(SpaceAdmissionException.class);

        // The listed agent writes; the unlisted member reads it but cannot take it.
        a.space().write(new TaskEntry("readable", 1), MINUTES_30);
        tickAll(2);
        assertThat(b.space().read(Template.of(TaskEntry.class)))
                .as("reads are open to the group").contains(new TaskEntry("readable", 1));
        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
    }

    /** Spec §7.5: an inbound record or claim whose issuer is not on the allowlist is dropped by an ALLOWLIST replica. */
    @Test
    void anUnlistedRemoteIssuersDeltaIsDropped() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        PeerIdentity bId = PeerIdentity.generate();
        AgentId allowed = aId.agent("worker");
        Peer a = newPeer("a", 1, aId, allowOnly(allowed));
        // B runs a permissive replica of the same space, so it can emit deltas
        // an ALLOWLIST replica must refuse.
        Peer b = newPeer("b", 2, bId, builder -> { }, "a");
        tickAll(4);

        b.space().write(new TaskEntry("smuggled", 1), MINUTES_30);
        tickAll(6); // rumor, then several anti-entropy rounds
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("the unlisted issuer's record never merged").isEmpty();
        assertThat(a.space().knownEntries()).isZero();

        // A's own entry replicates to B, but B's claim on it is refused by A.
        a.space().write(new TaskEntry("guarded", 2), MINUTES_30);
        tickAll(2);
        TakenEntry<TaskEntry> byB = b.space().take(
                Template.of(TaskEntry.class).where("topic", eq("guarded")), MINUTES_10,
                Duration.ZERO).orElseThrow();
        tickAll(4);
        var entryId = a.space().entryIdOf(new TaskEntry("guarded", 2)).orElseThrow();
        assertThat(a.space().currentClaim(entryId)).as("unlisted claim dropped").isEmpty();
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("still available on the ALLOWLIST replica").contains(new TaskEntry("guarded", 2));
        assertThat(byB.entry()).isEqualTo(new TaskEntry("guarded", 2));
    }

    /** Spec §7.5: agents on the allowlist replicate, take, and complete exactly as under GROUP admission. */
    @Test
    void allowedAgentsAreUnaffected() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        PeerIdentity bId = PeerIdentity.generate();
        Consumer<ReplicatedSpace.Builder> both = allowOnly(aId.agent("worker"), bId.agent("worker"));
        Peer a = newPeer("a", 1, aId, both);
        Peer b = newPeer("b", 2, bId, both, "a");
        tickAll(4);

        a.space().write(new TaskEntry("shared", 1), MINUTES_30);
        tickAll(2);
        assertThat(b.space().read(Template.of(TaskEntry.class))).contains(new TaskEntry("shared", 1));

        TakenEntry<TaskEntry> taken = b.space().take(Template.of(TaskEntry.class), MINUTES_10,
                Duration.ZERO).orElseThrow();
        b.space().complete(taken);
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(a.space().knownEntries()).as("the completion merged on A").isEqualTo(1);
    }

    /** Spec §7.5: the SpaceAdvertisement carries the CREDENTIAL and AUTHORIZER rules, on creation and on refresh, and never lists the credential type among its schema hints. */
    @Test
    void theAdvertisementCarriesTheAdmissionRule() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        PeerIdentity bId = PeerIdentity.generate();
        List<SignedAdvertisement<?>> published = new ArrayList<>();
        Peer a = newPeer("a", 1, aId, builder -> builder
                .advertise(published::add)
                .admission(SpaceAdmission.credentials(aId.peerId(), new CredentialIndex())));
        Peer b = newPeer("b", 2, bId, builder -> builder
                .advertise(published::add)
                .admission(SpaceAdmission.authorizer((peer, op, scope) -> true, "tasks")), "a");
        tickAll(2);

        SpaceAdvertisement credential = (SpaceAdvertisement) a.space().advertisement().orElseThrow().advertisement();
        SpaceAdvertisement authorizer = (SpaceAdvertisement) b.space().advertisement().orElseThrow().advertisement();
        assertThat(credential.admission()).isEqualTo(SpaceAdvertisement.Admission.CREDENTIAL);
        assertThat(authorizer.admission()).isEqualTo(SpaceAdvertisement.Admission.AUTHORIZER);
        assertThat(a.space().admission()).isEqualTo(SpaceAdvertisement.Admission.CREDENTIAL);
        assertThat(b.space().admission()).isEqualTo(SpaceAdvertisement.Admission.AUTHORIZER);
        assertThat(a.space().allowedAgents()).as("no allowlist under CREDENTIAL").isEmpty();
        assertThat(published).hasSize(2);

        // A grant writes a credential entry; the refreshed advertisement still
        // names the rule and hints only at the data the space carries.
        a.space().grant(bId.agent("worker"), Set.of(SpaceAdmission.Scope.WRITE), Duration.ofHours(1));
        a.space().write(new TaskEntry("hinted", 1), MINUTES_30);
        SpaceAdvertisement refreshed = (SpaceAdvertisement) a.space().refreshAdvertisement()
                .orElseThrow().advertisement();
        assertThat(refreshed.admission()).isEqualTo(SpaceAdvertisement.Admission.CREDENTIAL);
        assertThat(refreshed.schemaHints())
                .contains(ForgeSupport.schemaNameOf(TaskEntry.class))
                .doesNotContain(ForgeSupport.schemaNameOf(SpaceCredential.class));
        assertThat(published).hasSize(3);

        // The delegating overload maps the enum onto the same rules, with this
        // node as the credential issuer; AUTHORIZER needs an Authorizer.
        Peer c = newPeer("c", 3, PeerIdentity.generate(), builder -> builder
                .admission(SpaceAdvertisement.Admission.CREDENTIAL, Set.of()), "a");
        assertThat(c.space().admission()).isEqualTo(SpaceAdvertisement.Admission.CREDENTIAL);
        assertThat(c.space().credentialIndex()).isPresent();
        assertThatThrownBy(() -> ReplicatedSpace.builder(c.node().joinGroup(groupAd,
                        GroupMembership.Config.defaults(), List.of()), "other", PeerIdentity.generate(), "w")
                .admission(SpaceAdvertisement.Admission.AUTHORIZER, Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Spec §7.5: a space whose founders let its lease lapse becomes read-only: writes and takes are refused, reads keep working. */
    @Test
    void theSpaceBecomesReadOnlyWhenTheFounderLeaseLapses() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        PeerIdentity bId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, builder -> builder.founderLease(Duration.ofMinutes(1)));
        Peer b = newPeer("b", 2, bId, builder -> { }, "a");
        tickAll(4);

        a.space().write(new TaskEntry("before", 1), MINUTES_30);
        assertThat(a.space().founderLeaseExpired()).isFalse();
        assertThat(a.space().readOnly()).isFalse();

        clock.advance(Duration.ofMinutes(2));
        assertThat(a.space().founderLeaseExpired()).isTrue();
        assertThat(a.space().readOnly()).isTrue();
        assertThatThrownBy(() -> a.space().write(new TaskEntry("after", 2), MINUTES_30))
                .isInstanceOf(SpaceReadOnlyException.class);
        assertThatThrownBy(() -> a.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isInstanceOf(SpaceReadOnlyException.class);
        assertThatThrownBy(() -> a.space().renewFounderLease(Duration.ofMinutes(5)))
                .isInstanceOf(SpaceReadOnlyException.class);
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("reads keep working").contains(new TaskEntry("before", 1));
        assertThat(a.space().readAll(Template.of(TaskEntry.class), 10)).hasSize(1);

        // A replica without a founder lease is unaffected until told otherwise.
        assertThat(b.space().founderLeaseExpired()).isFalse();
        b.space().write(new TaskEntry("elsewhere", 3), MINUTES_30);
        b.space().markReadOnly();
        assertThatThrownBy(() -> b.space().write(new TaskEntry("late", 4), MINUTES_30))
                .isInstanceOf(SpaceReadOnlyException.class);
        assertThat(b.space().readAll(Template.of(TaskEntry.class), 10)).hasSize(2);
    }

    /** Spec §7.5: renewing the founder lease before it lapses keeps the space writable. */
    @Test
    void renewingTheFounderLeaseKeepsTheSpaceWritable() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, builder -> builder.founderLease(Duration.ofMinutes(1)));
        tickAll(2);

        clock.advance(Duration.ofSeconds(50));
        a.space().renewFounderLease(Duration.ofMinutes(10));
        clock.advance(Duration.ofMinutes(2));
        assertThat(a.space().readOnly()).isFalse();
        a.space().write(new TaskEntry("still-open", 1), MINUTES_30);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
    }
}
