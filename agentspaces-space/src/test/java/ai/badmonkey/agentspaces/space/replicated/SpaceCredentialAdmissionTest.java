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
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.ConsistencyHint;
import ai.badmonkey.agentspaces.space.SpaceAdmissionException;
import ai.badmonkey.agentspaces.space.replicated.SpaceAdmission.Scope;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Credential-based and authorizer-based space admission (SPEC §7.5, TECH-SPEC
 * §7.10) on a SimNetwork: the issuer's leased {@link SpaceCredential} entries
 * admit their grantees fleet-wide, a missing credential drops a delta without
 * a strike so anti-entropy can heal it, a forged credential strikes, and the
 * {@code AUTHORIZER} rule asks one {@code permits} question per scope.
 */
class SpaceCredentialAdmissionTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));
    private static final Duration ONE_HOUR = Duration.ofHours(1);
    private static final String STREAM = "space:tasks";

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zCredential");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zCredential", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "credential",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, PeerIdentity identity, AgentId agent,
                        GroupRuntime runtime, ReplicatedSpace space) {
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
        // Probe timeouts in minutes: these tests partition peers for several
        // ticks (seconds) and assert what anti-entropy does on heal, so the
        // membership layer must not evict the partitioned peer meanwhile
        // (evicted peers forget each other and never re-introduce themselves).
        GroupRuntime runtime = node.joinGroup(groupAd,
                new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofMinutes(1), 2), seeds);
        ReplicatedSpace.Builder builder = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock)
                .settleWindow(Duration.ZERO);
        customize.accept(builder);
        nodes.add(node);
        return new Peer(node, identity, identity.agent("worker"), runtime, builder.build());
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    /** Every replica of the space admits by credentials the given peer issues. */
    private static Consumer<ReplicatedSpace.Builder> credentialsFrom(PeerId issuer) {
        return builder -> builder.admission(SpaceAdmission.credentials(issuer, new CredentialIndex()));
    }

    /** Witnessed-violation strikes {@code node} holds against {@code peer} (PeerNode's test accessor). */
    private static int strikes(PeerNode node, PeerId peer) throws Exception {
        Method strikes = PeerNode.class.getDeclaredMethod("strikes", PeerId.class);
        strikes.setAccessible(true);
        return (int) strikes.invoke(node, peer);
    }

    private long now() {
        return clock.instant().toEpochMilli();
    }

    /** A validly signed, hand-built write by {@code author} for the shared space. */
    private SpaceWire.EntryStateDto handBuiltWrite(Peer author, Object entry) {
        return ForgeSupport.authoredWrite(author.identity(), "worker", author.space().id(), entry,
                new HybridLogicalClock(clock, author.node().peerId().value()).now(),
                now() + MINUTES_30.duration().toMillis());
    }

    /** A validly signed, hand-built claim by {@code holder} on an entry. */
    private SpaceWire.SignedClaim handBuiltClaim(Peer holder, EntryId entryId) {
        TakeClaim claim = new TakeClaim(entryId, holder.space().id(), 1,
                new HybridLogicalClock(clock, holder.node().peerId().value()).now(),
                holder.agent(), 0.0, now() + MINUTES_10.duration().toMillis());
        return ForgeSupport.signClaim(holder.identity(), claim);
    }

    private void publish(Peer from, SpaceWire.Delta delta, String itemId) {
        from.runtime().gossip().publish(STREAM, itemId, codec.toBytes(delta));
    }

    private static List<TaskEntry> tasks(ReplicatedSpace space) {
        return space.readAll(Template.of(TaskEntry.class), 10);
    }

    /** SPEC §7.5: the issuer grants WRITE to an agent by writing a leased credential entry, and the grantee's write then replicates to every peer. */
    @Test
    void theIssuerGrantsAndTheGranteeWrites() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, credentialsFrom(aId.peerId()));
        Peer b = newPeer("b", 2, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        Peer c = newPeer("c", 3, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        tickAll(4);
        assertThat(a.space().admission()).isEqualTo(SpaceAdvertisement.Admission.CREDENTIAL);
        assertThat(b.space().credentialIndex()).isPresent();
        assertThat(b.space().credentialIndex().get().admits(b.agent(), Scope.WRITE, now())).isFalse();

        EntryHandle credential = a.space().grant(b.agent(), Set.of(Scope.WRITE), ONE_HOUR);
        assertThat(credential).isNotNull();
        tickAll(4);
        assertThat(b.space().credentialIndex().get().admits(b.agent(), Scope.WRITE, now()))
                .as("the credential entry replicated to the grantee and was indexed").isTrue();
        assertThat(c.space().credentialIndex().get().admits(b.agent(), Scope.WRITE, now())).isTrue();

        b.space().write(new TaskEntry("granted", 1), MINUTES_30);
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class))).contains(new TaskEntry("granted", 1));
        assertThat(c.space().read(Template.of(TaskEntry.class), ConsistencyHint.FRESH, Duration.ZERO))
                .as("a FRESH read on a credential-admitted replica works as before")
                .contains(new TaskEntry("granted", 1));
        // The credential is an ordinary entry the issuer's node holds; the
        // grantee's own tasks are the only TaskEntry values anyone reads.
        assertThat(a.space().knownEntries()).as("credential plus task").isEqualTo(2);
        assertThat(tasks(a.space())).hasSize(1);
        assertThat(a.space().credentialIndex().get().entriesFor(b.agent(), now()))
                .containsExactly(credential.entryId());
    }

    /** SPEC §7.5: an agent without a credential fails locally with SpaceAdmissionException, and its hand-built delta is dropped remotely without a strike, since the credential may simply be late. */
    @Test
    void anUngrantedAgentIsRefusedLocallyAndDroppedRemotely() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, credentialsFrom(aId.peerId()));
        Peer c = newPeer("c", 2, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        tickAll(4);

        assertThatThrownBy(() -> c.space().write(new TaskEntry("refused", 1), MINUTES_30))
                .isInstanceOf(SpaceAdmissionException.class)
                .hasMessageContaining(c.agent().encoded());
        assertThatThrownBy(() -> c.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isInstanceOf(SpaceAdmissionException.class);

        publish(c, new SpaceWire.Delta(handBuiltWrite(c, new TaskEntry("smuggled", 1)), null, null),
                "forge:smuggled");
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("dropped on the issuer").isEmpty();
        assertThat(a.space().knownEntries()).isZero();
        assertThat(strikes(a.node(), c.node().peerId()))
                .as("a missing credential is never a strike (it may be late)").isZero();

        // Reads stay open to the group: the issuer's own entry is visible to C.
        a.space().write(new TaskEntry("readable", 2), MINUTES_30);
        tickAll(2);
        assertThat(c.space().read(Template.of(TaskEntry.class))).contains(new TaskEntry("readable", 2));
    }

    /** SPEC §7.5: a credential admits only its scopes; a WRITE-only grantee writes, but its take throws and its hand-built claim is dropped. */
    @Test
    void aScopeIsEnforced() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, credentialsFrom(aId.peerId()));
        Peer b = newPeer("b", 2, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        tickAll(4);
        a.space().grant(b.agent(), Set.of(Scope.WRITE), ONE_HOUR);
        tickAll(2);

        b.space().write(new TaskEntry("writable", 1), MINUTES_30);
        tickAll(2);
        assertThat(a.space().read(Template.of(TaskEntry.class))).contains(new TaskEntry("writable", 1));
        assertThatThrownBy(() -> b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isInstanceOf(SpaceAdmissionException.class)
                .hasMessageContaining("take");

        EntryId entryId = a.space().entryIdOf(new TaskEntry("writable", 1)).orElseThrow();
        publish(b, new SpaceWire.Delta(null, entryId, handBuiltClaim(b, entryId)), "forge:claim");
        tickAll(4);
        assertThat(a.space().currentClaim(entryId)).as("TAKE not granted: claim dropped").isEmpty();
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("still available on the issuer").contains(new TaskEntry("writable", 1));
        assertThat(strikes(a.node(), b.node().peerId())).isZero();
    }

    /** SPEC §7.5: a replica that has not yet seen the credential drops the grantee's write without a strike, and anti-entropy re-offers it until the credential is present. */
    @Test
    void aCredentialArrivingAfterTheWriteHealsThroughAntiEntropy() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, credentialsFrom(aId.peerId()));
        Peer b = newPeer("b", 2, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        Peer c = newPeer("c", 3, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        tickAll(4);

        // Isolate C fully (gossip relays through any connected peer), grant B,
        // and let B write while C knows neither the credential nor the entry.
        network.partition("a", "c");
        network.partition("b", "c");
        a.space().grant(b.agent(), Set.of(Scope.WRITE), ONE_HOUR);
        tickAll(3);
        b.space().write(new TaskEntry("late-credential", 1), MINUTES_30);
        tickAll(3);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(c.space().knownEntries()).as("C saw nothing while partitioned").isZero();
        assertThat(c.space().credentialIndex().get().admits(b.agent(), Scope.WRITE, now())).isFalse();

        // A hand-built copy of B's write pushed at C while C is cut off from A
        // strikes nobody: whether C has already learned the credential from B
        // (it legitimately may, through anti-entropy with B) or not yet, a
        // missing credential is never misbehaviour under CREDENTIAL admission.
        network.heal();
        network.partition("a", "c");
        publish(b, new SpaceWire.Delta(handBuiltWrite(b, new TaskEntry("early-copy", 2)), null, null),
                "forge:early");
        tickAll(2);
        assertThat(strikes(c.node(), b.node().peerId())).isZero();

        // Once C reconciles, it holds the credential and B's real entry.
        network.heal();
        tickAll(8);
        assertThat(c.space().credentialIndex().get().admits(b.agent(), Scope.WRITE, now())).isTrue();
        assertThat(tasks(c.space()))
                .as("anti-entropy re-offered the entry once the credential was present")
                .contains(new TaskEntry("late-credential", 1));
        assertThat(strikes(c.node(), b.node().peerId())).isZero();
    }

    /** SPEC §7.5: a credential's lease is its validity; once it lapses the grantee's local write throws and its remote deltas drop. */
    @Test
    void aLapsedCredentialStopsAdmitting() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, credentialsFrom(aId.peerId()));
        Peer b = newPeer("b", 2, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        tickAll(4);
        a.space().grant(b.agent(), Set.of(Scope.WRITE, Scope.TAKE), Duration.ofMinutes(1));
        tickAll(2);
        b.space().write(new TaskEntry("while-valid", 1), MINUTES_30);
        tickAll(2);
        assertThat(tasks(a.space())).containsExactly(new TaskEntry("while-valid", 1));
        assertThat(a.space().credentialIndex().get().size()).isEqualTo(1);

        clock.advance(Duration.ofMinutes(2));
        assertThatThrownBy(() -> b.space().write(new TaskEntry("too-late", 2), MINUTES_30))
                .isInstanceOf(SpaceAdmissionException.class);
        publish(b, new SpaceWire.Delta(handBuiltWrite(b, new TaskEntry("too-late", 2)), null, null),
                "forge:late");
        tickAll(4);
        assertThat(tasks(a.space())).as("the lapsed credential admits nothing")
                .containsExactly(new TaskEntry("while-valid", 1));
        assertThat(strikes(a.node(), b.node().peerId())).isZero();
        a.space().sweepNow();
        assertThat(a.space().credentialIndex().get().size())
                .as("the sweep drops the lapsed credential from the index").isZero();
        // The entry written while the credential was valid lives by its own lease.
        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
    }

    /** SPEC §7.5: revoke cancels the agent's live credential entries; the cancellation replicates like any removal, so every replica refuses the agent after one round. */
    @Test
    void revokeWithdrawsFleetWide() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, credentialsFrom(aId.peerId()));
        Peer b = newPeer("b", 2, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        Peer c = newPeer("c", 3, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        tickAll(4);
        a.space().grant(b.agent(), Set.of(Scope.WRITE), ONE_HOUR);
        tickAll(2);
        b.space().write(new TaskEntry("before", 1), MINUTES_30);
        tickAll(2);
        assertThat(tasks(c.space())).containsExactly(new TaskEntry("before", 1));

        // Only the issuer's node may grant or revoke.
        assertThatThrownBy(() -> b.space().revoke(b.agent()))
                .isInstanceOf(SpaceAdmissionException.class)
                .hasMessageContaining("not the credential issuer");
        assertThatThrownBy(() -> b.space().grant(b.agent(), Set.of(Scope.TAKE), ONE_HOUR))
                .isInstanceOf(SpaceAdmissionException.class);

        a.space().revoke(b.agent());
        tickAll(2);
        for (Peer peer : List.of(a, b, c)) {
            assertThat(peer.space().credentialIndex().get().admits(b.agent(), Scope.WRITE, now()))
                    .as("revoked on " + peer.node().peerId().display()).isFalse();
        }
        assertThatThrownBy(() -> b.space().write(new TaskEntry("after", 2), MINUTES_30))
                .isInstanceOf(SpaceAdmissionException.class);
        publish(b, new SpaceWire.Delta(handBuiltWrite(b, new TaskEntry("after", 2)), null, null),
                "forge:after");
        tickAll(4);
        assertThat(tasks(a.space())).containsExactly(new TaskEntry("before", 1));
        assertThat(tasks(c.space())).containsExactly(new TaskEntry("before", 1));
        assertThat(strikes(a.node(), b.node().peerId())).isZero();
        assertThat(strikes(c.node(), b.node().peerId())).isZero();
    }

    /** SPEC §7.5: a SpaceCredential record signed by anyone but the configured issuer is dropped and reported as misbehaviour, since the signer issued a credential it may not issue. */
    @Test
    void aForgedCredentialFromANonIssuerIsDroppedAndStruck() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, credentialsFrom(aId.peerId()));
        Peer c = newPeer("c", 2, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        tickAll(4);
        assertThat(strikes(a.node(), c.node().peerId())).isZero();

        SpaceCredential selfIssued = new SpaceCredential(c.agent(), Set.of(Scope.WRITE, Scope.TAKE));
        publish(c, new SpaceWire.Delta(handBuiltWrite(c, selfIssued), null, null), "forge:credential");
        tickAll(2);
        assertThat(a.space().knownEntries()).as("the forged credential never merged").isZero();
        assertThat(a.space().credentialIndex().get().admits(c.agent(), Scope.WRITE, now())).isFalse();
        assertThat(strikes(a.node(), c.node().peerId()))
                .as("a credential from a non-issuer is witnessed misbehaviour").isEqualTo(1);

        // And it bought C nothing: C's write is still refused everywhere.
        assertThatThrownBy(() -> c.space().write(new TaskEntry("still-refused", 1), MINUTES_30))
                .isInstanceOf(SpaceAdmissionException.class);
        publish(c, new SpaceWire.Delta(handBuiltWrite(c, new TaskEntry("still-refused", 1)), null, null),
                "forge:task");
        tickAll(2);
        assertThat(tasks(a.space())).isEmpty();
        assertThat(strikes(a.node(), c.node().peerId())).as("a plain missing credential adds no strike").isEqualTo(1);
    }

    /** SPEC §7.5: under AUTHORIZER the space asks permits(peer, SPACE_WRITE | SPACE_TAKE, spaceName) per scope, locally and remotely, and follows the answer. */
    @Test
    void theAuthorizerRuleAsksPermitsPerScope() throws Exception {
        record Asked(PeerId peer, Authorizer.Operation operation, String scope) {
        }
        List<Asked> asked = new CopyOnWriteArrayList<>();
        AtomicBoolean allowTakes = new AtomicBoolean(false);
        Authorizer recording = (peer, operation, scope) -> {
            asked.add(new Asked(peer, operation, scope));
            return operation == Authorizer.Operation.SPACE_WRITE
                    || (operation == Authorizer.Operation.SPACE_TAKE && allowTakes.get());
        };
        Consumer<ReplicatedSpace.Builder> byAuthorizer =
                builder -> builder.admission(SpaceAdmission.authorizer(recording, "tasks"));
        Peer a = newPeer("a", 1, PeerIdentity.generate(), byAuthorizer);
        Peer b = newPeer("b", 2, PeerIdentity.generate(), byAuthorizer, "a");
        tickAll(4);
        assertThat(a.space().admission()).isEqualTo(SpaceAdvertisement.Admission.AUTHORIZER);

        a.space().write(new TaskEntry("authorized", 1), MINUTES_30);
        tickAll(2);
        assertThat(b.space().read(Template.of(TaskEntry.class))).contains(new TaskEntry("authorized", 1));
        assertThat(asked).contains(
                new Asked(a.node().peerId(), Authorizer.Operation.SPACE_WRITE, "tasks"));

        assertThatThrownBy(() -> b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isInstanceOf(SpaceAdmissionException.class);
        assertThat(asked).contains(
                new Asked(b.node().peerId(), Authorizer.Operation.SPACE_TAKE, "tasks"));

        // A's replica asks the same question about B's remote claim and drops it, no strike.
        EntryId entryId = a.space().entryIdOf(new TaskEntry("authorized", 1)).orElseThrow();
        asked.clear();
        publish(b, new SpaceWire.Delta(null, entryId, handBuiltClaim(b, entryId)), "forge:claim");
        tickAll(2);
        assertThat(a.space().currentClaim(entryId)).isEmpty();
        assertThat(asked).contains(
                new Asked(b.node().peerId(), Authorizer.Operation.SPACE_TAKE, "tasks"));
        assertThat(asked).noneMatch(q -> q.operation() == Authorizer.Operation.SPACE_WRITE
                && q.peer().equals(b.node().peerId()));
        assertThat(strikes(a.node(), b.node().peerId())).isZero();

        // Allow takes: B takes and completes, and the completion lands on A.
        allowTakes.set(true);
        TakenEntry<TaskEntry> taken = b.space().take(Template.of(TaskEntry.class), MINUTES_10,
                Duration.ZERO).orElseThrow();
        b.space().complete(taken);
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(a.space().knownEntries()).as("the completion merged on A").isEqualTo(1);
    }

    /** SPEC §7.5: a completion is a TAKE-scoped mutation; a holder whose credential has lost TAKE cannot complete until it is granted again. */
    @Test
    void completionRequiresTheTakeScope() throws Exception {
        PeerIdentity aId = PeerIdentity.generate();
        Peer a = newPeer("a", 1, aId, credentialsFrom(aId.peerId()));
        Peer b = newPeer("b", 2, PeerIdentity.generate(), credentialsFrom(aId.peerId()), "a");
        tickAll(4);
        a.space().grant(b.agent(), Set.of(Scope.WRITE, Scope.TAKE), ONE_HOUR);
        a.space().write(new TaskEntry("to-finish", 1), MINUTES_30);
        tickAll(2);
        TakenEntry<TaskEntry> taken = b.space().take(Template.of(TaskEntry.class), MINUTES_10,
                Duration.ZERO).orElseThrow();
        tickAll(2);

        // The issuer narrows B to WRITE: the held claim stands, but completing needs TAKE.
        a.space().revoke(b.agent());
        a.space().grant(b.agent(), Set.of(Scope.WRITE), ONE_HOUR);
        tickAll(2);
        assertThat(b.space().credentialIndex().get().admits(b.agent(), Scope.TAKE, now())).isFalse();
        assertThatThrownBy(() -> b.space().complete(taken))
                .isInstanceOf(SpaceAdmissionException.class)
                .hasMessageContaining("complete");
        assertThat(a.space().knownEntries()).as("credential entries and the task").isEqualTo(3);

        a.space().grant(b.agent(), Set.of(Scope.TAKE), ONE_HOUR);
        tickAll(2);
        b.space().complete(taken);
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("completed on the issuer").isEmpty();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isEmpty();
    }
}
