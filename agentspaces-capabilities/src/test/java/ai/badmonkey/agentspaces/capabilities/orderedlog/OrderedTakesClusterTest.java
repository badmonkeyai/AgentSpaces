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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.space.replicated.TakeClaim;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ORDERED strategy over the Raft log: exactly-once takes decided by commit
 * order, and no takes at all for a partitioned minority (spec P1).
 */
class OrderedTakesClusterTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zOrdered");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zOrdered", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "ordered",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerNode node, ReplicatedSpace space, RaftLog raft,
                          OrderedTakes ordered) {
    }

    /** The committed command stream, captured for the determinism replay test. */
    private final List<byte[]> committedCommands = new ArrayList<>();
    private final List<Long> committedIndices = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private List<Member> cluster() throws IOException {
        return cluster(false);
    }

    /** As {@link #cluster()}; with {@code agentHolders} each member takes as a renewing subordinate agent. */
    private List<Member> cluster(boolean agentHolders) throws IOException {
        List<PeerIdentity> identities =
                List.of(PeerIdentity.generate(), PeerIdentity.generate(), PeerIdentity.generate());
        List<PeerId> memberIds = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < identities.size(); i++) {
            String address = "n" + i;
            PeerIdentity identity = identities.get(i);
            PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(200 + i).build();
            node.listen(network.register(address), address);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of()
                            : List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
            ai.badmonkey.agentspaces.api.spi.AgentIdentity holder = agentHolders
                    ? identity.renewingSubordinate("taker", Duration.ofHours(1), clock) : null;
            ReplicatedSpace.Builder spaceBuilder = ReplicatedSpace.builder(runtime, "commits", identity, "taker")
                    .clock(clock)
                    .settleWindow(Duration.ZERO);
            if (holder != null) {
                spaceBuilder.writer(holder);
            }
            ReplicatedSpace space = spaceBuilder.build();
            AtomicReference<OrderedTakes> ref = new AtomicReference<>();
            final int memberIndex = i;
            RaftLog raft = new RaftLog(new CapabilityPipes(runtime, codec), codec,
                    identity.peerId(), memberIds, clock, 2000 + i, (command, index) -> {
                OrderedTakes ordered = ref.get();
                if (ordered != null) {
                    ordered.onCommand(command, index);
                }
                if (memberIndex == 0) {
                    committedCommands.add(command);
                    committedIndices.add(index);
                }
            });
            OrderedTakes ordered = holder == null
                    ? new OrderedTakes(raft, space, identity, "taker", codec, clock)
                    : new OrderedTakes(raft, space, identity, holder, codec, clock);
            ref.set(ordered);
            nodes.add(node);
            members.add(new Member(node, space, raft, ordered));
        }
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
        return members;
    }

    private static void awaitLeader(List<Member> members) {
        for (int i = 0; i < 60; i++) {
            members.forEach(m -> m.raft().tick());
            if (members.stream().anyMatch(m -> m.raft().isLeader())) {
                return;
            }
        }
        throw new AssertionError("no leader");
    }

    /** Ticks Raft on the given members until the futures complete or rounds run out. */
    private static void tickUntilDone(List<Member> members, int rounds,
                                      CompletableFuture<?>... futures) throws Exception {
        for (int i = 0; i < rounds; i++) {
            members.forEach(m -> m.raft().tick());
            Thread.sleep(15);
            boolean allDone = true;
            for (CompletableFuture<?> future : futures) {
                allDone &= future.isDone();
            }
            if (allDone) {
                return;
            }
        }
    }

    @Test
    @Timeout(60)
    void exactlyOneCommittedTakeWinsPerEntry() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        Member a = members.get(0);
        Member b = members.get(1);
        Member c = members.get(2);

        a.space().write(new TaskEntry("commit-me", 1), MINUTES_30);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byB = CompletableFuture.supplyAsync(
                () -> b.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(20)));
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byC = CompletableFuture.supplyAsync(
                () -> c.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(20)));
        tickUntilDone(members, 1500, byB, byC);

        Optional<TakenEntry<TaskEntry>> takenB = byB.get();
        Optional<TakenEntry<TaskEntry>> takenC = byC.get();
        assertThat(takenB.isPresent() ^ takenC.isPresent())
                .as("exactly one of B/C wins the committed take")
                .isTrue();

        Member winner = takenB.isPresent() ? b : c;
        winner.space().complete(takenB.isPresent() ? takenB.get() : takenC.get());
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    /**
     * QA4 A4-5, the regression, written before the fix. A committed take is
     * completed by its winner while the winner is cut off, so the completion
     * rumor (which carries the holder's signed claim as proof) reaches nobody.
     * Every other member then has to converge by anti-entropy alone — and must:
     * the entry has to be absent at <em>every</em> replica, not just the winner's.
     *
     * <p>Today it is not. The Raft path installs the committed claim on each
     * follower through the bare {@code applyAuthorizedClaim}, which re-signs the
     * claim with the follower's own key: same claim content, useless signature.
     * The anti-entropy digest keys claims by content, so the follower advertises
     * "I hold this claim", the winner withholds the attested proof, and the
     * follower's {@code verifyState} rejects the completion forever. The Party
     * Bus's booking desk showed the symptom (orders confirmed once, never
     * drained); this is the mechanism, in a fixture that fits on one screen.
     */
    /** SPEC §4.2/§11a.4 v0.1.13 (review M-11): a subordinate holder's claim command carries its certificate, the log commits the agent-signed claim identically everywhere, and the holder's completion is agent-signed and drains the entry at every replica. */
    @Test
    @Timeout(60)
    void anAgentHeldOrderedTakeIsAgentSignedEndToEnd() throws Exception {
        List<Member> members = cluster(true);
        awaitLeader(members);
        Member a = members.get(0);
        Member b = members.get(1);
        a.space().write(new TaskEntry("agent-held", 1), MINUTES_30);
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byB = CompletableFuture.supplyAsync(
                () -> b.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(20)));
        tickUntilDone(members, 1500, byB);
        TakenEntry<TaskEntry> taken = byB.get().orElseThrow();
        for (int i = 0; i < 60 && !members.stream().allMatch(
                m -> m.space().claimProof(taken.entryId()).isPresent()); i++) {
            members.forEach(m -> m.raft().tick());
            Thread.sleep(15);
        }
        for (Member m : members) {
            assertThat(m.space().claimProof(taken.entryId()))
                    .as("the committed claim is agent-attested on " + m.node().peerId())
                    .hasValueSatisfying(proof -> assertThat(proof.agentAttested()).isTrue());
        }
        b.space().complete(taken);
        for (int i = 0; i < 20; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(a.space().signedState(taken.entryId()))
                .hasValueSatisfying(dto -> {
                    assertThat(dto.completed()).isTrue();
                    assertThat(dto.agentSigned()).as("the holder agent signed the completion").isTrue();
                });
        clock.advance(Duration.ofMinutes(11));
        for (int i = 0; i < 5; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
        for (Member m : members) {
            assertThat(m.space().read(Template.of(TaskEntry.class))).isEmpty();
        }
    }

    @Test
    @Timeout(60)
    void aCompletionDrainsTheEntryAtEveryReplica() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        Member a = members.get(0);
        Member b = members.get(1);
        Member c = members.get(2);

        a.space().write(new TaskEntry("drain-me", 1), MINUTES_30);
        assertThat(c.space().read(Template.of(TaskEntry.class))).isPresent();

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byB = CompletableFuture.supplyAsync(
                () -> b.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(20)));
        tickUntilDone(members, 1500, byB);
        TakenEntry<TaskEntry> taken = byB.get().orElseThrow();
        // The winner adopts its claim the moment it commits; the followers apply
        // the same committed command on their next Raft round. Give them that
        // round before asserting the log decided identically everywhere.
        for (int i = 0; i < 60 && !members.stream().allMatch(
                m -> m.space().currentClaim(taken.entryId()).isPresent()); i++) {
            members.forEach(m -> m.raft().tick());
            Thread.sleep(15);
        }
        // The log committed the claim everywhere: every member sees the same holder.
        for (Member m : members) {
            assertThat(m.space().currentClaim(taken.entryId()))
                    .as("claim committed on " + m.node().peerId())
                    .hasValueSatisfying(cl -> assertThat(cl.holder().peer())
                            .isEqualTo(b.node().peerId()));
        }

        // A taken entry is invisible to read() until it completes or reappears,
        // so observe the completion the way an application would: by event.
        List<SpaceEvent.Kind> seenAtA = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<SpaceEvent.Kind> seenAtC = new java.util.concurrent.CopyOnWriteArrayList<>();
        a.space().notify(Template.of(TaskEntry.class), e -> seenAtA.add(e.kind()), MINUTES_30);
        c.space().notify(Template.of(TaskEntry.class), e -> seenAtC.add(e.kind()), MINUTES_30);

        // Cut the winner off so its completion rumor reaches nobody, then complete.
        network.partition("n1", "n0");
        network.partition("n1", "n2");
        b.space().complete(taken);
        assertThat(seenAtA).as("the rumor was cut: a has not seen the completion")
                .doesNotContain(SpaceEvent.Kind.COMPLETED);

        // Heal and let anti-entropy alone carry the completion.
        network.heal();
        for (int i = 0; i < 40; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // one anti-entropy round per gossip period
        }
        assertThat(seenAtA).as("a learned the completion by anti-entropy")
                .contains(SpaceEvent.Kind.COMPLETED);
        assertThat(seenAtC).as("c learned the completion by anti-entropy")
                .contains(SpaceEvent.Kind.COMPLETED);

        // The symptom the Party Bus showed: if a replica never authenticated the
        // completion, the TAKE lease lapses there and the entry comes back as
        // takeable work. Run the lease out; nothing may reappear anywhere.
        clock.advance(Duration.ofMinutes(11));
        for (int i = 0; i < 5; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
        for (Member m : members) {
            assertThat(m.space().read(Template.of(TaskEntry.class)))
                    .as("completed work must not reappear at " + m.node().peerId())
                    .isEmpty();
        }
        assertThat(seenAtA).doesNotContain(SpaceEvent.Kind.REAPPEARED);
        assertThat(seenAtC).doesNotContain(SpaceEvent.Kind.REAPPEARED);
    }

    @Test
    @Timeout(60)
    void applyingCommittedCommandsIsIndependentOfTheWallClock() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        Member a = members.get(0);
        Member b = members.get(1);
        Member c = members.get(2);

        a.space().write(new TaskEntry("determinism", 1), MINUTES_30);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        // Capture the id while the entry is still available (unclaimed); once it
        // is taken, entryIdOf no longer returns it.
        EntryId entry = a.space().entryIdOf(new TaskEntry("determinism", 1)).orElseThrow();

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byB = CompletableFuture.supplyAsync(
                () -> b.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(20)));
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byC = CompletableFuture.supplyAsync(
                () -> c.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(20)));
        tickUntilDone(members, 1500, byB, byC);
        byB.get();
        byC.get();

        TakeClaim committed = a.space().currentClaim(entry).orElseThrow();

        // The applier must be a pure function of committed state. Re-apply the
        // exact committed command stream into a fresh applier whose wall clock
        // is skewed forward past the take lease (so the committed claim reads as
        // long expired): if application depended on the clock, that applier
        // would reach a different accept/reject decision and diverge. It must
        // reach the identical committed claim.
        clock.advance(Duration.ofMinutes(15)); // past the 10-min take lease, within the 30-min write lease
        OrderedTakes replay = freshApplier();
        List<byte[]> commands = List.copyOf(committedCommands);
        List<Long> indices = List.copyOf(committedIndices);
        for (int i = 0; i < commands.size(); i++) {
            replay.onCommand(commands.get(i), indices.get(i));
        }

        TakeClaim replayed = replayApplierSpace.currentClaim(entry).orElseThrow();
        assertThat(replayed.holder()).isEqualTo(committed.holder());
        assertThat(replayed.epoch()).isEqualTo(committed.epoch());
        // And the live members, whose clocks also advanced, still agree on the winner.
        assertThat(b.space().currentClaim(entry).orElseThrow().holder())
                .isEqualTo(committed.holder());
        assertThat(c.space().currentClaim(entry).orElseThrow().holder())
                .isEqualTo(committed.holder());
    }

    private ReplicatedSpace replayApplierSpace;

    /**
     * A standalone applier over its own space, for replaying a captured command
     * stream in isolation. Its {@code onCommand} is called directly (not via
     * Raft), and {@code applyAuthorizedClaim} sets the claim on {@code entry}
     * without the entry needing to be present.
     */
    private OrderedTakes freshApplier() throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(999).build();
        node.listen(network.register("replay"), "replay");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
        replayApplierSpace = ReplicatedSpace.builder(runtime, "commits", identity, "taker")
                .clock(clock).settleWindow(Duration.ZERO).build();
        RaftLog raft = new RaftLog(new CapabilityPipes(runtime, codec), codec,
                identity.peerId(), List.of(identity.peerId()), clock, 5999, (c, i) -> {
                });
        nodes.add(node);
        return new OrderedTakes(raft, replayApplierSpace, identity, "taker", codec, clock);
    }

    /** SPEC §8 / §7.4 ORDERED: a partitioned minority takes nothing, commits nothing, and adopts the majority's decision once healed (spec P1). */
    @Test
    @Timeout(60)
    void aPartitionedMinorityCannotTake() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        Member a = members.get(0);
        Member c = members.get(2);

        a.space().write(new TaskEntry("majority-only", 1), MINUTES_30);
        assertThat(c.space().read(Template.of(TaskEntry.class))).isPresent();
        EntryId entry = a.space().entryIdOf(new TaskEntry("majority-only", 1)).orElseThrow();

        // Isolate C: it keeps its replica but loses the quorum.
        network.partition("n2", "n0");
        network.partition("n2", "n1");

        // C's attempt window is longer than the coordinator's 3-second resubmit
        // period, so the empty result is the quorum's verdict, not a coincidence
        // of timeouts: C rescans, resubmits, and still cannot commit.
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byC = CompletableFuture.supplyAsync(
                () -> c.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(5)));
        tickUntilDone(members, 500, byC);
        assertThat(byC.get()).as("no quorum, no take (spec P1)").isEmpty();
        assertThat(c.space().currentClaim(entry)).as("nothing committed on the minority").isEmpty();
        assertThat(c.raft().isLeader()).as("a minority cannot elect itself").isFalse();

        // The majority side still takes with exactly-once semantics.
        List<Member> majority = List.of(members.get(0), members.get(1));
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byA = CompletableFuture.supplyAsync(
                () -> a.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(20)));
        tickUntilDone(majority, 1500, byA);
        assertThat(byA.get()).isPresent();
        TakeClaim decided = a.space().currentClaim(entry).orElseThrow();

        // Healed, the minority catches up on the log and applies the same claim.
        network.heal();
        for (int i = 0; i < 60; i++) {
            members.forEach(m -> m.raft().tick());
            if (c.space().currentClaim(entry).isPresent()) {
                break;
            }
        }
        TakeClaim onC = c.space().currentClaim(entry).orElseThrow();
        assertThat(onC.holder()).isEqualTo(decided.holder());
        assertThat(onC.epoch()).isEqualTo(decided.epoch());
        assertThat(members.stream().filter(m -> m.raft().isLeader())).hasSize(1);

        a.space().complete(byA.get().get());
    }

    /** SPEC §8 ordered-log: exactly-once holds across a leader failure mid-take — the lost submit is resubmitted to the successor and one generation commits once. */
    @Test
    @Timeout(60)
    void exactlyOnceSurvivesLeaderFailureMidTake() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        Member leader = members.stream().filter(m -> m.raft().isLeader()).findFirst().orElseThrow();
        List<Member> survivors = members.stream().filter(m -> m != leader).toList();
        Member x = survivors.get(0);
        Member y = survivors.get(1);

        members.get(0).space().write(new TaskEntry("failover", 1), MINUTES_30);
        for (Member member : members) {
            assertThat(member.space().read(Template.of(TaskEntry.class))).isPresent();
        }
        EntryId entry = x.space().entryIdOf(new TaskEntry("failover", 1)).orElseThrow();

        // The leader dies before either taker's first submit can reach it: both
        // followers forward their ClaimCommand to a leader hint that is gone.
        String leaderAddr = "n" + members.indexOf(leader);
        for (Member survivor : survivors) {
            network.partition("n" + members.indexOf(survivor), leaderAddr);
        }

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byX = CompletableFuture.supplyAsync(
                () -> x.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(12)));
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byY = CompletableFuture.supplyAsync(
                () -> y.ordered().take(Template.of(TaskEntry.class), MINUTES_10,
                        Duration.ofSeconds(12)));
        tickUntilDone(survivors, 1500, byX, byY);

        Optional<TakenEntry<TaskEntry>> takenX = byX.get();
        Optional<TakenEntry<TaskEntry>> takenY = byY.get();
        assertThat(takenX.isPresent() ^ takenY.isPresent())
                .as("exactly one of the survivors wins the take").isTrue();
        Member newLeader = survivors.stream().filter(m -> m.raft().isLeader()).findFirst()
                .orElseThrow(() -> new AssertionError("survivors elected no leader"));
        assertThat(newLeader).isNotSameAs(leader);

        TakeClaim decided = x.space().currentClaim(entry).orElseThrow();
        assertThat(decided.epoch()).as("first generation, committed once").isEqualTo(1);
        assertThat(y.space().currentClaim(entry).orElseThrow().holder()).isEqualTo(decided.holder());

        // The old leader rejoins and applies the same committed claim, no second
        // generation is ever committed, and the cluster has one leader.
        network.heal();
        for (int i = 0; i < 60; i++) {
            members.forEach(m -> m.raft().tick());
            if (leader.space().currentClaim(entry).isPresent()) {
                break;
            }
        }
        TakeClaim onOldLeader = leader.space().currentClaim(entry).orElseThrow();
        assertThat(onOldLeader.holder()).isEqualTo(decided.holder());
        assertThat(onOldLeader.epoch()).isEqualTo(1);
        assertThat(leader.raft().isLeader()).isFalse();
        assertThat(members.stream().filter(m -> m.raft().isLeader())).hasSize(1);
        for (Member member : members) {
            assertThat(member.space().currentClaim(entry).orElseThrow().epoch()).isEqualTo(1);
        }
    }
}
