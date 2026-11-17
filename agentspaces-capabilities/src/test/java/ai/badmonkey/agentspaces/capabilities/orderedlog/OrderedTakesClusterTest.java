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
import ai.badmonkey.agentspaces.space.replicated.SpaceWire;
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
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < identities.size(); i++) {
            members.add(start(identities, i, agentHolders));
        }
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
        return members;
    }

    /**
     * Starts member {@code i} of a log over every identity given: its node, its space, its Raft log and its ordered
     * takes. A member started after the others is a late joiner of a log that already counted it as a voter.
     */
    private Member start(List<PeerIdentity> identities, int i, boolean agentHolders) throws IOException {
        List<PeerId> memberIds = identities.stream().map(PeerIdentity::peerId).toList();
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
        return new Member(node, space, raft, ordered);
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

    /**
     * The 0.6.0 regression, 2026-11-10: a take whose claim the log commits after the caller's timeout returned empty
     * while the committed claim named the caller, so nobody adopted, drained or completed the entry until the claim's
     * lease lapsed. An ORDERED reduce on a 200 ms poll stalled 30 s at a time that way (agentspaces-clj
     * layer4_options_test). The taker now waits out its submitted claim's attempt window and holds the entry.
     */
    @Test
    @Timeout(60)
    void aClaimTheLogCommitsAfterTheTakersTimeoutStillHandsTheEntryToTheTaker() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        Member a = members.get(0);
        Member b = members.get(1);
        a.space().write(new TaskEntry("late-commit", 1), MINUTES_30);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();

        // the taker's frames wait in the network, so the log cannot commit its claim inside the taker's 100 ms
        network.hold("n1", "n0");
        network.hold("n1", "n2");
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byB = CompletableFuture.supplyAsync(
                () -> b.ordered().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofMillis(100)));
        Thread.sleep(400);
        network.release("n1", "n0", SimNetwork.Order.FIFO, 0);
        network.release("n1", "n2", SimNetwork.Order.FIFO, 0);
        tickUntilDone(members, 1500, byB);

        assertThat(byB.get()).as("the taker holds the entry its committed claim names").isPresent();
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

    /**
     * The ordered-log renewal defect the PetClinic rebuild found (2026-11-15): a holder drains for
     * longer than its take lease and renews as it goes. Every voter whose log has applied the claim
     * must merge each renewal, since a renewal is the same holder, stamp, epoch and bid with a later
     * expiry; the QA4 A4-5 rule refused it there, so the rival's replica saw the lease run out after
     * one lease length and the log committed the rival's next epoch while the holder still held the
     * ticket. Here the drain runs four lease lengths, every voter keeps the renewed expiry, and the
     * rival takes nothing until the holder stops renewing. The rival's take after the lapse shows the
     * log was live throughout, so the empty takes during the drain are the lease's verdict.
     */
    @Test
    @Timeout(120)
    void aDrainLongerThanTheTakeLeaseKeepsTheTakeAtEveryVoter() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        Member a = members.get(0);
        Member b = members.get(1);
        Member c = members.get(2);
        Lease takeLease = Lease.of(Duration.ofSeconds(30));
        a.space().write(new TaskEntry("ticket", 1), MINUTES_30);
        assertThat(c.space().read(Template.of(TaskEntry.class))).isPresent();

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byB = CompletableFuture.supplyAsync(
                () -> b.ordered().take(Template.of(TaskEntry.class), takeLease, Duration.ofSeconds(20)));
        tickUntilDone(members, 1500, byB);
        TakenEntry<TaskEntry> ticket = byB.get().orElseThrow();
        EntryId id = ticket.entryId();
        // Every voter's log applies the claim first, so the log-decided rule is in force everywhere
        // before the first renewal: the order in which the defect showed.
        for (int i = 0; i < 200 && !members.stream().allMatch(m -> m.space().currentClaim(id).isPresent()); i++) {
            members.forEach(m -> m.raft().tick());
            Thread.sleep(15);
        }
        long committedExpiry = c.space().currentClaim(id).orElseThrow().expiresAtMillis();
        long drainStart = clock.millis();

        int maxRounds = 0;
        for (int step = 0; step < 12; step++) {               // 12 renewals 10 s apart: four lease lengths
            clock.advance(Duration.ofSeconds(10));             // a step of the drain, then the holder renews
            ticket.renew(Duration.ofSeconds(30));
            long renewed = b.space().currentClaim(id).orElseThrow().expiresAtMillis();
            // A generous ceiling of 19 gossip rounds of 1 s: with the 10 s step that stays inside the 30 s lease.
            // The rounds used are measured.
            int rounds = 0;
            while (rounds < 19 && !members.stream().allMatch(m -> m.space().currentClaim(id)
                    .map(cl -> cl.expiresAtMillis() == renewed).orElse(false))) {
                nodes.forEach(PeerNode::tick);
                clock.advance(Duration.ofSeconds(1));
                rounds++;
            }
            maxRounds = Math.max(maxRounds, rounds);
            for (Member m : members) {
                String where = "renewal " + step + " at " + m.node().peerId();
                TakeClaim held = m.space().currentClaim(id).orElseThrow();
                assertThat(held.holder()).as(where + ": the holder").isEqualTo(b.ordered().holder());
                assertThat(held.epoch()).as(where + ": the epoch").isEqualTo(1L);
                assertThat(held.expiresAtMillis()).as(where + ": the renewed expiry").isEqualTo(renewed);
            }
            CompletableFuture<Optional<TakenEntry<TaskEntry>>> byC = CompletableFuture.supplyAsync(
                    () -> c.ordered().take(Template.of(TaskEntry.class), takeLease, Duration.ofMillis(300)));
            tickUntilDone(members, 400, byC);
            assertThat(byC.get()).as("the rival takes nothing while the holder renews (step " + step + ")")
                    .isEmpty();
        }
        long drained = clock.millis() - drainStart;
        System.out.println("ORDERED-RENEWAL-MEASURE drain " + drained + " ms on the test clock over a "
                + takeLease.duration().toMillis() + " ms lease; committed expiry passed by "
                + (clock.millis() - committedExpiry) + " ms; renewal reached every voter in at most "
                + maxRounds + " gossip rounds");
        assertThat(drained).as("the drain outlasted the take lease").isGreaterThan(3 * 30_000L);

        // The holder stops renewing. Once its lease runs out, the rival's take goes through the same
        // log at the next epoch, and the old holder can no longer renew.
        clock.advance(Duration.ofSeconds(31));
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byC = CompletableFuture.supplyAsync(
                () -> c.ordered().take(Template.of(TaskEntry.class), takeLease, Duration.ofSeconds(20)));
        tickUntilDone(members, 1500, byC);
        TakenEntry<TaskEntry> rival = byC.get().orElseThrow();
        assertThat(rival.entryId()).isEqualTo(id);
        for (int i = 0; i < 200 && b.space().currentClaim(id).map(cl -> cl.epoch() != 2).orElse(true); i++) {
            members.forEach(m -> m.raft().tick());
            Thread.sleep(15);
        }
        assertThat(b.space().currentClaim(id).orElseThrow().epoch()).isEqualTo(2L);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> ticket.renew(Duration.ofSeconds(30)))
                .isInstanceOf(ai.badmonkey.agentspaces.api.error.LeaseExpiredException.class);
        c.space().complete(rival);
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

    /**
     * HLC round six, 2026-11-08 (owner rulings F1 and F3): a committed command decodes only from
     * its own wire encoding. The holder's genuine signature inside a command whose claim fields
     * arrive in another order installs nothing at an applier, and the command as committed does.
     */
    @Test
    @Timeout(60)
    void aCommandInAnyFormButItsOwnEncodingInstallsNothing() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        Member a = members.get(0);
        Member b = members.get(1);
        a.space().write(new TaskEntry("relabelled command", 1), MINUTES_30);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        EntryId entry = a.space().entryIdOf(new TaskEntry("relabelled command", 1)).orElseThrow();
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byB = CompletableFuture.supplyAsync(
                () -> b.ordered().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofSeconds(20)));
        tickUntilDone(members, 1500, byB);
        assertThat(byB.get()).isPresent();
        for (int i = 0; i < 60 && a.space().currentClaim(entry).isEmpty(); i++) {
            members.forEach(m -> m.raft().tick()); // the first member applies, and records, the command
        }
        TakeClaim committed = a.space().currentClaim(entry).orElseThrow();
        byte[] claim = codec.toBytes(committed);
        int at = -1;
        byte[] honest = null;
        for (int i = 0; i < committedCommands.size() && honest == null; i++) {
            at = indexOf(committedCommands.get(i), claim);
            honest = at < 0 ? null : committedCommands.get(i);
        }
        assertThat(honest).as("the committed command").isNotNull();
        ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim reordered =
                new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim();
        reordered.put("spaceId", committed.spaceId().value());
        reordered.put("entryId", committed.entryId().value());
        reordered.put("epoch", committed.epoch());
        reordered.put("stamp", committed.stamp().encoded());
        reordered.put("holder", committed.holder().encoded());
        reordered.put("bid", committed.bid());
        reordered.put("expiresAtMillis", committed.expiresAtMillis());
        byte[] moved = codec.toBytes(reordered);
        assertThat(moved).hasSameSizeAs(claim).isNotEqualTo(claim);
        byte[] relabelled = honest.clone();
        System.arraycopy(moved, 0, relabelled, at, moved.length);

        OrderedTakes replay = freshApplier();
        replay.onCommand(relabelled, 1);
        assertThat(replayApplierSpace.currentClaim(entry)).as("the relabelled command installs nothing").isEmpty();
        replay.onCommand(honest, 1);
        assertThat(replayApplierSpace.currentClaim(entry)).map(TakeClaim::holder).contains(committed.holder());
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            if (java.util.Arrays.equals(haystack, i, i + needle.length, needle, 0, needle.length)) {
                return i;
            }
        }
        return -1;
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

    /** One event a late joiner heard: its kind, the entry's topic, and how many entries the replica knew then. */
    private record Heard(SpaceEvent.Kind kind, String topic, int known) {
    }

    private static Template<TaskEntry> topic(String topic) {
        return Template.of(TaskEntry.class).where("topic", ai.badmonkey.agentspaces.api.space.Matchers.eq(topic));
    }

    /** What the majority did while the late member was cut off: one entry taken and completed, one taken, one open. */
    private record AwayWork(Member late, List<Member> live, Member taker, EntryId done, EntryId held, EntryId open,
                            TakenEntry<TaskEntry> heldTake, List<Heard> heard) {

        /** The same work, with a listener on the late member's space recording every event from now on. */
        AwayWork joinedBy(Member late) {
            List<Heard> heard = new java.util.concurrent.CopyOnWriteArrayList<>();
            late.space().notify(Template.of(TaskEntry.class),
                    e -> heard.add(new Heard(e.kind(), e.entry().topic(), late.space().knownEntries())), MINUTES_30);
            return new AwayWork(late, live, taker, done, held, open, heldTake, heard);
        }
    }

    /**
     * The late member is a follower cut off from both others before anything is written; the two live members (a log
     * quorum) do the away work of {@link #workWithout}.
     */
    private AwayWork workWhileAway(List<Member> members) throws Exception {
        Member late = members.stream().filter(m -> !m.raft().isLeader()).findFirst().orElseThrow();
        List<Member> live = members.stream().filter(m -> m != late).toList();
        String lateAddress = "n" + members.indexOf(late);
        for (Member m : live) {
            network.partition(lateAddress, "n" + members.indexOf(m));
        }
        AwayWork away = workWithout(live);
        assertThat(late.space().knownEntries()).as("the late member heard nothing while away").isZero();
        assertThat(late.space().currentClaim(away.done())).isEmpty();
        return away.joinedBy(late);
    }

    /**
     * The away work: the first live member writes three entries; the second takes two through the log and completes
     * one of them.
     */
    private AwayWork workWithout(List<Member> live) throws Exception {
        Member writer = live.get(0);
        Member taker = live.get(1);
        writer.space().write(new TaskEntry("done-while-away", 1), MINUTES_30);
        writer.space().write(new TaskEntry("held-while-away", 2), MINUTES_30);
        writer.space().write(new TaskEntry("open-while-away", 3), MINUTES_30);
        EntryId done = writer.space().entryIdOf(new TaskEntry("done-while-away", 1)).orElseThrow();
        EntryId held = writer.space().entryIdOf(new TaskEntry("held-while-away", 2)).orElseThrow();
        EntryId open = writer.space().entryIdOf(new TaskEntry("open-while-away", 3)).orElseThrow();
        assertThat(taker.space().read(topic("held-while-away"))).isPresent();

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> first = CompletableFuture.supplyAsync(
                () -> taker.ordered().take(topic("done-while-away"), MINUTES_10, Duration.ofSeconds(20)));
        tickUntilDone(live, 1500, first);
        taker.space().complete(first.get().orElseThrow());
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> second = CompletableFuture.supplyAsync(
                () -> taker.ordered().take(topic("held-while-away"), MINUTES_10, Duration.ofSeconds(20)));
        tickUntilDone(live, 1500, second);
        TakenEntry<TaskEntry> heldTake = second.get().orElseThrow();
        return new AwayWork(null, live, taker, done, held, open, heldTake, List.of());
    }

    /** Anti-entropy only: node ticks, no Raft tick, so the log cannot tell the late member anything meanwhile. */
    private void antiEntropyUntil(java.util.function.BooleanSupplier done, int rounds) {
        for (int i = 0; i < rounds && !done.getAsBoolean(); i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /**
     * RBE regression matrix probe P4 (srad/replication-by-entry.md phase 1, RE-M-08): a late joiner learns the whole
     * space in one {@code PULL_RESP}, the committed claims and the completed state they authenticate together. The
     * batch merges its claims before its states, so the completion is accepted and drains the entry, the held entry
     * stays held by the log's winner, and the open one is takeable. The log then catches up: its committed claims
     * agree with what the batch brought, no second generation is committed, the late member adopts a take of its own
     * through the log, and nothing reappears when the take leases run out.
     */
    @Test
    @Timeout(90)
    void aLateJoinerTakesAClaimAndItsCompletionFromOneBatchThenAgreesWithTheLog() throws Exception {
        // The third voter starts after the work: a member that was never there gets no lazy-push announcement of the
        // older rumors (they predate its admission), so anti-entropy is its only source and one PULL_RESP carries it all.
        List<PeerIdentity> identities =
                List.of(PeerIdentity.generate(), PeerIdentity.generate(), PeerIdentity.generate());
        List<Member> live = List.of(start(identities, 0, false), start(identities, 1, false));
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
        awaitLeader(live);
        AwayWork work = workWithout(live);
        Member late = start(identities, 2, false);
        assertThat(late.space().knownEntries()).isZero();
        AwayWork away = work.joinedBy(late);
        List<Member> members = List.of(live.get(0), live.get(1), late);

        antiEntropyUntil(() -> late.space().knownEntries() == 3
                && late.space().signedState(away.done()).map(SpaceWire.EntryStateDto::completed).orElse(false), 40);

        assertThat(late.space().knownEntries()).isEqualTo(3);
        assertThat(away.heard()).as("the late member heard the away work").isNotEmpty();
        assertThat(away.heard()).as("one batch: every event fired after all three entries were in")
                .allSatisfy(h -> assertThat(h.known()).isEqualTo(3));
        assertThat(late.space().signedState(away.done()))
                .as("the completion was accepted beside the claim that authenticates it")
                .hasValueSatisfying(dto -> assertThat(dto.completed()).isTrue());
        for (EntryId id : List.of(away.done(), away.held())) {
            assertThat(late.space().currentClaim(id)).as("the log's claim, by anti-entropy, for " + id)
                    .hasValueSatisfying(c -> {
                        assertThat(c.holder().peer()).isEqualTo(away.taker().node().peerId());
                        assertThat(c.epoch()).isEqualTo(1);
                    });
        }
        assertThat(late.space().read(topic("done-while-away"))).as("completed work is gone").isEmpty();
        assertThat(late.space().read(topic("held-while-away"))).as("held work is invisible").isEmpty();
        assertThat(late.space().read(topic("open-while-away"))).as("open work is readable").isPresent();
        assertThat(away.heard()).extracting(Heard::kind).doesNotContain(SpaceEvent.Kind.REAPPEARED);
        assertThat(away.heard()).filteredOn(h -> h.topic().equals("done-while-away")).extracting(Heard::kind)
                .as("a completed entry learned late is no new work").doesNotContain(SpaceEvent.Kind.WRITTEN);

        lateAdoptsAndNothingReappears(members, away);
    }

    /**
     * RBE regression matrix probe P4, the other order: the log catches a cut-off member up first, so it holds the
     * committed claims as claim-only slots with no record (phase 1 keeps a claim in a slot of its own), and replication
     * then brings the records and the completion. A member that was there before the cut also hears the lazy-push
     * announcements of the rumors it missed, so the records come by grafted rumors and anti-entropy in whatever mix the
     * rounds give, not always in one batch: what is pinned is the outcome. The completion must still meet the log's
     * claim, and the slots must keep their claims beside the records.
     */
    @Test
    @Timeout(90)
    void aMemberHoldingOnlyTheLogsClaimsAcceptsTheRecordsAndTheCompletionAfter() throws Exception {
        List<Member> members = cluster();
        awaitLeader(members);
        AwayWork away = workWhileAway(members);
        Member late = away.late();

        network.heal();
        for (int i = 0; i < 300 && (late.space().currentClaim(away.done()).isEmpty()
                || late.space().currentClaim(away.held()).isEmpty()); i++) {
            members.forEach(m -> m.raft().tick());
            Thread.sleep(10);
        }
        assertThat(late.space().currentClaim(away.done())).as("the log installed the claim with no record here")
                .isPresent();
        assertThat(late.space().currentClaim(away.held())).isPresent();
        assertThat(late.space().knownEntries()).as("claims only: no record has arrived").isZero();

        antiEntropyUntil(() -> late.space().knownEntries() == 3
                && late.space().signedState(away.done()).map(SpaceWire.EntryStateDto::completed).orElse(false), 40);

        assertThat(late.space().knownEntries()).isEqualTo(3);
        assertThat(late.space().signedState(away.done()))
                .as("the completion met the claim the log installed")
                .hasValueSatisfying(dto -> assertThat(dto.completed()).isTrue());
        assertThat(late.space().currentClaim(away.held()).orElseThrow().holder().peer())
                .isEqualTo(away.taker().node().peerId());
        assertThat(late.space().read(topic("done-while-away"))).isEmpty();
        assertThat(late.space().read(topic("held-while-away"))).isEmpty();
        assertThat(late.space().read(topic("open-while-away"))).isPresent();

        lateAdoptsAndNothingReappears(members, away);
    }

    /** The end both orders share: the log agrees, the late member takes the open entry, and nothing comes back. */
    private void lateAdoptsAndNothingReappears(List<Member> members, AwayWork away) throws Exception {
        Member late = away.late();
        for (int i = 0; i < 100; i++) {
            members.forEach(m -> m.raft().tick());
        }
        for (EntryId id : List.of(away.done(), away.held())) {
            assertThat(late.space().currentClaim(id)).as("the log and the batch agree on " + id)
                    .hasValueSatisfying(c -> {
                        assertThat(c.holder().peer()).isEqualTo(away.taker().node().peerId());
                        assertThat(c.epoch()).as("no second generation").isEqualTo(1);
                    });
        }

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> byLate = CompletableFuture.supplyAsync(
                () -> late.ordered().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofSeconds(20)));
        tickUntilDone(members, 1500, byLate);
        TakenEntry<TaskEntry> mine = byLate.get().orElseThrow(
                () -> new AssertionError("the late member adopted no take of its own"));
        assertThat(mine.entry().topic()).as("only the open entry is takeable").isEqualTo("open-while-away");
        late.space().complete(mine);
        away.taker().space().complete(away.heldTake());
        antiEntropyUntil(() -> late.space().signedState(away.held()).map(SpaceWire.EntryStateDto::completed)
                .orElse(false), 40);
        assertThat(late.space().signedState(away.held()))
                .hasValueSatisfying(dto -> assertThat(dto.completed()).isTrue());

        clock.advance(Duration.ofMinutes(11)); // past every take lease, inside the write leases
        for (int i = 0; i < 5; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
        for (Member m : members) {
            assertThat(m.space().read(Template.of(TaskEntry.class)))
                    .as("nothing reappears at " + m.node().peerId()).isEmpty();
        }
        assertThat(away.heard()).extracting(Heard::kind).doesNotContain(SpaceEvent.Kind.REAPPEARED);
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
