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

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.AppendEntries;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.AppendReply;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.ClientSubmit;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.Frame;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.LogEntry;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.RequestVote;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.VoteReply;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.ObjLongConsumer;

/**
 * The {@code aspace:cap/ordered-log} capability (spec §8): a Raft group elected
 * among a fixed set of volunteering members, exposing an append-ordered log that
 * backs the {@code ORDERED} strategy. This is Raft's core (leader election with
 * randomized timeouts, log replication, commit on majority, leader-hint client
 * submits) over the group's capability pipes; membership change via joint
 * consensus is deferred, so the member set is fixed at construction, which is
 * the honest v0.1 of "a small quorum elected within the group".
 *
 * <p>Progress is tick-driven: call {@link #tick()} at a steady cadence (the peer
 * node's tick works). Committed commands reach the listener in log order, on
 * every member, exactly once per member.
 *
 * <p>The leader lease is an advertisement (spec §8): {@link #describe} carries
 * {@code role} ({@code leader|follower|candidate}), {@code term}, and, for the
 * leader only, {@code leaseUntil} = the issue instant plus this member's election
 * timeout expressed in wall time ({@code electionTimeout × tickPeriod}), so a
 * consumer finds the current leader through discovery instead of probing, and a
 * leader that stops refreshing lapses like any other offer. That arithmetic is
 * only honest if {@code tickPeriod} is the cadence the log is really driven at,
 * so a log registered on a {@code CapabilityRuntime} is told the true cadence
 * through {@link #driverCadence(Duration)} and the constructor's value becomes a
 * fallback for hand-driven hosts (QA3 A3-4).
 *
 * <p>Durability (srad/agentspaces-js.md JS-N-06): the term, the vote, and the
 * entries live in a {@link LogStore}, written before any reply that depends on
 * them leaves this member — a candidacy or a vote before the RequestVote or
 * VoteReply, a term bump before it is acted on, appended entries before the
 * AppendReply, and the leader's own entry before any AppendEntries carries it.
 * A store write that fails restores the in-memory state to what is durable,
 * sends nothing (the peer retries), and is counted in {@link #storeFailures()};
 * it never fails a tick. The constructors without a store use a
 * {@link MemoryLogStore}, which is the pre-JS-N-06 behaviour exactly: such a
 * voter forgets everything on restart. With a {@link FileLogStore} a restarted
 * voter rejoins at its last term with its vote and entries, so it cannot vote
 * twice in a term; its commit index starts at zero, so the applied listener
 * sees the committed prefix again from index 1 (Raft's usual replay; the
 * ordered-take applier is idempotent per generation, so that is harmless).
 *
 * <p>Thread-safety: all mutation happens under one lock; the applied listener is
 * invoked outside it.
 */
public final class RaftLog implements CapabilityProvider {

    private static final System.Logger LOG = System.getLogger(RaftLog.class.getName());

    /** The capability type URI. */
    public static final String TYPE = "aspace:cap/ordered-log";

    /** The wall-clock length of one {@link #tick()} assumed when none is given. */
    public static final Duration DEFAULT_TICK_PERIOD = Duration.ofSeconds(1);

    private enum Role {
        FOLLOWER, CANDIDATE, LEADER
    }

    private final CapabilityPipes pipes;
    private final CborCodec codec;
    private final PeerId self;
    private final List<PeerId> members;
    /** Who may drive the log (TODO-EFG §4): consulted with {@code RAFT_VOTER}
     * in {@link #scope} for every inbound frame, after the fixed-member check. */
    private final Authorizer authorizer;
    /** The authorizer's scope: the group id, or empty for the members-only default. */
    private final String scope;
    private final InstantSource clock;
    /**
     * The cadence {@link #tick()} is believed to arrive at, used to express the
     * election timeout in wall time. Declared at construction and corrected by
     * {@link #driverCadence(Duration)} when a runtime drives the log, so the
     * advertised lease matches the clock the log actually runs on (QA3 A3-4).
     */
    private volatile Duration tickPeriod;
    private final Random random;
    private final ObjLongConsumer<byte[]> applied;
    /** Where term, vote, and entries are made durable before a reply leaves (JS-N-06). */
    private final LogStore store;
    /** Store writes that failed; each left a reply unsent and the memory state as it was. */
    private final java.util.concurrent.atomic.AtomicLong storeFailures =
            new java.util.concurrent.atomic.AtomicLong();

    private final Object lock = new Object();
    private Role role = Role.FOLLOWER;
    private long term;
    private PeerId votedFor;
    private PeerId leaderHint;
    private final List<LogEntry> log = new ArrayList<>();
    private long commitIndex;
    private long lastApplied;
    /** Serializes {@link #applyCommitted()} drains; always outer to {@code lock}. */
    private final Object applyLock = new Object();
    /** Members whose vote this candidate holds for {@link #term}; distinct by
     * authenticated sender, so a replayed or duplicated grant counts once
     * (ASF-005). */
    private final java.util.Set<PeerId> votesGranted = new java.util.HashSet<>();
    private int electionElapsed;
    private int electionTimeout;
    private final Map<PeerId, Long> nextIndex = new HashMap<>();
    private final Map<PeerId, Long> matchIndex = new HashMap<>();

    /**
     * Creates a Raft member.
     *
     * @param pipes   the group's capability pipes
     * @param codec   the CBOR codec
     * @param self    this member's peer id
     * @param members the fixed member set, including {@code self}
     * @param clock   the time source (advertisement freshness)
     * @param seed    randomness seed for election timeouts (vary per member)
     * @param applied receives each committed command with its log index, in order
     */
    public RaftLog(CapabilityPipes pipes, CborCodec codec, PeerId self, List<PeerId> members,
                   InstantSource clock, long seed, ObjLongConsumer<byte[]> applied) {
        this(pipes, codec, self, members, clock, DEFAULT_TICK_PERIOD, seed, applied);
    }

    /**
     * Creates a Raft member whose term, vote, and entries are kept in
     * {@code store} (JS-N-06): the log loads the store's state before it joins
     * any election, and writes to it before every reply that depends on it.
     *
     * @param pipes   the group's capability pipes
     * @param codec   the CBOR codec
     * @param self    this member's peer id
     * @param members the fixed member set, including {@code self}
     * @param clock   the time source (advertisement freshness)
     * @param seed    randomness seed for election timeouts (vary per member)
     * @param applied receives each committed command with its log index, in order
     * @param store   the durable state; a {@link FileLogStore} survives a restart
     * @throws java.io.UncheckedIOException when the store cannot be read
     */
    public RaftLog(CapabilityPipes pipes, CborCodec codec, PeerId self, List<PeerId> members,
                   InstantSource clock, long seed, ObjLongConsumer<byte[]> applied,
                   LogStore store) {
        this(pipes, codec, self, members, clock, DEFAULT_TICK_PERIOD, seed, applied, store);
    }

    /**
     * Creates a Raft member whose advertised leader lease is sized from the tick
     * cadence the host drives it at.
     *
     * @param pipes      the group's capability pipes
     * @param codec      the CBOR codec
     * @param self       this member's peer id
     * @param members    the fixed member set, including {@code self}
     * @param clock      the time source (advertisement freshness)
     * @param tickPeriod the wall-clock period between {@link #tick()} calls; the
     *                   leader's {@code leaseUntil} is issued + electionTimeout ticks
     * @param seed       randomness seed for election timeouts (vary per member)
     * @param applied    receives each committed command with its log index, in order
     */
    public RaftLog(CapabilityPipes pipes, CborCodec codec, PeerId self, List<PeerId> members,
                   InstantSource clock, Duration tickPeriod, long seed,
                   ObjLongConsumer<byte[]> applied) {
        this(pipes, codec, self, members, clock, tickPeriod, seed, applied, new MemoryLogStore());
    }

    /**
     * As {@link #RaftLog(CapabilityPipes, CborCodec, PeerId, List, InstantSource, Duration, long,
     * ObjLongConsumer)}, with the member's durable state in {@code store} (JS-N-06).
     *
     * @param pipes      the group's capability pipes
     * @param codec      the CBOR codec
     * @param self       this member's peer id
     * @param members    the fixed member set, including {@code self}
     * @param clock      the time source (advertisement freshness)
     * @param tickPeriod the wall-clock period between {@link #tick()} calls
     * @param seed       randomness seed for election timeouts (vary per member)
     * @param applied    receives each committed command with its log index, in order
     * @param store      the durable state; a {@link FileLogStore} survives a restart
     * @throws java.io.UncheckedIOException when the store cannot be read
     */
    public RaftLog(CapabilityPipes pipes, CborCodec codec, PeerId self, List<PeerId> members,
                   InstantSource clock, Duration tickPeriod, long seed,
                   ObjLongConsumer<byte[]> applied, LogStore store) {
        this(pipes, codec, self, members, clock, tickPeriod, seed, applied,
                membersOnly(members), "", store);
    }

    /**
     * Creates a Raft member whose quorum is arbitrated by an {@link Authorizer}
     * (TODO-EFG §4, TODO item 6): an inbound frame is honoured only when its
     * authenticated sender is in the fixed member set <em>and</em>
     * {@code authorizer.permits(from, RAFT_VOTER, group)}. A member the
     * authorizer refuses can neither win an election (its vote requests are
     * dropped by every permitted member) nor commit a command (its forwarded
     * submits are dropped by the leader), while the permitted members keep
     * committing. The other constructors are the membership-rooted
     * convenience: an authorizer that permits exactly the member list.
     *
     * @param pipes      the group's capability pipes
     * @param codec      the CBOR codec
     * @param self       this member's peer id
     * @param members    the fixed member set, including {@code self}
     * @param clock      the time source (advertisement freshness)
     * @param tickPeriod the wall-clock period between {@link #tick()} calls
     * @param seed       randomness seed for election timeouts (vary per member)
     * @param applied    receives each committed command with its log index, in order
     * @param authorizer decides {@code RAFT_VOTER} per authenticated sender
     * @param group      the group the log runs in; the authorizer's scope
     */
    public RaftLog(CapabilityPipes pipes, CborCodec codec, PeerId self, List<PeerId> members,
                   InstantSource clock, Duration tickPeriod, long seed,
                   ObjLongConsumer<byte[]> applied, Authorizer authorizer, GroupId group) {
        this(pipes, codec, self, members, clock, tickPeriod, seed, applied, authorizer, group,
                new MemoryLogStore());
    }

    /**
     * As {@link #RaftLog(CapabilityPipes, CborCodec, PeerId, List, InstantSource, Duration, long,
     * ObjLongConsumer, Authorizer, GroupId)}, with the member's durable state in
     * {@code store} (JS-N-06).
     *
     * @param pipes      the group's capability pipes
     * @param codec      the CBOR codec
     * @param self       this member's peer id
     * @param members    the fixed member set, including {@code self}
     * @param clock      the time source (advertisement freshness)
     * @param tickPeriod the wall-clock period between {@link #tick()} calls
     * @param seed       randomness seed for election timeouts (vary per member)
     * @param applied    receives each committed command with its log index, in order
     * @param authorizer decides {@code RAFT_VOTER} per authenticated sender
     * @param group      the group the log runs in; the authorizer's scope
     * @param store      the durable state; a {@link FileLogStore} survives a restart
     * @throws java.io.UncheckedIOException when the store cannot be read
     */
    public RaftLog(CapabilityPipes pipes, CborCodec codec, PeerId self, List<PeerId> members,
                   InstantSource clock, Duration tickPeriod, long seed,
                   ObjLongConsumer<byte[]> applied, Authorizer authorizer, GroupId group,
                   LogStore store) {
        this(pipes, codec, self, members, clock, tickPeriod, seed, applied,
                Objects.requireNonNull(authorizer, "authorizer"),
                Objects.requireNonNull(group, "group").value(), store);
    }

    private RaftLog(CapabilityPipes pipes, CborCodec codec, PeerId self, List<PeerId> members,
                    InstantSource clock, Duration tickPeriod, long seed,
                    ObjLongConsumer<byte[]> applied, Authorizer authorizer, String scope,
                    LogStore store) {
        this.authorizer = authorizer;
        this.scope = scope;
        this.store = Objects.requireNonNull(store, "store");
        this.pipes = Objects.requireNonNull(pipes, "pipes");
        this.tickPeriod = Objects.requireNonNull(tickPeriod, "tickPeriod");
        if (tickPeriod.isZero() || tickPeriod.isNegative()) {
            throw new IllegalArgumentException("tickPeriod must be positive: " + tickPeriod);
        }
        this.codec = Objects.requireNonNull(codec, "codec");
        this.self = Objects.requireNonNull(self, "self");
        this.members = List.copyOf(members);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = new Random(seed);
        this.applied = Objects.requireNonNull(applied, "applied");
        if (!this.members.contains(self)) {
            throw new IllegalArgumentException("member set must include self");
        }
        this.electionTimeout = newElectionTimeout();
        // The store is read before the first frame or tick: a restarted voter
        // rejoins at its durable term, holding its vote and its entries (JS-N-06).
        LogStore.State durable;
        try {
            durable = store.load();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("cannot read the ordered log's store", e);
        }
        this.term = durable.term();
        this.votedFor = durable.votedFor();
        this.log.addAll(durable.entries());
        pipes.onCapability(TYPE, this::onFrame);
    }

    /** The membership-rooted default: exactly the fixed member set drives the log; any admitted peer may submit as a client. */
    private static Authorizer membersOnly(List<PeerId> members) {
        // Voting, leading, and replicating stay with the fixed member set (ASF-005); a command
        // from any admitted group peer is a Raft client's (JS-V-05), judged by onCommand.
        return (peer, operation, scope) -> operation == Authorizer.Operation.RAFT_CLIENT
                || members.contains(peer);
    }

    @Override
    public String capabilityType() {
        return TYPE;
    }

    @Override
    public CapabilityAdvertisement describe(GroupId group) {
        java.time.Instant issued = clock.instant();
        Map<String, String> parameters = new HashMap<>();
        parameters.put("members", String.valueOf(members.size()));
        synchronized (lock) {
            parameters.put("role", role.name().toLowerCase(java.util.Locale.ROOT));
            parameters.put("term", String.valueOf(term));
            if (role == Role.LEADER) {
                parameters.put("leaseUntil",
                        issued.plus(tickPeriod.multipliedBy(electionTimeout)).toString());
            }
        }
        return new CapabilityAdvertisement(
                "aspace://" + group.value() + "/cap/ordered-log/" + self.value(),
                self, group, issued, Duration.ofMinutes(15),
                TYPE, "0.1", "pipe", parameters, Map.of());
    }

    /** Returns the current term as this member knows it. */
    public long term() {
        synchronized (lock) {
            return term;
        }
    }

    /** Returns whether this member currently believes it is the leader. */
    public boolean isLeader() {
        synchronized (lock) {
            return role == Role.LEADER;
        }
    }

    /** Returns the current leader hint, when one is known. */
    public Optional<PeerId> leader() {
        synchronized (lock) {
            if (role == Role.LEADER) {
                return Optional.of(self);
            }
            return Optional.ofNullable(leaderHint);
        }
    }

    /** Returns the highest committed log index (1-based; 0 = nothing committed). */
    public long commitIndex() {
        synchronized (lock) {
            return commitIndex;
        }
    }

    /** Returns the highest log index this member holds, committed or not (0 = empty). */
    public long lastLogIndex() {
        synchronized (lock) {
            return log.size();
        }
    }

    /**
     * Store writes that failed (JS-N-06). Each left the in-memory state as the
     * store had it and the dependent reply, request, or submit undone, so the
     * peer retries; a rising count means the store needs attention.
     *
     * @return the number of failed durable writes since construction
     */
    public long storeFailures() {
        return storeFailures.get();
    }

    /**
     * Submits a command: appended directly when this member leads, forwarded to
     * the known leader otherwise.
     *
     * @param command the opaque command bytes
     * @return {@code true} when accepted locally or forwarded; {@code false} when
     *         no leader is known yet (retry after ticks), or when this leader's
     *         store refused the entry (counted in {@link #storeFailures()})
     */
    public boolean submit(byte[] command) {
        Objects.requireNonNull(command, "command");
        PeerId forwardTo = null;
        synchronized (lock) {
            if (role == Role.LEADER) {
                LogEntry entry = new LogEntry(term, command);
                log.add(entry);
                // The leader's own entry is durable before any AppendEntries carries it.
                if (!persistAppendLocked(log.size(), List.of(entry))) {
                    log.remove(log.size() - 1);
                    return false;
                }
                advanceCommitLocked();
            } else if (leaderHint != null) {
                forwardTo = leaderHint;
            } else {
                return false;
            }
        }
        if (forwardTo != null) {
            send(forwardTo, Frame.of(new ClientSubmit(command)));
        } else {
            broadcastAppends();
        }
        applyCommitted();
        return true;
    }

    /** Runs one protocol round: election timers, heartbeats, replication. */
    @Override
    public boolean requiresTick() {
        return true;
    }

    /**
     * Adopts the cadence the driver actually ticks at, so the advertised leader
     * lease ({@code electionTimeout × tickPeriod}) describes this log's real
     * election timeout rather than the one assumed at construction (QA3 A3-4).
     * The election timeout itself is counted in ticks and is unchanged; only the
     * wall-clock translation moves.
     *
     * @param period the true period between {@link #tick()} calls
     */
    @Override
    public void driverCadence(Duration period) {
        Objects.requireNonNull(period, "period");
        if (period.isZero() || period.isNegative()) {
            throw new IllegalArgumentException("cadence must be positive: " + period);
        }
        this.tickPeriod = period;
    }

    @Override
    public void tick() {
        List<Runnable> sends = new ArrayList<>();
        synchronized (lock) {
            if (role == Role.LEADER) {
                for (PeerId member : members) {
                    if (!member.equals(self)) {
                        sends.add(appendFor(member));
                    }
                }
            } else {
                electionElapsed++;
                if (electionElapsed >= electionTimeout) {
                    startElectionLocked(sends);
                }
            }
        }
        sends.forEach(Runnable::run);
        applyCommitted();
    }

    // ---------------------------------------------------------------- receive

    /** Client submits one non-member may make per second (a rate against floods, never a cap on clients). */
    public static final int CLIENT_SUBMITS_PER_SECOND = 32;
    private final Map<PeerId, long[]> clientBudgets = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong clientSubmits = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong clientSubmitsRefused =
            new java.util.concurrent.atomic.AtomicLong();

    /** Commands accepted from Raft clients (admitted non-members, {@code RAFT_CLIENT}). */
    public long clientSubmits() {
        return clientSubmits.get();
    }

    /** Client submits refused: not permitted, over the per-sender rate, or not a submit at all. */
    public long clientSubmitsRefused() {
        return clientSubmitsRefused.get();
    }

    private void onFrame(PeerId from, byte[] payload) {
        Frame frame;
        try {
            frame = codec.fromBytes(payload, Frame.class);
        } catch (RuntimeException e) {
            return;
        }
        if (frame == null) {
            return;
        }
        if (!members.contains(from)) {
            // ASF-005: only the fixed member set may drive elections or replicate
            // entries. An admitted non-member may submit a command as a Raft client
            // (srad/agentspaces-js.md JS-V-05) when the profile permits RAFT_CLIENT,
            // within a per-sender rate; the command's own signature is what onCommand
            // judges, so the log orders it and grants nothing by itself.
            if (frame.clientSubmit() == null || frame.clientSubmit().command() == null
                    || !authorizer.permits(from, Authorizer.Operation.RAFT_CLIENT, scope)
                    || !clientBudgetAdmits(from)) {
                clientSubmitsRefused.incrementAndGet();
                return;
            }
            clientSubmits.incrementAndGet();
            submit(frame.clientSubmit().command());
            return;
        }
        if (!authorizer.permits(from, Authorizer.Operation.RAFT_VOTER, scope)) {
            // TODO-EFG §4: a member the profile's authorizer refuses is a
            // replica, not a voter — its votes, appends, and submits are dropped.
            return;
        }
        if (frame.requestVote() != null) {
            onRequestVote(from, frame.requestVote());
        } else if (frame.voteReply() != null) {
            onVoteReply(from, frame.voteReply());
        } else if (frame.appendEntries() != null) {
            onAppendEntries(from, frame.appendEntries());
        } else if (frame.appendReply() != null) {
            onAppendReply(from, frame.appendReply());
        } else if (frame.clientSubmit() != null && frame.clientSubmit().command() != null) {
            submit(frame.clientSubmit().command());
        }
        applyCommitted();
    }

    /** A token bucket per client sender: CLIENT_SUBMITS_PER_SECOND, refilled each second of the log's clock. */
    private boolean clientBudgetAdmits(PeerId from) {
        long second = clock.instant().getEpochSecond();
        long[] bucket = clientBudgets.computeIfAbsent(from, f -> new long[]{second, 0});
        synchronized (bucket) {
            if (bucket[0] != second) {
                bucket[0] = second;
                bucket[1] = 0;
            }
            if (bucket[1] >= CLIENT_SUBMITS_PER_SECOND) {
                return false;
            }
            bucket[1]++;
        }
        if (clientBudgets.size() > 4096) {
            clientBudgets.entrySet().removeIf(e -> e.getValue()[0] < second - 60); // forget idle senders
        }
        return true;
    }

    private void onRequestVote(PeerId from, RequestVote request) {
        Frame reply;
        synchronized (lock) {
            if (request.term() > term && !becomeFollowerLocked(request.term())) {
                return; // the term bump is not durable: unanswered, the candidate retries
            }
            boolean upToDate = request.lastLogTerm() > lastLogTerm()
                    || (request.lastLogTerm() == lastLogTerm()
                            && request.lastLogIndex() >= log.size());
            boolean grant = request.term() == term && upToDate
                    && (votedFor == null || votedFor.equals(request.candidate()));
            if (grant) {
                PeerId priorVote = votedFor;
                votedFor = request.candidate();
                electionElapsed = 0;
                // The vote is durable before the reply leaves (JS-N-06): a voter
                // that restarts cannot grant this term to a second candidate.
                if (!persistTermVoteLocked(term, priorVote)) {
                    return;
                }
            }
            reply = Frame.of(new VoteReply(term, grant));
        }
        send(from, reply);
    }

    private void onVoteReply(PeerId from, VoteReply reply) {
        List<Runnable> sends = new ArrayList<>();
        synchronized (lock) {
            if (reply.term() > term) {
                becomeFollowerLocked(reply.term());
                return;
            }
            if (role != Role.CANDIDATE || reply.term() != term || !reply.granted()) {
                return;
            }
            if (!votesGranted.add(from)) {
                return;
            }
            if (votesGranted.size() > members.size() / 2) {
                role = Role.LEADER;
                leaderHint = self;
                for (PeerId member : members) {
                    nextIndex.put(member, (long) log.size() + 1);
                    matchIndex.put(member, 0L);
                }
                matchIndex.put(self, (long) log.size());
                for (PeerId member : members) {
                    if (!member.equals(self)) {
                        sends.add(appendFor(member));
                    }
                }
            }
        }
        sends.forEach(Runnable::run);
    }

    private void onAppendEntries(PeerId from, AppendEntries append) {
        Frame reply;
        synchronized (lock) {
            if ((append.term() > term || (append.term() == term && role != Role.FOLLOWER))
                    && !becomeFollowerLocked(append.term())) {
                return; // the term bump is not durable: unanswered, the leader retries
            }
            if (append.term() < term) {
                reply = Frame.of(new AppendReply(term, self, false, 0));
            } else {
                leaderHint = append.leader();
                electionElapsed = 0;
                boolean prevMatches = append.prevLogIndex() == 0
                        || (append.prevLogIndex() <= log.size()
                                && log.get((int) append.prevLogIndex() - 1).term()
                                        == append.prevLogTerm());
                if (!prevMatches) {
                    reply = Frame.of(new AppendReply(term, self, false, 0));
                } else {
                    long index = append.prevLogIndex();
                    // The first index this append changes, and the conflicting tail it
                    // displaced there, so a failed store write can put memory back.
                    long firstChange = -1;
                    List<LogEntry> displaced = List.of();
                    for (LogEntry entry : append.entries()) {
                        index++;
                        if (index <= log.size()) {
                            if (log.get((int) index - 1).term() != entry.term()) {
                                firstChange = index;
                                displaced = new ArrayList<>(log.subList((int) index - 1, log.size()));
                                while (log.size() >= index) {
                                    log.remove(log.size() - 1);
                                }
                                log.add(entry);
                            }
                        } else {
                            if (firstChange < 0) {
                                firstChange = index;
                            }
                            log.add(entry);
                        }
                    }
                    // The entries are durable before the acknowledgement (JS-N-06):
                    // the leader may count this member toward a commit once it hears
                    // success, so what it hears must survive this member's restart.
                    if (firstChange > 0 && !persistAppendLocked(firstChange,
                            new ArrayList<>(log.subList((int) firstChange - 1, log.size())))) {
                        while (log.size() >= firstChange) {
                            log.remove(log.size() - 1);
                        }
                        log.addAll(displaced);
                        return;
                    }
                    commitIndex = Math.max(commitIndex,
                            Math.min(append.leaderCommit(), log.size()));
                    reply = Frame.of(new AppendReply(term, self, true, index));
                }
            }
        }
        send(from, reply);
    }

    private void onAppendReply(PeerId from, AppendReply reply) {
        synchronized (lock) {
            if (reply.term() > term) {
                becomeFollowerLocked(reply.term());
                return;
            }
            if (role != Role.LEADER || reply.term() != term) {
                return;
            }
            // The follower identity is the authenticated frame sender; the
            // reply's own follower field is unauthenticated payload (ASF-005).
            if (reply.success()) {
                matchIndex.put(from, reply.matchIndex());
                nextIndex.put(from, reply.matchIndex() + 1);
                advanceCommitLocked();
            } else {
                nextIndex.merge(from, 1L, (v, one) -> Math.max(1, v - 1));
            }
        }
    }

    // ---------------------------------------------------------------- internals

    private void startElectionLocked(List<Runnable> sends) {
        long priorTerm = term;
        PeerId priorVote = votedFor;
        role = Role.CANDIDATE;
        term++;
        votedFor = self;
        votesGranted.clear();
        votesGranted.add(self);
        electionElapsed = 0;
        electionTimeout = newElectionTimeout();
        // The candidacy (new term, vote for self) is durable before any request
        // leaves (JS-N-06); if it cannot be, no request leaves and the timer
        // simply runs again.
        if (!persistTermVoteLocked(priorTerm, priorVote)) {
            role = Role.FOLLOWER;
            votesGranted.clear();
            return;
        }
        RequestVote request = new RequestVote(term, self, log.size(), lastLogTerm());
        for (PeerId member : members) {
            if (!member.equals(self)) {
                sends.add(() -> send(member, Frame.of(request)));
            }
        }
    }

    /**
     * Steps down, adopting {@code newTerm} when it is higher. A higher term is
     * written to the store before it is acted on (JS-N-06).
     *
     * @return {@code true} when the resulting term is durable; {@code false} when
     *         the bump could not be written and the prior term stands, in which
     *         case the caller must not reply or act on the higher term
     */
    private boolean becomeFollowerLocked(long newTerm) {
        long priorTerm = term;
        PeerId priorVote = votedFor;
        if (newTerm > term) {
            term = newTerm;
            votedFor = null;
        }
        role = Role.FOLLOWER;
        votesGranted.clear();
        electionElapsed = 0;
        electionTimeout = newElectionTimeout();
        return term == priorTerm || persistTermVoteLocked(priorTerm, priorVote);
    }

    /**
     * Writes the current term and vote to the store. On failure the prior
     * values are restored, so memory never claims more than the store holds.
     *
     * @return whether the write became durable
     */
    private boolean persistTermVoteLocked(long priorTerm, PeerId priorVote) {
        try {
            store.saveTermVote(term, votedFor);
            return true;
        } catch (java.io.IOException | RuntimeException e) {
            term = priorTerm;
            votedFor = priorVote;
            storeFailed("term " + term + " and vote", e);
            return false;
        }
    }

    /**
     * Writes entries from {@code fromIndex} to the store. The caller restores
     * the in-memory log on failure, since only it knows what it displaced.
     *
     * @return whether the write became durable
     */
    private boolean persistAppendLocked(long fromIndex, List<LogEntry> entries) {
        try {
            store.append(fromIndex, entries);
            return true;
        } catch (java.io.IOException | RuntimeException e) {
            storeFailed(entries.size() + " entries from index " + fromIndex, e);
            return false;
        }
    }

    private void storeFailed(String what, Exception e) {
        storeFailures.incrementAndGet();
        // The message, not the trace: a store that fails on every tick must not
        // flood the log with stacks; storeFailures() is the signal to watch.
        LOG.log(System.Logger.Level.WARNING, "ordered log " + self.display()
                + ": store refused " + what + " (" + e + "); nothing sent, the peer will retry");
    }

    private Runnable appendFor(PeerId member) {
        long next = nextIndex.getOrDefault(member, (long) log.size() + 1);
        long prevIndex = next - 1;
        long prevTerm = prevIndex == 0 ? 0 : log.get((int) prevIndex - 1).term();
        List<LogEntry> entries = new ArrayList<>(
                log.subList((int) prevIndex, log.size()));
        AppendEntries append = new AppendEntries(term, self, prevIndex, prevTerm,
                entries, commitIndex);
        return () -> send(member, Frame.of(append));
    }

    private void broadcastAppends() {
        List<Runnable> sends = new ArrayList<>();
        synchronized (lock) {
            if (role != Role.LEADER) {
                return;
            }
            for (PeerId member : members) {
                if (!member.equals(self)) {
                    sends.add(appendFor(member));
                }
            }
        }
        sends.forEach(Runnable::run);
    }

    private void advanceCommitLocked() {
        matchIndex.put(self, (long) log.size());
        for (long candidate = log.size(); candidate > commitIndex; candidate--) {
            if (log.get((int) candidate - 1).term() != term) {
                continue;
            }
            long votes = 0;
            for (PeerId member : members) {
                if (matchIndex.getOrDefault(member, 0L) >= candidate) {
                    votes++;
                }
            }
            if (votes > members.size() / 2) {
                commitIndex = candidate;
                break;
            }
        }
    }

    /**
     * Drains newly committed entries to the applied listener, strictly in log
     * order. The dedicated apply mutex is essential: submit, tick, and inbound
     * frames all drain, and without mutual exclusion around the whole drain two
     * threads could each pick consecutive indexes and apply them concurrently,
     * so a later command could take effect before an earlier one. State machines
     * (the ordered-take coordinator) depend on strict order.
     */
    private void applyCommitted() {
        synchronized (applyLock) {
            while (true) {
                byte[] command;
                long index;
                synchronized (lock) {
                    if (lastApplied >= commitIndex) {
                        return;
                    }
                    lastApplied++;
                    index = lastApplied;
                    command = log.get((int) lastApplied - 1).command();
                }
                applied.accept(command, index);
            }
        }
    }

    private long lastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term();
    }

    private int newElectionTimeout() {
        return 5 + random.nextInt(5);
    }

    private void send(PeerId to, Frame frame) {
        pipes.send(to, TYPE, codec.toBytes(frame));
    }
}
