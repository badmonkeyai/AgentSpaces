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
package ai.badmonkey.agentspaces.store;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.agent.content.SignedPin;
import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.agent.content.StoreReceipt;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations;
import ai.badmonkey.agentspaces.connect.AssetProvider;
import ai.badmonkey.agentspaces.connect.ConnectorRuntime;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.Fetch;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.stores.Stores;
import ai.badmonkey.agentspaces.pdt.store.MetaStore;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * A content store peer (PDT 2 phase 3, group S; srad/agentspaces-pdt2.md §3.2): a member that holds other
 * members' roots after they leave and proves it holds them. A store is an asset (principle 1 of §6.1), so it
 * reuses the connector SDK: a {@link ConnectorRuntime} publishes its {@code content-store} card, answers the
 * rows of its ledger as a {@code DataQuery} asset (P2-S-13), and serves the {@code pin}, {@code unpin},
 * {@code status}, and {@code challenge} invokes (P2-S-09). It adds no peer role and opens no socket.
 *
 * <p>Taking pins (P2-S-03, S2 of §6.4). A requester writes a {@link SignedPin} into the content space: its
 * {@link StorePin} terms under its own signature. The store reads the pins, judges each by its
 * {@link StorePolicy}, and takes one it can honour under the space's conflict strategy, as a connector takes a
 * {@code DataQuery}. It fetches the root (a tree by {@code fetchTree} with the pin's held haves, an artifact by the
 * flat swarm), pins it, publishes its holding, and completes the taken pin with a {@link StoreReceipt} tagged
 * {@code root} and {@code holder}. When copies are left it writes the same signed pin again with one fewer left
 * (decision 6). A store never takes a pin it wrote, takes a pin naming stores in {@link Stores#STORES_ATTR} only
 * when it is named, and takes a pin for a root it already holds only when the pin extends its lease by more than
 * a quarter. A store that cannot fetch within its fetch timeout lets the take lapse, so the pin returns for
 * another store.
 *
 * <p>Consent (PDT 2 follow-ups, 2026-11-09, F1). A store charges only the member who signed the pin: it takes a
 * pin only when its requester signed the terms for this group ({@link SignedPin#consented}) and the signature's
 * deadline has not passed, whoever wrote the entry. A receipt gives its writer no right to forward for another
 * member. The store refuses a pin its requester did not sign, counts it once in {@link Status#unsigned}, and
 * strikes nobody, since the writer may only relay what a forger made. A store takes one signature once: a copy of a
 * signed pin it took already, which any member can write before the deadline, renews nothing here and stays for the
 * next store, so a replay never stretches the lease the requester signed. The store remembers each signature it
 * took until the signature's deadline, in memory, so a store that restarts within the deadline may take one copy
 * more. A store also refuses a signed pin whose deadline lies more than {@link Stores#PIN_ENTRY_LEASE} plus
 * {@link #DEADLINE_SKEW} ahead, counted once in {@link Status#refused} (PDT 2 phase 5, owner ruling R2 of §10.11), so
 * no copy of a pin can be replayed for longer than a requester that follows {@link Stores} signs.
 *
 * <p>Grant without an authorizer (F2). A store whose policy is {@link StorePolicy.Accept#GRANT} and which has no
 * authorizer starts, listens, and waits: it takes no pins, so they stay for a store that can admit them, its card
 * carries {@code waiting: authorizer} ({@link Stores#WAITING_ACCESS}), it logs that once, and it starts admitting
 * when {@link #authorizer(Authorizer)} gives it one.
 *
 * <p>Leases (P2-S-04). A requester renews by writing the pin again. A holding whose lease ended a holding
 * interval ago is released: the store unpins the root, withdraws its holding when no requester is left, and
 * withdraws its receipt, the one it wrote before a restart included. The {@code unpin} invoke from the requester
 * releases at once.
 *
 * <p>Blind holding (P2-S-08, §3.2.5). A blind store has no content key. It holds a tree only when the tree's
 * root node is a sealed envelope (the sealed-nodes mode), refuses every tree while the group seals per message
 * ({@code group-key}), and holds only sealed-hashing artifacts; it verifies every node by CID, serves what it
 * holds, and answers challenges, which need no key.
 *
 * <p>Challenges (P2-S-07). The swarm, built at feature level 2, answers tag 12 {@code CHALLENGE} itself; the
 * ledger rows count the challenges answered per root.
 *
 * <p>Reconciliation (PDT 2 phase 5, P2-K-03, P2-Q-REL-06; S4 of §6.4). {@link #reconcile} compares the roots this
 * store holds complete with another store's by negentropy over the swarm ({@code Content.reconcileHeld}). A root the
 * other holds that this store holds a pin for but lacks, as after a truncated pack, is fetched again from the other
 * store; a root no requester pinned here stays where it is, since a store holds and charges only what a requester
 * signed for it (F1). The tick reconciles with one other store every {@code reconcile-every}, in turn, and the turn
 * passes on after a reconcile that fails as after one that succeeds.
 */
public final class ContentStore implements AssetProvider, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ContentStore.class.getName());

    /** The prefix of a store's asset name. */
    public static final String ASSET_PREFIX = "content-store:";

    /** The invoke operations a store's card lists in {@code invoke-ops} (P2-S-09). */
    public static final String OP_PIN = "pin";
    public static final String OP_UNPIN = "unpin";
    public static final String OP_STATUS = "status";
    public static final String OP_CHALLENGE = "challenge";

    /** How often the store refreshes its card and judges leases, unless the builder says otherwise. */
    public static final Duration DEFAULT_HOLDING_INTERVAL = Duration.ofSeconds(10);

    /** The bytes a {@code challenge} invoke returns at most, base64 in its result. */
    public static final int MAX_INVOKE_CHALLENGE_BYTES = 64 * 1024;

    /** Pins one scan reads. */
    static final int SCAN = 64;

    /** How often the tick reconciles with one other store, unless the builder says otherwise (appendix C). */
    public static final Duration DEFAULT_RECONCILE_EVERY = Duration.ofMinutes(10);

    /**
     * The outcome of one reconcile with another store (P2-K-03).
     *
     * @param store     the other store
     * @param have      the roots this store holds complete and the other lacks
     * @param need      the roots the other holds complete and this store lacks
     * @param repairing the needed roots this store holds pins for, which it fetches again from the other store
     * @param unpinned  the needed roots no requester pinned here, which this store does not take (F1)
     * @param rounds    the rounds of the session
     * @param bytes     the bytes of the session
     */
    public record Reconciled(PeerId store, int have, int need, List<String> repairing, int unpinned, int rounds, long bytes) {
    }

    /**
     * The clock skew a signed pin's deadline is judged with (PDT 2 phase 5, owner ruling R2 of
     * srad/agentspaces-pdt2.md §10.11): the ten minutes that advertisements and join delegations allow
     * ({@code AdCache}, {@code JoinCredentials.EXPIRY_SKEW}). A store takes a signed pin only while its
     * {@code notAfter} lies within {@link Stores#PIN_ENTRY_LEASE} plus this of now.
     */
    public static final Duration DEADLINE_SKEW = Duration.ofMinutes(10);

    /**
     * What {@link #status()} answers.
     *
     * @param name        the store's name
     * @param capacity    the capacity in bytes
     * @param used        the bytes held, each root once
     * @param free        the bytes the store can still take before it refuses pins
     * @param dedup       the bytes charged beyond the bytes held
     * @param roots       the roots held
     * @param byRequester bytes and roots per requester
     * @param byGroup     bytes and roots per group
     * @param taken       pins taken and completed
     * @param refused     pins judged and refused
     * @param released    holdings released
     * @param abandoned   takes abandoned for a failed or slow fetch, or a root over capacity
     * @param unsigned    pins refused, each once, because their requester did not sign them (F1)
     * @param full        whether the store refuses pins for want of room (P2-Q-AVL-02)
     * @param waiting     whether a grant store waits for an authorizer and takes no pins (F2)
     */
    public record Status(String name, long capacity, long used, long free, long dedup, int roots,
                         Map<PeerId, StoreLedger.Totals> byRequester, Map<String, StoreLedger.Totals> byGroup,
                         long taken, long refused, long released, long abandoned, long unsigned, boolean full,
                         boolean waiting) {
    }

    /** Pins refused for their signature or its deadline, remembered so each is counted once. */
    static final int UNSIGNED_SEEN = 4096;

    /** The pause between scans that find nothing to take, or that fail. */
    static final Duration SCAN_PAUSE = Duration.ofMillis(200);

    /** What a requester signed, read from a signed pin: one consent, one take per store. */
    private record Consent(StorePin pin, long notAfter) {
        static Consent of(SignedPin signed) {
            return new Consent(signed.pin(), signed.notAfter());
        }
    }

    /** One pin being fetched; {@code signed} is null for a pin asked by invoke, whose signer is the requester. */
    private static final class InFlight {
        final StorePin pin;
        final SignedPin signed;
        final TakenEntry<SignedPin> taken;
        volatile TreeFetch probe;
        volatile TreeFetch tree;
        volatile Fetch artifact;
        long renewedMillis;
        /** The bytes the fetch had received at the last progress, and when (swarm retries, ROBUSTNESS.md §10.3 B9). */
        long progressBytes;
        long progressedMillis;
        /** The bytes a blind store's probe received before its tree fetch began. */
        long probeBytes;

        InFlight(StorePin pin, SignedPin signed, TakenEntry<SignedPin> taken, long now) {
            this.pin = pin;
            this.signed = signed;
            this.taken = taken;
            this.renewedMillis = now;
            this.progressedMillis = now;
        }

        /** The bytes the pin's fetches have received so far, a blind store's probe included. */
        long received() {
            TreeFetch t = tree;
            TreeFetch p = probe;
            Fetch a = artifact;
            return probeBytes + (p != null ? p.progress().bytesReceived() : 0) + (t != null ? t.progress().bytesReceived() : 0)
                    + (a != null ? a.progress().bytesReceived() : 0);
        }

        /** Cancels whatever fetch is under way, so an abandoned pin leaves no fetch running unrecorded. */
        void cancel() {
            for (TreeFetch t : new TreeFetch[]{probe, tree}) {
                if (t != null && !t.isDoneOrFailed()) {
                    t.cancel();
                }
            }
            Fetch a = artifact;
            if (a != null && !a.isDoneOrFailed()) {
                a.cancel();
            }
        }
    }

    private final Pdt pdt;
    private final Content content;
    private final Space space;
    private final DiscoveryService discovery;
    private final PeerIdentity identity;
    private final PeerId self;
    private final String name;
    private final StorePolicy policy;
    private final StoreLedger ledger;
    private final InstantSource clock;
    private volatile Authorizer authorizer;
    private final Predicate<PeerId> requesters;
    private final Duration holdingInterval;
    private final Duration takeLease;
    private final Duration fetchTimeout;
    private final Duration reconcileEvery;
    /** Pinned roots fetched again after a reconcile showed them lost here, with their fetches (P2-K-03). */
    private final Map<String, Object> repairs = new ConcurrentHashMap<>();
    private volatile java.util.concurrent.CompletableFuture<Reconciled> reconciling;
    private volatile Reconciled lastReconcile;
    private volatile PeerId lastReconciled;
    private long reconciledMillis;
    private final Map<String, InFlight> inFlight = new ConcurrentHashMap<>();
    private final Map<String, EntryHandle> receipts = new ConcurrentHashMap<>();
    private final AtomicLong taken = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();
    private final AtomicLong released = new AtomicLong();
    private final AtomicLong abandoned = new AtomicLong();
    private final AtomicLong unsigned = new AtomicLong();
    /** The signatures this store took, each until its deadline. */
    private final Set<Consent> consentsTaken = ConcurrentHashMap.newKeySet();
    private final Set<SignedPin> refusedSeen = Collections.synchronizedSet(Collections.newSetFromMap(
            new LinkedHashMap<>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<SignedPin, Boolean> eldest) {
                    return size() > UNSIGNED_SEEN;
                }
            }));
    private volatile boolean full;
    private volatile boolean running;
    private volatile boolean withdrawn;
    private ConnectorRuntime runtime;
    private Thread loop;
    private long refreshedMillis = Long.MIN_VALUE / 4;

    private ContentStore(Builder b) {
        this.pdt = b.pdt;
        this.content = Objects.requireNonNull(b.pdt.swarm(), "attach the facade to its content swarm first").content();
        this.space = content.spaces().space();
        this.discovery = b.discovery;
        this.identity = b.identity;
        this.self = identity.peerId();
        this.name = b.name == null ? self.value() : b.name;
        this.policy = Objects.requireNonNull(b.policy, "policy");
        this.clock = b.clock;
        this.authorizer = b.authorizer;
        this.requesters = b.requesters;
        this.holdingInterval = b.holdingInterval;
        this.takeLease = b.takeLease;
        this.fetchTimeout = b.fetchTimeout;
        this.reconcileEvery = b.reconcileEvery;
        this.ledger = new StoreLedger(content.group().value(),
                pdt.store().backend() instanceof MetaStore meta ? meta : null, name);
    }

    /**
     * Starts a builder.
     *
     * @param pdt       the group's facade, attached to its content swarm
     * @param discovery the group's discovery service, where the card is published
     * @param identity  this peer's identity, which signs the card
     * @return the builder
     */
    public static Builder builder(Pdt pdt, DiscoveryService discovery, PeerIdentity identity) {
        return new Builder(pdt, discovery, identity);
    }

    /** Builds a {@link ContentStore}. */
    public static final class Builder {
        private final Pdt pdt;
        private final DiscoveryService discovery;
        private final PeerIdentity identity;
        private String name;
        private StorePolicy policy;
        private InstantSource clock = InstantSource.system();
        private Authorizer authorizer;
        private Predicate<PeerId> requesters = peer -> true;
        private Duration holdingInterval = DEFAULT_HOLDING_INTERVAL;
        private Duration takeLease = Duration.ofMinutes(1);
        private Duration fetchTimeout = Duration.ofMinutes(10);
        private Duration reconcileEvery = DEFAULT_RECONCILE_EVERY;

        private Builder(Pdt pdt, DiscoveryService discovery, PeerIdentity identity) {
            this.pdt = Objects.requireNonNull(pdt, "pdt");
            this.discovery = Objects.requireNonNull(discovery, "discovery");
            this.identity = Objects.requireNonNull(identity, "identity");
        }

        /** The card name ({@code agentspaces.store.name}); the peer id by default. */
        public Builder name(String value) {
            if (value == null || value.isBlank() || !value.matches("[A-Za-z0-9._-]{1,64}")) {
                throw new IllegalArgumentException("a store name is 1 to 64 of [A-Za-z0-9._-], got " + value);
            }
            this.name = value;
            return this;
        }

        public Builder policy(StorePolicy value) {
            this.policy = Objects.requireNonNull(value, "policy");
            return this;
        }

        public Builder clock(InstantSource value) {
            this.clock = Objects.requireNonNull(value, "clock");
            return this;
        }

        /**
         * The authorizer the {@link StorePolicy.Accept#GRANT} rule asks for {@code CONTENT_FETCH} on the root; a grant
         * store without one waits for {@link ContentStore#authorizer(Authorizer)} (F2).
         */
        public Builder authorizer(Authorizer value) {
            this.authorizer = Objects.requireNonNull(value, "authorizer");
            return this;
        }

        /** A further rule on requesters beside {@code acceptRequesters} (§3.2.3: a predicate). */
        public Builder requesters(Predicate<PeerId> value) {
            this.requesters = Objects.requireNonNull(value, "requesters");
            return this;
        }

        /** How often the card's {@code free} is refreshed and leases judged; also the release grace (P2-S-04). */
        public Builder holdingInterval(Duration value) {
            this.holdingInterval = Objects.requireNonNull(value, "holdingInterval");
            return this;
        }

        /** The take lease on a pin while its root is fetched, renewed at half-life. */
        public Builder takeLease(Duration value) {
            this.takeLease = Objects.requireNonNull(value, "takeLease");
            return this;
        }

        /**
         * How long a fetch may go without receiving a byte before the store lets its take lapse (S8 of §6.4): a fetch
         * that keeps making progress runs on however long the root takes (swarm retries, ROBUSTNESS.md §10.3 B9).
         */
        public Builder fetchTimeout(Duration value) {
            this.fetchTimeout = Objects.requireNonNull(value, "fetchTimeout");
            return this;
        }

        /** How often the tick reconciles with one other store ({@code agentspaces.store.reconcile-every}); zero never. */
        public Builder reconcileEvery(Duration value) {
            this.reconcileEvery = Objects.requireNonNull(value, "reconcileEvery");
            if (value.isNegative()) {
                throw new IllegalArgumentException("reconcile-every is not negative");
            }
            return this;
        }

        public ContentStore build() {
            return new ContentStore(this);
        }
    }

    // ------------------------------------------------------------ lifecycle

    /**
     * Publishes the card and starts taking pins. The swarm must advertise feature level 2 (P2-S-01), and a
     * blind store must hold no content key.
     *
     * @throws IllegalStateException when the swarm is at level 1 or a blind store holds a key
     */
    public synchronized void start() {
        if (content.pdtLevel() < Content.PDT_LEVEL) {
            throw new IllegalStateException("a content store advertises pdt level " + Content.PDT_LEVEL
                    + ": build its content swarm with Content.Builder.pdtLevel(" + Content.PDT_LEVEL + ")");
        }
        if (policy.blind() && content.keyRing().filter(r -> !r.epochs().isEmpty()).isPresent()) {
            throw new IllegalStateException("a blind store holds no content key, and this swarm has one"
                    + " (agentspaces.store.blind=true)");
        }
        if (running) {
            return;
        }
        running = true;
        if (waiting()) {
            LOG.log(System.Logger.Level.WARNING, "store " + name + " has accept-requesters=grant and no authorizer: it"
                    + " takes no pins and its card says it waits for an authorizer, until the host gives it one");
        }
        for (StoreLedger.Held held : ledger.holdings()) {
            // a restarted store: the node store kept its pins, so only the holding is published again
            if (StorePin.KIND_TREE.equals(held.kind())) {
                content.holdTree(held.root());
            }
        }
        runtime = new ConnectorRuntime(space, discovery, identity, "store-" + name, this, content.group(), clock,
                holdingInterval);
        if (authorizer != null) {
            runtime.authorizer(authorizer);
        }
        runtime.start();
        refreshedMillis = clock.millis();
        reconciledMillis = refreshedMillis;
        loop = Thread.ofVirtual().name("content-store-" + name).start(this::takeLoop);
    }

    /**
     * Gives a store its authorizer, at build or later (F2): a grant store that waited for one starts admitting
     * pins, and its next card no longer says it waits.
     *
     * @param value the authorizer the grant rule and the invokes ask
     */
    public synchronized void authorizer(Authorizer value) {
        Objects.requireNonNull(value, "authorizer");
        boolean waited = waiting();
        this.authorizer = value;
        if (runtime != null) {
            runtime.authorizer(value);
        }
        if (waited && running) {
            LOG.log(System.Logger.Level.INFO, "store " + name + " has an authorizer and admits pins");
            runtime.refreshCards();
            refreshedMillis = clock.millis();
        }
    }

    /** Whether the store waits for an authorizer: a grant policy with none (F2). */
    public boolean waiting() {
        return policy.acceptRequesters() == StorePolicy.Accept.GRANT && authorizer == null;
    }

    /** The periodic work, on the group's tick: drives fetches, renews takes, releases lapsed leases, refreshes the card. */
    public void tick() {
        if (!running) {
            return;
        }
        long now = clock.millis();
        for (InFlight f : List.copyOf(inFlight.values())) {
            drive(f, now);
        }
        for (StoreLedger.Held lapsed : ledger.lapsed(now - holdingInterval.toMillis())) {
            release(lapsed, "its lease ended");
        }
        consentsTaken.removeIf(consent -> consent.notAfter() < now);
        driveRepairs();
        if (!reconcileEvery.isZero() && reconciling == null && now - reconciledMillis >= reconcileEvery.toMillis()) {
            reconciledMillis = now;
            nextStore().ifPresent(other -> {
                // the turn passes on whatever the outcome, so a store that never answers holds no other back (PDT 2
                // phase 5 adversary); the session is recorded before its callback, which may run at once
                lastReconciled = other;
                java.util.concurrent.CompletableFuture<Reconciled> session = reconcile(other);
                reconciling = session;
                session.whenComplete((done, failed) -> {
                    reconciling = null;
                    if (failed != null) {
                        LOG.log(System.Logger.Level.INFO, "store " + name + " could not reconcile with " + other.display() + ": "
                                + failed.getMessage());
                    }
                });
            });
        }
        if (now - refreshedMillis >= holdingInterval.toMillis()) {
            refreshedMillis = now;
            runtime.refreshCards();
        }
    }

    /**
     * Stops taking pins and withdraws the card: it is published once more with {@code free} 0, so no planner
     * chooses the store, and a time to live of two holding intervals, after which no cache shows it.
     */
    @Override
    public synchronized void close() {
        if (!running) {
            return;
        }
        running = false;
        withdrawn = true;
        if (loop != null) {
            loop.interrupt();
        }
        try {
            runtime.refreshCards();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "withdrawing the card of store " + name + " failed", e);
        }
        runtime.close();
    }

    // ------------------------------------------------------------ the card, rows, and invokes

    /** The asset name of this store's card. */
    public String assetName() {
        return ASSET_PREFIX + name;
    }

    @Override
    public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
        // withdrawn: a last card with no free bytes supersedes the live one and lapses after two holding
        // intervals, since a cache refuses a card that arrives expired
        return List.of(withdrawn ? card(group, issuer, now, holdingInterval.multipliedBy(2), name, policy, 0,
                content.keyRing().isPresent(), holdingInterval, waiting()) : card(group, issuer, now, Duration.ofMinutes(15)));
    }

    /** This store's card now. */
    AssetCard card(GroupId group, PeerId issuer, Instant now, Duration ttl) {
        return card(group, issuer, now, ttl, name, policy, free(), content.keyRing().isPresent(), holdingInterval, waiting());
    }

    /**
     * The {@code content-store} card of §3.2.1 (P2-S-01): every {@code access} and {@code costHints} value is text
     * (rule 19), integers in canonical decimal, booleans {@code true} or {@code false}, lists comma separated
     * without spaces; the price hint appears only when the owner sets one, and {@code freshness} is the holding
     * interval. A store that waits for an authorizer adds {@code waiting: authorizer} (F2), so a requester's
     * chooser passes it by; a ready store's card has no {@code waiting} key.
     *
     * @param group           the group
     * @param issuer          the store's peer
     * @param now             the issue instant
     * @param ttl             the card's time to live
     * @param name            the store's name
     * @param policy          the store's policy
     * @param free            the bytes the store can still take
     * @param keyed           whether the store holds the group's content key
     * @param holdingInterval the holding interval
     * @param waiting         whether the store waits for an authorizer
     * @return the card
     */
    public static AssetCard card(GroupId group, PeerId issuer, Instant now, Duration ttl, String name, StorePolicy policy,
                                 long free, boolean keyed, Duration holdingInterval, boolean waiting) {
        Map<String, String> access = new LinkedHashMap<>();
        access.put("kind", StoreChooser.KIND);
        access.put("capacity", Long.toString(policy.capacityBytes()));
        access.put("free", Long.toString(free));
        access.put("maxObject", Long.toString(policy.maxObjectBytes()));
        access.put("codecs", "dag-cbor,raw");
        access.put("hashes", "sha2-256");
        access.put("blind", Boolean.toString(policy.blind()));
        access.put("sealing", policy.blind() ? "sealed-nodes" : keyed ? "none,group-key,sealed-nodes" : "none,sealed-nodes");
        access.put("leaseMax", Long.toString(policy.leaseMax().toSeconds()));
        access.put("leaseDefault", Long.toString(policy.leaseDefault().toSeconds()));
        access.put("copiesMax", Integer.toString(policy.copiesMax()));
        access.put("pin", "request,invoke");
        access.put("challenge", "true");
        access.put("tier", policy.tier());
        access.put("delegates", "");
        access.put("policy", policy.acceptRequesters().name().toLowerCase(java.util.Locale.ROOT));
        access.put(AssetInvocations.INVOKE_OPS, String.join(",", OP_PIN, OP_UNPIN, OP_STATUS, OP_CHALLENGE));
        if (waiting) {
            access.put(Stores.WAITING_ACCESS, "authorizer");
        }
        Map<String, String> cost = policy.pricePerGibMonth() > 0
                ? Map.of(StoreChooser.PRICE_HINT, Long.toString(policy.pricePerGibMonth())) : Map.of();
        return new AssetCard("aspace://" + group.value() + "/asset/content-store/" + name + "/" + issuer.value(), issuer, group,
                now, ttl, ASSET_PREFIX + name, "aspace://" + group.value() + "/store/" + name,
                "content store " + name + ": holds pinned roots and answers possession challenges", "store",
                holdingInterval.toString(), cost, access);
    }

    /**
     * The ledger as rows (P2-S-13): {@code root, kind, bytes, requester, until, copies, challengesPassed}.
     */
    @Override
    public QueryResult query(String asset, Map<String, String> parameters) {
        if (!assetName().equals(asset)) {
            return QueryResult.failed("unknown asset " + asset);
        }
        List<Map<String, String>> rows = new ArrayList<>();
        for (StoreLedger.Held h : ledger.holdings()) {
            Map<String, String> row = new LinkedHashMap<>();
            row.put("root", h.root());
            row.put("kind", h.kind());
            row.put("bytes", Long.toString(h.bytes()));
            row.put("requester", h.requester().value());
            row.put("until", Long.toString(h.until()));
            row.put("copies", Integer.toString(h.copies()));
            row.put("challengesPassed", Long.toString(content.challengesAnswered(h.root())));
            rows.add(row);
        }
        return QueryResult.of(rows);
    }

    @Override
    public InvokeResult invoke(String asset, String operation, Map<String, String> params, String invocationId,
                               PeerId requester) {
        if (!assetName().equals(asset)) {
            return InvokeResult.failed("unknown-operation", "asset", "no asset " + asset);
        }
        if (requester == null) {
            return InvokeResult.failed("refused-scope", "requester", "the invocation's signer is unknown");
        }
        return switch (operation) {
            case OP_STATUS -> InvokeResult.of(statusResult());
            case OP_PIN -> invokePin(params, requester);
            case OP_UNPIN -> invokeUnpin(params, requester);
            case OP_CHALLENGE -> invokeChallenge(params, requester);
            default -> InvokeResult.failed("unknown-operation", "invoke-ops", operation + " is not an operation of a store");
        };
    }

    private Map<String, String> statusResult() {
        Status s = status();
        Map<String, String> out = new LinkedHashMap<>();
        out.put("name", s.name());
        out.put("capacity", Long.toString(s.capacity()));
        out.put("used", Long.toString(s.used()));
        out.put("free", Long.toString(s.free()));
        out.put("dedup", Long.toString(s.dedup()));
        out.put("roots", Integer.toString(s.roots()));
        out.put("unsigned", Long.toString(s.unsigned()));
        out.put("full", Boolean.toString(s.full()));
        out.put("waiting", Boolean.toString(s.waiting()));
        return out;
    }

    private InvokeResult invokePin(Map<String, String> params, PeerId requester) {
        String root = params.get("root");
        String kind = params.getOrDefault("kind", artifactRoot(root) ? StorePin.KIND_ARTIFACT : StorePin.KIND_TREE);
        long lease;
        try {
            lease = Long.parseLong(params.getOrDefault("lease", Long.toString(policy.leaseDefault().toSeconds())));
        } catch (NumberFormatException e) {
            return InvokeResult.failed("bad-params", "lease", "lease is seconds in canonical decimal");
        }
        if (root == null) {
            return InvokeResult.failed("bad-params", "root", "pin names a root");
        }
        if (waiting()) {
            return InvokeResult.failed("target-error", "policy", "this store waits for an authorizer and takes no pins");
        }
        // the invocation's signature is the requester's consent
        StorePin pin = new StorePin(StorePin.VERSION, root, kind, lease, 1, requester, List.of(), null);
        Optional<String> refusal = pin.violation().or(() -> judge(pin));
        if (refusal.isPresent()) {
            refused.incrementAndGet();
            return InvokeResult.failed("target-error", "policy", refusal.get());
        }
        begin(pin, null, null);
        return InvokeResult.of(Map.of("root", root, "state", "fetching"));
    }

    private InvokeResult invokeUnpin(Map<String, String> params, PeerId requester) {
        String root = params.get("root");
        Optional<StoreLedger.Held> held = root == null ? Optional.empty() : ledger.holding(root, requester);
        if (held.isEmpty()) {
            return InvokeResult.failed("target-error", "unpin", "this store holds no pin of that root for the requester");
        }
        release(held.get(), "its requester unpinned it");
        return InvokeResult.of(Map.of("root", root, "state", "released"));
    }

    private InvokeResult invokeChallenge(Map<String, String> params, PeerId requester) {
        String root = params.get("root");
        String picks = params.get("picks");
        if (root == null || picks == null || picks.isBlank()) {
            return InvokeResult.failed("bad-params", "challenge", "challenge names a root and picks, comma separated");
        }
        if (authorizer != null && !authorizer.permits(requester, Authorizer.Operation.CONTENT_FETCH, root)) {
            return InvokeResult.failed("refused-scope", "CONTENT_FETCH", "the requester may not fetch " + root);
        }
        List<String> cids = List.of(picks.split(","));
        if (cids.size() > ai.badmonkey.agentspaces.content.Messages.MAX_PICKS) {
            return InvokeResult.failed("bad-params", "picks", "at most " + ai.badmonkey.agentspaces.content.Messages.MAX_PICKS + " picks");
        }
        boolean artifact = artifactRoot(root);
        for (String cid : cids) {
            // as the tree server grants a CHALLENGE: a tree's picks one by one, since a grant on one root does
            // not open the nodes of another root this store holds
            if (!artifact && authorizer != null && !authorizer.permits(requester, Authorizer.Operation.CONTENT_FETCH, cid)) {
                return InvokeResult.failed("refused-scope", "CONTENT_FETCH", "the requester may not fetch " + cid);
            }
        }
        Map<String, String> out = new LinkedHashMap<>();
        int bytes = 0;
        for (String cid : cids) {
            Optional<byte[]> node = held(root, artifact, cid);
            if (node.isEmpty() || bytes + node.get().length > MAX_INVOKE_CHALLENGE_BYTES) {
                out.put(cid, "");
                continue;
            }
            bytes += node.get().length;
            out.put(cid, Base64.getEncoder().encodeToString(node.get()));
        }
        return InvokeResult.of(out);
    }

    /** Whether a root names an artifact: the raw CID of its piece table. */
    private static boolean artifactRoot(String root) {
        return root != null && Cids.isCid(root) && Cids.parse(root).codec() == Cids.RAW;
    }

    /** A held node of a tree root, or a held piece of an artifact root named by its raw CID. */
    private Optional<byte[]> held(String root, boolean artifact, String cid) {
        if (!ledger.holds(root)) {
            return Optional.empty();
        }
        if (!artifact) {
            return pdt.store().stored(cid);
        }
        List<String> pieces = content.pieceCids(root);
        int index = pieces.indexOf(cid);
        Optional<Manifest> manifest = content.store().manifest(root);
        if (index < 0 || manifest.isEmpty() || !content.store().have(root).get(index)) {
            return Optional.empty();
        }
        return content.store().read(root, index, 0, manifest.get().pieceLength(index));
    }

    /** The ledger and counters (P2-S-06). */
    public Status status() {
        return new Status(name, policy.capacityBytes(), ledger.usedBytes(), free(), ledger.dedupBytes(), ledger.byGroup()
                .values().stream().mapToInt(StoreLedger.Totals::roots).sum(), ledger.byRequester(), ledger.byGroup(),
                taken.get(), refused.get(), released.get(), abandoned.get(), unsigned.get(), full, waiting());
    }

    /** The ledger. */
    public StoreLedger ledger() {
        return ledger;
    }

    /** The policy. */
    public StorePolicy policy() {
        return policy;
    }

    private long free() {
        return Math.max(0, policy.usableBytes() - ledger.usedBytes());
    }

    // ------------------------------------------------------------ reconciling (P2-K-03)

    /**
     * Reconciles this store's held roots with another store's (P2-K-03, P2-Q-REL-06), as the class comment says.
     *
     * @param other a store at feature level 2
     * @return the outcome once the session ends; the fetches of {@link Reconciled#repairing} run on after it
     */
    public java.util.concurrent.CompletableFuture<Reconciled> reconcile(PeerId other) {
        Objects.requireNonNull(other, "other");
        return content.reconcileHeld(other).thenApply(outcome -> {
            Map<String, StoreLedger.Held> pinned = new java.util.HashMap<>();
            for (StoreLedger.Held h : ledger.holdings()) {
                pinned.putIfAbsent(h.root(), h);
            }
            List<String> repairing = new ArrayList<>();
            int unpinned = 0;
            for (String root : outcome.need()) {
                StoreLedger.Held held = pinned.get(root);
                if (held == null) {
                    unpinned++;
                } else if (!inFlight.containsKey(root) && repair(held, other)) {
                    repairing.add(root);
                }
            }
            Reconciled done = new Reconciled(other, outcome.have().size(), outcome.need().size(), List.copyOf(repairing), unpinned,
                    outcome.rounds(), outcome.bytes());
            lastReconcile = done;
            lastReconciled = other;
            if (!repairing.isEmpty()) {
                LOG.log(System.Logger.Level.INFO, "store " + name + " fetches " + repairing.size() + " pinned roots again from "
                        + other.display() + " after a reconcile");
            }
            return done;
        });
    }

    /** The last reconcile this store finished, by call or by the tick. */
    public Optional<Reconciled> lastReconcile() {
        return Optional.ofNullable(lastReconcile);
    }

    /** Starts fetching a pinned root this store lost; false when a fetch of it runs already. */
    private boolean repair(StoreLedger.Held held, PeerId from) {
        FetchOptions options = FetchOptions.defaults().withTimeout(fetchTimeout).prefer(from);
        boolean[] started = {false};
        repairs.computeIfAbsent(held.root(), root -> {
            started[0] = true;
            return StorePin.KIND_TREE.equals(held.kind()) ? content.fetchTree(root, List.of(), options) : content.fetch(root, options);
        });
        return started[0];
    }

    /** Drops each finished repair; a tree that came back complete publishes its holding at once. */
    private void driveRepairs() {
        for (Map.Entry<String, Object> e : List.copyOf(repairs.entrySet())) {
            Object fetch = e.getValue();
            boolean complete = fetch instanceof TreeFetch tree ? tree.complete() : ((Fetch) fetch).complete();
            Optional<Throwable> failure = fetch instanceof TreeFetch tree ? tree.failure() : ((Fetch) fetch).failure();
            if (complete || failure.isPresent()) {
                repairs.remove(e.getKey(), fetch);
                if (complete && fetch instanceof TreeFetch) {
                    content.holdTree(e.getKey());
                }
                failure.ifPresent(f -> LOG.log(System.Logger.Level.INFO, "store " + name + " could not fetch " + e.getKey()
                        + " again: " + f.getMessage()));
            }
        }
    }

    /** The next other store at feature level 2 after the last one the tick tried or a call reconciled with, by peer id. */
    private Optional<PeerId> nextStore() {
        List<PeerId> others = discovery.find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind"))
                        && !c.issuer().equals(self) && content.pdtLevel(c.issuer()) >= Content.PDT_LEVEL)
                .stream().map(AssetCard::issuer).distinct().sorted(java.util.Comparator.comparing(PeerId::value)).toList();
        if (others.isEmpty()) {
            return Optional.empty();
        }
        PeerId last = lastReconciled;
        for (PeerId other : others) {
            if (last == null || other.value().compareTo(last.value()) > 0) {
                return Optional.of(other);
            }
        }
        return Optional.of(others.get(0));
    }

    // ------------------------------------------------------------ taking pins

    private void takeLoop() {
        while (running) {
            try {
                Set<String> wanted = new HashSet<>();
                if (!waiting()) {
                    for (Space.Issued<SignedPin> issued : space.readAllIssued(Template.of(SignedPin.class), SCAN)) {
                        if (judge(issued.entry(), issued.issuer().peer()).isEmpty()) {
                            wanted.add(issued.entry().pin().root());
                        }
                    }
                }
                if (wanted.isEmpty()) {
                    Thread.sleep(SCAN_PAUSE);
                    continue;
                }
                Optional<TakenEntry<SignedPin>> took = space.take(Template.of(SignedPin.class)
                        .whereTag(StorePin.ROOT_TAG, Matchers.in(wanted.toArray())),
                        Lease.of(takeLease), Duration.ofSeconds(1));
                took.ifPresent(this::taken);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                if (!running) {
                    return;
                }
                // an authorizer or a space that fails costs one scan a pause, as an idle scan does
                LOG.log(System.Logger.Level.WARNING, "store " + name + ": taking pins failed", e);
                try {
                    Thread.sleep(SCAN_PAUSE);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void taken(TakenEntry<SignedPin> took) {
        SignedPin signed = took.entry();
        PeerId issuer = space instanceof ReplicatedSpace replicated
                ? replicated.recordOf(took.entryId()).map(EntryRecord::issuer).map(a -> a.peer()).orElse(null) : signed.pin().requester();
        Optional<String> refusal = issuer == null ? Optional.of("the pin's issuer is unknown") : judge(signed, issuer);
        if (refusal.isPresent()) {
            // a race with the judgement of the scan: the take lapses and the pin returns for another store
            refused.incrementAndGet();
            LOG.log(System.Logger.Level.DEBUG, "store " + name + " let a pin of " + signed.pin().root() + " lapse: "
                    + refusal.get());
            return;
        }
        begin(signed.pin(), signed, took);
    }

    /**
     * Judges a pin entry (F1): its bounds, its writer, its deadline, its requester's signature, and whether this store
     * took that signature already, then its terms. A pin its requester did not sign is counted once in
     * {@link Status#unsigned}; nobody is struck.
     *
     * @return why the store does not take it, or empty when it does
     */
    Optional<String> judge(SignedPin signed, PeerId issuer) {
        Optional<String> bad = signed.violation();
        if (bad.isPresent()) {
            return bad;
        }
        if (issuer.equals(self)) {
            return Optional.of("this store wrote the pin");
        }
        long now = clock.millis();
        if (now > signed.notAfter()) {
            return Optional.of("the requester's signature lapsed");
        }
        if (!signed.consented(content.group())) {
            if (refusedSeen.add(signed)) {
                unsigned.incrementAndGet();
                LOG.log(System.Logger.Level.INFO, "store " + name + " refused a pin of " + signed.pin().root()
                        + " that names " + signed.pin().requester().display() + ", who did not sign it");
            }
            return Optional.of("the requester did not sign the pin");
        }
        if (signed.notAfter() - now > Stores.PIN_ENTRY_LEASE.plus(DEADLINE_SKEW).toMillis()) {
            // R2: a far deadline would let any member carry copies of the pin to new stores until then
            if (refusedSeen.add(signed)) {
                refused.incrementAndGet();
                LOG.log(System.Logger.Level.INFO, "store " + name + " refused a pin of " + signed.pin().root()
                        + " whose signature lasts past the entry lease and the clock skew");
            }
            return Optional.of("the signature's deadline lies beyond " + Stores.PIN_ENTRY_LEASE + " and the clock skew");
        }
        if (consentsTaken.contains(Consent.of(signed))) {
            return Optional.of("this store took that signature");
        }
        return judge(signed.pin());
    }

    /**
     * Judges a pin's terms by the rules of the class comment and the policy (P2-S-03, P2-S-05, P2-S-08,
     * P2-Q-SEC-06); the caller checked the bounds and the requester's consent.
     *
     * @return why the store does not take it, or empty when it does
     */
    Optional<String> judge(StorePin pin) {
        if (!policy.acceptKinds().contains(pin.kind())) {
            return Optional.of("this store takes no " + pin.kind() + " pins");
        }
        if (pin.copies() > policy.copiesMax()) {
            return Optional.of(pin.copies() + " copies exceed copies-max " + policy.copiesMax());
        }
        String named = pin.attrs() == null ? null : pin.attrs().get(Stores.STORES_ATTR);
        if (named != null && !List.of(named.split(",")).contains(self.value())) {
            return Optional.of("the pin names other stores");
        }
        Authorizer grant = authorizer;
        if (policy.acceptRequesters() == StorePolicy.Accept.GRANT && grant == null) {
            return Optional.of("this store waits for an authorizer");
        }
        if (policy.acceptRequesters() == StorePolicy.Accept.GRANT
                && !grant.permits(pin.requester(), Authorizer.Operation.CONTENT_FETCH, pin.root())) {
            return Optional.of("the requester may not fetch the root (CONTENT_FETCH)");
        }
        if (!requesters.test(pin.requester())) {
            return Optional.of("the store's requester rule refuses " + pin.requester().display());
        }
        if (inFlight.containsKey(pin.root())) {
            return Optional.of("the root is being fetched");
        }
        long now = clock.millis();
        long lease = leaseMillis(pin);
        Optional<StoreLedger.Held> held = ledger.holding(pin.root(), pin.requester());
        if (held.isPresent() && now + lease - held.get().until() <= lease / 4) {
            return Optional.of("this store already holds the root for the lease");
        }
        if (StorePin.KIND_ARTIFACT.equals(pin.kind())) {
            Optional<Manifest> manifest = content.spaces().manifest(pin.root());
            if (manifest.isEmpty()) {
                return Optional.of("no manifest of the artifact is in the content space");
            }
            if (policy.blind() && !Manifest.HASHING_SEALED.equals(manifest.get().hashing())) {
                return Optional.of("a blind store holds sealed-hashing artifacts only");
            }
            if (manifest.get().size() > policy.maxObjectBytes()) {
                return Optional.of("the artifact exceeds max-object");
            }
            if (!ledger.holds(pin.root()) && manifest.get().size() > free()) {
                markFull();
                return Optional.of("the artifact does not fit in the free bytes");
            }
        } else if (policy.blind() && content.groupSeals()) {
            return Optional.of("a blind store refuses group-key trees, whose nodes it would read");
        }
        if (!ledger.holds(pin.root()) && free() <= 0) {
            markFull();
            return Optional.of("the store is full");
        }
        return Optional.empty();
    }

    private void markFull() {
        if (!full) {
            full = true;
            LOG.log(System.Logger.Level.WARNING, "store " + name + " is at capacity: it refuses new pins and keeps serving");
        }
    }

    private long leaseMillis(StorePin pin) {
        return Math.min(pin.lease(), policy.leaseMax().toSeconds()) * 1000;
    }

    /** Starts holding a pin: at once when the root is held here already, else after a fetch. */
    private void begin(StorePin pin, SignedPin signed, TakenEntry<SignedPin> took) {
        long now = clock.millis();
        InFlight f = new InFlight(pin, signed, took, now);
        if (inFlight.putIfAbsent(pin.root(), f) != null) {
            return;
        }
        if (ledger.holds(pin.root())) {
            finish(f, now);
            return;
        }
        FetchOptions options = FetchOptions.defaults().withTimeout(fetchTimeout).prefer(pin.requester());
        if (StorePin.KIND_ARTIFACT.equals(pin.kind())) {
            f.artifact = content.fetch(pin.root(), options);
        } else if (policy.blind()) {
            f.probe = content.fetchNodes(List.of(pin.root()), options);
        } else {
            f.tree = content.fetchTree(pin.root(), heldHaves(pin), options);
        }
    }

    private List<String> heldHaves(StorePin pin) {
        return pin.haves().stream().filter(h -> pdt.store().complete(h)).toList();
    }

    private void drive(InFlight f, long now) {
        long received = f.received();
        if (received > f.progressBytes) {
            f.progressBytes = received;
            f.progressedMillis = now;
        }
        if (now - f.progressedMillis > fetchTimeout.toMillis()) {
            abandon(f, "the fetch received nothing for " + fetchTimeout);
            return;
        }
        if (f.taken != null && now - f.renewedMillis >= takeLease.toMillis() / 2) {
            try {
                f.taken.renew(takeLease);
                f.renewedMillis = now;
            } catch (RuntimeException lapsed) {
                abandon(f, "the take lapsed");
                return;
            }
        }
        if (f.probe != null) {
            if (f.probe.failure().isPresent()) {
                abandon(f, "the root node did not arrive");
            } else if (f.probe.complete()) {
                f.probeBytes = f.probe.progress().bytesReceived();
                f.probe = null;
                boolean sealed = pdt.store().stored(f.pin.root()).map(b -> Kind.SN.equals(NodeCodec.kindOf(b))).orElse(false);
                if (!sealed) {
                    abandon(f, "a blind store holds sealed-nodes trees only, and this root is plaintext");
                    return;
                }
                f.tree = content.fetchTree(f.pin.root(), heldHaves(f.pin),
                        FetchOptions.defaults().withTimeout(fetchTimeout).prefer(f.pin.requester()));
            }
            return;
        }
        if (f.tree == null && f.artifact == null) {
            return; // the take thread has not started the fetch yet
        }
        Optional<Throwable> failure = f.tree != null ? f.tree.failure() : f.artifact.failure();
        if (failure.isPresent()) {
            abandon(f, "the fetch failed: " + failure.get().getMessage());
        } else if (f.tree != null ? f.tree.complete() : f.artifact.complete()) {
            finish(f, now);
        }
    }

    private void abandon(InFlight f, String why) {
        if (inFlight.remove(f.pin.root(), f)) {
            f.cancel();
            abandoned.incrementAndGet();
            LOG.log(System.Logger.Level.INFO, "store " + name + " abandoned a pin of " + f.pin.root() + ": " + why);
        }
    }

    /** Pins and holds the root, records it, completes the pin with a receipt, and forwards the copies left. */
    private void finish(InFlight f, long now) {
        StorePin pin = f.pin;
        String root = pin.root();
        boolean tree = StorePin.KIND_TREE.equals(pin.kind());
        long bytes = tree ? treeBytes(root) : content.store().manifest(root).map(Manifest::size).orElse(0L);
        if (!ledger.holds(root) && bytes > free()) {
            markFull();
            abandon(f, "the root's " + bytes + " bytes exceed the free bytes");
            return;
        }
        if (bytes > policy.maxObjectBytes()) {
            abandon(f, "the root's " + bytes + " bytes exceed max-object");
            return;
        }
        long until = now + leaseMillis(pin);
        StoreReceipt receipt = new StoreReceipt(StoreReceipt.VERSION, root, self, bytes, until, policy.blind());
        Lease receiptLease = Lease.of(Duration.ofMillis(until - now).plus(holdingInterval));
        EntryHandle handle;
        try {
            handle = f.taken != null ? space.complete(f.taken, receipt, receiptLease, receipt.tags())
                    : space.write(receipt, receiptLease, receipt.tags());
        } catch (RuntimeException lapsed) {
            // the take lapsed or another store won it: the pin is not this store's, so it holds nothing for it,
            // charges the requester nothing, and forwards no copies (the winner does)
            abandon(f, "the take lapsed or another store won it before the receipt: " + lapsed.getMessage());
            return;
        }
        if (f.signed != null) {
            consentsTaken.add(Consent.of(f.signed));
        }
        boolean renewal = ledger.holding(root, pin.requester()).isPresent();
        if (!renewal) {
            if (tree) {
                pdt.swarm().hold(root);
            } else {
                content.pin(root);
            }
        }
        ledger.hold(new StoreLedger.Held(root, pin.kind(), bytes, pin.requester(), until, pin.copies()));
        withdraw(receipts.put(key(root, pin.requester()), handle));
        if (f.signed != null && f.signed.left() > 1 && f.signed.notAfter() > now) {
            // the requester's own signed terms with one copy fewer left: this store speaks for nobody (F1)
            SignedPin rest = f.signed.forward();
            // the pin's lease caps in seconds first: a lease near Long.MAX_VALUE seconds overflows in milliseconds
            Duration entry = Duration.ofMillis(Math.min(rest.notAfter() - now,
                    Math.min(pin.lease(), Stores.PIN_ENTRY_LEASE.toSeconds()) * 1000));
            space.write(rest, Lease.of(entry), rest.tags());
        }
        taken.incrementAndGet();
        full = free() <= 0;
        inFlight.remove(root, f);
    }

    private long treeBytes(String root) {
        long total = 0;
        for (String cid : pdt.store().reachable(root)) {
            total += pdt.store().stored(cid).map(b -> (long) b.length).orElse(0L);
        }
        return total;
    }

    /** Releases one requester's holding: unpins, withdraws the holding when no requester is left, withdraws the receipt. */
    private void release(StoreLedger.Held held, String why) {
        if (ledger.release(held.root(), held.requester()).isEmpty()) {
            return;
        }
        if (StorePin.KIND_TREE.equals(held.kind())) {
            pdt.swarm().release(held.root());
        } else if (!ledger.holds(held.root())) {
            content.unpin(held.root());
        }
        EntryHandle receipt = receipts.remove(key(held.root(), held.requester()));
        if (receipt != null) {
            withdraw(receipt);
        } else {
            withdrawWritten(held);
        }
        released.incrementAndGet();
        full = free() <= 0;
        LOG.log(System.Logger.Level.INFO, "store " + name + " released " + held.root() + " for "
                + held.requester().display() + ": " + why);
    }

    private static void withdraw(EntryHandle handle) {
        if (handle != null) {
            try {
                handle.cancel();
            } catch (RuntimeException lapsed) {
                // the receipt lapsed by its lease already
            }
        }
    }

    /**
     * Withdraws a receipt this store wrote before a restart, whose handle went with the process (P2-S-04,
     * P2-Q-REL-02): it takes the receipt that names the root, this store, and the holding's lease end, and
     * completes it, so the requester stops counting a copy the store released.
     */
    private void withdrawWritten(StoreLedger.Held held) {
        try {
            space.take(Template.of(StoreReceipt.class).whereTag(StoreReceipt.ROOT_TAG, eq(held.root()))
                            .whereTag(StoreReceipt.HOLDER_TAG, eq(self.value())).where("until", eq(held.until())),
                    Lease.of(takeLease), Duration.ZERO).ifPresent(space::complete);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "store " + name + " could not withdraw its receipt for " + held.root()
                    + "; it lapses with its lease", e);
        }
    }

    private static String key(String root, PeerId requester) {
        return root + "|" + requester.value();
    }
}
