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

import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.SpaceListener;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.identity.AgentCertificates;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.space.ConsistencyHint;
import ai.badmonkey.agentspaces.space.DiscoveryPublisher;
import ai.badmonkey.agentspaces.space.SpaceAdmissionException;
import ai.badmonkey.agentspaces.space.SpaceReadOnlyException;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.space.EntryView;
import ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The replicated AgentSpace (spec §7): the same {@link Space} interface as
 * {@code LocalSpace}, backed by the delta-CRDT replica and the group's gossip
 * bus. Writes, completions, and take claims ride the rumor channel as deltas;
 * anti-entropy reconciles anything rumors missed; and every replica evaluates the
 * same deterministic rules, so replicas that have seen the same updates agree.
 *
 * <p>Exclusive take runs the {@code LEASE_RACE} strategy (spec §7.4): a claim is
 * a {@link TakeClaim} lattice value published as a delta; after the settle window
 * the deterministic merge decides the winner identically on every replica. At
 * most one {@code complete} survives per entry (the CRDT's monotone completed
 * flag), and transient duplicate work during partitions is the documented
 * trade-off, so tasks should be idempotent.
 *
 * <p>Entry records are signed over their immutable identity (id, space, type,
 * payload, issuer, issue stamp, tags), and receivers verify the signature and
 * the writer's self-certifying identity before merging a delta.
 *
 * <p>Lifecycle (spec §7.5): a space built with {@link Builder#advertise}
 * publishes its signed {@code SpaceAdvertisement} on creation and on
 * {@link #refreshAdvertisement()}; {@link Builder#admission(SpaceAdmission)}
 * restricts who may write, take, and complete (under {@code CREDENTIAL} the
 * issuer's node hands out leased {@link SpaceCredential} entries with
 * {@link #grant} and withdraws them with {@link #revoke}); and
 * {@link Builder#founderLease} or
 * {@link #markReadOnly()} turns the space read-only. Lease lapses surface as
 * {@code EXPIRED} and {@code REAPPEARED} events from the sweep that the
 * anti-entropy tick (or {@link #sweepNow()}) runs, and the same sweep collects
 * tombstones twice the maximum lease after their lease lapsed (spec §7.3).
 */
public final class ReplicatedSpace implements Space, AutoCloseable {

    private final String name;
    private final SpaceId id;
    private final PeerIdentity identity;
    /**
     * Who signs this handle's records (spec §4.2, QA4 A4-7): the peer-signed
     * identity for {@code agentName} unless the builder was given another. Claims
     * and state transitions stay signed by the peer key in phase 1; the record
     * signature is the writer's.
     */
    private final AgentIdentity writer;
    private final AgentId issuer;
    /** Certificates of agent-attested writers, per entry, carried beside the record. */
    private final Map<EntryId, AgentCertificate> issuerCertificates = new HashMap<>();
    private final AgentCertificates certificates;
    private final GroupRuntime runtime;
    private final InstantSource clock;
    private final HybridLogicalClock hlc;
    private final CborCodec codec;
    private final SchemaRegistry schemas;
    private final Duration settleWindow;
    private final ai.badmonkey.agentspaces.api.space.ConflictStrategyType strategy;
    private volatile java.util.function.ToDoubleFunction<Object> bidFunction;
    private final ai.badmonkey.agentspaces.peering.blocks.BlockExchange blocks;
    /** The group's content keys by epoch (SPEC §11a.3); null for an unencrypted space. */
    private final ai.badmonkey.agentspaces.common.crypto.GroupKeyRing keyRing;
    private final java.util.function.Predicate<Map<String, String>> tagShard;
    private final String streamId;
    /** The bus registrations this handle owns; closed by {@link #close()} (QA4 A4-9). */
    private final AutoCloseable streamRegistration;
    private final AutoCloseable reconcileRegistration;

    /** Bounds {@link #shardDropped} so a sharded replica offered a long stream
     * of out-of-shard entries does not accumulate acknowledgement lines forever. */
    private static final int SHARD_DROPPED_CAP = 8192;

    /** Bounds {@link #refusedAck}. */
    private static final int REFUSED_ACK_CAP = 8192;

    /** Bounds {@link #rejectedClaims}. */
    private static final int REJECTED_CLAIMS_CAP = 4096;

    /**
     * Ceiling on the lease duration that feeds the tombstone GC horizon (spec
     * §7.3). Tracking the observed maximum lets a hostile member freeze GC
     * fleet-wide by writing one entry with a century-long lease; the cap keeps
     * that damage to two days of retention. A longer lease still protects its
     * own entry, since nothing is collected before its lease has lapsed.
     */
    static final long MAX_TRACKED_LEASE_MILLIS = Duration.ofHours(24).toMillis();

    /** How long a background block fetch waits before a later read may retry it. */
    static final long BLOCK_FETCH_TIMEOUT_MILLIS = 800;

    /** Default TTL of the published space advertisement (spec §7.5). */
    static final Duration DEFAULT_ADVERTISEMENT_TTL = Duration.ofMinutes(15);

    private final Object lock = new Object();
    private SpaceStateCrdt crdt = SpaceStateCrdt.empty();
    private final Map<EntryId, SpaceWire.SignedClaim> claims = new HashMap<>();
    /**
     * Entries whose current claim was decided by the ordered log, with the
     * decided epoch (QA4 A4-5). A log decision is authoritative for its epoch:
     * a gossip-borne claim at the same or a lower epoch cannot displace it,
     * which is the priority the old log-index re-stamp gave committed claims
     * by accident and this gives them by rule. An identical claim may still
     * merge, so an attestation upgrade through a completion delta is allowed.
     */
    private final Map<EntryId, Long> logDecidedEpochs = new HashMap<>();
    private final Map<EntryId, byte[]> issuerKeys = new HashMap<>();
    /** The latest actor-signed state DTO per entry, so anti-entropy forwards the
     * original {@code stateSig} (a relay cannot re-sign state it did not author).
     * Kept in step with {@link #crdt} by {@link #recordSignedState}. */
    private final Map<EntryId, SpaceWire.EntryStateDto> signedStates = new HashMap<>();
    /** Digest lines for entries this sharded replica saw and dropped, so
     * anti-entropy partners stop re-offering them; bounded LRU. */
    /**
     * Digest lines ({@code e:<id>=<hash>} or {@code c:<id>=<hash>}) for states
     * and claims this replica authenticated and refused because their actor is
     * revoked (ASF-047). Advertised as {@code a:<line>} so anti-entropy
     * partners stop re-offering them; bounded LRU.
     */
    private final Map<String, Boolean> refusedAck =
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > REFUSED_ACK_CAP;
                }
            };
    /** Entries whose claim was refused for a revoked holder, so the completion
     * that would authenticate against that claim is acknowledged, not re-offered. */
    private final Set<EntryId> refusedClaimEntries = java.util.Collections.newSetFromMap(
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<EntryId, Boolean> eldest) {
                    return size() > REFUSED_ACK_CAP;
                }
            });
    private final Map<EntryId, String> shardDropped =
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<EntryId, String> eldest) {
                    return size() > SHARD_DROPPED_CAP;
                }
            };
    private final Map<EntryId, Object> valueCache = new ConcurrentHashMap<>();
    private final List<Sub> subscriptions = new CopyOnWriteArrayList<>();
    private final AtomicLong dotCounter = new AtomicLong();

    // ---- tombstone GC and lease-lapse events (spec §7.2, §7.3)
    /** Builder override for the GC horizon, or null to use the observed maximum. */
    private final Duration maxLeaseOverride;
    /** Largest lease duration this replica has observed for the space, capped. */
    private volatile long maxLeaseObservedMillis;
    /** Entries whose EXPIRED event already fired here. Under {@link #lock}. */
    private final Set<EntryId> expiredFired = new HashSet<>();
    /** Claim epoch per entry whose REAPPEARED event already fired here. Under {@link #lock}. */
    private final Map<EntryId, Long> reappearedEpochs = new HashMap<>();
    /** Hashes of remote claims already rejected here, so a lagging honest replica
     * re-offering the same contested claim is not struck once per round. */
    private final Set<String> rejectedClaims = java.util.Collections.newSetFromMap(
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > REJECTED_CLAIMS_CAP;
                }
            });

    // ---- non-blocking block fetches (spec §7.2, P7)
    /** CIDs with a background fetch in flight, so a polling read does not re-send WANTs. */
    private final Set<String> fetching = ConcurrentHashMap.newKeySet();

    // ---- lifecycle: advertisement, admission, read-only (spec §7.5)
    private final DiscoveryPublisher publisher;
    private final Duration advertisementTtl;
    private final AdvertisementSigner adSigner;
    /** Schema names registered through this space, for the advertisement's hints. */
    private final Set<String> schemaNames = ConcurrentHashMap.newKeySet();
    private final SpaceAdmission admission;
    /** The credential rule (issuer and index) when {@link #admission} is CREDENTIAL, else null. */
    private final SpaceAdmission.CredentialAdmission credentialRule;
    /** The schema name of {@link SpaceCredential}, registered on every replica so it decodes everywhere. */
    private final String credentialSchema;
    private volatile long founderDeadlineMillis;
    private volatile boolean readOnly;
    private volatile SignedAdvertisement<SpaceAdvertisement> advertisement;

    private ReplicatedSpace(Builder builder) {
        this.name = builder.name;
        this.runtime = builder.runtime;
        this.identity = builder.identity;
        this.writer = builder.writer != null ? builder.writer
                : builder.identity.agentIdentity(builder.agentName);
        if (!writer.id().peer().equals(builder.identity.peerId())) {
            throw new IllegalArgumentException("writer " + writer.id().encoded()
                    + " does not belong to this peer " + builder.identity.peerId().display());
        }
        this.issuer = writer.id();
        // The same canonical codec the space signs and verifies everything with.
        this.certificates = new AgentCertificates(CborCodec.defaultCodec());
        this.clock = builder.clock;
        this.codec = CborCodec.defaultCodec();
        this.schemas = builder.schemas;
        this.settleWindow = builder.settleWindow;
        this.strategy = builder.strategy;
        this.bidFunction = builder.bidFunction;
        this.blocks = builder.blocks;
        this.keyRing = builder.keyRing;
        this.tagShard = builder.tagShard;
        this.maxLeaseOverride = builder.maxLease;
        this.publisher = builder.publisher;
        this.advertisementTtl = builder.advertisementTtl;
        this.adSigner = new AdvertisementSigner(codec);
        this.admission = builder.admission;
        this.credentialRule = builder.admission instanceof SpaceAdmission.CredentialAdmission rule
                ? rule : null;
        this.credentialSchema = schemas.register(SpaceCredential.class);
        this.founderDeadlineMillis = builder.founderLease == null
                ? Long.MAX_VALUE
                : builder.clock.instant().toEpochMilli() + builder.founderLease.toMillis();
        this.hlc = new HybridLogicalClock(builder.clock, issuer.encoded());
        this.id = SpaceId.local(runtime.id().value() + "/" + builder.name);
        this.streamId = "space:" + builder.name;
        for (Class<?> hint : builder.schemaHints) {
            registerSchema(hint);
        }

        // One replica per space name per node (QA4 A4-9): the bus refuses a
        // second registration under this stream id, so a second handle for a
        // name that is already open fails here, loudly, instead of silently
        // stealing the first handle's replication.
        this.streamRegistration = runtime.gossip().onStream(streamId,
                (from, itemId, payload) -> onDelta(from, payload));
        this.reconcileRegistration = runtime.gossip().reconcile(streamId, new ReconcilableState() {
            @Override
            public byte[] digest() {
                return ReplicatedSpace.this.digest();
            }

            @Override
            public byte[] deltaFor(byte[] remoteDigest) {
                return ReplicatedSpace.this.deltaFor(remoteDigest);
            }

            @Override
            public void applyDelta(byte[] delta) {
                ReplicatedSpace.this.applySyncDelta(delta);
            }
        });
        // Spec §7.5: a member creates a space by publishing its advertisement.
        refreshAdvertisement();
    }

    /**
     * Starts building a replicated space attached to a group.
     *
     * @param runtime   the group runtime
     * @param name      the space name within the group
     * @param identity  the local peer identity (signs entries)
     * @param agentName the local agent name writes are attributed to
     * @return the builder
     */
    public static Builder builder(GroupRuntime runtime, String name,
                                  PeerIdentity identity, String agentName) {
        return new Builder(runtime, name, identity, agentName);
    }

    /** Builder for {@link ReplicatedSpace}. */
    public static final class Builder {
        private final GroupRuntime runtime;
        private final String name;
        private final PeerIdentity identity;
        private final String agentName;
        private AgentIdentity writer;
        private InstantSource clock = InstantSource.system();
        private SchemaRegistry schemas = new SimpleSchemaRegistry();
        private Duration settleWindow = Duration.ofMillis(200);
        private ai.badmonkey.agentspaces.api.space.ConflictStrategyType strategy =
                ai.badmonkey.agentspaces.api.space.ConflictStrategyType.LEASE_RACE;
        private java.util.function.ToDoubleFunction<Object> bidFunction;
        private ai.badmonkey.agentspaces.peering.blocks.BlockExchange blocks;
        private ai.badmonkey.agentspaces.common.crypto.GroupKeyRing keyRing;
        private java.util.function.Predicate<Map<String, String>> tagShard;
        private Duration maxLease;
        private DiscoveryPublisher publisher;
        private Duration advertisementTtl = DEFAULT_ADVERTISEMENT_TTL;
        private final List<Class<?>> schemaHints = new ArrayList<>();
        private SpaceAdmission admission = SpaceAdmission.group();
        private Duration founderLease;

        private Builder(GroupRuntime runtime, String name, PeerIdentity identity, String agentName) {
            this.runtime = Objects.requireNonNull(runtime, "runtime");
            this.name = Objects.requireNonNull(name, "name");
            this.identity = Objects.requireNonNull(identity, "identity");
            this.agentName = Objects.requireNonNull(agentName, "agentName");
        }

        /**
         * Who this handle signs records as (spec §4.2, QA4 A4-7). Defaults to the
         * peer-signed identity for the builder's agent name, which is exactly the
         * behaviour every existing caller gets. Give a subordinate identity
         * ({@code identity.subordinate(name)}) and every record this handle writes
         * carries the agent's own signature and the peer's certificate for it, so
         * readers see the entry as {@code AGENT_ATTESTED}. The writer must belong
         * to this peer.
         *
         * @param writer the signing identity
         * @return this builder
         */
        public Builder writer(AgentIdentity writer) {
            this.writer = Objects.requireNonNull(writer, "writer");
            return this;
        }

        /**
         * Injects the time source.
         *
         * @param clock the time source
         * @return this builder
         */
        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /**
         * Uses a specific schema registry.
         *
         * @param schemas the registry
         * @return this builder
         */
        public Builder schemaRegistry(SchemaRegistry schemas) {
            this.schemas = Objects.requireNonNull(schemas, "schemas");
            return this;
        }

        /**
         * Sets the LEASE_RACE settle window (default 2x a typical gossip period;
         * {@code Duration.ZERO} for deterministic single-threaded tests).
         *
         * @param settleWindow the window
         * @return this builder
         */
        public Builder settleWindow(Duration settleWindow) {
            this.settleWindow = Objects.requireNonNull(settleWindow, "settleWindow");
            return this;
        }

        /**
         * Declares the space's conflict strategy for the space's own
         * {@code take} (spec §7.4). {@code LEASE_RACE} is the default and
         * {@code AUCTION} requires a {@link #bidFunction}; both are decided in
         * the space's claim lattice. {@code ORDERED} is not a space strategy:
         * exactly-once takes are driven by the ordered-log capability's
         * {@code OrderedTakes} coordinator over a Raft quorum, not by
         * {@code Space.take}, so it is rejected here. Build the space with the
         * default strategy and take through {@code OrderedTakes} instead.
         *
         * @param strategy the strategy
         * @return this builder
         */
        public Builder strategy(ai.badmonkey.agentspaces.api.space.ConflictStrategyType strategy) {
            if (strategy == ai.badmonkey.agentspaces.api.space.ConflictStrategyType.ORDERED) {
                throw new UnsupportedOperationException(
                        "ORDERED takes are driven by the ordered-log capability's "
                                + "OrderedTakes coordinator, not by a space strategy; "
                                + "build with the default strategy and take through it");
            }
            this.strategy = Objects.requireNonNull(strategy, "strategy");
            return this;
        }

        /**
         * Supplies the cost function for {@code AUCTION} takes: given the entry
         * value, return this agent's cost; the lowest bid across the fleet wins.
         *
         * @param bidFunction the cost function
         * @return this builder
         */
        public Builder bidFunction(java.util.function.ToDoubleFunction<Object> bidFunction) {
            this.bidFunction = Objects.requireNonNull(bidFunction, "bidFunction");
            return this;
        }

        /**
         * Attaches a block exchange so payloads above the inline limit are
         * content-addressed and fetched out of band (spec §7.1).
         *
         * @param blocks the group's block exchange
         * @return this builder
         */
        public Builder blocks(ai.badmonkey.agentspaces.peering.blocks.BlockExchange blocks) {
            this.blocks = Objects.requireNonNull(blocks, "blocks");
            return this;
        }

        /**
         * Encrypts entry payloads with a symmetric group content key (spec §11):
         * gossip and anti-entropy then carry ciphertext, signatures cover the
         * ciphertext (so keyless replicas still verify and relay records), and
         * only replicas holding the key can read entry contents.
         *
         * @param groupKey the group content key
         * @return this builder
         */
        public Builder groupKey(ai.badmonkey.agentspaces.common.crypto.GroupKey groupKey) {
            this.keyRing = ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(
                    Objects.requireNonNull(groupKey, "groupKey"));
            return this;
        }

        /**
         * Encrypts payloads under a content-key ring (SPEC §11a.3, v0.1.13):
         * writers seal under the ring's epoch in effect, readers open with the
         * epoch the record names. Spaces of one group share the group's ring, so
         * a rotation reaches all of them at once.
         *
         * @param keyRing the group's ring
         * @return this builder
         */
        public Builder keyRing(ai.badmonkey.agentspaces.common.crypto.GroupKeyRing keyRing) {
            this.keyRing = Objects.requireNonNull(keyRing, "keyRing");
            return this;
        }

        /**
         * Makes this replica partial (spec §7.5): it stores, matches, and serves
         * only entries whose tags satisfy the shard predicate. Out-of-shard
         * entries seen in deltas are acknowledged in digests but never stored.
         * This replica's own writes are always stored, whatever their tags.
         *
         * @param tagShard the shard predicate over an entry's tags
         * @return this builder
         */
        public Builder tagShard(java.util.function.Predicate<Map<String, String>> tagShard) {
            this.tagShard = Objects.requireNonNull(tagShard, "tagShard");
            return this;
        }

        /**
         * Fixes the maximum lease the tombstone garbage collector reckons with
         * (spec §7.3) instead of the largest lease this replica has observed.
         * Completed, withdrawn, and lapsed entries are dropped once their lease
         * expired more than twice this long ago.
         *
         * @param maxLease the maximum lease
         * @return this builder
         */
        public Builder maxLease(Duration maxLease) {
            this.maxLease = Objects.requireNonNull(maxLease, "maxLease");
            return this;
        }

        /**
         * Publishes a signed {@link SpaceAdvertisement} for the space on build
         * and on every {@link ReplicatedSpace#refreshAdvertisement()} (spec
         * §7.5): the space's name, schema hints, strategy, admission rule, and
         * replication mode, signed by the founding peer.
         *
         * @param publisher where advertisements go ({@code DiscoveryService::publish})
         * @return this builder
         */
        public Builder advertise(DiscoveryPublisher publisher) {
            this.publisher = Objects.requireNonNull(publisher, "publisher");
            return this;
        }

        /**
         * Sets the TTL of the published advertisement; the founders' refresh
         * loop must republish within it. Default fifteen minutes.
         *
         * @param ttl the advertisement time-to-live
         * @return this builder
         */
        public Builder advertisementTtl(Duration ttl) {
            this.advertisementTtl = Objects.requireNonNull(ttl, "ttl");
            return this;
        }

        /**
         * Declares entry types up front so the first advertisement already
         * carries their schema hints; types written later are added on refresh.
         *
         * @param types the entry classes
         * @return this builder
         */
        public Builder schemaHints(Class<?>... types) {
            for (Class<?> type : types) {
                schemaHints.add(Objects.requireNonNull(type, "type"));
            }
            return this;
        }

        /**
         * Sets the space's admission rule by name (spec §7.5), delegating to
         * {@link #admission(SpaceAdmission)}. Under
         * {@link SpaceAdvertisement.Admission#ALLOWLIST} only the listed agents
         * may write, take, or complete: a local caller outside the list gets a
         * {@link SpaceAdmissionException}, and an inbound record or claim from an
         * unlisted agent is dropped and reported as misbehavior. Everyone in the
         * group may still read. {@link SpaceAdvertisement.Admission#GROUP}, the
         * default, admits every group member and ignores the list.
         * {@link SpaceAdvertisement.Admission#CREDENTIAL} makes this node the
         * credential issuer (the founder's configuration) and ignores the list;
         * {@link SpaceAdvertisement.Admission#AUTHORIZER} needs an
         * {@code Authorizer} and must be built with
         * {@link SpaceAdmission#authorizer}.
         *
         * @param admission the rule
         * @param allowed   the admitted agents under ALLOWLIST
         * @return this builder
         */
        public Builder admission(SpaceAdvertisement.Admission admission, Set<AgentId> allowed) {
            Objects.requireNonNull(admission, "admission");
            Set<AgentId> agents = Set.copyOf(Objects.requireNonNull(allowed, "allowed"));
            return admission(switch (admission) {
                case GROUP -> SpaceAdmission.group();
                case ALLOWLIST -> SpaceAdmission.allowlist(agents);
                case CREDENTIAL -> SpaceAdmission.credentials(identity.peerId(), new CredentialIndex());
                case AUTHORIZER -> throw new IllegalArgumentException(
                        "AUTHORIZER admission needs an Authorizer: build it with "
                                + "admission(SpaceAdmission.authorizer(authorizer, spaceName))");
            });
        }

        /**
         * Sets the space's admission rule (spec §7.5, TECH-SPEC §7.10). The rule
         * is asked on every local write, take, and completion and on every
         * inbound record or claim after its signature verifies: a refused
         * local caller gets a {@link SpaceAdmissionException}; a refused remote
         * delta is dropped, and struck as misbehavior only when the rule says a
         * refusal cannot be a late credential
         * ({@link SpaceAdmission#strikesOnRefusal()}). Under
         * {@link SpaceAdmission#credentials} this replica indexes the issuer's
         * {@link SpaceCredential} entries as they merge, lapse, and are
         * cancelled, and the issuer's own node may {@link ReplicatedSpace#grant}
         * and {@link ReplicatedSpace#revoke}. Reads stay open to the group.
         *
         * @param admission the rule
         * @return this builder
         */
        public Builder admission(SpaceAdmission admission) {
            this.admission = Objects.requireNonNull(admission, "admission");
            return this;
        }

        /**
         * Leases the space itself to its founders (spec §7.5): once this long
         * has passed on the space's clock the space becomes read-only, so
         * writes and takes throw {@link SpaceReadOnlyException} while reads keep
         * working. {@link ReplicatedSpace#renewFounderLease} extends it.
         *
         * @param founderLease how long the founders hold the space
         * @return this builder
         */
        public Builder founderLease(Duration founderLease) {
            this.founderLease = Objects.requireNonNull(founderLease, "founderLease");
            return this;
        }

        /** Builds and attaches the space. A bid function may also arrive later
         * via {@link ReplicatedSpace#bidFunction}, e.g. from an agent binder. */
        public ReplicatedSpace build() {
            return new ReplicatedSpace(this);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public java.util.Optional<AgentId> writer() {
        return java.util.Optional.of(issuer);
    }

    /**
     * A per-agent view of this replica (SPEC §4.2, QA4 A4-7 phase 3): the same
     * space — one replica, one gossip registration, one clock per node — seen
     * through another agent's identity. Records the view writes are issued and
     * signed by {@code actor} (attested when it is a subordinate identity), take
     * claims and completions through it name {@code actor} as the holder, and
     * admission is judged for {@code actor}, so two views of one replica can be
     * admitted differently under ALLOWLIST or CREDENTIAL. Reads and
     * subscriptions are the replica's own. The view has no resources of its own:
     * closing this space ends every view, and views do not close.
     *
     * @param actor the agent the view acts as; must belong to this peer
     * @return the view
     * @throws IllegalArgumentException when the actor is another peer's agent
     */
    public Space as(AgentIdentity actor) {
        Objects.requireNonNull(actor, "actor");
        if (!actor.id().peer().equals(identity.peerId())) {
            throw new IllegalArgumentException("agent " + actor.id().encoded()
                    + " does not belong to this peer " + identity.peerId().display());
        }
        return actor.id().equals(issuer) && actor == writer ? this : new View(actor);
    }

    /** The delegating view: identity differs, everything else is the replica. */
    private final class View implements Space {

        private final AgentIdentity actor;

        private View(AgentIdentity actor) {
            this.actor = actor;
        }

        ReplicatedSpace owner() {
            return ReplicatedSpace.this;
        }

        @Override
        public String name() {
            return ReplicatedSpace.this.name();
        }

        @Override
        public SpaceId id() {
            return ReplicatedSpace.this.id();
        }

        @Override
        public java.util.Optional<AgentId> writer() {
            return java.util.Optional.of(actor.id());
        }

        @Override
        public <T> EntryHandle write(T entry, Lease lease) {
            return writeAs(actor, entry, lease, Map.of());
        }

        @Override
        public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
            return writeAs(actor, entry, lease, tags);
        }

        @Override
        public <T> Optional<T> read(Template<T> template) {
            return ReplicatedSpace.this.read(template);
        }

        @Override
        public <T> Optional<T> read(Template<T> template, Duration timeout) {
            return ReplicatedSpace.this.read(template, timeout);
        }

        @Override
        public <T> List<T> readAll(Template<T> template, int limit) {
            return ReplicatedSpace.this.readAll(template, limit);
        }

        @Override
        public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
            return ReplicatedSpace.this.readAllIssued(template, limit);
        }

        @Override
        public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease,
                                                Duration timeout) {
            return takeAs(actor, template, takeLease, timeout);
        }

        @Override
        public <T> List<Entry<T>> readAllEntries(Template<T> template, int limit) {
            return ReplicatedSpace.this.readAllEntries(template, limit);
        }

        @Override
        public void complete(TakenEntry<?> taken) {
            completeInternal(actor, taken, null, null, Map.of());
        }

        @Override
        public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease) {
            return complete(taken, result, resultLease, Map.of());
        }

        @Override
        public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease,
                                        Map<String, String> tags) {
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(resultLease, "resultLease");
            Objects.requireNonNull(tags, "tags");
            return completeInternal(actor, taken, result, resultLease, tags);
        }

        @Override
        public <T> Subscription notify(Template<T> template, SpaceListener<T> listener,
                                       Lease lease) {
            return ReplicatedSpace.this.notify(template, listener, lease);
        }

        @Override
        public String toString() {
            return "ReplicatedSpace[" + name + "] as " + actor.id().encoded();
        }
    }

    @Override
    public SpaceId id() {
        return id;
    }

    /** Returns the space's declared conflict strategy. */
    public ai.badmonkey.agentspaces.api.space.ConflictStrategyType strategy() {
        return strategy;
    }

    /**
     * Replaces the AUCTION bid function (for binder-driven wiring).
     *
     * @param bidFunction the cost function
     */
    public void bidFunction(java.util.function.ToDoubleFunction<Object> bidFunction) {
        Objects.requireNonNull(bidFunction, "bidFunction");
        // QA4 A4-8: a space consults one cost function. This used to be a plain
        // setter, so a second bidder on the same peer silently displaced the
        // first and the first never priced anything. Single-assignment; the
        // builder's value counts as the first.
        if (this.bidFunction != null) {
            throw new IllegalStateException("space '" + name + "' already has a bid function;"
                    + " one bid function per space per peer — a second bidder belongs on its"
                    + " own peer");
        }
        this.bidFunction = bidFunction;
    }

    // ------------------------------------------------------------ lifecycle (§7.5)

    /**
     * Re-publishes the space's signed advertisement with a fresh issue instant
     * and the schema hints registered so far (spec §7.5). The founders' refresh
     * loop calls this within the advertisement TTL. A no-op returning empty
     * when the space was built without {@link Builder#advertise} or has become
     * read-only: letting the advertisement lapse is how founders release a
     * space.
     *
     * @return the advertisement published, when one was
     */
    public Optional<SignedAdvertisement<SpaceAdvertisement>> refreshAdvertisement() {
        if (publisher == null || readOnly()) {
            return Optional.empty();
        }
        List<String> hints = new ArrayList<>(schemaNames);
        java.util.Collections.sort(hints);
        SpaceAdvertisement ad = new SpaceAdvertisement(
                "aspace://" + runtime.id().value() + "/" + name, identity.peerId(),
                runtime.id(), clock.instant(), advertisementTtl, name, hints, strategy,
                admission.rule(),
                tagShard == null ? SpaceAdvertisement.Replication.FULL
                        : SpaceAdvertisement.Replication.TAG_SHARDED);
        SignedAdvertisement<SpaceAdvertisement> signed = adSigner.sign(ad, identity);
        advertisement = signed;
        publisher.publish(signed);
        return Optional.of(signed);
    }

    /** Returns the most recently published advertisement, when the space advertises. */
    public Optional<SignedAdvertisement<SpaceAdvertisement>> advertisement() {
        return Optional.ofNullable(advertisement);
    }

    /** Returns the space's advertised admission rule (spec §7.5). */
    public SpaceAdvertisement.Admission admission() {
        return admission.rule();
    }

    /** Returns the admission rule object the space asks (spec §7.5, TECH-SPEC §7.10). */
    public SpaceAdmission admissionRule() {
        return admission;
    }

    /** Returns the agents admitted under {@code ALLOWLIST}; empty under every other rule. */
    public Set<AgentId> allowedAgents() {
        return admission instanceof SpaceAdmission.AllowlistAdmission allowlist
                ? allowlist.agents() : Set.of();
    }

    /** Returns the credential index this replica maintains under {@code CREDENTIAL} admission. */
    public Optional<CredentialIndex> credentialIndex() {
        return Optional.ofNullable(credentialRule).map(SpaceAdmission.CredentialAdmission::index);
    }

    /**
     * Issues a space credential (spec §7.5): writes a {@link SpaceCredential}
     * entry admitting {@code agent} for {@code scopes}, with a write lease of
     * {@code validity} that is the credential's expiry. Because it is an
     * ordinary signed entry it replicates like any write, and a replica admits
     * the agent once the entry is present there. The returned handle renews or
     * cancels the credential like any entry; a credential nobody renews lapses
     * on its own. Only the configured credential issuer's node may grant.
     *
     * @param agent    the agent to admit
     * @param scopes   the scopes granted
     * @param validity how long the credential lives unless renewed
     * @return the credential entry's handle
     * @throws SpaceAdmissionException when this space does not admit by
     *                                 credential or this node is not its issuer
     */
    public EntryHandle grant(AgentId agent, Set<SpaceAdmission.Scope> scopes, Duration validity) {
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(scopes, "scopes");
        Objects.requireNonNull(validity, "validity");
        requireIssuer("grant");
        return write(new SpaceCredential(agent, scopes), Lease.of(validity));
    }

    /**
     * Withdraws every live credential this replica holds for an agent (spec
     * §7.5) by cancelling the credential entries. Cancellation replicates like
     * any removal, so the revocation is fleet-wide within one gossip round.
     * Only the configured credential issuer's node may revoke.
     *
     * @param agent the agent whose credentials are withdrawn
     * @throws SpaceAdmissionException when this space does not admit by
     *                                 credential or this node is not its issuer
     */
    public void revoke(AgentId agent) {
        Objects.requireNonNull(agent, "agent");
        requireIssuer("revoke");
        List<EntryId> credentials;
        synchronized (lock) {
            credentials = credentialRule.index().entriesFor(agent, nowMillis());
        }
        for (EntryId entryId : credentials) {
            new Handle(entryId, writer).cancel();
        }
    }

    private void requireIssuer(String operation) {
        if (credentialRule == null) {
            throw new SpaceAdmissionException("space '" + name
                    + "' does not admit by credential; " + operation + " refused");
        }
        if (!credentialRule.issuer().equals(identity.peerId())) {
            throw new SpaceAdmissionException("peer " + identity.peerId().display()
                    + " is not the credential issuer " + credentialRule.issuer().display()
                    + " of space '" + name + "'; " + operation + " refused");
        }
    }

    /**
     * Whether the founders' lease on the space has lapsed on this replica's
     * clock (spec §7.5). Always {@code false} when no founder lease was set.
     *
     * @return {@code true} once the deadline has passed
     */
    public boolean founderLeaseExpired() {
        return nowMillis() >= founderDeadlineMillis;
    }

    /** Whether the space refuses writes and takes: founder lease lapsed or marked read-only. */
    public boolean readOnly() {
        return readOnly || founderLeaseExpired();
    }

    /** Makes the space read-only now (spec §7.5); reads keep working. Irreversible. */
    public void markReadOnly() {
        readOnly = true;
    }

    /**
     * Extends the founders' lease on the space from now.
     *
     * @param extension how much longer the founders hold the space
     * @throws SpaceReadOnlyException when the space is already read-only
     */
    public void renewFounderLease(Duration extension) {
        Objects.requireNonNull(extension, "extension");
        if (readOnly()) {
            throw new SpaceReadOnlyException("space '" + name + "' is read-only; its founder lease cannot be renewed");
        }
        founderDeadlineMillis = nowMillis() + extension.toMillis();
    }

    /**
     * Applies lease lapses now (spec §7.2, §7.3): fires {@code EXPIRED} for
     * entries whose write lease lapsed and {@code REAPPEARED} for entries whose
     * take claim lapsed without completion, once per transition, and collects
     * tombstones older than twice the maximum lease. The anti-entropy tick runs
     * the same sweep; call this to sweep without traffic.
     */
    public void sweepNow() {
        List<Runnable> events;
        synchronized (lock) {
            events = sweepLocked();
        }
        events.forEach(Runnable::run);
    }

    /** The maximum lease the tombstone collector reckons with right now, in milliseconds. */
    public long maxLeaseMillis() {
        return maxLeaseOverride != null ? maxLeaseOverride.toMillis() : maxLeaseObservedMillis;
    }

    // ------------------------------------------------------------ operations (§7.2)

    @Override
    public <T> EntryHandle write(T entry, Lease lease) {
        return write(entry, lease, Map.of());
    }

    @Override
    public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
        return writeAs(writer, entry, lease, tags);
    }

    private <T> EntryHandle writeAs(AgentIdentity actor, T entry, Lease lease,
                                    Map<String, String> tags) {
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(tags, "tags");
        requireWritable(actor, "write", SpaceAdmission.Scope.WRITE);
        return commitWrite(prepareWrite(actor, entry, lease, tags));
    }

    /** Spec §7.5: refuses mutation from an unadmitted local agent or on a read-only space. */
    private void requireWritable(AgentIdentity actor, String operation, SpaceAdmission.Scope scope) {
        if (readOnly()) {
            throw new SpaceReadOnlyException("space '" + name + "' is read-only: " + operation
                    + " refused (founder lease lapsed or space marked read-only)");
        }
        requireAdmitted(actor, operation, scope);
    }

    /** Admission is judged per acting agent (spec §7.5), so two views of one replica can differ. */
    private void requireAdmitted(AgentIdentity actor, String operation, SpaceAdmission.Scope scope) {
        if (!admits(actor.id(), scope, nowMillis())) {
            throw new SpaceAdmissionException("agent " + actor.id().encoded()
                    + " is not admitted to " + operation + " on space '" + name
                    + "' (" + admission + ")");
        }
    }

    /** Whether an agent may perform the scope here now (spec §7.5): WRITE for records, TAKE for claims and completions. */
    private boolean admits(AgentId agent, SpaceAdmission.Scope scope, long nowMillis) {
        return admission.admits(agent, scope, nowMillis);
    }

    /**
     * Registers an entry type and remembers its schema name for the
     * advertisement. The reserved {@link SpaceCredential} type is registered
     * on every replica but never advertised as a hint: it is admission
     * infrastructure, not the shape of the data the space carries.
     */
    private String registerSchema(Class<?> type) {
        String schemaName = schemas.register(type);
        if (!schemaName.equals(credentialSchema)) {
            schemaNames.add(schemaName);
        }
        return schemaName;
    }

    /**
     * Reads with a consistency hint (spec §7.2): {@link ConsistencyHint#FRESH}
     * performs exactly one anti-entropy pull toward one random group member
     * before the local match; {@link ConsistencyHint#LOCAL} is
     * {@link #read(Template, Duration)}.
     *
     * @param template the template
     * @param hint     how current the read must be
     * @param timeout  how long to wait for a match
     * @param <T>      the entry type
     * @return the first match
     */
    public <T> Optional<T> read(Template<T> template, ConsistencyHint hint, Duration timeout) {
        Objects.requireNonNull(hint, "hint");
        pullIfFresh(hint);
        return read(template, timeout);
    }

    /**
     * Reads all matches with a consistency hint (spec §7.2); see
     * {@link #read(Template, ConsistencyHint, Duration)}.
     *
     * @param template the template
     * @param hint     how current the read must be
     * @param limit    the maximum number of results
     * @param <T>      the entry type
     * @return the matches, up to {@code limit}
     */
    public <T> List<T> readAll(Template<T> template, ConsistencyHint hint, int limit) {
        Objects.requireNonNull(hint, "hint");
        pullIfFresh(hint);
        return readAll(template, limit);
    }

    /** One anti-entropy round (digest to one random member) for FRESH reads. */
    private void pullIfFresh(ConsistencyHint hint) {
        if (hint == ConsistencyHint.FRESH) {
            runtime.gossip().antiEntropyTick();
        }
    }

    /** A fully-built, signed entry ready to commit; construction may fail, commit does not. */
    private record PreparedWrite(EntryId entryId, EntryRecord signed, Dot dot,
                                 LwwRegister<LeaseInfo> lease, Object value, AgentIdentity actor,
                                 AgentCertificate certificate) {
    }

    /**
     * Builds and signs an entry record without mutating the space. Everything
     * that can fail (serialization, encryption, the oversize-without-blocks
     * check, the block store) happens here, so a caller that must not commit
     * one change until another is guaranteed to succeed (see
     * {@link #completeInternal}) can prepare first and commit last.
     */
    private <T> PreparedWrite prepareWrite(AgentIdentity actor, T entry, Lease lease,
                                           Map<String, String> tags) {
        if (actorRevoked(actor)) {
            // v0.1.13: every receiver would refuse it; fail where the caller can see why.
            throw new IllegalStateException(actor.id().encoded() + " is revoked in this group");
        }
        registerSchema(entry.getClass());
        observeLease(lease.duration().toMillis());
        long expiresAt = nowMillis() + lease.duration().toMillis();
        EntryId entryId = EntryId.newId();
        LeaseInfo writeLease = new LeaseInfo(actor.id(), expiresAt, LeaseKind.WRITE);
        byte[] payload = codec.toBytes(entry);
        Long keyEpoch = null;
        if (keyRing != null) {
            // SPEC §11a.3 v0.1.13: seal under the newest epoch in effect; the
            // record names the epoch (absent for 0) and the AAD binds it.
            long epoch = keyRing.writeEpoch(clock.instant());
            payload = keyRing.sealingKey(epoch).orElseThrow(() -> new IllegalStateException(
                    "content key epoch " + epoch + " is not held")).encrypt(payload, aadFor(entryId, epoch));
            keyEpoch = epoch == 0 ? null : epoch;
        }
        String payloadRef = null;
        if (payload.length > EntryRecord.INLINE_PAYLOAD_LIMIT) {
            if (blocks == null) {
                throw new IllegalStateException("payload of " + payload.length
                        + " bytes exceeds the inline limit; configure builder.blocks(...)");
            }
            payloadRef = blocks.put(payload);
            payload = null;
        }
        EntryRecord unsigned = new EntryRecord(entryId, id,
                schemas.schemaNameOf(entry.getClass()), payload, payloadRef,
                actor.id(), hlc.now(), writeLease, tags, null, keyEpoch);
        // SPEC §4.2 v0.1.13: receivers judge the certificate at the record's
        // issue stamp, so the writer attaches the one covering that stamp.
        AgentCertificate certificate = coveringCertificate(actor, unsigned.issued());
        EntryRecord signed = withSignature(unsigned, actor);
        Dot dot = new Dot(identity.peerId().value(), dotCounter.incrementAndGet());
        return new PreparedWrite(entryId, signed, dot,
                new LwwRegister<>(signed.issued(), writeLease), entry, actor, certificate);
    }

    /** Commits a prepared entry into the space; does not fail. */
    private EntryHandle commitWrite(PreparedWrite prepared) {
        SpaceWire.EntryStateDto dto;
        synchronized (lock) {
            crdt = crdt.add(prepared.signed(), prepared.dot(), prepared.lease());
            issuerKeys.put(prepared.entryId(), identity.rawPublicKey());
            if (prepared.certificate() != null) {
                issuerCertificates.put(prepared.entryId(), prepared.certificate());
            }
            valueCache.put(prepared.entryId(), prepared.value());
            indexCredential(prepared.entryId());
            dto = signedDto(prepared.entryId(), identity.rawPublicKey(), prepared.actor());
        }
        publishDelta(new SpaceWire.Delta(dto, null, null), "w:" + prepared.entryId());
        fire(SpaceEvent.Kind.WRITTEN, prepared.entryId(), prepared.value(), prepared.actor().id(),
                prepared.actor().id(), prepared.signed());
        return new Handle(prepared.entryId(), prepared.actor());
    }

    @Override
    public <T> Optional<T> read(Template<T> template) {
        Objects.requireNonNull(template, "template");
        registerSchema(template.type());
        // Snapshot the candidate records under the lock, then decode outside it.
        // decodeIfMatches never waits on the network (P7): a missing block only
        // starts a background fetch, so nothing here holds the space lock while
        // a frame carrying the block needs it on the transport reader thread.
        List<EntryRecord> candidates;
        synchronized (lock) {
            candidates = availableRecords(schemas.schemaNameOf(template.type()));
        }
        for (EntryRecord record : candidates) {
            Optional<T> value = decodeIfMatches(template, record);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    @Override
    public <T> Optional<T> read(Template<T> template, Duration timeout) {
        return awaiting(timeout, () -> read(template));
    }

    @Override
    public <T> List<T> readAll(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        registerSchema(template.type());
        // Snapshot under the lock, decode outside it; entries whose block has not
        // arrived are skipped this time (see read()).
        List<EntryRecord> candidates;
        synchronized (lock) {
            candidates = availableRecords(schemas.schemaNameOf(template.type()));
        }
        List<T> results = new ArrayList<>();
        for (EntryRecord record : candidates) {
            decodeIfMatches(template, record).ifPresent(results::add);
            if (results.size() == limit) {
                break;
            }
        }
        return results;
    }

    @Override
    public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        registerSchema(template.type());
        List<EntryRecord> candidates;
        synchronized (lock) {
            candidates = availableRecords(schemas.schemaNameOf(template.type()));
        }
        // The issuer comes off the entry record, whose signature mergeState
        // verified against that issuer's key, so it is the authenticated writer.
        List<Issued<T>> results = new ArrayList<>();
        for (EntryRecord record : candidates) {
            decodeIfMatches(template, record).ifPresent(value ->
                    results.add(new Issued<>(value, record.issuer(),
                            issuerCertificates.containsKey(record.entryId())
                                    ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED)));
            if (results.size() == limit) {
                break;
            }
        }
        return results;
    }

    @Override
    public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease, Duration timeout) {
        return takeAs(writer, template, takeLease, timeout);
    }

    private <T> Optional<TakenEntry<T>> takeAs(AgentIdentity actor, Template<T> template,
                                               Lease takeLease, Duration timeout) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(takeLease, "takeLease");
        Objects.requireNonNull(timeout, "timeout");
        registerSchema(template.type());
        requireWritable(actor, "take", SpaceAdmission.Scope.TAKE);
        observeLease(takeLease.duration().toMillis());
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Optional<TakenEntry<T>> taken = tryTakeOnce(actor, template, takeLease);
            if (taken.isPresent()) {
                return taken;
            }
            if (System.nanoTime() >= deadline) {
                return Optional.empty();
            }
            sleep(Duration.ofMillis(20));
        }
    }

    @Override
    public <T> List<Entry<T>> readAllEntries(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        registerSchema(template.type());
        // Issue #16 §9.2: the readAll visibility rule (live, unclaimed, decodable
        // now) over the records this replica holds; the attestation is read
        // under the lock beside the candidates, as the certificate table is.
        List<EntryRecord> candidates;
        Set<EntryId> attested = new HashSet<>();
        synchronized (lock) {
            candidates = availableRecords(schemas.schemaNameOf(template.type()));
            for (EntryRecord record : candidates) {
                if (issuerCertificates.containsKey(record.entryId())) {
                    attested.add(record.entryId());
                }
            }
        }
        List<Entry<T>> results = new ArrayList<>();
        for (EntryRecord record : candidates) {
            decodeIfMatches(template, record).ifPresent(value ->
                    results.add(EntryView.of(record, value, attested.contains(record.entryId())
                            ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED)));
            if (results.size() == limit) {
                break;
            }
        }
        return results;
    }

    /**
     * The record this replica currently holds for an entry (issue #16 §9.3):
     * present while the CRDT reports the entry present, whether it is available
     * or claimed, and empty once it is completed, withdrawn, or unknown here.
     * The ordered join uses it to read a ticket's tags for an entry id it learned
     * from a committed claim, which a template read cannot see while claimed.
     * The lease on the returned record is the one in force.
     *
     * @param entryId the entry
     * @return the record, when the entry is present here
     */
    public Optional<EntryRecord> recordOf(EntryId entryId) {
        return recordOf(entryId, false);
    }

    /**
     * {@link #recordOf(EntryId)}, optionally answering for a completed entry
     * too, from the tombstone the CRDT keeps until the collection horizon
     * (SPEC §7.3). The ordered join needs this: a member may learn of the first
     * committed ticket claim for a key after that ticket's taker has already
     * completed it, and must still read the ticket's key to know that a later
     * ticket for the same key does not fire.
     *
     * @param entryId      the entry
     * @param completedToo whether a completed entry's record is returned as well
     * @return the record, when the replica holds one
     */
    public Optional<EntryRecord> recordOf(EntryId entryId, boolean completedToo) {
        Objects.requireNonNull(entryId, "entryId");
        synchronized (lock) {
            return crdt.state(entryId)
                    .filter(state -> state.present() || (completedToo && state.completed()))
                    .map(state -> state.record().withLease(state.lease().value()));
        }
    }

    /**
     * The schema name this replica registers {@code type} under (issue #16
     * §10.2), registering it first when it is new, as a read or subscription
     * would. A binder compares this with the name it advertises so a card and
     * the space agree on a type's name.
     *
     * @param type the entry type
     * @return the schema name
     */
    public String schemaNameOf(Class<?> type) {
        Objects.requireNonNull(type, "type");
        return registerSchema(type);
    }

    @Override
    public void complete(TakenEntry<?> taken) {
        completeInternal(writer, taken, null, null, Map.of());
    }

    @Override
    public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease) {
        return complete(taken, result, resultLease, Map.of());
    }

    @Override
    public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease,
                                    Map<String, String> tags) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(resultLease, "resultLease");
        Objects.requireNonNull(tags, "tags");
        return completeInternal(writer, taken, result, resultLease, tags);
    }

    @Override
    public <T> Subscription notify(Template<T> template, SpaceListener<T> listener, Lease lease) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(lease, "lease");
        // A subscription declares interest in a type: register it so replicated
        // deltas of that type decode into events on this replica.
        registerSchema(template.type());
        Sub sub = new Sub(template, listener, nowMillis() + lease.duration().toMillis());
        subscriptions.add(sub);
        return sub;
    }

    @Override
    public void close() {
        subscriptions.clear();
        // Leave the bus, so the name can be opened again and a delta for this
        // replica is dropped rather than applied to a handle nobody holds.
        try {
            streamRegistration.close();
            reconcileRegistration.close();
        } catch (Exception e) {
            throw new IllegalStateException("failed deregistering space '" + name + "'", e);
        }
    }

    /**
     * Returns the current take claim on an entry, when any replica claim is known
     * here. Exposed for the ordered-log coordinator and for fleet consoles.
     *
     * @param entryId the entry
     * @return the current claim
     */
    public Optional<TakeClaim> currentClaim(EntryId entryId) {
        synchronized (lock) {
            return Optional.ofNullable(claimOf(entryId));
        }
    }

    /**
     * Installs a claim the ordered log committed, <em>with the holder's own
     * attestation</em> (spec §7.4 ORDERED, QA4 A4-5). The coordinator verified
     * {@code signature} over {@code claim} under {@code holderKey} before the
     * command was ever committed; passing them through unchanged is what lets
     * every replica later authenticate the holder's completion, because a
     * completion is verified against the key the stored claim carries. The
     * claim is never re-published as a rumor; the log is its replication
     * channel, and the log's decision is authoritative for the claim's epoch.
     *
     * @param entryId   the entry
     * @param claim     the committed claim, exactly as the holder signed it
     * @param holderKey the holder's raw Ed25519 public key
     * @param signature the holder's signature over the claim's canonical bytes
     * @throws IllegalArgumentException when the key does not belong to the
     *                                  claim's holder or the signature does not
     *                                  verify — the caller has proof that does
     *                                  not prove what it claims
     */
    public void applyAuthorizedClaim(EntryId entryId, TakeClaim claim,
                                     byte[] holderKey, byte[] signature) {
        applyAuthorizedClaim(entryId, claim, holderKey, signature, null);
    }

    /**
     * {@link #applyAuthorizedClaim(EntryId, TakeClaim, byte[], byte[])} for a claim
     * the holder signed with a subordinate agent key: the certificate travels with
     * the proof and the two-key rule verifies it (QA4 A4-7 phase 3).
     *
     * @param entryId     the entry
     * @param claim       the committed claim
     * @param holderKey   the holder's peer key
     * @param signature   the holder's signature, under its agent key when a certificate is given
     * @param certificate the peer's certificate for the agent key, or null for a peer-signed claim
     */
    public void applyAuthorizedClaim(EntryId entryId, TakeClaim claim,
                                     byte[] holderKey, byte[] signature,
                                     ai.badmonkey.agentspaces.api.security.AgentCertificate certificate) {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(claim, "claim");
        Objects.requireNonNull(holderKey, "holderKey");
        Objects.requireNonNull(signature, "signature");
        SpaceWire.SignedClaim attested = new SpaceWire.SignedClaim(claim, holderKey, signature,
                certificate);
        String refusal = claimRefusal(entryId, attested);
        if (refusal != null) {
            throw new IllegalArgumentException(refusal);
        }
        synchronized (lock) {
            claims.merge(entryId, attested, SpaceWire.SignedClaim::merge);
            logDecidedEpochs.merge(entryId, claim.epoch(), Math::max);
        }
    }

    /**
     * Whether a log-committed claim would be accepted by
     * {@link #applyAuthorizedClaim(EntryId, TakeClaim, byte[], byte[], AgentCertificate)},
     * without installing it (issue #16): the ordered-log coordinator asks this
     * for a committed claim whose generation this replica already holds through
     * gossip, so that listeners on the committed order still hear of every
     * authentic claim.
     */
    public boolean verifiesAuthorizedClaim(EntryId entryId, TakeClaim claim, byte[] holderKey,
                                           byte[] signature,
                                           ai.badmonkey.agentspaces.api.security.AgentCertificate certificate) {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(claim, "claim");
        if (holderKey == null || signature == null) {
            return false;
        }
        return claimRefusal(entryId, new SpaceWire.SignedClaim(claim, holderKey, signature,
                certificate)) == null;
    }

    /** The reason a log-committed claim is refused, or null when it is accepted. */
    private String claimRefusal(EntryId entryId, SpaceWire.SignedClaim attested) {
        TakeClaim claim = attested.claim();
        byte[] holderKey = attested.holderKey();
        if (!attested.holderAttested()) {
            return "claim on " + entryId.value() + " is held by agent '"
                    + claim.holder().localName() + "' of " + claim.holder().peer().display()
                    + " but the key offered as its attestation belongs to "
                    + (holderKey.length == Ed25519.RAW_PUBLIC_KEY_LENGTH
                            ? PeerId.fromPublicKey(holderKey).display() : "no valid peer")
                    + "; a completion could never be authenticated against it";
        }
        if (!verifyClaim(entryId, attested)) {
            return "claim on " + entryId.value()
                    + " does not verify under its holder's key, or is bound to another entry";
        }
        if (runtime.revocationView().refuses(claim.holder(), claimKey(attested), null)) {
            // v0.1.13: the log path refuses a revoked holder exactly as gossip does.
            return "claim on " + entryId.value() + " is held by "
                    + claim.holder().encoded() + ", which is revoked in this group";
        }
        return null;
    }

    /**
     * Installs a claim the ordered log committed <em>without</em> the holder's
     * attestation: the stored proof is signed by this node, so it decides local
     * arbitration but cannot authenticate the holder's completion at any other
     * replica. Prefer {@link #applyAuthorizedClaim(EntryId, TakeClaim, byte[], byte[])}
     * whenever the holder's signature is available, which for a log command it
     * always is. Kept for callers that arbitrate locally and never complete.
     *
     * @param entryId the entry
     * @param claim   the ordered claim
     */
    public void applyAuthorizedClaim(EntryId entryId, TakeClaim claim) {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(claim, "claim");
        synchronized (lock) {
            claims.merge(entryId, sign(claim, writer), SpaceWire.SignedClaim::merge);
            logDecidedEpochs.merge(entryId, claim.epoch(), Math::max);
        }
    }

    /**
     * Adopts an entry whose current claim this agent already holds (placed
     * through the ordered-log coordinator), returning a {@link TakenEntry} that
     * {@link #complete(TakenEntry)} accepts.
     *
     * @param template the template naming the entry type
     * @param entryId  the claimed entry
     * @param <T>      the entry type
     * @return the taken entry, or empty when this agent does not hold the claim
     */
    public <T> Optional<TakenEntry<T>> adoptClaim(Template<T> template, EntryId entryId) {
        return adoptClaimAs(writer, template, entryId);
    }

    /**
     * Adopts a log-decided claim as the given holder identity (v0.1.13), so a
     * completion is signed by the agent that holds the claim, as an
     * agent-attested claim's completion must be (SPEC §11a.4).
     *
     * @param template the entry's template
     * @param entryId  the claimed entry
     * @param holder   the identity the claim names as its holder
     * @param <T>      the entry type
     * @return the taken entry, or empty when the claim is not this holder's
     */
    public <T> Optional<TakenEntry<T>> adoptClaim(Template<T> template, EntryId entryId,
                                                  AgentIdentity holder) {
        return adoptClaimAs(Objects.requireNonNull(holder, "holder"), template, entryId);
    }

    private <T> Optional<TakenEntry<T>> adoptClaimAs(AgentIdentity actor, Template<T> template,
                                                     EntryId entryId) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(entryId, "entryId");
        registerSchema(template.type());
        synchronized (lock) {
            TakeClaim claim = claimOf(entryId);
            if (claim == null || !claim.holder().equals(actor.id())
                    || claim.expired(nowMillis())) {
                return Optional.empty();
            }
            for (EntryRecord record : liveRecords()) {
                if (record.entryId().equals(entryId)) {
                    return decodeIfMatches(template, record)
                            .map(value -> new Taken<>(entryId, value, claim.epoch(), actor));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Reverse-maps a value returned by {@code read}/{@code readAll} to its entry
     * id among currently available entries. Exposed for take coordinators.
     *
     * @param value the entry value
     * @return the entry id, when the value corresponds to an available entry
     */
    public Optional<EntryId> entryIdOf(Object value) {
        Objects.requireNonNull(value, "value");
        synchronized (lock) {
            for (EntryRecord record : availableRecords()) {
                if (value.equals(valueCache.get(record.entryId()))) {
                    return Optional.of(record.entryId());
                }
            }
        }
        return Optional.empty();
    }

    /** Returns how many entries this replica knows about, tombstones included. */
    public int knownEntries() {
        synchronized (lock) {
            return crdt.knownEntries();
        }
    }

    // -------------------------------------------------------------- take / claims

    private <T> Optional<TakenEntry<T>> tryTakeOnce(AgentIdentity actor, Template<T> template,
                                                    Lease takeLease) {
        if (actorRevoked(actor)) {
            return Optional.empty(); // v0.1.13: a revoked agent takes nothing, even locally
        }
        AgentId holder = actor.id();
        EntryId claimedId = null;
        TakeClaim myClaim = null;
        T claimedValue = null;
        EntryRecord claimedRecord = null;
        synchronized (lock) {
            long now = nowMillis();
            // Gather matching candidates; under AUCTION, bid on the candidate this
            // agent is cheapest on first (the CBBA task-selection rule).
            record Candidate<V>(EntryRecord entry, V value, double bid) {
            }
            List<Candidate<T>> candidates = new ArrayList<>();
            for (EntryRecord record : liveRecords(schemas.schemaNameOf(template.type()))) {
                Optional<T> value = decodeIfMatches(template, record);
                if (value.isEmpty()) {
                    continue;
                }
                double bid = 0.0;
                if (strategy == ai.badmonkey.agentspaces.api.space.ConflictStrategyType.AUCTION) {
                    if (bidFunction == null) {
                        throw new IllegalStateException(
                                "AUCTION take requires a bidFunction on space '" + name + "'");
                    }
                    bid = bidFunction.applyAsDouble(value.get());
                }
                candidates.add(new Candidate<>(record, value.get(), bid));
            }
            if (strategy == ai.badmonkey.agentspaces.api.space.ConflictStrategyType.AUCTION) {
                candidates.sort(java.util.Comparator.comparingDouble(Candidate::bid));
            }
            for (Candidate<T> candidate : candidates) {
                TakeClaim current = claimOf(candidate.entry().entryId());
                if (current != null && !current.expired(now)) {
                    // Held. Under AUCTION a strictly better bid may still counter-claim
                    // while the incumbent's settle window is open (CBAA-style).
                    boolean canOutbid =
                            strategy == ai.badmonkey.agentspaces.api.space.ConflictStrategyType.AUCTION
                                    && candidate.bid() < current.bid()
                                    && now - current.stamp().physical() <= settleWindow.toMillis();
                    if (!canOutbid) {
                        continue;
                    }
                    myClaim = new TakeClaim(candidate.entry().entryId(), id,
                            current.epoch(), hlc.now(), holder,
                            candidate.bid(), now + takeLease.duration().toMillis());
                } else {
                    long epoch = current == null ? 1 : current.epoch() + 1;
                    myClaim = new TakeClaim(candidate.entry().entryId(), id,
                            epoch, hlc.now(), holder,
                            candidate.bid(), now + takeLease.duration().toMillis());
                }
                claims.merge(candidate.entry().entryId(), sign(myClaim, actor),
                        SpaceWire.SignedClaim::merge);
                claimedId = candidate.entry().entryId();
                claimedValue = candidate.value();
                claimedRecord = candidate.entry();
                break;
            }
        }
        if (claimedId == null) {
            return Optional.empty();
        }
        publishDelta(new SpaceWire.Delta(null, claimedId, sign(myClaim, actor)),
                "c:" + claimedId + ":" + myClaim.stamp().encoded());
        sleep(settleWindow);
        synchronized (lock) {
            TakeClaim winner = claimOf(claimedId);
            boolean won = winner != null && winner.holder().equals(holder)
                    && winner.epoch() == myClaim.epoch() && !winner.expired(nowMillis());
            if (!won) {
                return Optional.empty(); // lost the race; retry elsewhere
            }
        }
        fire(SpaceEvent.Kind.TAKEN, claimedId, claimedValue, claimedRecord.issuer(),
                claimedRecord.issuer(), claimedRecord);
        return Optional.of(new Taken<>(claimedId, claimedValue, myClaim.epoch(), actor));
    }

    private EntryHandle completeInternal(AgentIdentity actor, TakenEntry<?> taken, Object result,
                                         Lease resultLease, Map<String, String> tags) {
        Objects.requireNonNull(taken, "taken");
        if (!(taken instanceof Taken<?> t)) {
            throw new IllegalArgumentException("foreign TakenEntry implementation: " + taken.getClass());
        }
        // Spec §7.5: completion is a mutation the admission rule covers (it
        // needs the TAKE scope, like the claim it finishes); on a read-only
        // space a bare completion of work already held may still land, but its
        // result is a write and is refused like any other.
        requireAdmitted(actor, "complete", SpaceAdmission.Scope.TAKE);
        if (result != null) {
            requireWritable(actor, "complete with result", SpaceAdmission.Scope.WRITE);
        }
        // #2 atomicity: build and validate the result record BEFORE committing
        // the completion, so a result that cannot be written (oversize on a
        // blocks-less space, unserializable, block-store failure) never leaves
        // the task completed-but-resultless and fleet-wide unrecoverable.
        PreparedWrite preparedResult = result == null
                ? null : prepareWrite(actor, result, resultLease, tags);
        SpaceWire.EntryStateDto dto;
        SpaceWire.SignedClaim proof;
        Object completedValue;
        synchronized (lock) {
            TakeClaim claim = claimOf(t.entryId());
            long now = nowMillis();
            boolean holding = claim != null && claim.holder().equals(actor.id())
                    && claim.epoch() == t.epoch && !claim.expired(now);
            EntryState state = crdt.state(t.entryId()).orElse(null);
            if (!holding || state == null || !state.present()) {
                throw new LeaseExpiredException(
                        "take lease lapsed or lost for entry " + t.entryId());
            }
            crdt = crdt.complete(t.entryId());
            completedValue = valueCache.get(t.entryId());
            // The completed state is signed by us, the holder; the receiver
            // authenticates it against our winning claim, which we carry along.
            dto = signedDto(t.entryId(),
                    issuerKeys.getOrDefault(t.entryId(), identity.rawPublicKey()), actor);
            proof = claims.get(t.entryId());
            valueCache.remove(t.entryId()); // #11: stop pinning the completed value
        }
        publishDelta(new SpaceWire.Delta(dto, t.entryId(), proof), "d:" + t.entryId());
        fire(SpaceEvent.Kind.COMPLETED, t.entryId(),
                completedValue == null ? t.entry() : completedValue,
                dto.record().issuer(), actor.id(), dto.record());
        return preparedResult == null ? null : commitWrite(preparedResult);
    }

    // ------------------------------------------------------------ delta handling

    private void onDelta(PeerId from, byte[] payload) {
        SpaceWire.Delta delta;
        try {
            delta = codec.fromBytes(payload, SpaceWire.Delta.class);
        } catch (RuntimeException e) {
            return;
        }
        if (delta == null) {
            return;
        }
        // Merge the claim first: a completion state authenticates against the
        // holder's claim, which must be present before the state is verified.
        if (delta.claim() != null && delta.claimEntry() != null
                && verifyClaim(delta.claimEntry(), delta.claim())) {
            mergeClaim(delta.claimEntry(), delta.claim());
        }
        if (delta.state() != null) {
            mergeState(delta.state());
        }
    }

    /** Most dots a single received state may carry per set (ASF-025): honest
     * entries hold a handful; a validly signed DTO inflated with fabricated
     * dots would otherwise be stored and re-sorted forever. */
    static final int MAX_DOTS_PER_STATE = 1_024;

    private void mergeState(SpaceWire.EntryStateDto dto) {
        if ((dto.adds() != null && dto.adds().size() > MAX_DOTS_PER_STATE)
                || (dto.removes() != null && dto.removes().size() > MAX_DOTS_PER_STATE)) {
            return; // inflated dot set: hostile, drop before verification cost
        }
        if (dto.record() == null || dto.issuerPublicKey() == null
                || !id.equals(dto.record().spaceId()) // a record from another space is not ours (QA4 A4-11)
                || !dotsBoundToIssuer(dto)
                || !verifyRecord(dto.record(), dto.issuerPublicKey(), dto.agentCertificate())) {
            return;
        }
        if (!verifyState(dto)) {
            // Drop any state whose mutable fields are not authenticated by the
            // party authorized for the transition: this is what makes a forged
            // completion, removal, or lease unable to delete a signed entry.
            return;
        }
        // After verifyState: the revocation judgement reads the state's signing
        // time (signedAt, the lease stamp), so those must be authenticated, and
        // only an authentic state may earn a refused-and-acknowledged line.
        if (refusedForRevocation(dto)) {
            return; // ASF-047: authentic, but its actor is revoked in this group
        }
        EntryId entryId = dto.record().entryId();
        if (!admitsState(dto)) {
            return; // spec §7.5: authentic, but its actor is not admitted here
        }
        observeLease(dto.leaseValue().expiresAtMillis() - dto.leaseStamp().physical());
        if (SpaceStateCrdt.collectable(dto.toState(), nowMillis(), maxLeaseMillis())) {
            // Spec §7.3: a tombstone past the GC horizon, re-offered by a lagging
            // replica. Refusing it at the door is what keeps a collected tombstone
            // from bouncing between replicas that sweep at different moments.
            return;
        }
        // Spec §11: remote stamps feed the space clock under the bounded-drift
        // regime (a far-future stamp is clamped, never adopted).
        hlc.update(dto.record().issued());
        hlc.update(dto.leaseStamp());
        if (tagShard != null && !credentialSchema.equals(dto.record().type())
                && !tagShard.test(dto.record().tags())) {
            // Out of shard: acknowledge the state in our digest so anti-entropy
            // partners stop offering it, but never store the record. Credential
            // entries are admission infrastructure and are stored on every shard.
            synchronized (lock) {
                if (!crdt.state(entryId).isPresent()) {
                    shardDropped.put(entryId, stateHash(dto.toState()));
                    return;
                }
            }
        }
        boolean isNew;
        boolean nowCompleted;
        boolean visible;
        synchronized (lock) {
            boolean wasKnown = crdt.state(entryId).isPresent();
            boolean wasCompleted = crdt.state(entryId).map(EntryState::completed).orElse(false);
            crdt = crdt.merge(SpaceStateCrdt.of(entryId, dto.toState()));
            issuerKeys.putIfAbsent(entryId, dto.issuerPublicKey());
            if (dto.agentCertificate() != null) {
                issuerCertificates.putIfAbsent(entryId, dto.agentCertificate());
            }
            recordSignedState(entryId, dto);
            indexCredential(entryId);
            isNew = !wasKnown;
            EntryState merged = crdt.state(entryId).orElseThrow();
            nowCompleted = !wasCompleted && merged.completed();
            // WRITTEN is for an entry a reader could see: present and unexpired.
            // A withdrawn or lapsed entry learned late is history, not news.
            visible = merged.present() && merged.lease().value().expiresAtMillis() > nowMillis();
        }
        if (isNew && visible) {
            Optional<Object> value = decodeValue(dto.record());
            if (value.isPresent()) {
                fire(SpaceEvent.Kind.WRITTEN, entryId, value.get(), dto.record().issuer(),
                        dto.record().issuer(), dto.record());
            } else if (dto.record().payload() == null && dto.record().payloadRef() != null
                    && blocks != null) {
                // A content-addressed entry whose block has not landed: fetch it now
                // and fire the deferred WRITTEN when it does, so a notify subscriber
                // learns of large entries without anyone having to read (a poll was
                // the only way before; the flagships all polled for this reason).
                EntryRecord record = dto.record();
                fetchInBackground(record, () -> decodeValue(record).ifPresent(v ->
                        fire(SpaceEvent.Kind.WRITTEN, entryId, v, record.issuer(),
                                record.issuer(), record)));
            }
        }
        if (nowCompleted) {
            AgentId completer;
            synchronized (lock) {
                SpaceWire.SignedClaim claim = claims.get(entryId);
                completer = claim == null ? dto.record().issuer() : claim.claim().holder();
            }
            decodeValue(dto.record()).ifPresent(value ->
                    fire(SpaceEvent.Kind.COMPLETED, entryId, value, dto.record().issuer(), completer,
                            dto.record()));
            valueCache.remove(entryId); // #11: stop pinning a completed entry's decoded value
        }
    }

    /**
     * Spec §7.5: the admission check for an inbound state, applied to the actor
     * of the transition it carries. A new entry needs its issuer admitted to
     * {@code WRITE}; a completion of a known entry needs the claim holder
     * admitted to {@code TAKE}; a lease renewal (a newer lease register) needs
     * the issuer admitted to {@code WRITE} again; a withdrawal is never a
     * privilege and passes, the state signature having already proved the
     * issuer authored it. Re-judging an entry that merged while its writer was
     * admitted would let a lapsed or revoked credential retroactively delete
     * the writer's work, so a known entry is only re-judged on a privileged
     * transition. A {@link SpaceCredential} record is honoured only from the
     * configured issuer; from anyone else it is dropped and reported, since
     * the signer issued a credential it may not issue. Any other refusal is
     * reported only when the rule says a refusal cannot be a late credential.
     */
    private boolean admitsState(SpaceWire.EntryStateDto dto) {
        EntryRecord record = dto.record();
        if (credentialRule != null && credentialSchema.equals(record.type())
                && !credentialRule.issuer().equals(record.issuer().peer())) {
            runtime.reportMisbehavior(record.issuer().peer(),
                    "space credential signed by a non-issuer for space '" + name + "'");
            return false;
        }
        AgentId actor = record.issuer();
        SpaceAdmission.Scope scope = SpaceAdmission.Scope.WRITE;
        synchronized (lock) {
            EntryState local = crdt.state(record.entryId()).orElse(null);
            if (local != null) {
                if (dto.completed() && !local.completed()) {
                    SpaceWire.SignedClaim claim = claims.get(record.entryId());
                    if (claim == null) {
                        return false; // verifyState guarantees a holder; defensive
                    }
                    actor = claim.claim().holder();
                    scope = SpaceAdmission.Scope.TAKE;
                } else if (dto.completed()
                        || dto.leaseStamp().compareTo(local.lease().stamp()) <= 0) {
                    return true; // already-known state, or a withdrawal: no privilege
                }
                // Otherwise a renewal: the issuer must still be admitted to write.
            }
        }
        if (admits(actor, scope, nowMillis())) {
            return true;
        }
        if (admission.strikesOnRefusal()) {
            // The record (or claim) is authentic, so the refusal is attributable,
            // and the rule rests on local configuration that cannot be late.
            runtime.reportMisbehavior(actor.peer(), (scope == SpaceAdmission.Scope.TAKE
                    ? "completion by unadmitted agent" : "record from unadmitted issuer")
                    + " for space '" + name + "'");
        }
        return false;
    }

    /**
     * Keeps the {@link CredentialIndex} in step with the CRDT for one entry
     * (spec §7.5): a present, unexpired {@link SpaceCredential} is indexed
     * under its current lease; anything else (withdrawn, completed, lapsed,
     * undecodable) is dropped from the index. Call under {@link #lock}; a
     * no-op on replicas that do not admit by credential and for other types.
     */
    private void indexCredential(EntryId entryId) {
        if (credentialRule == null) {
            return;
        }
        EntryState state = crdt.state(entryId).orElse(null);
        if (state == null || !credentialSchema.equals(state.record().type())) {
            return;
        }
        long expiresAt = state.lease().value().expiresAtMillis();
        if (state.present() && expiresAt > nowMillis()) {
            Object value = decodeValue(state.record()).orElse(null);
            if (value instanceof SpaceCredential credential) {
                credentialRule.index().put(entryId, credential, expiresAt);
                return;
            }
        }
        credentialRule.index().remove(entryId);
    }

    /**
     * Largest epoch advance a remote claim may assert over the locally observed
     * epoch (ASF-004). Honest epochs grow by 1 per re-take, so any real history
     * stays far inside the bound; the bound's job is to keep a hostile claim from
     * jumping near {@code Long.MAX_VALUE}, where it would win every merge forever
     * and make the +1 re-take epoch unrepresentable. A generous bound (not the
     * strict +1) is deliberate: a replica that joins late may legitimately first
     * observe an entry at epoch N and must still converge.
     */
    static final long MAX_CLAIM_EPOCH_JUMP = 1L << 20;

    /**
     * Longest TAKE hold a remote claim may assert beyond this replica's clock
     * (ASF-004). Bounds the freeze a hostile claim can cause: however large its
     * epoch, the claim lapses and the entry becomes takeable again.
     */
    static final long MAX_CLAIM_HOLD_MILLIS = Duration.ofHours(24).toMillis();

    /**
     * Whether every dot in a received state names the entry's own writer as its
     * replica (ASF-025). Dots are minted only by the entry's writer (one add
     * per write; a cancellation copies adds into removes), so any other replica
     * string is fabricated — spoofed provenance that would be stored and
     * re-sorted forever.
     */
    private static boolean dotsBoundToIssuer(SpaceWire.EntryStateDto dto) {
        String issuerPeer = dto.record().issuer().peer().value();
        for (Dot dot : dto.adds() == null ? List.<Dot>of() : dto.adds()) {
            if (!issuerPeer.equals(dot.replica())) {
                return false;
            }
        }
        for (Dot dot : dto.removes() == null ? List.<Dot>of() : dto.removes()) {
            if (!issuerPeer.equals(dot.replica())) {
                return false;
            }
        }
        return true;
    }

    /**
     * How far before its entry's issue stamp a claim's stamp may lie (spec
     * §7.4/§11). A claim answers a write, so an honest claimant's HLC has
     * already absorbed the record's stamp and its claim sorts after it; the
     * allowance covers a writer whose clock runs ahead by up to the drift
     * ceiling (whose stamp the claimant's clock clamped). Anchoring on the
     * signed issue stamp rather than on wall time makes the bound identical on
     * every replica and lets anti-entropy re-offer a legitimately old standing
     * claim to a late joiner without penalty.
     */
    static final long MAX_CLAIM_PREDATES_ISSUE_MILLIS = HybridLogicalClock.MAX_DRIFT_MILLIS;

    private void mergeClaim(EntryId entryId, SpaceWire.SignedClaim claim) {
        if (runtime.revocationView().refuses(claim.claim().holder(), claimKey(claim), null)) {
            // ASF-047: a revoked peer takes nothing, however its claim arrives.
            // v0.1.13 (review M-1): nor does a revoked agent or agent key, whatever
            // the claim's stamp, since a renewal keeps it; its take lapses.
            synchronized (lock) {
                refusedAck.put("c:" + entryId.value() + "=" + claimHash(claim.claim()), Boolean.TRUE);
                refusedClaimEntries.add(entryId);
            }
            return;
        }
        // Spec §7.5: a claim needs the TAKE scope. Judged before the lock, since
        // an AUTHORIZER rule may consult a cache; under a rule whose evidence
        // can be late (CREDENTIAL, AUTHORIZER) the claim is dropped silently and
        // anti-entropy re-offers it once the evidence is here.
        boolean admitted = admits(claim.claim().holder(), SpaceAdmission.Scope.TAKE, nowMillis());
        if (!admitted && !admission.strikesOnRefusal()) {
            return;
        }
        String violation = null;
        boolean firstRejection = false;
        synchronized (lock) {
            // ASF-004: bound what a remote claim may assert before it enters the
            // lattice, where higher epochs win by design. Local claims (tryTakeOnce)
            // advance by exactly 1 and are not routed through here.
            TakeClaim offered = claim.claim();
            TakeClaim current = claimOf(entryId);
            long now = nowMillis();
            long observed = current == null ? 0 : current.epoch();
            HlcTimestamp issued = crdt.state(entryId)
                    .map(state -> state.record().issued()).orElse(null);
            if (!admitted) {
                violation = "claim by unadmitted agent"; // spec §7.5, ALLOWLIST strikes
            } else if (offered.epoch() > observed + MAX_CLAIM_EPOCH_JUMP) {
                violation = "claim epoch jump";
            } else if (offered.expiresAtMillis() > now + MAX_CLAIM_HOLD_MILLIS) {
                violation = "claim hold beyond bound";
            } else if (issued != null
                    && offered.stamp().physical() < issued.physical() - MAX_CLAIM_PREDATES_ISSUE_MILLIS) {
                // Spec §7.4/§11: a claim cannot predate the entry it answers.
                violation = "claim stamp predates its entry";
            } else if (current != null && !current.holder().equals(offered.holder())
                    && offered.stamp().physical()
                            < now - HybridLogicalClock.MAX_DRIFT_MILLIS - settleWindow.toMillis()) {
                // Spec §7.4/§11: a claim contesting another holder's claim with a
                // stamp older than the drift ceiling (plus one settle window of
                // race allowance) is a queue-jump: LEASE_RACE picks the lowest
                // stamp, and an honest race is decided within the settle window.
                violation = "far-past claim stamp contesting a held entry";
            }
            Long decided = logDecidedEpochs.get(entryId);
            if (violation == null && decided != null && offered.epoch() <= decided
                    && !offered.equals(current)) {
                // QA4 A4-5: the ordered log already decided this epoch. A different
                // gossip claim at the same or a lower epoch is not a contender;
                // only the identical claim (an attestation upgrade) may merge.
                return;
            }
            if (violation == null) {
                claims.merge(entryId, claim, SpaceWire.SignedClaim::merge);
                hlc.update(offered.stamp()); // spec §11: bounded-drift regime
                observeLease(offered.expiresAtMillis() - offered.stamp().physical());
            } else {
                firstRejection = rejectedClaims.add(claimHash(offered) + "@" + entryId.value());
            }
        }
        if (violation != null && firstRejection) {
            // The hostile claim carries its author's own verified signature
            // (verifyClaim ran first), so this is witnessed, attributable
            // misbehavior — report it toward local quarantine (WS5). Once per
            // distinct claim: an honest lagging replica re-offering the same
            // contested claim every round must not quarantine its holder.
            runtime.reportMisbehavior(claim.claim().holder().peer(),
                    violation + " for entry " + entryId);
        }
    }

    /**
     * ASF-047: whether an authenticated state is refused because the actor of
     * its transition is revoked in this group. The actor is the claim holder
     * for a completion and the record's issuer otherwise, so an honest
     * holder's completion of a revoked peer's entry still lands while the
     * revoked peer can neither write, renew, cancel, nor complete. A refusal
     * is acknowledged in the digest so partners stop re-offering it.
     */
    private boolean refusedForRevocation(SpaceWire.EntryStateDto dto) {
        EntryId entryId = dto.record().entryId();
        boolean refused;
        ai.badmonkey.agentspaces.api.security.RevocationView view = runtime.revocationView();
        synchronized (lock) {
            SpaceWire.SignedClaim claim = claims.get(entryId);
            if (dto.completed()) {
                refused = claim == null
                        ? refusedClaimEntries.contains(entryId)
                        : view.refuses(claim.claim().holder(), claimKey(claim), stateSigningTime(dto, null));
            } else {
                // v0.1.13 freeze rule: the issuer is judged at the state's own
                // signing time: an agent-signed state's signedAt, else the lease
                // stamp of its last write or renewal (the record's issue stamp for
                // an unrenewed write). A late joiner keeps a retired agent's
                // history, and nothing new from it lands, renewals included.
                AgentCertificate recordCertificate = dto.agentCertificate();
                refused = view.refuses(dto.record().issuer(),
                        recordCertificate == null ? null : recordCertificate.agentPublicKey(),
                        stateSigningTime(dto, Instant.ofEpochMilli(dto.leaseStamp().physical())))
                        || (dto.stateCertificate() != null && dto.signer() != null
                                && view.refuses(dto.record().issuer(),
                                        dto.stateCertificate().agentPublicKey(),
                                        stateSigningTime(dto, null)));
            }
            if (refused) {
                refusedAck.put("e:" + entryId.value() + "=" + stateHash(dto.toState()), Boolean.TRUE);
            }
        }
        return refused;
    }

    /** Whether the group has revoked an acting identity (its peer, the agent, or its own key). */
    private boolean actorRevoked(AgentIdentity actor) {
        return runtime.revocationView().revoked(actor.id(), actor.isSubordinate() ? actor.publicKey() : null);
    }

    /** The key a claim was signed with, when its holder signed with an agent key of its own. */
    private static byte[] claimKey(SpaceWire.SignedClaim claim) {
        return claim.holderCertificate() == null ? null : claim.holderCertificate().agentPublicKey();
    }

    /** An agent-signed state's signing time, else the fallback. */
    private static Instant stateSigningTime(SpaceWire.EntryStateDto dto, Instant fallback) {
        return dto.signedAt() == null ? fallback : Instant.ofEpochMilli(dto.signedAt().physical());
    }

    private TakeClaim claimOf(EntryId entryId) {
        SpaceWire.SignedClaim signed = claims.get(entryId);
        return signed == null ? null : signed.claim();
    }

    /**
     * Signs a claim as the acting identity (QA4 A4-7 phase 3). The peer key
     * always rides as {@code holderKey}, so {@code holderAttested()} and the
     * completion branch of {@code verifyState} are unchanged; a subordinate
     * actor signs with its own key and its certificate rides beside the proof.
     */
    private SpaceWire.SignedClaim sign(TakeClaim claim, AgentIdentity actor) {
        byte[] bytes = codec.toBytes(claim);
        return new SpaceWire.SignedClaim(claim, identity.rawPublicKey(), actor.sign(bytes),
                coveringCertificate(actor, claim.stamp()));
    }

    private boolean verifyClaim(EntryId expectedEntry, SpaceWire.SignedClaim signed) {
        if (signed.claim() == null || signed.holderKey() == null || signed.signature() == null
                || signed.holderKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !PeerId.fromPublicKey(signed.holderKey())
                        .equals(signed.claim().holder().peer())) {
            return false;
        }
        // ASF-002: the claim is bound to its entry and space by its signed bytes,
        // so a signed claim cannot be transplanted onto another entry via the
        // unsigned map key it travels under.
        if (!signed.claim().entryId().equals(expectedEntry)
                || !signed.claim().spaceId().equals(id)) {
            return false;
        }
        byte[] bytes = codec.toBytes(signed.claim());
        if (signed.holderCertificate() == null) {
            return Ed25519.verify(Ed25519.publicKeyFromRaw(signed.holderKey()), bytes,
                    signed.signature());
        }
        // The two-key rule of TECH-SPEC §7.2, applied to claims (QA4 A4-7 phase 3):
        // the peer key verifies the certificate for exactly this holder, and the
        // certificate's agent key verifies the claim. A claim carrying a
        // certificate but a peer-key signature is refused, as a record would be.
        // SPEC §4.2 v0.1.13: judged at the claim's own stamp, which a renewal keeps.
        return certificates.verifyAt(signed.holderCertificate(), signed.holderKey(),
                signed.claim().holder(), Instant.ofEpochMilli(signed.claim().stamp().physical()),
                clock.instant())
                && Ed25519.verifyRaw(signed.holderCertificate().agentPublicKey(), bytes,
                        signed.signature());
    }

    /**
     * The stored proof of the current claim on an entry, when one is held: the
     * claim, the holder's peer key, its signature, and — when the holder took
     * through an agent key of its own — the peer's certificate for that key.
     *
     * @param entryId the entry
     * @return the signed claim, or empty
     */
    public Optional<SpaceWire.SignedClaim> claimProof(EntryId entryId) {
        Objects.requireNonNull(entryId, "entryId");
        synchronized (lock) {
            return Optional.ofNullable(claims.get(entryId));
        }
    }

    /**
     * The most advanced actor-signed state this replica holds for an entry, as
     * anti-entropy would forward it: the record, the issuer's peer key (and
     * certificate, under a subordinate identity), the CRDT fields, and the
     * state signature. Empty for an entry this replica does not know.
     *
     * @param entryId the entry
     * @return the signed state
     */
    public Optional<SpaceWire.EntryStateDto> signedState(EntryId entryId) {
        Objects.requireNonNull(entryId, "entryId");
        synchronized (lock) {
            return Optional.ofNullable(signedStates.get(entryId));
        }
    }

    // ------------------------------------------------------------- anti-entropy

    byte[] digest() { // package-private for tests
        sweepNow(); // spec §7.3: collect tombstones before advertising what we hold
        TreeMap<String, String> summary = new TreeMap<>();
        synchronized (lock) {
            for (EntryRecord record : allRecords()) {
                EntryState state = crdt.state(record.entryId()).orElseThrow();
                summary.put("e:" + record.entryId().value(), stateHash(state));
            }
            for (Map.Entry<EntryId, SpaceWire.SignedClaim> e : claims.entrySet()) {
                summary.put("c:" + e.getKey().value(), claimHash(e.getValue().claim()));
            }
            for (Map.Entry<EntryId, String> e : shardDropped.entrySet()) {
                summary.putIfAbsent("e:" + e.getKey().value(), e.getValue());
            }
        }
        StringBuilder sb = new StringBuilder();
        summary.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        synchronized (lock) {
            // ASF-047: refused-and-acknowledged lines, so partners stop re-offering them.
            new TreeSet<>(refusedAck.keySet()).forEach(line ->
                    sb.append("a:").append(line).append('\n'));
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    byte[] deltaFor(byte[] remoteDigest) { // package-private for tests
        sweepNow(); // spec §7.3: never offer a tombstone we would collect ourselves
        Set<String> remote = new HashSet<>(List.of(
                new String(remoteDigest, StandardCharsets.UTF_8).split("\n")));
        List<SpaceWire.EntryStateDto> states = new ArrayList<>();
        Map<String, SpaceWire.SignedClaim> missingClaims = new HashMap<>();
        synchronized (lock) {
            for (EntryRecord record : allRecords()) {
                EntryState state = crdt.state(record.entryId()).orElseThrow();
                String key = "e:" + record.entryId().value() + "=" + stateHash(state);
                if (!remote.contains(key) && !remote.contains("a:" + key)) {
                    // Forward the stored actor signature; a relay cannot re-sign
                    // state it did not author, so an entry we never saw a signed
                    // DTO for (should not happen for authentic entries) is skipped.
                    SpaceWire.EntryStateDto signed = signedStates.get(record.entryId());
                    if (signed != null) {
                        states.add(signed);
                    }
                }
            }
            for (Map.Entry<EntryId, SpaceWire.SignedClaim> e : claims.entrySet()) {
                String key = "c:" + e.getKey().value() + "=" + claimHash(e.getValue().claim());
                if (!remote.contains(key) && !remote.contains("a:" + key)) {
                    missingClaims.put(e.getKey().value(), e.getValue());
                }
            }
        }
        if (states.isEmpty() && missingClaims.isEmpty()) {
            return new byte[0];
        }
        return codec.toBytes(new SpaceWire.SyncDelta(states, missingClaims));
    }

    private void applySyncDelta(byte[] delta) {
        if (delta.length == 0) {
            return;
        }
        SpaceWire.SyncDelta sync;
        try {
            sync = codec.fromBytes(delta, SpaceWire.SyncDelta.class);
        } catch (RuntimeException e) {
            return;
        }
        if (sync == null) {
            return;
        }
        // Claims before states: a completed state authenticates against the
        // holder's claim, so the claim must be merged first.
        if (sync.claims() != null) {
            for (Map.Entry<String, SpaceWire.SignedClaim> e : sync.claims().entrySet()) {
                EntryId claimEntry = EntryId.of(e.getKey());
                if (verifyClaim(claimEntry, e.getValue())) {
                    mergeClaim(claimEntry, e.getValue());
                }
            }
        }
        if (sync.states() != null) {
            sync.states().forEach(this::mergeState);
        }
    }

    // ---------------------------------------------------------------- internals

    private List<EntryRecord> allRecords() {
        List<EntryRecord> records = new ArrayList<>(crdt.liveRecords());
        // liveRecords omits completed entries; digests must include tombstones so
        // completion propagates. Walk known ids through claims and value cache:
        for (EntryId known : knownIds()) {
            if (records.stream().noneMatch(r -> r.entryId().equals(known))) {
                crdt.state(known).map(EntryState::record).ifPresent(records::add);
            }
        }
        return records;
    }

    private Set<EntryId> knownIds() {
        Set<EntryId> ids = new HashSet<>(issuerKeys.keySet());
        ids.addAll(claims.keySet());
        return ids;
    }

    private List<EntryRecord> liveRecords() {
        return liveRecords(null);
    }

    /**
     * Live records, optionally filtered to one schema type (PERF1 phase 2):
     * the type comparison is one string equality, so records of other types
     * cost nothing beyond it, before any claim lookup, list growth, or decode.
     *
     * @param typeName the schema name to keep, or null for every type
     */
    private List<EntryRecord> liveRecords(String typeName) {
        List<EntryRecord> live = new ArrayList<>();
        for (EntryRecord record : crdt.liveRecords()) {
            if (typeName != null && !typeName.equals(record.type())) {
                continue;
            }
            LeaseInfo lease = record.lease();
            if (lease.kind() == LeaseKind.WRITE && lease.expired(clock.instant())) {
                continue;
            }
            live.add(record);
        }
        return live;
    }

    /** Live entries with no active claim: what read and readAll may see. */
    private List<EntryRecord> availableRecords() {
        return availableRecords(null);
    }

    /** Available entries of one schema type, or of every type when null. */
    private List<EntryRecord> availableRecords(String typeName) {
        long now = nowMillis();
        List<EntryRecord> available = new ArrayList<>();
        for (EntryRecord record : liveRecords(typeName)) {
            TakeClaim claim = claimOf(record.entryId());
            if (claim == null || claim.expired(now)) {
                available.add(record);
            }
        }
        return available;
    }

    private <T> Optional<T> decodeIfMatches(Template<T> template, EntryRecord record) {
        if (!record.type().equals(schemas.schemaNameOf(template.type()))) {
            return Optional.empty();
        }
        // Issue #16 §9.2 matching order: type, tags, then decode and fields. The
        // tags sit on the record, so a tagged template never decodes an entry it
        // would not select.
        if (!template.matchesTags(record.tags())) {
            return Optional.empty();
        }
        Object value = valueCache.get(record.entryId());
        if (value == null) {
            byte[] payload = record.payload();
            if (payload == null && record.payloadRef() != null && blocks != null) {
                // Spec §7.2 / P7: reads never block on the network. Serve the
                // block when it is already here; otherwise start one background
                // fetch and report the entry as not yet visible. A blocking read
                // polls within its own timeout and sees the entry once the block
                // lands; readAll simply skips it this time.
                payload = blocks.local(record.payloadRef()).orElse(null);
                if (payload == null) {
                    fetchInBackground(record);
                }
            }
            if (payload == null) {
                return Optional.empty(); // block not fetched yet; invisible for now
            }
            if (keyRing != null) {
                payload = open(record, payload);
                if (payload == null) {
                    return Optional.empty(); // sealed under a key this replica lacks
                }
            }
            try {
                value = codec.fromBytes(payload, template.type());
            } catch (RuntimeException e) {
                return Optional.empty(); // ciphertext without the key, or foreign bytes
            }
            if (value == null) {
                // Foreign bytes that happen to decode as CBOR null (a sealed
                // payload whose nonce starts with 0xF6, once in 256 writes):
                // the same verdict as a decode failure, and never cached.
                return Optional.empty();
            }
            valueCache.put(record.entryId(), value);
        }
        if (!template.matches(value)) {
            return Optional.empty();
        }
        return Optional.of(template.type().cast(value));
    }

    private Optional<Object> decodeValue(EntryRecord record) {
        Object cached = valueCache.get(record.entryId());
        if (cached != null) {
            return Optional.of(cached);
        }
        byte[] payload = record.payload();
        if (payload == null && record.payloadRef() != null && blocks != null) {
            // A block-stored entry: fire its event once the block is available
            // locally. Consulting the local store only (no network) keeps this
            // event path non-blocking; the block usually arrives moments after
            // the record, and a later merge/read fires the deferred event.
            // Without this, notify() subscribers never saw large entries at all.
            payload = blocks.local(record.payloadRef()).orElse(null);
        }
        if (payload == null) {
            return Optional.empty();
        }
        if (keyRing != null) {
            payload = open(record, payload);
            if (payload == null) {
                return Optional.empty();
            }
        }
        byte[] plain = payload;
        return schemas.classFor(record.type()).flatMap(type -> {
            Object value;
            try {
                value = codec.fromBytes(plain, type);
            } catch (RuntimeException e) {
                return Optional.empty(); // ciphertext without the key, or foreign bytes
            }
            if (value == null) {
                return Optional.empty(); // foreign bytes decoding as CBOR null (see decodeIfMatches)
            }
            valueCache.put(record.entryId(), value);
            return Optional.of(value);
        });
    }

    /**
     * Starts one background fetch of a content-addressed payload, at most one
     * per CID at a time. The block exchange stores a verified delivery locally
     * before completing, so the next poll finds it through {@code local()}.
     */
    private void fetchInBackground(EntryRecord record) {
        fetchInBackground(record, () -> { });
    }

    /**
     * As {@link #fetchInBackground(EntryRecord)}, running {@code onArrival} once
     * the block is stored locally (not on a failed or duplicate fetch). Used to
     * fire the WRITTEN event a merge had to defer.
     */
    private void fetchInBackground(EntryRecord record, Runnable onArrival) {
        String cid = record.payloadRef();
        if (!fetching.add(cid)) {
            return; // a fetch for this block is already in flight
        }
        List<PeerId> candidates = new ArrayList<>();
        candidates.add(record.issuer().peer());
        candidates.addAll(runtime.sampler().randomMembers(2));
        Thread.ofVirtual().name("space-block-fetch-" + name).start(() -> {
            boolean landed = false;
            try {
                landed = blocks.fetch(cid, candidates, BLOCK_FETCH_TIMEOUT_MILLIS).isPresent();
            } catch (RuntimeException e) {
                // A failed fetch is retried by the next read that misses the block.
            } finally {
                fetching.remove(cid);
            }
            if (landed) {
                onArrival.run();
            }
        });
    }

    /**
     * Records a lease duration this replica has observed for the space, for
     * the tombstone GC horizon (spec §7.3). Capped at
     * {@link #MAX_TRACKED_LEASE_MILLIS}; non-positive values are ignored.
     */
    private void observeLease(long leaseMillis) {
        long capped = Math.min(leaseMillis, MAX_TRACKED_LEASE_MILLIS);
        if (capped > maxLeaseObservedMillis) {
            synchronized (lock) {
                if (capped > maxLeaseObservedMillis) {
                    maxLeaseObservedMillis = capped;
                }
            }
        }
    }

    /**
     * The lease sweep (spec §7.2, §7.3). Callers hold {@link #lock}; the
     * returned event deliveries run after it is released.
     *
     * <p>Events fire once per transition: {@code EXPIRED} once per entry whose
     * write lease has lapsed, {@code REAPPEARED} once per lapsed claim epoch,
     * which is the same per-entry dedup the WRITTEN and COMPLETED paths use.
     * Then tombstones (completed, withdrawn, or lapsed entries whose lease
     * expired more than twice the maximum lease ago) are collected, along with
     * every side table keyed by their id, so digests stop advertising them.
     */
    private List<Runnable> sweepLocked() {
        List<Runnable> events = new ArrayList<>();
        long now = nowMillis();
        for (EntryRecord record : crdt.liveRecords()) {
            EntryId entryId = record.entryId();
            if (record.lease().expiresAtMillis() <= now) {
                if (expiredFired.add(entryId)) {
                    deferredEvent(events, SpaceEvent.Kind.EXPIRED, record);
                }
                continue;
            }
            TakeClaim claim = claimOf(entryId);
            if (claim != null && claim.expired(now)
                    && !Long.valueOf(claim.epoch()).equals(reappearedEpochs.get(entryId))) {
                reappearedEpochs.put(entryId, claim.epoch());
                deferredEvent(events, SpaceEvent.Kind.REAPPEARED, record);
            }
        }
        if (credentialRule != null) {
            credentialRule.index().expire(now); // spec §7.5: a lapsed credential stops admitting
        }
        Set<EntryId> collected = crdt.collectable(now, maxLeaseMillis());
        if (!collected.isEmpty()) {
            crdt = crdt.without(collected);
            for (EntryId entryId : collected) {
                if (credentialRule != null) {
                    credentialRule.index().remove(entryId);
                }
                issuerKeys.remove(entryId);
                issuerCertificates.remove(entryId);
                logDecidedEpochs.remove(entryId);
                claims.remove(entryId);
                signedStates.remove(entryId);
                valueCache.remove(entryId);
                shardDropped.remove(entryId);
                expiredFired.remove(entryId);
                reappearedEpochs.remove(entryId);
            }
        }
        return events;
    }

    /** Queues an event for delivery outside the lock; silent when the value cannot be decoded here. */
    private void deferredEvent(List<Runnable> events, SpaceEvent.Kind kind, EntryRecord record) {
        events.add(() -> decodeValue(record).ifPresent(value ->
                fire(kind, record.entryId(), value, record.issuer(), record.issuer(), record)));
    }

    /** Binds a ciphertext to its entry within this space (and so this group). */
    private byte[] aadFor(EntryId entryId, long epoch) {
        // Epoch 0 keeps the v0.1.12 binding; a later epoch is bound as well, so
        // a ciphertext cannot be relabelled with another epoch (SPEC §11a.1).
        String bound = epoch == 0 ? id.value() + "|" + entryId.value()
                : id.value() + "|" + entryId.value() + "|" + epoch;
        return bound.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Opens a sealed payload with the keys held for the epoch its record names,
     * or returns null (sealed under a key this replica lacks; the ring reports
     * the epoch missing so the content-key agent can fetch it).
     */
    private byte[] open(EntryRecord record, byte[] sealed) {
        long epoch = record.sealedEpoch();
        byte[] aad = aadFor(record.entryId(), epoch);
        List<ai.badmonkey.agentspaces.common.crypto.GroupKey> keys = keyRing.openingKeys(epoch);
        for (ai.badmonkey.agentspaces.common.crypto.GroupKey key : keys) {
            Optional<byte[]> opened = key.decrypt(sealed, aad);
            if (opened.isPresent()) {
                return opened.get();
            }
        }
        if (!keys.isEmpty()) {
            // Keys are held for the epoch but none opens it: another rotator's
            // key for the same epoch exists (SPEC §11a.3); ask for it again.
            keyRing.reportUnopenable(epoch);
        }
        return null;
    }

    /**
     * The group's revocations as this space enforces them (SPEC §6.1, v0.1.13),
     * for capabilities built over the space that must apply the same view.
     *
     * @return the revocation view
     */
    public ai.badmonkey.agentspaces.api.security.RevocationView revocationView() {
        return runtime.revocationView();
    }

    /**
     * The revocation view behind a space handle: a replicated space's, or the
     * one its per-agent view ({@link #as}) shares; {@code NONE} otherwise.
     *
     * @param space a space handle
     * @return the view it enforces
     */
    public static ai.badmonkey.agentspaces.api.security.RevocationView revocationViewOf(Space space) {
        if (space instanceof ReplicatedSpace replicated) {
            return replicated.revocationView();
        }
        if (space instanceof View view) {
            return view.owner().revocationView();
        }
        return ai.badmonkey.agentspaces.api.security.RevocationView.NONE;
    }

    /**
     * This space's content-key ring (SPEC §11a.3), when the space is encrypted.
     *
     * @return the ring
     */
    public Optional<ai.badmonkey.agentspaces.common.crypto.GroupKeyRing> keyRing() {
        return Optional.ofNullable(keyRing);
    }

    /**
     * The certificate a subordinate actor attaches to a signature stamped
     * {@code stamp} (SPEC §4.2, v0.1.13), or null for a peer-signed actor. An
     * actor holding no certificate that covers the stamp cannot sign: the
     * signature would be refused everywhere, so the write fails here, loudly.
     */
    private static AgentCertificate coveringCertificate(AgentIdentity actor, HlcTimestamp stamp) {
        if (!actor.isSubordinate()) {
            return null;
        }
        Instant signingTime = Instant.ofEpochMilli(stamp.physical());
        return actor.certificateCovering(signingTime).orElseThrow(() ->
                new IllegalStateException("agent " + actor.id().encoded()
                        + " holds no certificate covering " + signingTime
                        + "; re-issue it (a renewing identity does so itself)"));
    }

    private EntryRecord withSignature(EntryRecord unsigned, AgentIdentity actor) {
        byte[] canonical = signableBytes(unsigned);
        return new EntryRecord(unsigned.entryId(), unsigned.spaceId(), unsigned.type(),
                unsigned.payload(), unsigned.payloadRef(), unsigned.issuer(), unsigned.issued(),
                unsigned.lease(), unsigned.tags(), actor.sign(canonical), unsigned.keyEpoch());
    }

    private boolean verifyRecord(EntryRecord record, byte[] issuerKey) {
        return verifyRecord(record, issuerKey, null);
    }

    /**
     * Verifies a record's signature (TECH-SPEC §7.2, QA4 A4-7). The offered key
     * must always hash to the record's issuing peer. With no certificate the
     * record must verify under that peer key, as it always has. With one, the
     * certificate must verify under the peer key, name exactly this issuer, and
     * be unexpired, and the record must then verify under the certificate's agent
     * key: the agent, not merely its peer, signed this.
     */
    private boolean verifyRecord(EntryRecord record, byte[] issuerKey, AgentCertificate certificate) {
        if (record.sig() == null || issuerKey == null
                || issuerKey.length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !PeerId.fromPublicKey(issuerKey).equals(record.issuer().peer())) {
            return false;
        }
        if (certificate == null) {
            return Ed25519.verify(Ed25519.publicKeyFromRaw(issuerKey),
                    signableBytes(record), record.sig());
        }
        // SPEC §4.2 v0.1.13: judged at the record's issue stamp, not at receipt,
        // so an agent's history stays mergeable after its certificate lapses.
        return certificates.verifyAt(certificate, issuerKey, record.issuer(),
                Instant.ofEpochMilli(record.issued().physical()), clock.instant())
                && Ed25519.verifyRaw(certificate.agentPublicKey(), signableBytes(record), record.sig());
    }

    /** The signature covers the record's immutable identity; leases change freely. */
    private byte[] signableBytes(EntryRecord record) {
        return codec.toBytes(new SignView(record.entryId(), record.spaceId(), record.type(),
                record.payload(), record.payloadRef(), record.issuer(),
                record.issued(), record.tags(), record.keyEpoch()));
    }

    private record SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                            String payloadRef, AgentId issuer, HlcTimestamp issued,
                            Map<String, String> tags,
                            // v0.1.13, omitted when null: epoch-0 and unencrypted records
                            // sign exactly the v0.1.12 bytes (golden sign_view_cbor)
                            @com.fasterxml.jackson.annotation.JsonInclude(
                                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                            Long keyEpoch) {
    }

    /**
     * Builds a signed state DTO for the entry: {@link SpaceWire.EntryStateDto}
     * plus a signature by this node over the mutable state. The local node is
     * the authorized actor for every state it publishes (the issuer for its own
     * writes/renewals/cancels, the take-claim holder for its completions), so it
     * always signs with its own key.
     */
    /**
     * Signs the entry's current state as {@code actor} (SPEC §11a.4, v0.1.13).
     * A subordinate actor signs with its own key: the DTO names it as signer,
     * stamps the signing time, and carries the certificate covering that time.
     * A peer-signed actor signs with the peer key, exactly as before, and the
     * DTO is byte-identical to the v0.1.12 form.
     */
    private SpaceWire.EntryStateDto signedDto(EntryId entryId, byte[] issuerKey, AgentIdentity actor) {
        SpaceWire.EntryStateDto dto = SpaceWire.EntryStateDto.from(
                crdt.state(entryId).orElseThrow(), issuerKey, issuerCertificates.get(entryId));
        if (actor.isSubordinate()) {
            HlcTimestamp signedAt = hlc.now();
            dto = dto.withAgentSigner(actor.id().encoded(), signedAt,
                    coveringCertificate(actor, signedAt));
            dto = dto.withStateSig(actor.sign(stateSignBytes(dto)));
        } else {
            dto = dto.withStateSig(identity.sign(stateSignBytes(dto)));
        }
        recordSignedState(entryId, dto);
        return dto;
    }

    /**
     * SPEC §11a.4 v0.1.13: only the agent that wrote an agent-attested entry
     * renews or cancels it, so a sibling agent on the same peer cannot act on
     * another agent's entry (its signed state would be refused everywhere).
     */
    private void requireIssuerActs(EntryId entryId, EntryRecord record, AgentIdentity actor) {
        if (issuerCertificates.containsKey(entryId) && !actor.id().equals(record.issuer())) {
            throw new IllegalStateException(actor.id().encoded() + " cannot act on "
                    + record.issuer().encoded() + "'s agent-attested entry " + entryId);
        }
    }

    /**
     * Remembers the most advanced actor-signed state DTO seen for an entry, so
     * anti-entropy can forward it verbatim. Call under {@link #lock}.
     */
    private void recordSignedState(EntryId entryId, SpaceWire.EntryStateDto dto) {
        SpaceWire.EntryStateDto current = signedStates.get(entryId);
        if (current == null || advances(dto, current)) {
            signedStates.put(entryId, dto);
        }
    }

    /** Whether {@code a} represents a strictly more advanced state than {@code b}. */
    private static boolean advances(SpaceWire.EntryStateDto a, SpaceWire.EntryStateDto b) {
        if (a.completed() != b.completed()) {
            return a.completed(); // completion is terminal and dominates
        }
        if (a.removes().size() != b.removes().size()) {
            return a.removes().size() > b.removes().size();
        }
        int stampCmp = a.leaseStamp().compareTo(b.leaseStamp());
        if (stampCmp != 0) {
            return stampCmp > 0; // the newer lease register
        }
        return a.adds().size() >= b.adds().size();
    }

    /**
     * Verifies that a received state DTO's mutable fields were signed by the
     * party authorized for the transition: a completed state by the holder of a
     * valid take claim on the entry, any other state by the record's issuer.
     * This is what stops an admitted member from forging a completion, a
     * removal, or a lease that would make a signed entry vanish.
     */
    private boolean verifyState(SpaceWire.EntryStateDto dto) {
        if (dto.stateSig() == null) {
            return false;
        }
        // The party authorized for this transition (SPEC §11a.4): the claim
        // holder for a completion, the record's issuer otherwise; with that
        // party's peer key and whether that party is agent-attested.
        byte[] signerKey;
        AgentId party;
        boolean partyAttested;
        if (dto.completed()) {
            EntryId entryId = dto.record().entryId();
            SpaceWire.SignedClaim claim;
            synchronized (lock) {
                claim = claims.get(entryId);
            }
            if (claim == null || !verifyClaim(entryId, claim)) {
                return false; // no authenticated holder to attribute the completion to
            }
            signerKey = claim.holderKey();
            party = claim.claim().holder();
            partyAttested = claim.holderCertificate() != null;
        } else {
            signerKey = dto.issuerPublicKey();
            party = dto.record().issuer();
            partyAttested = dto.agentCertificate() != null;
        }
        if (signerKey == null || signerKey.length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return false;
        }
        if (dto.agentSigned()) {
            // v0.1.13 two-key rule for transitions: the certificate verifies under
            // the party's peer key for exactly that party and covers the signing
            // time, and the state verifies under the certificate's agent key.
            return dto.signedAt() != null && dto.stateCertificate() != null
                    && party.encoded().equals(dto.signer())
                    && certificates.verifyAt(dto.stateCertificate(), signerKey, party,
                            Instant.ofEpochMilli(dto.signedAt().physical()), clock.instant())
                    && Ed25519.verifyRaw(dto.stateCertificate().agentPublicKey(),
                            stateSignBytes(dto), dto.stateSig());
        }
        if (partyAttested) {
            // A6: an agent-attested party's transitions must be agent-signed, so
            // its peer (or a sibling agent on it) cannot act in its name.
            return false;
        }
        return Ed25519.verify(Ed25519.publicKeyFromRaw(signerKey),
                stateSignBytes(dto), dto.stateSig());
    }

    /** The bytes an actor signs to authenticate a state transition. */
    private byte[] stateSignBytes(SpaceWire.EntryStateDto dto) {
        return codec.toBytes(new StateSignView(dto.record().spaceId(),
                dto.record().entryId(), sortedDots(dto.adds()), sortedDots(dto.removes()),
                dto.leaseStamp(), dto.leaseValue(), dto.completed(), dto.signer(), dto.signedAt()));
    }

    private static List<Dot> sortedDots(List<Dot> dots) {
        List<Dot> sorted = new ArrayList<>(dots);
        sorted.sort(java.util.Comparator.comparing(Dot::replica)
                .thenComparingLong(Dot::counter));
        return sorted;
    }

    private record StateSignView(SpaceId spaceId, EntryId entryId, List<Dot> adds,
                                 List<Dot> removes, HlcTimestamp leaseStamp,
                                 LeaseInfo leaseValue, boolean completed,
                                 // v0.1.13, omitted when null so a peer-signed state's
                                 // signed bytes are unchanged (golden state_sign_view_cbor)
                                 @com.fasterxml.jackson.annotation.JsonInclude(
                                         com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                                 String signer,
                                 @com.fasterxml.jackson.annotation.JsonInclude(
                                         com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                                 HlcTimestamp signedAt) {
    }

    private void publishDelta(SpaceWire.Delta delta, String itemId) {
        runtime.gossip().publish(streamId, itemId, codec.toBytes(delta));
    }

    /**
     * Delivers an event to the subscriptions whose template accepts the entry's
     * type, tags and fields (issue #16 §9.2). Every caller holds the record the
     * event is about, so the event carries the {@link EntryView} over it; the
     * attestation is read under the lock, as {@code readAllEntries} reads it.
     * Callers do not hold the lock.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void fire(SpaceEvent.Kind kind, EntryId entryId, Object value, AgentId writer,
                      AgentId actor, EntryRecord record) {
        long now = nowMillis();
        subscriptions.removeIf(sub -> sub.expiryMillis <= now || sub.closed);
        if (value == null) {
            return;
        }
        EntryView<Object> details = null;
        for (Sub sub : subscriptions) {
            if (!sub.closed && sub.template.matchesTags(record.tags())
                    && sub.template.matches(value)) {
                if (details == null) {
                    boolean attested;
                    synchronized (lock) {
                        attested = issuerCertificates.containsKey(entryId);
                    }
                    details = EntryView.of(record, value, attested
                            ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED);
                }
                sub.listener.onEvent(new SpaceEvent(kind, entryId, value, writer, actor, details));
            }
        }
    }

    private static String stateHash(EntryState state) {
        return Integer.toHexString(Objects.hash(state.adds(), state.removes(),
                state.lease().stamp(), state.completed()));
    }

    private static String claimHash(TakeClaim claim) {
        return claim.epoch() + ":" + claim.stamp().encoded() + ":" + claim.bid()
                + ":" + claim.expiresAtMillis();
    }

    private long nowMillis() {
        return clock.instant().toEpochMilli();
    }

    private static void sleep(Duration duration) {
        if (duration.isZero()) {
            return;
        }
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private <V> Optional<V> awaiting(Duration timeout, java.util.function.Supplier<Optional<V>> poll) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Optional<V> result = poll.get();
            if (result.isPresent() || System.nanoTime() >= deadline) {
                return result;
            }
            sleep(Duration.ofMillis(20));
        }
    }

    private final class Handle implements EntryHandle {
        private final EntryId entryId;
        private final AgentIdentity actor;

        private Handle(EntryId entryId, AgentIdentity actor) {
            this.entryId = entryId;
            this.actor = actor;
        }

        @Override
        public EntryId entryId() {
            return entryId;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            SpaceWire.EntryStateDto dto;
            synchronized (lock) {
                EntryState state = crdt.state(entryId).orElse(null);
                if (state == null || !state.present()
                        || state.lease().value().expired(clock.instant())) {
                    throw new LeaseExpiredException("write lease lapsed for entry " + entryId);
                }
                requireIssuerActs(entryId, state.record(), actor);
                LeaseInfo renewed = new LeaseInfo(actor.id(),
                        nowMillis() + extension.toMillis(), LeaseKind.WRITE);
                crdt = crdt.setLease(entryId, new LwwRegister<>(hlc.now(), renewed));
                indexCredential(entryId);
                dto = signedDto(entryId,
                        issuerKeys.getOrDefault(entryId, identity.rawPublicKey()), actor);
            }
            publishDelta(new SpaceWire.Delta(dto, null, null), "r:" + entryId + ":" + nowMillis());
        }

        @Override
        public void cancel() {
            SpaceWire.EntryStateDto dto = null;
            synchronized (lock) {
                if (crdt.state(entryId).isPresent()) {
                    requireIssuerActs(entryId, crdt.state(entryId).get().record(), actor);
                    crdt = crdt.remove(entryId);
                    indexCredential(entryId);
                    dto = signedDto(entryId,
                            issuerKeys.getOrDefault(entryId, identity.rawPublicKey()), actor);
                }
            }
            if (dto != null) {
                publishDelta(new SpaceWire.Delta(dto, null, null), "x:" + entryId);
            }
        }
    }

    private final class Taken<T> implements TakenEntry<T> {
        private final EntryId entryId;
        private final T value;
        private final long epoch;
        private final AgentIdentity actor;

        private Taken(EntryId entryId, T value, long epoch, AgentIdentity actor) {
            this.entryId = entryId;
            this.value = value;
            this.epoch = epoch;
            this.actor = actor;
        }

        @Override
        public T entry() {
            return value;
        }

        @Override
        public EntryId entryId() {
            return entryId;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            TakeClaim renewed;
            synchronized (lock) {
                TakeClaim current = claimOf(entryId);
                if (current == null || !current.holder().equals(actor.id())
                        || current.epoch() != epoch || current.expired(nowMillis())) {
                    throw new LeaseExpiredException("take lease lapsed for entry " + entryId);
                }
                if (actorRevoked(actor)) {
                    // v0.1.13 (M-1): every receiver refuses the renewal; let the take lapse.
                    throw new LeaseExpiredException("take lease of entry " + entryId + " cannot be renewed: "
                            + actor.id().encoded() + " is revoked in this group");
                }
                renewed = new TakeClaim(current.entryId(), current.spaceId(),
                        current.epoch(), current.stamp(), actor.id(),
                        current.bid(), nowMillis() + extension.toMillis());
                claims.merge(entryId, sign(renewed, actor), SpaceWire.SignedClaim::merge);
            }
            publishDelta(new SpaceWire.Delta(null, entryId, sign(renewed, actor)),
                    "c:" + entryId + ":renew:" + nowMillis());
        }
    }

    private final class Sub implements Subscription {
        private final Template<?> template;
        @SuppressWarnings("rawtypes")
        private final SpaceListener listener;
        private volatile long expiryMillis;
        private volatile boolean closed;

        private Sub(Template<?> template, SpaceListener<?> listener, long expiryMillis) {
            this.template = template;
            this.listener = listener;
            this.expiryMillis = expiryMillis;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            if (closed || expiryMillis <= nowMillis()) {
                throw new LeaseExpiredException("subscription lease already lapsed");
            }
            expiryMillis = nowMillis() + extension.toMillis();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
