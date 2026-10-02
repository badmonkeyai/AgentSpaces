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
package ai.badmonkey.agentspaces.capabilities.keywrap;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap;
import ai.badmonkey.agentspaces.common.crypto.X25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;

import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * The {@code aspace:cap/key-wrap} capability (SPEC §11a.2, §11a.3): sealed
 * per-member distribution of a group's content keys, by epoch. A holder serves
 * the keys of its {@link GroupKeyRing} to authorized members; each request
 * carries the requester's X25519 public key inside a signed envelope, so the
 * holder knows cryptographically who is asking, applies its authorization
 * predicate to that verified identity, and answers with each key sealed under
 * {@link GroupKeyWrap} for that exchange alone (ASF-046). Relays carry the
 * response without being able to read it.
 *
 * <p>Rotation (v0.1.13). An authorized rotator — the group's founder, or a peer
 * granted {@code KEY_ROTATE} — mints the next epoch with {@link #rotate} and
 * signs a commitment to it: {@code (group, epoch, cutover, sha256(key))}. The
 * commitment travels with the key wherever it is served, so a member installs
 * an epoch key only when an authorized rotator vouched for exactly that key,
 * whichever holder relayed it. Holders advertise the epochs they hold; followers
 * ({@link #follow}) read those advertisements on the capability tick, announce
 * cutovers to their ring only from authorized rotators, and fetch epochs they
 * lack from a holder that lists them, as they do whenever their ring reports an
 * epoch missing. Fetches run off the delivery and tick threads.
 *
 * <p>Authorization stays a policy the deployment supplies (P6).
 */
public final class GroupKeyDistributor implements CapabilityProvider {

    private static final System.Logger LOG = System.getLogger(GroupKeyDistributor.class.getName());

    /** The capability type URI. */
    public static final String TYPE = "aspace:cap/key-wrap";

    private record KeyRequest(long nonce, byte[] encryptionPublicKey,
                              @com.fasterxml.jackson.annotation.JsonInclude(
                                      com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                              List<Long> epochs,
                              @com.fasterxml.jackson.annotation.JsonInclude(
                                      com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                              AgentRequest agent) {
    }

    /**
     * An agent asking for itself (SPEC §11a.2, v0.1.13): its AgentID, the
     * certificate that certifies its X25519 key, and its peer's Ed25519 key,
     * which must hash to the authenticated sender so the certificate can be
     * checked against it.
     */
    private record AgentRequest(String agent,
                                ai.badmonkey.agentspaces.api.security.AgentCertificate certificate,
                                byte[] peerKey) {
    }

    private record KeyResponse(long nonce, boolean granted,
                               byte[] ephemeralPublicKey, byte[] sealed,
                               @com.fasterxml.jackson.annotation.JsonInclude(
                                       com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                               List<EpochKey> keys) {
    }

    /** One epoch key in a response: who minted it, its cutover, the minter's proof, the wrap. */
    private record EpochKey(long epoch, String rotator, String cutover,
                            @com.fasterxml.jackson.annotation.JsonInclude(
                                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                            byte[] proof,
                            byte[] ephemeralPublicKey, byte[] sealed) {
    }

    /** A rotator's proof: its raw Ed25519 key and its signature over the {@link EpochCommitment}. */
    record EpochProof(byte[] rotatorKey, byte[] signature) {
    }

    /** What a rotator signs to vouch for an epoch key. */
    record EpochCommitment(String group, long epoch, String cutover, byte[] keyDigest) {
    }

    /**
     * What one wrap is bound to (SPEC §11a.2, v2): the group, the holder that
     * sealed it, the requester it was sealed for, the request's nonce, and — for
     * an epoch key — the epoch and its rotator. Its canonical CBOR is digested
     * into the wrap, so a wrap made for any other exchange or epoch does not open.
     */
    record WrapBinding(String group, String holder, String requester, long nonce,
                       @com.fasterxml.jackson.annotation.JsonInclude(
                               com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                       Long epoch,
                       @com.fasterxml.jackson.annotation.JsonInclude(
                               com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                       String rotator,
                       @com.fasterxml.jackson.annotation.JsonInclude(
                               com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                       String agent) {
    }

    /** A request in flight: the holder it was sent to, and its decoded answer. */
    private record Pending(PeerId holder, CompletableFuture<KeyResponse> answer) {
    }

    private final CapabilityPipes pipes;
    private final PeerId self;
    private final CborCodec codec;
    private final InstantSource clock;
    private final KeyPair encryptionKeys;
    private final byte[] encryptionPublicRaw;
    private final SecureRandom nonces = new SecureRandom();
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    private final Set<Long> fetching = ConcurrentHashMap.newKeySet();
    private volatile GroupKeyRing ring;
    private volatile Predicate<PeerId> allowed;
    private volatile Predicate<PeerId> rotators;
    private volatile Predicate<ai.badmonkey.agentspaces.common.id.AgentId> agentsAllowed;
    private volatile Predicate<ai.badmonkey.agentspaces.common.id.AgentId> refusedAgents = agent -> false;
    private final ai.badmonkey.agentspaces.identity.AgentCertificates certificates =
            new ai.badmonkey.agentspaces.identity.AgentCertificates();
    private volatile DiscoveryService discovery;
    private volatile AutoRotation autoRotation;

    /** A rotator's schedule (review M-10): rotate every {@code every}, cutting over after {@code delay}. */
    private record AutoRotation(PeerIdentity rotator, Duration every, Duration delay, Instant since) {
    }

    /**
     * Creates the capability with a fresh X25519 decryption keypair. Every
     * instance can request; {@link #serve} additionally makes it a holder.
     *
     * @param pipes the group's capability pipes
     * @param self  the local peer id
     * @param codec the CBOR codec
     * @param clock the time source
     */
    public GroupKeyDistributor(CapabilityPipes pipes, PeerId self,
                               CborCodec codec, InstantSource clock) {
        this(pipes, self, codec, clock, X25519.generate());
    }

    /**
     * Creates the capability over a persisted X25519 keypair (the peer
     * keystore's), so keys sealed to this peer stay openable across restarts.
     *
     * @param pipes          the group's capability pipes
     * @param self           the local peer id
     * @param codec          the CBOR codec
     * @param clock          the time source
     * @param encryptionKeys this peer's X25519 keypair
     */
    public GroupKeyDistributor(CapabilityPipes pipes, PeerId self, CborCodec codec,
                               InstantSource clock, KeyPair encryptionKeys) {
        this.pipes = Objects.requireNonNull(pipes, "pipes");
        this.self = Objects.requireNonNull(self, "self");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.encryptionKeys = Objects.requireNonNull(encryptionKeys, "encryptionKeys");
        this.encryptionPublicRaw = X25519.rawPublicKey(encryptionKeys.getPublic());
        PeerId founder = pipes.founder();
        this.rotators = founder::equals;
        pipes.onCapability(TYPE, this::onFrame);
    }

    @Override
    public String capabilityType() {
        return TYPE;
    }

    @Override
    public CapabilityAdvertisement describe(GroupId group) {
        Map<String, String> parameters = new LinkedHashMap<>();
        GroupKeyRing held = ring;
        boolean holder = held != null && allowed != null && !held.epochs().isEmpty();
        parameters.put("holder", String.valueOf(holder));
        if (holder) {
            // The newest epochs held (bounded), and the newest one's cutover,
            // which followers announce when this peer is an authorized rotator.
            List<Long> epochs = new ArrayList<>(held.epochs());
            List<Long> recent = epochs.subList(Math.max(0, epochs.size() - 64), epochs.size());
            parameters.put("epochs", String.join(",", recent.stream().map(String::valueOf).toList()));
            long newest = held.newestEpoch();
            parameters.put("epoch", String.valueOf(newest));
            held.cutover(newest).ifPresent(cutover -> parameters.put("cutover", cutover.toString()));
        }
        return new CapabilityAdvertisement(
                "aspace://" + group.value() + "/cap/key-wrap/" + self.value(),
                self, group, clock.instant(), Duration.ofMinutes(15),
                TYPE, "0.2", "pipe", Map.copyOf(parameters), Map.of());
    }

    // ------------------------------------------------------------ serving

    /**
     * Holds the key as epoch 0 under the membership default (ASF-026): sealed
     * only for peers currently admitted to the group.
     *
     * @param key        the group content key
     * @param membership the group's membership view, consulted per request
     */
    public void serve(GroupKey key, GroupMembership membership) {
        Objects.requireNonNull(membership, "membership");
        serve(key, peer -> membership.member(peer).isPresent());
    }

    /**
     * Holds the key as epoch 0 under the fleet's {@link Authorizer}: a requester
     * is served when {@code permits(requester, KEY_HOLDER, group)}, and a rotator
     * is trusted when it is the founder or {@code permits(peer, KEY_ROTATE, group)}.
     *
     * @param key        the group content key
     * @param authorizer decides KEY_HOLDER and KEY_ROTATE
     * @param group      the group the key protects; the authorizer's scope
     */
    public void serve(GroupKey key, Authorizer authorizer, GroupId group) {
        serve(GroupKeyRing.of(Objects.requireNonNull(key, "key")), authorizer, group);
    }

    /**
     * Holds the key as epoch 0 under an explicit policy over verified requesters.
     *
     * @param key     the group content key
     * @param allowed the authorization predicate
     */
    public void serve(GroupKey key, Predicate<PeerId> allowed) {
        serve(GroupKeyRing.of(Objects.requireNonNull(key, "key")), allowed);
    }

    /**
     * Holds every epoch of a ring (v0.1.13) under the fleet's {@link Authorizer}.
     *
     * @param ring       the group's content-key ring, shared with its spaces
     * @param authorizer decides KEY_HOLDER and KEY_ROTATE
     * @param group      the authorizer's scope
     */
    public void serve(GroupKeyRing ring, Authorizer authorizer, GroupId group) {
        Objects.requireNonNull(authorizer, "authorizer");
        String scope = Objects.requireNonNull(group, "group").value();
        PeerId founder = pipes.founder();
        this.rotators = peer -> peer.equals(founder)
                || authorizer.permits(peer, Authorizer.Operation.KEY_ROTATE, scope);
        serve(ring, peer -> authorizer.permits(peer, Authorizer.Operation.KEY_HOLDER, scope));
        // Per-agent wrap: an agent asking for itself is judged at agent granularity.
        this.agentsAllowed = agent -> authorizer.permits(agent, Authorizer.Operation.KEY_HOLDER, scope);
    }

    /**
     * Holds every epoch of a ring under an explicit requester policy; rotators
     * stay the founder unless {@link #rotators(Predicate)} widens them.
     *
     * @param ring    the group's content-key ring
     * @param allowed the authorization predicate over verified requesters
     */
    public void serve(GroupKeyRing ring, Predicate<PeerId> allowed) {
        this.ring = Objects.requireNonNull(ring, "ring");
        this.allowed = Objects.requireNonNull(allowed, "allowed");
        if (this.agentsAllowed == null) {
            this.agentsAllowed = agent -> allowed.test(agent.peer());
        }
    }

    /**
     * Sets which agents asking for themselves are served (default: agents of an
     * allowed peer, or the authorizer's per-agent KEY_HOLDER).
     *
     * @param agentsAllowed the per-agent policy
     * @return this distributor
     */
    public GroupKeyDistributor agentsAllowed(Predicate<ai.badmonkey.agentspaces.common.id.AgentId> agentsAllowed) {
        this.agentsAllowed = Objects.requireNonNull(agentsAllowed, "agentsAllowed");
        return this;
    }

    /**
     * Sets the agents never served, whatever the policy: the group's revoked
     * agents and agent keys (SPEC §5.6, v0.1.13).
     *
     * @param refused the refusal predicate
     * @return this distributor
     */
    public GroupKeyDistributor refuseAgents(Predicate<ai.badmonkey.agentspaces.common.id.AgentId> refused) {
        this.refusedAgents = Objects.requireNonNull(refused, "refused");
        return this;
    }

    /**
     * Sets who may rotate (default: the founder alone).
     *
     * @param rotators the rotation authority
     * @return this distributor
     */
    public GroupKeyDistributor rotators(Predicate<PeerId> rotators) {
        this.rotators = Objects.requireNonNull(rotators, "rotators");
        return this;
    }

    /** Whether a peer may rotate this group's content key. */
    public boolean rotator(PeerId peer) {
        return rotators.test(peer);
    }

    /** The ring this distributor serves and follows, when any. */
    public Optional<GroupKeyRing> ring() {
        return Optional.ofNullable(ring);
    }

    // ------------------------------------------------------------ rotation

    /**
     * Mints the next epoch (SPEC §11a.3): a fresh key effective for writers at
     * {@code cutover}, with this rotator's signed commitment, installed in the
     * ring and advertised on the next capability refresh.
     *
     * @param rotator this peer's identity, an authorized rotator
     * @param cutover when writers switch to the new epoch
     * @return the new epoch number
     */
    public long rotate(PeerIdentity rotator, Instant cutover) {
        Objects.requireNonNull(rotator, "rotator");
        Objects.requireNonNull(cutover, "cutover");
        GroupKeyRing held = ring;
        if (held == null) {
            throw new IllegalStateException("no content-key ring to rotate; serve or follow one first");
        }
        if (!rotator.peerId().equals(self) || !rotators.test(self)) {
            throw new IllegalStateException(self.display() + " may not rotate the content key of "
                    + pipes.group().value() + " (founder or KEY_ROTATE grant required)");
        }
        long epoch = held.newestEpoch() + 1;
        GroupKey key = GroupKey.generate();
        byte[] proof = codec.toBytes(new EpochProof(rotator.rawPublicKey(),
                rotator.sign(commitment(epoch, cutover, key))));
        held.install(epoch, self.value(), key, cutover, proof);
        if (!held.persistent()) {
            LOG.log(System.Logger.Level.WARNING, () -> "minted epoch " + epoch + " of "
                    + pipes.group().value() + " in a ring no keystore persists: it is lost on"
                    + " restart unless another member holds it");
        }
        return epoch;
    }

    /**
     * Rotates on a schedule (SPEC §11a.3, v0.1.13, review M-10): once
     * {@code every} has passed since the newest epoch's cutover (or since this
     * call, before any rotation), the next capability tick mints a new epoch
     * cutting over {@code cutoverDelay} later. Rotation is scheduled by time
     * because the nonce budget is fleet-wide and no node can count every
     * writer's seals.
     *
     * @param rotator      this peer's identity, an authorized rotator
     * @param every        the rotation period
     * @param cutoverDelay how long members have to fetch a new epoch before writers switch
     * @return this distributor
     */
    public GroupKeyDistributor autoRotate(PeerIdentity rotator, Duration every, Duration cutoverDelay) {
        Objects.requireNonNull(rotator, "rotator");
        if (every.isZero() || every.isNegative() || cutoverDelay.isNegative()) {
            throw new IllegalArgumentException("rotation period must be positive and delay non-negative");
        }
        this.autoRotation = new AutoRotation(rotator, every, cutoverDelay, clock.instant());
        return this;
    }

    private void autoRotateIfDue() {
        AutoRotation schedule = autoRotation;
        GroupKeyRing held = ring;
        if (schedule == null || held == null || !rotators.test(self)) {
            return;
        }
        long newest = held.newestEpoch();
        Instant since = newest <= 0 ? schedule.since()
                : held.cutover(newest).orElse(schedule.since());
        Instant now = clock.instant();
        if (!now.isBefore(since.plus(schedule.every()))) {
            rotate(schedule.rotator(), now.plus(schedule.delay()));
        }
    }

    private byte[] commitment(long epoch, Instant cutover, GroupKey key) {
        return codec.toBytes(new EpochCommitment(pipes.group().value(), epoch, cutover.toString(),
                sha256(key.rawBytes())));
    }

    /** Verifies an epoch key's proof: an authorized rotator vouched for exactly this key. */
    private boolean vouched(long epoch, String rotator, Instant cutover, GroupKey key, byte[] proof) {
        if (epoch == 0) {
            // Epoch 0 is the configured key, trusted as the legacy key always was,
            // but only under its own empty rotator name: a holder offering an
            // epoch-0 key under any other name could otherwise plant a key that
            // sorts first and have writers seal under it.
            return rotator.isEmpty();
        }
        if (proof == null) {
            return false;
        }
        EpochProof decoded;
        try {
            decoded = codec.fromBytes(proof, EpochProof.class);
        } catch (RuntimeException e) {
            return false;
        }
        if (decoded == null || decoded.rotatorKey() == null || decoded.signature() == null
                || decoded.rotatorKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return false;
        }
        PeerId minter = PeerId.fromPublicKey(decoded.rotatorKey());
        return minter.value().equals(rotator) && rotators.test(minter)
                && Ed25519.verifyRaw(decoded.rotatorKey(), commitment(epoch, cutover, key),
                        decoded.signature());
    }

    // ------------------------------------------------------------ following

    /**
     * Follows the group's rotations into a ring (SPEC §11a.3, v0.1.13): on each
     * capability tick, holders' advertisements announce cutovers (only an
     * authorized rotator's) and list the epochs they hold, and any epoch this
     * ring lacks is fetched from a holder listing it; the ring's own
     * missing-epoch reports trigger the same fetch.
     *
     * @param ring      the ring to keep current
     * @param discovery the group's discovery, where holders advertise
     * @return this distributor
     */
    public GroupKeyDistributor follow(GroupKeyRing ring, DiscoveryService discovery) {
        Objects.requireNonNull(ring, "ring");
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        if (this.ring == null) {
            this.ring = ring;
        } else if (this.ring != ring) {
            throw new IllegalStateException("this distributor already serves another ring");
        }
        ring.onMissingEpoch(this::fetchInBackground);
        return this;
    }

    @Override
    public void tick() {
        autoRotateIfDue();
        DiscoveryService watched = discovery;
        GroupKeyRing followed = ring;
        if (watched == null || followed == null) {
            return;
        }
        for (CapabilityAdvertisement ad : watched.find(CapabilityAdvertisement.class,
                ad -> TYPE.equals(ad.capabilityType()) && !ad.issuer().equals(self))) {
            Map<String, String> parameters = ad.parameters();
            if (!"true".equals(parameters.get("holder"))) {
                continue;
            }
            if (rotators.test(ad.issuer()) && parameters.containsKey("epoch")
                    && parameters.containsKey("cutover")) {
                try {
                    followed.announce(Long.parseLong(parameters.get("epoch")),
                            Instant.parse(parameters.get("cutover")));
                } catch (RuntimeException ignored) {
                    // a malformed announcement announces nothing
                }
            }
            for (long epoch : listedEpochs(parameters)) {
                if (!followed.epochs().contains(epoch)) {
                    fetchInBackground(epoch);
                }
            }
        }
    }

    private static List<Long> listedEpochs(Map<String, String> parameters) {
        String listed = parameters.get("epochs");
        List<Long> epochs = new ArrayList<>();
        if (listed == null || listed.isBlank()) {
            return epochs;
        }
        for (String part : listed.split(",")) {
            try {
                epochs.add(Long.parseLong(part.trim()));
            } catch (NumberFormatException ignored) {
                // skip
            }
        }
        return epochs;
    }

    private void fetchInBackground(long epoch) {
        DiscoveryService watched = discovery;
        if (watched == null || !fetching.add(epoch)) {
            return;
        }
        Thread.ofVirtual().name("key-wrap-fetch-" + epoch).start(() -> {
            try {
                GroupKeyRing followed = ring;
                int before = followed == null ? 0 : heldKeys(followed, epoch);
                // Holders that list the epoch first, then any other holder: an ad
                // lists only the newest 64 epochs, and an older one may still be
                // held. Stop once a key this ring lacked is installed, so a ring
                // holding one racing rotator's key goes on to fetch the other's.
                List<CapabilityAdvertisement> listing = new ArrayList<>();
                List<CapabilityAdvertisement> others = new ArrayList<>();
                for (CapabilityAdvertisement ad : watched.find(CapabilityAdvertisement.class,
                        ad -> TYPE.equals(ad.capabilityType()) && !ad.issuer().equals(self)
                                && "true".equals(ad.parameters().get("holder")))) {
                    (listedEpochs(ad.parameters()).contains(epoch) ? listing : others).add(ad);
                }
                listing.addAll(others);
                for (CapabilityAdvertisement ad : listing) {
                    List<GroupKeyRing.Held> received = fetch(ad.issuer(), List.of(epoch), Duration.ofSeconds(5));
                    if (followed == null ? !received.isEmpty() : heldKeys(followed, epoch) > before) {
                        return;
                    }
                }
            } finally {
                fetching.remove(epoch);
            }
        });
    }

    private static int heldKeys(GroupKeyRing ring, long epoch) {
        return ring.epochs().contains(epoch) ? ring.openingKeys(epoch).size() : 0;
    }

    // ------------------------------------------------------------ requesting

    /**
     * Requests the group key (epoch 0, the pre-rotation exchange) from a holder.
     *
     * @param holder  the key-holding peer
     * @param timeout how long to wait for the sealed answer
     * @return the key, or empty when denied, unreachable, or unsealable
     */
    public Optional<GroupKey> request(PeerId holder, Duration timeout) {
        KeyResponse response = exchange(holder, nonce -> new KeyRequest(nonce, encryptionPublicRaw,
                null, null), timeout);
        if (response == null || !response.granted() || response.ephemeralPublicKey() == null
                || response.sealed() == null) {
            return Optional.empty();
        }
        Optional<GroupKey> key = GroupKeyWrap.unwrapBound(
                new GroupKeyWrap.WrappedKey(response.ephemeralPublicKey(), response.sealed()),
                encryptionKeys.getPrivate(), encryptionPublicRaw,
                binding(holder, self, response.nonce(), null, null, null));
        if (key.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "dropped the group key from "
                    + holder.display() + ": it does not open under this request's binding (F-1)");
        }
        return key;
    }

    /**
     * Fetches epoch keys from a holder (SPEC §11a.3, v0.1.13), keeping only
     * those an authorized rotator vouched for, and installs them in the
     * followed ring when there is one.
     *
     * @param holder  the key-holding peer
     * @param epochs  the epochs wanted
     * @param timeout how long to wait
     * @return the verified keys received
     */
    public List<GroupKeyRing.Held> fetch(PeerId holder, List<Long> epochs, Duration timeout) {
        Objects.requireNonNull(epochs, "epochs");
        List<Long> wanted = List.copyOf(epochs);
        KeyResponse response = exchange(holder, nonce -> new KeyRequest(nonce, encryptionPublicRaw,
                wanted, null), timeout);
        return receive(holder, response, wanted, null,
                wrapped -> GroupKeyWrap.unwrapBound(wrapped.key(), encryptionKeys.getPrivate(),
                        encryptionPublicRaw, wrapped.binding()));
    }

    /**
     * Fetches epoch keys for one agent of this peer, sealed to that agent's own
     * certified X25519 key (SPEC §11a.2, v0.1.13): the holder judges the agent,
     * not merely its peer, and only the agent's key opens the reply. The keys
     * are installed in the followed ring, like {@link #fetch}.
     *
     * @param peer    this peer's identity (its key verifies the agent's certificate)
     * @param agent   the agent asking, holding a certificate with an encryption key
     * @param holder  the key-holding peer
     * @param epochs  the epochs wanted
     * @param timeout how long to wait
     * @return the verified keys received
     */
    public List<GroupKeyRing.Held> requestAs(PeerIdentity peer,
                                             ai.badmonkey.agentspaces.api.spi.AgentIdentity agent,
                                             PeerId holder, List<Long> epochs, Duration timeout) {
        Objects.requireNonNull(peer, "peer");
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(epochs, "epochs");
        Instant now = clock.instant();
        ai.badmonkey.agentspaces.api.security.AgentCertificate certificate =
                agent.certificateCovering(now).orElseThrow(() -> new IllegalStateException(
                        agent.id().encoded() + " holds no current certificate"));
        byte[] agentEncryption = certificate.encryptionPublicKey();
        if (agentEncryption == null) {
            throw new IllegalStateException(agent.id().encoded()
                    + "'s certificate certifies no encryption key");
        }
        List<Long> wanted = List.copyOf(epochs);
        KeyResponse response = exchange(holder, nonce -> new KeyRequest(nonce, agentEncryption,
                wanted, new AgentRequest(agent.id().encoded(), certificate, peer.rawPublicKey())),
                timeout);
        return receive(holder, response, wanted, agent.id().encoded(),
                wrapped -> GroupKeyWrap.unwrapBound(wrapped.key(), agent::agreeEncryption,
                        agentEncryption, wrapped.binding()));
    }

    /** A wrap to open and the binding it must open under. */
    private record Opening(GroupKeyWrap.WrappedKey key, byte[] binding) {
    }

    private List<GroupKeyRing.Held> receive(PeerId holder, KeyResponse response, List<Long> wanted,
                                            String agent,
                                            java.util.function.Function<Opening, Optional<GroupKey>> opener) {
        List<GroupKeyRing.Held> received = new ArrayList<>();
        if (response == null || !response.granted() || response.keys() == null) {
            return received;
        }
        for (EpochKey offered : response.keys()) {
            if (offered == null || offered.rotator() == null || offered.cutover() == null
                    || !wanted.contains(offered.epoch())) {
                continue; // only the epochs this request asked for are installed
            }
            Instant cutover;
            try {
                cutover = Instant.parse(offered.cutover());
            } catch (RuntimeException e) {
                continue;
            }
            Optional<GroupKey> key = opener.apply(new Opening(
                    new GroupKeyWrap.WrappedKey(offered.ephemeralPublicKey(), offered.sealed()),
                    binding(holder, self, response.nonce(), offered.epoch(), offered.rotator(), agent)));
            if (key.isEmpty()) {
                LOG.log(System.Logger.Level.DEBUG, () -> "dropped epoch " + offered.epoch() + " from "
                        + holder.display() + ": it does not open under this request's binding (F-1)");
                continue;
            }
            if (!vouched(offered.epoch(), offered.rotator(), cutover, key.get(), offered.proof())) {
                LOG.log(System.Logger.Level.DEBUG, () -> "dropped epoch " + offered.epoch() + " from "
                        + holder.display() + ": no authorized rotator vouched for it");
                continue;
            }
            received.add(new GroupKeyRing.Held(offered.epoch(), offered.rotator(), key.get(),
                    cutover, offered.proof()));
        }
        GroupKeyRing followed = ring;
        if (followed != null) {
            for (GroupKeyRing.Held held : received) {
                followed.install(held.epoch(), held.rotator(), held.key(), held.cutover(), held.proof());
            }
        }
        return received;
    }

    private KeyResponse exchange(PeerId holder, java.util.function.LongFunction<KeyRequest> request,
                                 Duration timeout) {
        Objects.requireNonNull(holder, "holder");
        Objects.requireNonNull(timeout, "timeout");
        // A random nonce, recorded with the holder it was sent to: only that
        // holder's answer completes the request (ASF-046).
        long nonce;
        CompletableFuture<KeyResponse> future = new CompletableFuture<>();
        do {
            nonce = nonces.nextLong();
        } while (pending.putIfAbsent(nonce, new Pending(holder, future)) != null);
        try {
            pipes.send(holder, TYPE, codec.toBytes(request.apply(nonce)));
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (java.util.concurrent.ExecutionException | TimeoutException e) {
            return null;
        } finally {
            pending.remove(nonce);
        }
    }

    // ---------------------------------------------------------------- internals

    private void onFrame(PeerId from, byte[] payload) {
        Object frame = decode(payload);
        if (frame instanceof KeyRequest request) {
            answer(from, request);
        } else if (frame instanceof KeyResponse response) {
            Pending waiting = pending.get(response.nonce());
            if (waiting == null || !waiting.holder().equals(from)) {
                return; // not an answer to anything we asked this peer (ASF-046)
            }
            waiting.answer().complete(response);
        }
    }

    private Object decode(byte[] payload) {
        try {
            KeyRequest request = codec.fromBytes(payload, KeyRequest.class);
            if (request != null && request.encryptionPublicKey() != null) {
                return request;
            }
        } catch (RuntimeException ignored) {
            // Not a request; try the response shape.
        }
        try {
            return codec.fromBytes(payload, KeyResponse.class);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private void answer(PeerId from, KeyRequest request) {
        GroupKeyRing held = ring;
        Predicate<PeerId> policy = allowed;
        if (held == null || policy == null
                || request.encryptionPublicKey().length != X25519.RAW_PUBLIC_KEY_LENGTH) {
            deny(from, request.nonce());
            return;
        }
        String agentName = null;
        byte[] sealTo = request.encryptionPublicKey();
        if (request.agent() != null) {
            // Per-agent wrap: judge the agent, seal to its certified key.
            byte[] agentKey = admittedAgentKey(from, request.agent());
            if (agentKey == null || request.epochs() == null) {
                deny(from, request.nonce());
                return;
            }
            agentName = request.agent().agent();
            sealTo = agentKey;
        } else if (!policy.test(from)) {
            deny(from, request.nonce());
            return;
        }
        if (request.epochs() == null) {
            // The pre-rotation exchange: epoch 0's key in the response's own fields.
            Optional<GroupKey> epochZero = held.sealingKey(0);
            if (epochZero.isEmpty()) {
                deny(from, request.nonce());
                return;
            }
            GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrapBound(epochZero.get(),
                    request.encryptionPublicKey(), binding(self, from, request.nonce(), null, null, null));
            pipes.send(from, TYPE, codec.toBytes(new KeyResponse(request.nonce(), true,
                    wrapped.ephemeralPublicKey(), wrapped.sealed(), null)));
            return;
        }
        List<EpochKey> keys = new ArrayList<>();
        Set<Long> wanted = Set.copyOf(request.epochs());
        for (GroupKeyRing.Held key : held.held()) {
            if (!wanted.contains(key.epoch())) {
                continue;
            }
            GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrapBound(key.key(), sealTo,
                    binding(self, from, request.nonce(), key.epoch(), key.rotator(), agentName));
            keys.add(new EpochKey(key.epoch(), key.rotator(), key.cutover().toString(), key.proof(),
                    wrapped.ephemeralPublicKey(), wrapped.sealed()));
        }
        pipes.send(from, TYPE, codec.toBytes(new KeyResponse(request.nonce(), true, null, null,
                List.copyOf(keys))));
    }

    /**
     * The agent's certified X25519 key when it may be served (SPEC §11a.2,
     * v0.1.13), else null: the peer key must hash to the authenticated sender,
     * the certificate must verify under it for exactly the named agent of that
     * peer, be current, and certify an encryption key, and the agent must be
     * permitted and not revoked.
     */
    private byte[] admittedAgentKey(PeerId from, AgentRequest request) {
        if (request.agent() == null || request.certificate() == null || request.peerKey() == null
                || request.peerKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !PeerId.fromPublicKey(request.peerKey()).equals(from)) {
            return null;
        }
        ai.badmonkey.agentspaces.common.id.AgentId agent;
        try {
            agent = ai.badmonkey.agentspaces.common.id.AgentId.parse(request.agent());
        } catch (RuntimeException e) {
            return null;
        }
        Instant now = clock.instant();
        if (!agent.peer().equals(from) || request.certificate().encryptionPublicKey() == null
                || !certificates.verifyAt(request.certificate(), request.peerKey(), agent, now, now)
                || refusedAgents.test(agent)
                || pipes.revocations().revoked(agent, request.certificate().agentPublicKey())) {
            return null;
        }
        Predicate<ai.badmonkey.agentspaces.common.id.AgentId> policy = agentsAllowed;
        return policy != null && policy.test(agent) ? request.certificate().encryptionPublicKey() : null;
    }

    private void deny(PeerId to, long nonce) {
        pipes.send(to, TYPE, codec.toBytes(new KeyResponse(nonce, false, null, null, null)));
    }

    /** The canonical bytes a wrap between {@code holder} and {@code requester} is bound to. */
    private byte[] binding(PeerId holder, PeerId requester, long nonce, Long epoch, String rotator,
                           String agent) {
        return codec.toBytes(new WrapBinding(pipes.group().value(), holder.value(),
                requester.value(), nonce, epoch, rotator, agent));
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
