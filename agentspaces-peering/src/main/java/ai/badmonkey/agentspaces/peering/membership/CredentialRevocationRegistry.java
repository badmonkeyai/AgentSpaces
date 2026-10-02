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
package ai.badmonkey.agentspaces.peering.membership;

import ai.badmonkey.agentspaces.api.ad.CredentialRevocation;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * One group's accepted {@link CredentialRevocation}s (SPEC §6.1, v0.1.13): the
 * revocations of agents, agent keys, X.509 leaves, and join credentials, held
 * for the life of the process and reconciled by anti-entropy like the peer
 * {@link RevocationRegistry}.
 *
 * <p>A registry keeps <em>one</em> record per target, chosen by a total order
 * every replica computes identically, so digests converge (review M-5): the
 * issuer's authority rank first ({@link Authority#FOUNDER} &gt;
 * {@link Authority#TRUST_ROOT} &gt; {@link Authority#OWN_PEER} &gt;
 * {@link Authority#OTHER}), then the newer {@code issued}, then the larger
 * SHA-256 of the signed bytes. A lower-ranked statement therefore never
 * replaces a higher-ranked one, whatever its stamp. The acceptance listeners
 * fire once per target, on its first accepted record.
 */
public final class CredentialRevocationRegistry implements ReconcilableState {

    /** Authority ranks, highest first. */
    public static final class Authority {
        /** The group founder. */
        public static final int FOUNDER = 3;
        /** A trust root proving the revocation with evidence (a CA, SPEC §6.1). */
        public static final int TRUST_ROOT = 2;
        /** The peer that certified the revoked agent or agent key. */
        public static final int OWN_PEER = 1;
        /** Any other issuer a deployment's validator authorizes. */
        public static final int OTHER = 0;
        /** Not authorized. */
        public static final int NONE = -1;

        private Authority() {
        }
    }

    /**
     * Decides a verified revocation's authority rank, or {@link Authority#NONE}
     * when its issuer may not revoke that target.
     */
    @FunctionalInterface
    public interface Validator {

        /**
         * @param revocation the signature-verified revocation
         * @param group      the group's advertisement (carrying the founder)
         * @return the authority rank, or {@link Authority#NONE}
         */
        int rank(CredentialRevocation revocation, GroupAdvertisement group);

        /**
         * The default: the founder revokes anything; an agent's own peer
         * revokes that agent and its keys.
         */
        static Validator defaults() {
            return (revocation, group) -> {
                if (revocation.issuer().equals(group.issuer())) {
                    return Authority.FOUNDER;
                }
                AgentId agent = revocation.target().agent();
                String kind = revocation.target().kind();
                if ((CredentialRevocation.Target.AGENT.equals(kind)
                        || CredentialRevocation.Target.AGENT_KEY.equals(kind))
                        && agent != null && agent.peer().equals(revocation.issuer())) {
                    return Authority.OWN_PEER;
                }
                return Authority.NONE;
            };
        }

        /**
         * The highest rank any of the validators grants.
         *
         * @param validators the validators
         * @return the combined validator
         */
        static Validator anyOf(Validator... validators) {
            List<Validator> all = List.of(validators);
            return (revocation, group) -> {
                int best = Authority.NONE;
                for (Validator validator : all) {
                    best = Math.max(best, validator.rank(revocation, group));
                }
                return best;
            };
        }
    }

    /** How far in the future a revocation's {@code issued} may run (ASF-027). */
    private static final Duration MAX_ISSUED_SKEW = Duration.ofMinutes(10);

    /** Bound on retained revocations; beyond it new targets are refused loudly. */
    static final int MAX_RETAINED = 4_096;

    private static final System.Logger LOG =
            System.getLogger(CredentialRevocationRegistry.class.getName());

    private record Accepted(CredentialRevocation ad, RevocationRegistry.SignedRevocation signed,
                            int rank, String hash) {
    }

    private final GroupAdvertisement group;
    private final Validator validator;
    private final CborCodec codec;
    private final InstantSource clock;
    private final List<Consumer<CredentialRevocation>> listeners = new CopyOnWriteArrayList<>();
    /** Target key ({@code kind:canonical}) to the chosen record. */
    private final Map<String, Accepted> accepted = new ConcurrentHashMap<>();

    /**
     * Creates the registry for one group.
     *
     * @param group     the group's advertisement (the founder anchor)
     * @param validator the authority ranking
     * @param codec     the CBOR codec
     * @param clock     the time source
     */
    public CredentialRevocationRegistry(GroupAdvertisement group, Validator validator,
                                        CborCodec codec, InstantSource clock) {
        this.group = Objects.requireNonNull(group, "group");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Adds a listener invoked once per newly revoked target.
     *
     * @param listener the listener
     */
    public void addListener(Consumer<CredentialRevocation> listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * The record held for a target.
     *
     * @param target the target
     * @return the chosen revocation, when the target is revoked
     */
    public Optional<CredentialRevocation> revocationOf(CredentialRevocation.Target target) {
        Accepted held = accepted.get(target.key());
        return held == null ? Optional.empty() : Optional.of(held.ad());
    }

    /** A snapshot of every held revocation. */
    public List<CredentialRevocation> all() {
        return accepted.values().stream().map(Accepted::ad).toList();
    }

    /**
     * Whether a new signature by an agent is refused under its agent-wide or
     * (given the key) key revocation and the freeze rule.
     *
     * @param agent       the agent
     * @param agentKey    its raw public key, or null
     * @param signingTime the signing time, or null when untrustworthy
     * @return whether a revocation refuses it
     */
    public boolean refuses(AgentId agent, byte[] agentKey, Instant signingTime) {
        Objects.requireNonNull(agent, "agent");
        Accepted whole = accepted.get(CredentialRevocation.Target.agent(agent).key());
        if (whole != null && whole.ad().refusesAt(signingTime)) {
            return true;
        }
        if (agentKey == null) {
            return false;
        }
        Accepted key = accepted.get(
                CredentialRevocation.Target.agentKey(agent, Digests.sha256(agentKey)).key());
        return key != null && key.ad().refusesAt(signingTime);
    }

    /**
     * Whether a join credential is revoked.
     *
     * @param credentialHash SHA-256 of the credential's bytes
     * @return whether it is revoked
     */
    public boolean joinCredentialRevoked(byte[] credentialHash) {
        return accepted.containsKey(CredentialRevocation.Target.joinCredential(credentialHash).key());
    }

    /**
     * Whether an X.509 leaf is revoked.
     *
     * @param certificateFingerprint SHA-256 of the leaf's DER
     * @return whether it is revoked
     */
    public boolean leafRevoked(byte[] certificateFingerprint) {
        return accepted.containsKey(
                CredentialRevocation.Target.x509Leaf(null, null, certificateFingerprint).key());
    }

    /**
     * Verifies and accepts one signed revocation off the wire.
     *
     * @param payload the CBOR signed form
     * @return the revocation when it was valid, authorized, and newly revoked its target
     */
    public Optional<CredentialRevocation> accept(byte[] payload) {
        RevocationRegistry.SignedRevocation signed;
        try {
            signed = codec.fromBytes(payload, RevocationRegistry.SignedRevocation.class);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        return accept(signed);
    }

    /**
     * Verifies and accepts one signed revocation.
     *
     * @param signed the signed form
     * @return the revocation when it was valid, authorized, and newly revoked its target
     */
    public Optional<CredentialRevocation> accept(RevocationRegistry.SignedRevocation signed) {
        CredentialRevocation ad = verify(signed);
        if (ad == null) {
            return Optional.empty();
        }
        int rank = validator.rank(ad, group);
        String key = ad.target().key();
        if (accepted.size() >= MAX_RETAINED && !accepted.containsKey(key)) {
            LOG.log(System.Logger.Level.ERROR, "credential revocation registry full ("
                    + MAX_RETAINED + "); refusing new revocation of " + key
                    + " — investigate, this should never happen");
            return Optional.empty();
        }
        Accepted candidate = new Accepted(ad, signed, rank,
                HexFormat.of().formatHex(Digests.sha256(signed.adBytes())));
        boolean[] added = {false};
        accepted.compute(key, (k, previous) -> {
            if (previous == null) {
                added[0] = true;
                return candidate;
            }
            return outranks(candidate, previous) ? candidate : previous;
        });
        if (!added[0]) {
            return Optional.empty(); // a known target: at most a higher-ranked record replaced silently
        }
        for (Consumer<CredentialRevocation> listener : listeners) {
            listener.accept(ad);
        }
        return Optional.of(ad);
    }

    /** The total order of review M-5: rank, then newer issued, then larger hash. */
    private static boolean outranks(Accepted a, Accepted b) {
        if (a.rank() != b.rank()) {
            return a.rank() > b.rank();
        }
        int byIssued = a.ad().issued().compareTo(b.ad().issued());
        if (byIssued != 0) {
            return byIssued > 0;
        }
        return a.hash().compareTo(b.hash()) > 0;
    }

    /**
     * Verifies one signed revocation without accepting it: exact canonical
     * bytes signed by the key hashing to the issuer, this group, the canonical
     * id for its target, an issue stamp inside skew, an effective instant no
     * later than issue, and an authorized issuer. Expiry is not checked: the
     * TTL bounds re-gossip only.
     *
     * @param signed the signed form
     * @return the revocation, or {@code null} when it proves nothing
     */
    public CredentialRevocation verify(RevocationRegistry.SignedRevocation signed) {
        if (signed == null || signed.adBytes() == null || signed.publicKey() == null
                || signed.signature() == null
                || signed.publicKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return null;
        }
        CredentialRevocation ad;
        try {
            ad = codec.fromBytes(signed.adBytes(), CredentialRevocation.class);
        } catch (RuntimeException e) {
            return null;
        }
        if (ad == null
                || !PeerId.fromPublicKey(signed.publicKey()).equals(ad.issuer())
                || !Ed25519.verifyRaw(signed.publicKey(), signed.adBytes(), signed.signature())
                || !ad.group().equals(group.group())
                || !ad.id().equals(CredentialRevocation.idFor(ad.group(), ad.target()))
                || ad.issued().isAfter(clock.instant().plus(MAX_ISSUED_SKEW))
                || (ad.effectiveFrom() != null && ad.effectiveFrom().isAfter(ad.issued()))) {
            return null;
        }
        return validator.rank(ad, group) < Authority.OTHER ? null : ad;
    }

    // ------------------------------------------------------------ anti-entropy

    private static String line(String key, Accepted held) {
        return "c:" + key + "=" + held.hash();
    }

    @Override
    public byte[] digest() {
        TreeMap<String, Accepted> sorted = new TreeMap<>(accepted);
        StringBuilder sb = new StringBuilder();
        sorted.forEach((key, held) -> sb.append(line(key, held)).append('\n'));
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] deltaFor(byte[] remoteDigest) {
        Set<String> remote = new HashSet<>(List.of(
                new String(remoteDigest, StandardCharsets.UTF_8).split("\n")));
        List<RevocationRegistry.SignedRevocation> missing = new ArrayList<>();
        accepted.forEach((key, held) -> {
            if (!remote.contains(line(key, held))) {
                missing.add(held.signed());
            }
        });
        return missing.isEmpty() ? new byte[0] : codec.toBytes(missing);
    }

    @Override
    public void applyDelta(byte[] delta) {
        if (delta.length == 0) {
            return;
        }
        RevocationRegistry.SignedRevocation[] entries;
        try {
            entries = codec.fromBytes(delta, RevocationRegistry.SignedRevocation[].class);
        } catch (RuntimeException e) {
            return;
        }
        for (RevocationRegistry.SignedRevocation signed : entries) {
            accept(signed);
        }
    }
}
