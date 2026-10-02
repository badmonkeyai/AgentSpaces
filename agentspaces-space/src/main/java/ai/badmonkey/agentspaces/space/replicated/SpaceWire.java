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

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Wire DTOs for replicated-space deltas and anti-entropy. */
public final class SpaceWire {

    private SpaceWire() {
    }

    /**
     * One entry's replicated state in wire form, together with the writer's raw
     * public key so receivers can verify the record signature, and a signature
     * over the mutable state so receivers can verify the transition itself.
     *
     * <p>The record signature covers only the record's immutable identity. The
     * {@code stateSig} closes the gap the record signature leaves open: it
     * authenticates the mutable CRDT fields ({@code adds}, {@code removes},
     * {@code leaseStamp}, {@code leaseValue}, {@code completed}) so that no
     * group member can forge a completion, a removal, or a lease that makes a
     * victim's signed entry silently vanish fleet-wide. The signer is implied
     * by the transition: a completed state is signed by the take-claim holder,
     * every other state (write, renew, cancel) by the issuer. See
     * {@code ReplicatedSpace#verifyState}.
     *
     * @param record          the entry record (its signature covers the record's
     *                        immutable identity; see {@code ReplicatedSpace})
     * @param issuerPublicKey the writer's raw Ed25519 public key
     * @param adds            observed add dots
     * @param removes         observed remove dots
     * @param leaseStamp      the lease register's HLC stamp
     * @param leaseValue      the lease register's value
     * @param completed       the monotone completion flag
     * @param stateSig        the actor's signature over the mutable state, or
     *                        {@code null} before signing
     */
    public record EntryStateDto(
            EntryRecord record,
            byte[] issuerPublicKey,
            List<Dot> adds,
            List<Dot> removes,
            HlcTimestamp leaseStamp,
            LeaseInfo leaseValue,
            boolean completed,
            byte[] stateSig,
            // Omitted from the wire when absent, so a fleet that never opts into
            // subordinate keys stays byte-identical to the pre-certificate DTO and the
            // v0.1.10 golden vectors hold as they are (QA4 A4-7 phase 1).
            @com.fasterxml.jackson.annotation.JsonInclude(
                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            ai.badmonkey.agentspaces.api.security.AgentCertificate agentCertificate,
            // Agent-signed state transitions (SPEC §11a.4, v0.1.13), appended and
            // omitted when absent: a peer-signed state is byte-identical to the
            // v0.1.12 DTO. When present, stateSig verifies under stateCertificate's
            // agent key; signer names the authorized party (the record issuer, or
            // the claim holder for a completion) and signedAt is the signing time
            // the certificate must cover. signer and signedAt are also signed.
            @com.fasterxml.jackson.annotation.JsonInclude(
                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            ai.badmonkey.agentspaces.api.security.AgentCertificate stateCertificate,
            @com.fasterxml.jackson.annotation.JsonInclude(
                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            String signer,
            @com.fasterxml.jackson.annotation.JsonInclude(
                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            HlcTimestamp signedAt) {

        /** The v0.1.12 shape: a peer-signed state, possibly over an agent-signed record. */
        public EntryStateDto(EntryRecord record, byte[] issuerPublicKey, List<Dot> adds,
                             List<Dot> removes, HlcTimestamp leaseStamp, LeaseInfo leaseValue,
                             boolean completed, byte[] stateSig,
                             ai.badmonkey.agentspaces.api.security.AgentCertificate agentCertificate) {
            this(record, issuerPublicKey, adds, removes, leaseStamp, leaseValue, completed,
                    stateSig, agentCertificate, null, null, null);
        }

        /** Whether this state is signed by an agent's own key (v0.1.13). */
        public boolean agentSigned() {
            return signer != null;
        }

        /**
         * The pre-certificate shape (QA4 A4-7 phase 1). {@code agentCertificate}
         * is an appended field older decoders ignore; {@code issuerPublicKey}
         * remains the <em>peer</em> key, which verifies the certificate and the
         * PeerId binding, while the record signature verifies under the
         * certificate's agent key when one is present and under the peer key
         * otherwise.
         */
        public EntryStateDto(EntryRecord record, byte[] issuerPublicKey, List<Dot> adds,
                             List<Dot> removes, HlcTimestamp leaseStamp, LeaseInfo leaseValue,
                             boolean completed, byte[] stateSig) {
            this(record, issuerPublicKey, adds, removes, leaseStamp, leaseValue, completed,
                    stateSig, null);
        }

        /** Converts a CRDT entry state to wire form, unsigned, with no certificate. */
        public static EntryStateDto from(EntryState state, byte[] issuerPublicKey) {
            return from(state, issuerPublicKey, null);
        }

        /** Converts a CRDT entry state to wire form, unsigned, carrying the writer's certificate. */
        public static EntryStateDto from(EntryState state, byte[] issuerPublicKey,
                ai.badmonkey.agentspaces.api.security.AgentCertificate agentCertificate) {
            return new EntryStateDto(state.record(), issuerPublicKey,
                    List.copyOf(state.adds()), List.copyOf(state.removes()),
                    state.lease().stamp(), state.lease().value(), state.completed(), null,
                    agentCertificate);
        }

        /** Returns a copy carrying the given state signature. */
        public EntryStateDto withStateSig(byte[] stateSig) {
            return new EntryStateDto(record, issuerPublicKey, adds, removes,
                    leaseStamp, leaseValue, completed, stateSig, agentCertificate,
                    stateCertificate, signer, signedAt);
        }

        /** Returns a copy naming its agent signer, signing time, and certificate, unsigned. */
        public EntryStateDto withAgentSigner(String signer, HlcTimestamp signedAt,
                ai.badmonkey.agentspaces.api.security.AgentCertificate stateCertificate) {
            return new EntryStateDto(record, issuerPublicKey, adds, removes,
                    leaseStamp, leaseValue, completed, null, agentCertificate,
                    stateCertificate, signer, signedAt);
        }

        /** Converts wire form back to a CRDT entry state. */
        public EntryState toState() {
            return new EntryState(record, new java.util.HashSet<>(adds),
                    new java.util.HashSet<>(removes),
                    new LwwRegister<>(leaseStamp, leaseValue), completed);
        }
    }

    /**
     * A take claim with its holder's proof: the claim, the holder's raw public
     * key, and the holder's signature over the claim's canonical bytes. Because
     * {@link TakeClaim#merge} always returns one of its inputs, the winning claim
     * keeps a valid proof through any number of merges and forwarding hops.
     *
     * @param claim     the claim
     * @param holderKey the holder's raw Ed25519 public key
     * @param signature the holder's signature over the claim's canonical bytes
     */
    public record SignedClaim(
            TakeClaim claim,
            byte[] holderKey,
            byte[] signature,
            // Appended in QA4 A4-7 phase 3 and omitted from the wire when absent, so
            // a peer-signed claim is byte-identical to before. When present, the
            // holder took through a subordinate identity: holderKey is still the
            // holder's *peer* key (it hashes to the holder's peer and verifies the
            // certificate), and the signature verifies under the certificate's agent
            // key — the same two-key rule EntryStateDto.agentCertificate follows.
            @com.fasterxml.jackson.annotation.JsonInclude(
                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            ai.badmonkey.agentspaces.api.security.AgentCertificate holderCertificate) {

        /** A peer-signed claim proof: the holder's peer key and its signature. */
        public SignedClaim(TakeClaim claim, byte[] holderKey, byte[] signature) {
            this(claim, holderKey, signature, null);
        }

        /** Whether the holder took through an agent key of its own (SPEC §4.2). */
        public boolean agentAttested() {
            return holderCertificate != null;
        }

        /**
         * Merges two signed claims, keeping the winning claim's proof. When both
         * carry the same claim, the holder-attested proof (the one signed under
         * the holder's own key) wins over a locally attested placeholder. The
         * ordered-log coordinator applies each committed claim on every member
         * under that member's own key; only the holder's own proof, which arrives
         * with its completion, can authenticate the completion (SPEC §11a.4), so
         * it must displace the placeholder when it lands.
         */
        public static SignedClaim merge(SignedClaim a, SignedClaim b) {
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            TakeClaim winner = TakeClaim.merge(a.claim(), b.claim());
            if (!winner.equals(b.claim())) {
                return a;
            }
            if (!winner.equals(a.claim())) {
                return b;
            }
            return b.holderAttested() && !a.holderAttested() ? b : a;
        }

        /** Whether this proof is signed under the claim holder's own key. */
        public boolean holderAttested() {
            return holderKey != null && claim != null
                    && holderKey.length == Ed25519.RAW_PUBLIC_KEY_LENGTH
                    && PeerId.fromPublicKey(holderKey).equals(claim.holder().peer());
        }
    }

    /**
     * A rumor delta: one entry state, one signed claim, or both.
     *
     * @param state      the entry state; may be {@code null}
     * @param claimEntry the entry a claim applies to; {@code null} without a claim
     * @param claim      the signed take claim; may be {@code null}
     */
    public record Delta(EntryStateDto state, EntryId claimEntry, SignedClaim claim) {
    }

    /**
     * An anti-entropy delta: everything the remote replica was missing.
     *
     * @param states the missing or differing entry states
     * @param claims the missing or differing signed claims, keyed by entry id
     */
    public record SyncDelta(List<EntryStateDto> states, Map<String, SignedClaim> claims) {

        /** Returns an empty sync delta. */
        public static SyncDelta empty() {
            return new SyncDelta(new ArrayList<>(), Map.of());
        }
    }
}
