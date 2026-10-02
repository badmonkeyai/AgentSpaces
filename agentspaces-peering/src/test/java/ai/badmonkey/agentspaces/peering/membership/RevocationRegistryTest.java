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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.RevocationValidator;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.SignedRevocation;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The revocation registry's acceptance rules (spec v0.1.9): a revocation is
 * authoritative because of who signed it, scoped to its group, byte-stable,
 * bounded against future skew, held for the life of the process regardless of
 * its TTL, and reconciled to late joiners through anti-entropy.
 */
class RevocationRegistryTest {

    private final TestClock clock = TestClock.create();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final PeerIdentity founder = PeerIdentity.generate();
    private final PeerIdentity officer = PeerIdentity.generate();
    private final PeerId victim = PeerIdentity.generate().peerId();
    private final GroupId group = GroupId.of("zRevocationRegistry");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRevocationRegistry", founder.peerId(), group, Instant.EPOCH,
            Duration.ofDays(1), "fleet", GroupAdvertisement.MembershipPolicy.OPEN,
            ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    private final List<RevocationAdvertisement> fired = new ArrayList<>();

    private RevocationRegistry registry(RevocationValidator validator) {
        return new RevocationRegistry(groupAd, validator, codec, clock, fired::add);
    }

    private RevocationAdvertisement ad(PeerIdentity issuer, GroupId scope, Instant issued,
                                       Duration ttl, PeerId revoked, PeerId successor) {
        return new RevocationAdvertisement(
                "aspace://" + scope.value() + "/revocation/" + revoked.value(),
                issuer.peerId(), scope, issued, ttl, revoked, "test", successor);
    }

    /** Mirrors {@code PeerNode.issueRevocation}: exact canonical bytes, raw key, signature. */
    private SignedRevocation sign(PeerIdentity signer, RevocationAdvertisement ad) {
        byte[] adBytes = codec.toBytes(ad);
        return new SignedRevocation(adBytes, signer.rawPublicKey(), signer.sign(adBytes));
    }

    private SignedRevocation founderRevocation() {
        return sign(founder, ad(founder, group, clock.instant(), Duration.ofDays(30), victim, null));
    }

    /** Spec v0.1.9: the TTL bounds re-gossip, never the withdrawal of trust itself. */
    @Test
    void anExpiredRevocationStillWithdrawsTrust() {
        RevocationRegistry registry = registry(RevocationValidator.founderRooted());
        SignedRevocation longExpired = sign(founder, ad(founder, group,
                clock.instant().minus(Duration.ofDays(100)), Duration.ofDays(30), victim, null));

        assertThat(registry.accept(longExpired)).isPresent();
        assertThat(registry.revoked(victim)).isTrue();
        assertThat(fired).hasSize(1);
        assertThat(registry.deltaFor(new byte[0]))
                .as("late joiners are still converged on it via anti-entropy")
                .isNotEmpty();
    }

    /** Spec v0.1.9: a revocation is scoped to the group it names. */
    @Test
    void aRevocationForAnotherGroupIsRefused() {
        RevocationRegistry registry = registry(RevocationValidator.founderRooted());
        SignedRevocation other = sign(founder, ad(founder, GroupId.of("zOther"),
                clock.instant(), Duration.ofDays(30), victim, null));

        assertThat(registry.accept(other)).isEmpty();
        assertThat(registry.revoked(victim)).isFalse();
        assertThat(fired).isEmpty();
    }

    /** Spec v0.1.9 / §11: the signature covers the exact bytes and the key must hash to the issuer. */
    @Test
    void aTamperedOrMisattributedRevocationIsRefused() {
        RevocationRegistry registry = registry(RevocationValidator.founderRooted());

        SignedRevocation genuine = founderRevocation();
        byte[] tampered = genuine.adBytes().clone();
        tampered[tampered.length / 2] ^= 0x01;
        assertThat(registry.accept(new SignedRevocation(
                tampered, genuine.publicKey(), genuine.signature()))).isEmpty();

        // The founder's key and signature, but the ad names another issuer.
        RevocationAdvertisement misattributed = ad(officer, group, clock.instant(),
                Duration.ofDays(30), victim, null);
        byte[] bytes = codec.toBytes(misattributed);
        assertThat(registry.accept(new SignedRevocation(
                bytes, founder.rawPublicKey(), founder.sign(bytes)))).isEmpty();

        // Garbage off the wire proves nothing and throws nothing.
        assertThat(registry.accept(new byte[]{9, 9, 9})).isEmpty();
        assertThat(registry.accept(new SignedRevocation(null, null, null))).isEmpty();

        assertThat(registry.revoked(victim)).isFalse();
        assertThat(fired).isEmpty();
    }

    /** Spec §11 (ASF-027): an issue stamp beyond the skew window is refused; within it, accepted. */
    @Test
    void aFarFutureIssueStampIsRefused() {
        RevocationRegistry registry = registry(RevocationValidator.founderRooted());

        assertThat(registry.accept(sign(founder, ad(founder, group,
                clock.instant().plus(Duration.ofMinutes(11)), Duration.ofDays(30), victim, null))))
                .isEmpty();
        assertThat(registry.revoked(victim)).isFalse();

        assertThat(registry.accept(sign(founder, ad(founder, group,
                clock.instant().plus(Duration.ofMinutes(9)), Duration.ofDays(30), victim, null))))
                .isPresent();
        assertThat(registry.revoked(victim)).isTrue();
    }

    /** Spec v0.1.9: authoritative solely by signer; the authority check is pluggable. */
    @Test
    void aNonFounderIsRefusedByTheDefaultValidatorButAcceptedByAPluggableOne() {
        RevocationAdvertisement byOfficer = ad(officer, group, clock.instant(),
                Duration.ofDays(30), victim, null);

        RevocationRegistry founderRooted = registry(RevocationValidator.founderRooted());
        assertThat(founderRooted.accept(sign(officer, byOfficer))).isEmpty();
        assertThat(founderRooted.revoked(victim)).isFalse();

        RevocationRegistry officerRooted = registry(
                (ad, g) -> ad.issuer().equals(officer.peerId()));
        assertThat(officerRooted.accept(sign(officer, byOfficer))).isPresent();
        assertThat(officerRooted.revoked(victim)).isTrue();
        assertThat(officerRooted.accept(founderRevocation()))
                .as("under the substituted trust root the founder no longer revokes")
                .isEmpty();
    }

    /** Spec v0.1.9: rotation may follow a plain revoke; the newer statement wins, enforcement fires once. */
    @Test
    void theNewerStatementWinsAndFiresEnforcementOnce() {
        RevocationRegistry registry = registry(RevocationValidator.founderRooted());
        PeerId fresh = PeerIdentity.generate().peerId();
        Instant t0 = clock.instant();

        assertThat(registry.accept(sign(founder, ad(founder, group, t0,
                Duration.ofDays(30), victim, null)))).isPresent();
        assertThat(registry.successorOf(victim)).isEmpty();

        assertThat(registry.accept(sign(founder, ad(founder, group, t0.plusSeconds(1),
                Duration.ofDays(30), victim, fresh))))
                .as("a refreshed statement on an already-revoked peer is not a new revocation")
                .isEmpty();
        assertThat(registry.successorOf(victim)).contains(fresh);
        assertThat(fired).hasSize(1);

        PeerId stale = PeerIdentity.generate().peerId();
        registry.accept(sign(founder, ad(founder, group, t0.minusSeconds(1),
                Duration.ofDays(30), victim, stale)));
        assertThat(registry.successorOf(victim))
                .as("an older statement never overwrites a newer one").contains(fresh);
        assertThat(registry.allRevoked()).containsExactly(victim);
    }

    /**
     * v0.1.13 (review M-5, successor erasure): under a validator admitting both
     * the founder and an officer, the officer's newer statement never replaces
     * the founder's record, so the founder's named successor survives.
     */
    @Test
    void aLowerRankedNewerStatementNeverReplacesTheFoundersRecord() {
        RevocationRegistry registry = registry((ad, g) -> ad.issuer().equals(founder.peerId())
                || ad.issuer().equals(officer.peerId()));
        PeerId fresh = PeerIdentity.generate().peerId();
        assertThat(registry.accept(sign(founder, ad(founder, group, clock.instant(),
                Duration.ofDays(30), victim, fresh)))).isPresent();
        registry.accept(sign(officer, ad(officer, group, clock.instant().plusSeconds(5),
                Duration.ofDays(30), victim, null)));
        assertThat(registry.successorOf(victim)).as("the founder's rotation stands").contains(fresh);

        // And in the other order: the founder's later statement replaces the officer's.
        PeerId other = PeerIdentity.generate().peerId();
        RevocationRegistry second = registry((ad, g) -> ad.issuer().equals(founder.peerId())
                || ad.issuer().equals(officer.peerId()));
        second.accept(sign(officer, ad(officer, group, clock.instant().plusSeconds(5),
                Duration.ofDays(30), other, null)));
        second.accept(sign(founder, ad(founder, group, clock.instant(), Duration.ofDays(30), other, fresh)));
        assertThat(second.successorOf(other)).as("higher rank wins whatever the stamps").contains(fresh);
        assertThat(second.digest()).isNotEmpty();
    }

    /** v0.1.13: only the founder names a successor, whatever the validator admits. */
    @Test
    void aSuccessorFromAnyoneButTheFounderIsRefused() {
        RevocationRegistry registry = registry((ad, g) -> true);
        assertThat(registry.accept(sign(officer, ad(officer, group, clock.instant(),
                Duration.ofDays(30), victim, PeerIdentity.generate().peerId())))).isEmpty();
        assertThat(registry.revoked(victim)).isFalse();
        assertThat(registry.accept(sign(officer, ad(officer, group, clock.instant(),
                Duration.ofDays(30), victim, null)))).as("a plain revocation is the validator's call")
                .isPresent();
    }

    /** v0.1.13 (M-5): two replicas receiving equal-rank, equal-stamp statements in either order agree. */
    @Test
    void equalStatementsConvergeWhateverTheArrivalOrder() {
        Instant at = clock.instant();
        SignedRevocation one = sign(founder, ad(founder, group, at, Duration.ofDays(30), victim,
                PeerIdentity.generate().peerId()));
        SignedRevocation two = sign(founder, ad(founder, group, at, Duration.ofDays(30), victim,
                PeerIdentity.generate().peerId()));
        RevocationRegistry left = registry(RevocationValidator.founderRooted());
        RevocationRegistry right = registry(RevocationValidator.founderRooted());
        left.accept(one);
        left.accept(two);
        right.accept(two);
        right.accept(one);
        assertThat(left.digest()).isEqualTo(right.digest());
        assertThat(left.successorOf(victim)).isEqualTo(right.successorOf(victim));
    }

    /** Spec v0.1.9 / §5.3: the retained set reconciles through digest, delta, and apply. */
    @Test
    void antiEntropyRoundTripsRetainedRevocations() {
        RevocationRegistry a = registry(RevocationValidator.founderRooted());
        RevocationRegistry b = new RevocationRegistry(groupAd,
                RevocationValidator.founderRooted(), codec, clock, ad -> {
                });
        a.accept(founderRevocation());

        byte[] delta = a.deltaFor(b.digest());
        assertThat(delta).isNotEmpty();
        b.applyDelta(delta);

        assertThat(b.revoked(victim)).isTrue();
        assertThat(b.digest()).isEqualTo(a.digest());
        assertThat(a.deltaFor(b.digest())).as("converged: nothing left to send").isEmpty();

        b.applyDelta(new byte[]{9, 9, 9});
        b.applyDelta(new byte[0]);
        assertThat(b.allRevoked()).as("garbage deltas are no-ops").containsExactly(victim);
    }
}
