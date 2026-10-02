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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.SignedRevocation;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SPEC §5.6 v0.1.13 (TODO-9-10-11 C2, review M-5): the credential revocation
 * registry. Who may revoke what, the freeze rule, one record per target under
 * a total order, the retention cap, and anti-entropy round trips.
 */
class CredentialRevocationRegistryTest {

    private final TestClock clock = TestClock.create();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final PeerIdentity founder = PeerIdentity.generate();
    private final PeerIdentity host = PeerIdentity.generate();
    private final PeerIdentity stranger = PeerIdentity.generate();
    private final AgentId agent = host.agent("planner");
    private final GroupId group = GroupId.of("zCredentialRevocations");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zCredentialRevocations", founder.peerId(), group, Instant.EPOCH,
            Duration.ofDays(1), "fleet", GroupAdvertisement.MembershipPolicy.OPEN,
            ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    private final List<CredentialRevocation> fired = new ArrayList<>();

    private CredentialRevocationRegistry registry() {
        return registry(CredentialRevocationRegistry.Validator.defaults());
    }

    private CredentialRevocationRegistry registry(CredentialRevocationRegistry.Validator validator) {
        CredentialRevocationRegistry registry =
                new CredentialRevocationRegistry(groupAd, validator, codec, clock);
        registry.addListener(fired::add);
        return registry;
    }

    private SignedRevocation sign(PeerIdentity issuer, CredentialRevocation.Target target, String reason,
                                  Instant issued, Instant effectiveFrom) {
        CredentialRevocation ad = new CredentialRevocation(CredentialRevocation.idFor(group, target),
                issuer.peerId(), group, issued, Duration.ofDays(30), target, reason, effectiveFrom, null);
        byte[] bytes = codec.toBytes(ad);
        return new SignedRevocation(bytes, issuer.rawPublicKey(), issuer.sign(bytes));
    }

    private SignedRevocation sign(PeerIdentity issuer, CredentialRevocation.Target target, String reason) {
        return sign(issuer, target, reason, clock.instant(), null);
    }

    @Test
    void anAgentIsRevocableByItsOwnPeerOrTheFounderAndNobodyElse() {
        CredentialRevocation.Target target = CredentialRevocation.Target.agent(agent);
        assertThat(registry().accept(sign(stranger, target, CredentialRevocation.RETIRED))).isEmpty();
        assertThat(registry().accept(sign(host, target, CredentialRevocation.RETIRED))).isPresent();
        assertThat(registry().accept(sign(founder, target, CredentialRevocation.RETIRED))).isPresent();
    }

    @Test
    void joinCredentialsAndLeavesAreTheFoundersAlone() {
        CredentialRevocation.Target credential =
                CredentialRevocation.Target.joinCredential(Digests.sha256(new byte[] {1}));
        CredentialRevocation.Target leaf =
                CredentialRevocation.Target.x509Leaf("CN=ca", "01", Digests.sha256(new byte[] {2}));
        CredentialRevocationRegistry registry = registry();
        assertThat(registry.accept(sign(host, credential, CredentialRevocation.UNSPECIFIED))).isEmpty();
        assertThat(registry.accept(sign(host, leaf, CredentialRevocation.UNSPECIFIED))).isEmpty();
        assertThat(registry.accept(sign(founder, credential, CredentialRevocation.UNSPECIFIED))).isPresent();
        assertThat(registry.accept(sign(founder, leaf, CredentialRevocation.KEY_COMPROMISE))).isPresent();
        assertThat(registry.joinCredentialRevoked(Digests.sha256(new byte[] {1}))).isTrue();
        assertThat(registry.leafRevoked(Digests.sha256(new byte[] {2}))).isTrue();
        assertThat(registry.joinCredentialRevoked(Digests.sha256(new byte[] {3}))).isFalse();
    }

    @Test
    void theFreezeRuleKeepsHistoryUnderRetirementAndRefusesEverythingUnderCompromise() {
        Instant t = clock.instant();
        CredentialRevocationRegistry retired = registry();
        retired.accept(sign(host, CredentialRevocation.Target.agent(agent), CredentialRevocation.RETIRED,
                t, t.minusSeconds(60)));
        assertThat(retired.refuses(agent, null, t.minusSeconds(61))).as("signed before the effect").isFalse();
        assertThat(retired.refuses(agent, null, t.minusSeconds(60))).as("at the effect").isTrue();
        assertThat(retired.refuses(agent, null, null)).as("an untrustworthy time").isTrue();
        assertThat(retired.refuses(host.agent("sibling"), null, t)).isFalse();

        CredentialRevocationRegistry compromised = registry();
        compromised.accept(sign(host, CredentialRevocation.Target.agent(agent),
                CredentialRevocation.KEY_COMPROMISE));
        assertThat(compromised.refuses(agent, null, t.minus(Duration.ofDays(365))))
                .as("a stolen key can back-date").isTrue();
    }

    @Test
    void anAgentKeyRevocationRefusesOnlyThatKey() {
        byte[] oldKey = Ed25519.rawPublicKey(Ed25519.generate().getPublic());
        byte[] newKey = Ed25519.rawPublicKey(Ed25519.generate().getPublic());
        CredentialRevocationRegistry registry = registry();
        registry.accept(sign(host, CredentialRevocation.Target.agentKey(agent, Digests.sha256(oldKey)),
                CredentialRevocation.KEY_COMPROMISE));
        assertThat(registry.refuses(agent, oldKey, clock.instant())).isTrue();
        assertThat(registry.refuses(agent, newKey, clock.instant())).isFalse();
        assertThat(registry.refuses(agent, null, clock.instant()))
                .as("a peer-signed agent is not that key").isFalse();
    }

    @Test
    void oneRecordPerTargetChosenByRankThenStampThenHash() {
        CredentialRevocation.Target target = CredentialRevocation.Target.agent(agent);
        Instant t = clock.instant();
        CredentialRevocationRegistry registry = registry();
        assertThat(registry.accept(sign(founder, target, CredentialRevocation.RETIRED, t, null))).isPresent();
        assertThat(registry.accept(sign(host, target, CredentialRevocation.KEY_COMPROMISE,
                t.plusSeconds(10), null))).as("not a new target").isEmpty();
        assertThat(registry.revocationOf(target).orElseThrow().reason())
                .as("the agent's own peer never displaces the founder").isEqualTo(CredentialRevocation.RETIRED);
        registry.accept(sign(founder, target, CredentialRevocation.KEY_COMPROMISE, t.plusSeconds(20), null));
        assertThat(registry.revocationOf(target).orElseThrow().reason())
                .as("the founder's newer statement replaces its own").isEqualTo(CredentialRevocation.KEY_COMPROMISE);
        assertThat(fired).as("listeners fire once per target").hasSize(1);

        SignedRevocation a = sign(host, CredentialRevocation.Target.agent(host.agent("x")),
                CredentialRevocation.RETIRED, t, null);
        SignedRevocation b = sign(host, CredentialRevocation.Target.agent(host.agent("x")),
                CredentialRevocation.SUPERSEDED, t, null);
        CredentialRevocationRegistry left = registry();
        CredentialRevocationRegistry right = registry();
        left.accept(a);
        left.accept(b);
        right.accept(b);
        right.accept(a);
        assertThat(left.digest()).as("ties break by hash, whatever the order").isEqualTo(right.digest());
    }

    @Test
    void forgedMisplacedOrPostDatedRevocationsAreRefused() {
        CredentialRevocation.Target target = CredentialRevocation.Target.agent(agent);
        SignedRevocation honest = sign(host, target, CredentialRevocation.RETIRED);
        CredentialRevocationRegistry registry = registry();
        byte[] tampered = honest.adBytes().clone();
        tampered[tampered.length - 1] ^= 1;
        assertThat(registry.accept(new SignedRevocation(tampered, honest.publicKey(), honest.signature())))
                .isEmpty();
        assertThat(registry.accept(new SignedRevocation(honest.adBytes(), stranger.rawPublicKey(),
                stranger.sign(honest.adBytes())))).as("signed by someone other than the issuer").isEmpty();
        assertThat(registry.accept(sign(host, target, CredentialRevocation.RETIRED,
                clock.instant().plus(Duration.ofHours(1)), null))).as("far-future issue").isEmpty();
        assertThat(registry.accept(sign(host, target, CredentialRevocation.RETIRED,
                clock.instant(), clock.instant().plusSeconds(1)))).as("effective after issue").isEmpty();

        CredentialRevocation misnamed = new CredentialRevocation("aspace://elsewhere", host.peerId(), group,
                clock.instant(), Duration.ofDays(1), target, CredentialRevocation.RETIRED, null, null);
        byte[] bytes = codec.toBytes(misnamed);
        assertThat(registry.accept(new SignedRevocation(bytes, host.rawPublicKey(), host.sign(bytes))))
                .as("a non-canonical id").isEmpty();
        assertThat(registry.all()).isEmpty();
        assertThatThrownBy(() -> sign(host, target, "because"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void antiEntropyRoundTripsTheRetainedSet() {
        CredentialRevocationRegistry source = registry();
        source.accept(sign(host, CredentialRevocation.Target.agent(agent), CredentialRevocation.RETIRED));
        source.accept(sign(founder, CredentialRevocation.Target.joinCredential(Digests.sha256(new byte[] {9})),
                CredentialRevocation.UNSPECIFIED));
        CredentialRevocationRegistry late = registry();
        late.applyDelta(source.deltaFor(late.digest()));
        assertThat(late.digest()).isEqualTo(source.digest());
        assertThat(source.deltaFor(late.digest())).isEmpty();
        assertThat(late.refuses(agent, null, clock.instant())).isTrue();
    }

    @Test
    void theRegistryRefusesNewTargetsLoudlyWhenFull() {
        CredentialRevocationRegistry registry = registry((r, g) -> CredentialRevocationRegistry.Authority.FOUNDER);
        for (int i = 0; i < CredentialRevocationRegistry.MAX_RETAINED; i++) {
            assertThat(registry.accept(sign(host, CredentialRevocation.Target.agent(host.agent("a" + i)),
                    CredentialRevocation.RETIRED))).isPresent();
        }
        assertThat(registry.accept(sign(host, CredentialRevocation.Target.agent(host.agent("one-more")),
                CredentialRevocation.RETIRED))).isEmpty();
    }
}
