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
package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CredentialRevocationTest {

    private static final PeerId FOUNDER = PeerId.of("zFounder");
    private static final GroupId GROUP = GroupId.of("zGroup");
    private static final AgentId AGENT = AgentId.parse("zP/agent");
    private static final Instant ISSUED = Instant.ofEpochMilli(1_000_000);
    private static final Duration TTL = Duration.ofHours(1);

    private static byte[] sha(int fill) {
        byte[] value = new byte[32];
        Arrays.fill(value, (byte) fill);
        return value;
    }

    @Test
    void eachTargetKindRequiresItsOwnFields() {
        assertThatThrownBy(() -> Target.agent(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Target.agentKey(AGENT, new byte[31]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Target.agentKey(null, sha(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Target.x509Leaf("CN=ca", "0a", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Target.joinCredential(new byte[33]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Target("PEER", AGENT, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Target(null, AGENT, null, null, null, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void canonicalTextAndKeyAreUniquePerTarget() {
        assertThat(Target.agent(AGENT).canonical()).isEqualTo(AGENT.encoded());
        assertThat(Target.agentKey(AGENT, sha(0xab)).canonical())
                .isEqualTo(AGENT.encoded() + "#" + "ab".repeat(32));
        assertThat(Target.x509Leaf("CN=ca", "0a", sha(0x01)).canonical()).isEqualTo("01".repeat(32));
        assertThat(Target.joinCredential(sha(0x02)).key()).isEqualTo("JOIN_CREDENTIAL:" + "02".repeat(32));
    }

    @Test
    void targetsAreEqualByKeyAndCopyTheirDigests() {
        byte[] fingerprint = sha(3);
        Target leaf = Target.x509Leaf("CN=ca", "0a", fingerprint);
        fingerprint[0] = 9;

        // Issuer DN and serial are informational; the fingerprint is the identity.
        assertThat(leaf).isEqualTo(Target.x509Leaf("CN=other", "0b", sha(3)))
                .hasSameHashCodeAs(Target.x509Leaf("CN=other", "0b", sha(3)));
        assertThat(leaf).isNotEqualTo(Target.joinCredential(sha(3)));
        assertThat(leaf).isNotEqualTo("not a target");
        assertThat(leaf.certificateFingerprint()).isEqualTo(sha(3));
        leaf.certificateFingerprint()[1] = 9;
        assertThat(leaf.certificateFingerprint()).isEqualTo(sha(3));

        Target key = Target.agentKey(AGENT, sha(4));
        assertThat(key.keyFingerprint()).isEqualTo(sha(4));
        assertThat(key.credentialHash()).isNull();
        assertThat(key.certificateFingerprint()).isNull();
        assertThat(Target.joinCredential(sha(5)).credentialHash()).isEqualTo(sha(5));
        assertThat(Target.agent(AGENT).keyFingerprint()).isNull();
    }

    @Test
    void ofBuildsTheCanonicalIdEffectiveFromIssue() {
        CredentialRevocation revocation = CredentialRevocation.of(FOUNDER, GROUP, ISSUED, TTL,
                Target.agent(AGENT), CredentialRevocation.RETIRED);

        assertThat(revocation.id())
                .isEqualTo("aspace://" + GROUP.value() + "/revocation/agent/" + AGENT.encoded())
                .isEqualTo(CredentialRevocation.idFor(GROUP, Target.agent(AGENT)));
        assertThat(revocation.effectiveFrom()).isNull();
        assertThat(revocation.effectiveSince()).isEqualTo(ISSUED);
        assertThat(revocation.evidence()).isNull();
    }

    @Test
    void aMissingReasonIsUnspecifiedAndAnUnknownOneIsRefused() {
        assertThat(CredentialRevocation.of(FOUNDER, GROUP, ISSUED, TTL, Target.agent(AGENT), null).reason())
                .isEqualTo(CredentialRevocation.UNSPECIFIED);
        assertThat(CredentialRevocation.of(FOUNDER, GROUP, ISSUED, TTL, Target.agent(AGENT), " ").reason())
                .isEqualTo(CredentialRevocation.UNSPECIFIED);
        assertThatThrownBy(() -> CredentialRevocation.of(FOUNDER, GROUP, ISSUED, TTL,
                Target.agent(AGENT), "bored"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keyCompromiseRefusesEverySignatureWhateverItsClaimedTime() {
        CredentialRevocation compromise = CredentialRevocation.of(FOUNDER, GROUP, ISSUED, TTL,
                Target.agent(AGENT), CredentialRevocation.KEY_COMPROMISE);

        assertThat(compromise.compromise()).isTrue();
        // A stolen key can back-date, so a pre-revocation signing time buys nothing.
        assertThat(compromise.refusesAt(ISSUED.minusSeconds(3600))).isTrue();
        assertThat(compromise.refusesAt(ISSUED.plusSeconds(1))).isTrue();
    }

    @Test
    void anOrdinaryRevocationStillAcceptsHistoryBeforeItTookEffect() {
        Instant effective = ISSUED.minusSeconds(60);
        CredentialRevocation retired = new CredentialRevocation(
                CredentialRevocation.idFor(GROUP, Target.agent(AGENT)), FOUNDER, GROUP, ISSUED, TTL,
                Target.agent(AGENT), CredentialRevocation.SUPERSEDED, effective, null);

        assertThat(retired.compromise()).isFalse();
        assertThat(retired.effectiveSince()).isEqualTo(effective);
        assertThat(retired.refusesAt(effective.minusMillis(1))).isFalse();
        assertThat(retired.refusesAt(effective)).isTrue();
        assertThat(retired.refusesAt(null)).as("unknown signing time").isTrue();
    }

    @Test
    void evidenceIsBoundedAndCopied() {
        byte[] evidence = {1, 2, 3};
        CredentialRevocation revocation = new CredentialRevocation("id", FOUNDER, GROUP, ISSUED, TTL,
                Target.joinCredential(sha(1)), CredentialRevocation.PRIVILEGE_WITHDRAWN, null, evidence);
        evidence[0] = 9;

        assertThat(revocation.evidence()).containsExactly(1, 2, 3);
        revocation.evidence()[0] = 9;
        assertThat(revocation.evidence()).containsExactly(1, 2, 3);
        assertThat(new CredentialRevocation("id", FOUNDER, GROUP, ISSUED, TTL, Target.joinCredential(sha(1)),
                null, null, new byte[CredentialRevocation.MAX_EVIDENCE]).evidence())
                .hasSize(CredentialRevocation.MAX_EVIDENCE);
        assertThatThrownBy(() -> new CredentialRevocation("id", FOUNDER, GROUP, ISSUED, TTL,
                Target.joinCredential(sha(1)), null, null, new byte[CredentialRevocation.MAX_EVIDENCE + 1]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CredentialRevocation(null, FOUNDER, GROUP, ISSUED, TTL,
                Target.joinCredential(sha(1)), null, null, null))
                .isInstanceOf(NullPointerException.class);
    }
}
