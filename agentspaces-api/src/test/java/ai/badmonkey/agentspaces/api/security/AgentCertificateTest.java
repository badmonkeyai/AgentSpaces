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
package ai.badmonkey.agentspaces.api.security;

import ai.badmonkey.agentspaces.common.id.AgentId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentCertificateTest {

    private static final AgentId AGENT = AgentId.parse("zP/agent");
    private static final Instant ISSUED = Instant.ofEpochMilli(1_000_000);
    private static final Duration TTL = Duration.ofMinutes(10);

    @Test
    void ttlMustBePositiveAndAnEncryptionKeyMustBeThirtyTwoBytes() {
        assertThatThrownBy(() -> new AgentCertificate(AGENT, new byte[32], ISSUED, Duration.ZERO, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AgentCertificate(AGENT, new byte[32], ISSUED,
                Duration.ofSeconds(-1), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AgentCertificate(AGENT, new byte[32], ISSUED, TTL, null,
                new byte[31]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AgentCertificate(null, new byte[32], ISSUED, TTL, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void theV0112ShapeCarriesNoEncryptionKey() {
        assertThat(new AgentCertificate(AGENT, new byte[32], ISSUED, TTL, null).encryptionPublicKey())
                .isNull();
    }

    @Test
    void keysAreCopiedInSoACallerCannotMutateACertificate() {
        byte[] key = new byte[32];
        byte[] signature = {1, 2, 3};
        AgentCertificate certificate = new AgentCertificate(AGENT, key, ISSUED, TTL, signature);
        key[0] = 9;
        signature[0] = 9;

        assertThat(certificate.agentPublicKey()[0]).isZero();
        assertThat(certificate.peerSignature()[0]).isEqualTo((byte) 1);
    }

    @Test
    void signingAndUnsigningRoundTripTheBody() {
        AgentCertificate body = new AgentCertificate(AGENT, new byte[32], ISSUED, TTL, null, new byte[32]);
        AgentCertificate signed = body.signed(new byte[]{7});

        assertThat(body.unsigned()).isSameAs(body);
        assertThat(signed.peerSignature()).containsExactly(7);
        assertThat(signed.unsigned()).isEqualTo(body).hasSameHashCodeAs(body);
        assertThat(signed).isNotEqualTo(body);
        assertThat(signed.encryptionPublicKey()).hasSize(32);
        assertThatThrownBy(() -> body.signed(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void equalityComparesKeyContentsNotArrayIdentity() {
        AgentCertificate a = new AgentCertificate(AGENT, new byte[32], ISSUED, TTL, new byte[]{1});
        AgentCertificate b = new AgentCertificate(AGENT, new byte[32], ISSUED, TTL, new byte[]{1});

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(new AgentCertificate(AGENT, new byte[32], ISSUED,
                TTL.plusSeconds(1), new byte[]{1}));
        assertThat(a).isNotEqualTo("not a certificate");
    }

    @Test
    void expiryIsInclusiveAtTheBoundary() {
        AgentCertificate certificate = new AgentCertificate(AGENT, new byte[32], ISSUED, TTL, null);

        assertThat(certificate.expiresAt()).isEqualTo(ISSUED.plus(TTL));
        assertThat(certificate.expired(ISSUED.plus(TTL).minusMillis(1))).isFalse();
        assertThat(certificate.expired(ISSUED.plus(TTL))).isTrue();
    }

    @Test
    void theValidityWindowOpensAtTheIssueMillisecondAndClosesAtExpiry() {
        // An issue instant with sub-millisecond precision still covers a
        // signature stamped in that same whole millisecond.
        Instant issued = ISSUED.plusNanos(400_000);
        AgentCertificate certificate = new AgentCertificate(AGENT, new byte[32], issued, TTL, null);

        assertThat(certificate.covers(ISSUED)).isTrue();
        assertThat(certificate.covers(ISSUED.minusMillis(1))).isFalse();
        assertThat(certificate.covers(issued.plus(TTL).minusMillis(1))).isTrue();
        assertThat(certificate.covers(issued.plus(TTL))).isFalse();
    }

    @Test
    void toStringSaysWhetherTheCertificateIsSigned() {
        AgentCertificate body = new AgentCertificate(AGENT, new byte[32], ISSUED, TTL, null);

        assertThat(body.toString()).contains(AGENT.encoded()).endsWith(", unsigned]");
        assertThat(body.signed(new byte[]{1}).toString()).doesNotContain("unsigned");
    }
}
