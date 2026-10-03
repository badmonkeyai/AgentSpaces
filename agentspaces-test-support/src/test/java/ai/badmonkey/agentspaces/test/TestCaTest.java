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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.security.cert.CRLReason;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class TestCaTest {

    private static byte[] identityKey(int fill) {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) fill);
        return key;
    }

    @Test
    void theCaIsASelfSignedCertificateAuthority() throws Exception {
        X509Certificate ca = TestCa.create("second-ca").certificate();

        ca.verify(ca.getPublicKey());
        ca.checkValidity();
        assertThat(ca.getSubjectX500Principal().getName()).isEqualTo("CN=second-ca");
        assertThat(ca.getIssuerX500Principal()).isEqualTo(ca.getSubjectX500Principal());
        assertThat(ca.getBasicConstraints()).as("a CA, any path length").isEqualTo(Integer.MAX_VALUE);
        assertThat(TestCa.create().certificate().getSubjectX500Principal().getName())
                .isEqualTo("CN=agentspaces-test-ca");
    }

    @Test
    void aLeafCarriesTheIdentityKeyInItsCnAndChainsToTheCa() throws Exception {
        TestCa ca = TestCa.create();
        byte[] key = identityKey(7);
        TestCa.Issued issued = ca.issue(key);
        X509Certificate leaf = issued.certificate();

        leaf.verify(ca.certificate().getPublicKey());
        leaf.checkValidity();
        assertThat(leaf.getSubjectX500Principal().getName()).isEqualTo("CN=" + Multibase.base58btc(key));
        assertThat(leaf.getIssuerX500Principal()).isEqualTo(ca.certificate().getSubjectX500Principal());
        assertThat(leaf.getBasicConstraints()).as("not a CA").isEqualTo(-1);
        assertThat(leaf.getPublicKey()).isEqualTo(issued.key().getPublic());
        assertThat(issued.chain(ca)).containsExactly(leaf, ca.certificate());
        assertThat(TestCa.peerIdOf(key)).isEqualTo(PeerId.fromPublicKey(key));
    }

    @Test
    void leavesGetDistinctSerialsAndAChosenWindow() {
        TestCa ca = TestCa.create();
        Instant notBefore = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.SECONDS);
        Instant notAfter = notBefore.plus(Duration.ofDays(2));
        X509Certificate future = ca.issue(identityKey(1), notBefore, notAfter).certificate();

        assertThat(future.getNotBefore()).isEqualTo(Date.from(notBefore));
        assertThat(future.getNotAfter()).isEqualTo(Date.from(notAfter));
        assertThat(future.getSerialNumber())
                .isNotEqualTo(ca.issue(identityKey(1)).certificate().getSerialNumber());
    }

    @Test
    void aCrlListsExactlyTheRevokedLeaves() throws Exception {
        TestCa ca = TestCa.create();
        X509Certificate revoked = ca.issue(identityKey(1)).certificate();
        X509Certificate kept = ca.issue(identityKey(2)).certificate();

        X509CRL crl = ca.crl(revoked);
        crl.verify(ca.certificate().getPublicKey());
        assertThat(crl.isRevoked(revoked)).isTrue();
        assertThat(crl.isRevoked(kept)).isFalse();
        assertThat(crl.getRevokedCertificate(revoked).getRevocationReason())
                .isEqualTo(CRLReason.KEY_COMPROMISE);
        assertThat(crl.getNextUpdate()).isAfter(new Date());
        assertThat(ca.crl().getRevokedCertificates()).as("nobody revoked").isNull();
    }

    @Test
    void aCrlCanBeStaleWithAChosenReason() {
        TestCa ca = TestCa.create();
        X509Certificate leaf = ca.issue(identityKey(3)).certificate();
        Instant thisUpdate = Instant.now().minus(Duration.ofDays(3)).truncatedTo(ChronoUnit.SECONDS);
        Instant nextUpdate = thisUpdate.plus(Duration.ofDays(1));

        X509CRL stale = ca.crl(thisUpdate, nextUpdate, CRLReason.SUPERSEDED, leaf);

        assertThat(stale.getThisUpdate()).isEqualTo(Date.from(thisUpdate));
        assertThat(stale.getNextUpdate()).isEqualTo(Date.from(nextUpdate)).isBefore(new Date());
        assertThat(stale.getRevokedCertificate(leaf).getRevocationReason())
                .isEqualTo(CRLReason.SUPERSEDED);
        assertThat(stale.getRevokedCertificate(leaf).getRevocationDate()).isEqualTo(Date.from(thisUpdate));
    }
}
