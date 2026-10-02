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
package ai.badmonkey.agentspaces.identity;

import ai.badmonkey.agentspaces.test.TestCa;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SPEC §5.6 v0.1.13 (item 9): CRL files re-read on a cadence. The same trust
 * instance until a file changes; a replaced CRL takes effect at the next
 * refresh; a broken file keeps the last good trust.
 */
class ReloadingChannelTrustTest {

    private final TestCa ca = TestCa.create();
    private final PeerIdentity peer = PeerIdentity.generate();
    private final TestCa.Issued leaf = ca.issue(peer.rawPublicKey());
    private final TestClock clock = TestClock.create();

    @Test
    void aReplacedCrlTakesEffectAtTheNextRefreshAndABrokenOneIsIgnored(@TempDir Path dir) throws Exception {
        Path crl = dir.resolve("ca.crl");
        Files.write(crl, ca.crl().getEncoded());
        ReloadingChannelTrust trust = new ReloadingChannelTrust(List.of(ca.certificate()), List.of(crl),
                Duration.ofMinutes(5), false, clock);
        ChannelTrust first = trust.get();
        assertThat(first.attest(leaf.chain(ca))).contains(peer.peerId());

        Files.write(crl, ca.crl(leaf.certificate()).getEncoded());
        assertThat(trust.get()).as("not yet due").isSameAs(first);
        clock.advance(Duration.ofMinutes(6));
        ChannelTrust second = trust.get();
        assertThat(second).isNotSameAs(first);
        assertThat(second.attest(leaf.chain(ca))).as("the new CRL revokes the leaf").isEmpty();

        clock.advance(Duration.ofMinutes(6));
        assertThat(trust.get()).as("unchanged files keep the instance").isSameAs(second);

        Files.write(crl, new byte[] {1, 2, 3});
        clock.advance(Duration.ofMinutes(6));
        assertThat(trust.get()).as("a broken file keeps the last good trust").isSameAs(second);
    }
}
