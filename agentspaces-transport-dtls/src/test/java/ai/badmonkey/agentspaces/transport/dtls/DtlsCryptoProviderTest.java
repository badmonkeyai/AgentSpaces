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
package ai.badmonkey.agentspaces.transport.dtls;

import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Test;

import java.security.Provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The crypto provider seam (SRAD DT-F-07): a provider can be set before first
 * use and the transport still opens a listener with it; after first use the
 * seam refuses (review-110226-deadcode-parity D14 asked for this test).
 */
class DtlsCryptoProviderTest {

    @Test
    void aProviderIsTakenBeforeFirstUseAndRefusedAfter() throws Exception {
        Provider provider = new BouncyCastleProvider();
        DtlsTransport transport = new DtlsTransport(PeerIdentity.generate());
        assertThat(transport.cryptoProvider(provider)).isSameAs(transport);
        try (AutoCloseable listener = transport.listen("127.0.0.1:0", connection -> { })) {
            assertThat(listener).isNotNull();
            assertThatThrownBy(() -> transport.cryptoProvider(provider))
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}
