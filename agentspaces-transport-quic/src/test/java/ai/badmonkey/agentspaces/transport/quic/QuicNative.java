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
package ai.badmonkey.agentspaces.transport.quic;

import io.netty.incubator.codec.quic.Quic;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Skips a QUIC test class, with the native library's own reason, where quiche cannot load. */
final class QuicNative {

    private QuicNative() {
    }

    static void assumeAvailable() {
        assumeTrue(Quic.isAvailable(), () -> "QUIC native library unavailable: "
                + Quic.unavailabilityCause());
    }

    /**
     * Dials with up to three attempts. A loopback QUIC handshake can miss the
     * 10-second dial window when the whole build loads the machine; a listener
     * that genuinely refuses connections fails every attempt, so a retry hides
     * load, not defects.
     */
    static ai.badmonkey.agentspaces.api.spi.TransportConnection dial(QuicTransport transport,
                                                                      String address)
            throws java.io.IOException {
        java.io.IOException last = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                return transport.dial(address);
            } catch (java.io.IOException e) {
                last = e;
            }
        }
        throw last;
    }
}
