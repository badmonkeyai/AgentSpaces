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
package ai.badmonkey.agentspaces.examples.storegraph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.file.Path;
import java.util.function.IntSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The store half of example 34 on {@code dtls} (srad/agentspaces-pdt2.md P2-O-03): every peer listens on the
 * datagram binding alone, so the pin, the stores' fetches, the late reader's fetch, the challenges, and the
 * reconcile all cross DTLS, and the same rows hold as on tcp.
 */
class PdtBindingsTest {

    private static final IntSupplier FREE_UDP = () -> {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    };

    @Test
    @Timeout(600)
    void theStoreHalfRunsOnDtls(@TempDir Path dir) throws Exception {
        StoreHalf.Report report = StoreHalf.run(dir, "dtls", FREE_UDP, StoreHalf.Sizes.bindings(), System.out);
        System.out.print(report.summary());
        assertThat(report.binding()).isEqualTo("dtls");
        StoreAndGraphTest.assertStoreHalf(report, StoreHalf.Sizes.bindings());
    }
}
