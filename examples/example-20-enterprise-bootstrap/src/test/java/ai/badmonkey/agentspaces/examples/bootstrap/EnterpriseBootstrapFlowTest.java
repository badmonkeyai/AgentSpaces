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
package ai.badmonkey.agentspaces.examples.bootstrap;

import ai.badmonkey.agentspaces.api.spi.LocatePolicy;
import ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The example's flow: one name, every address dialed, the trace kept, a required empty answer refused. */
class EnterpriseBootstrapFlowTest {

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void aWorkerWithOneNameDialsEveryAddressBehindIt() throws Exception {
        int port = freePort();
        InetAddress v4 = InetAddress.getByName("127.0.0.1");
        InetAddress v6 = InetAddress.getByName("::1");
        // Two seeds on one port behind one name, as two A records would be: one on IPv4 loopback, one on IPv6.
        try (EnterpriseBootstrapFleet.Peer a = EnterpriseBootstrapFleet.startSeed("127.0.0.1", port, true, List.of());
             EnterpriseBootstrapFleet.Peer b = EnterpriseBootstrapFleet.startSeed("::1", port, false,
                     List.of("127.0.0.1:" + port))) {
            try (EnterpriseBootstrapFleet.Peer worker = EnterpriseBootstrapFleet.startWorker(
                    "seeds.corp.example:" + port,
                    host -> host.equals("seeds.corp.example") ? List.of(v4, v6) : List.of(),
                    LocatePolicy.DEFAULT)) {
                for (int i = 0; i < 80 && worker.runtime().membership().allMembers().size() < 2; i++) {
                    Thread.sleep(100);
                }
                assertThat(worker.runtime().membership().allMembers()).hasSize(2);
                assertThat(worker.node().lastBeaconHint(worker.runtime().id())).hasValueSatisfying(hint -> {
                    assertThat(hint.evidence()).contains("seeds");
                    assertThat(hint.endpoints()).as("every address of the name, not the first")
                            .extracting(e -> e.address())
                            .containsExactly("127.0.0.1:" + port, "[0:0:0:0:0:0:0:1]:" + port);
                });
                List<BootstrapChain.Trace> trace = worker.node().lastBootstrapTrace(worker.runtime().id());
                assertThat(trace).extracting(BootstrapChain.Trace::locator).containsExactly("seeds");
                assertThat(a.runtime().membership().allMembers()).hasSize(2);
                assertThat(b.runtime().membership().allMembers()).hasSize(2);
            }
        }
    }

    @Test
    void aRequiredChainWithNothingBehindTheNameRefuses() {
        // A name with no records keeps the name as an endpoint (the transport gets its own try); a blank
        // configuration is what answers nothing, and that is what `require` refuses.
        assertThatThrownBy(() -> EnterpriseBootstrapFleet.startWorker("",
                host -> List.of(), LocatePolicy.DEFAULT.withRequire(true)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("enterprise-fleet");
    }
}
