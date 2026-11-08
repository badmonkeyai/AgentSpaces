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
package ai.badmonkey.agentspaces.examples.pdt;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.content.Content;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Steps 1 to 3 of §7.3 on {@code dtls} with one holder behind a relay
 * (PD-Q-TST-03, srad/agentspaces-pdt.md §7.3), after example 29's
 * {@code ContentBindingsTest} and {@code ContentRelaySitesTest}: the curator
 * and analyst-1 listen on dtls, a relay with the RELAY role listens on dtls
 * and on tcp, and analyst-2 is dial-only with a tcp transport alone, seeded
 * at the relay's tcp port. Loopback cannot hide a listener, so the binding
 * keeps analyst-2 apart: it can dial no dtls listener, and every head, inline
 * delta, and tree node it exchanges crosses the relay on a pinned path
 * ({@code hopsTo} counts the relays between two peers, so one relay reads 1).
 */
class PdtBindingsTest {

    private static final long KIB = 1024;
    private static final long MIB = 1024 * 1024;
    private static final Pattern CHUNK = Pattern.compile("edited chunk (\\d+) bytes");

    private static int freeUdp() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static int freeTcp() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(600)
    void stepsOneToThreeRunOnDtlsWithAHolderBehindARelay(@TempDir Path dir) throws Exception {
        int curatorPort = freeUdp();
        int relayDtls = freeUdp();
        int relayTcp = freeTcp();
        String seed = "dtls://127.0.0.1:" + curatorPort;
        try (PersistentValuesFleet.Peer curator = PersistentValuesFleet.start("curator", Map.of("dtls", curatorPort), Set.of(),
                     List.of(), dir.resolve("curator"), true);
             PersistentValuesFleet.Peer relay = PersistentValuesFleet.start("relay", Map.of("dtls", relayDtls, "tcp", relayTcp),
                     Set.of(PeerAdvertisement.PeerRole.RELAY), List.of(seed), dir.resolve("relay"), false);
             PersistentValuesFleet.Peer a1 = PersistentValuesFleet.start("analyst-1", Map.of("dtls", freeUdp()), Set.of(),
                     List.of(seed), dir.resolve("a1"), true);
             PersistentValuesFleet.Peer a2 = PersistentValuesFleet.start("analyst-2", Map.of(), Set.of(),
                     List.of("tcp://127.0.0.1:" + relayTcp), dir.resolve("a2"), true)) {
            List<PersistentValuesFleet.Peer> fleetPeers = List.of(curator, relay, a1, a2);
            boolean behindRelay = PersistentValuesFleet.await(Duration.ofSeconds(90),
                    () -> curator.runtime().hopsTo(a2.id()) == 1 && a1.runtime().hopsTo(a2.id()) == 1
                            && !curator.runtime().viaRelays().isEmpty());
            StringBuilder topology = new StringBuilder();
            for (PersistentValuesFleet.Peer from : fleetPeers) {
                topology.append(from.name()).append(": members ").append(from.runtime().membership().allMembers().size())
                        .append(", via ").append(from.runtime().viaRelays()).append(", hops");
                for (PersistentValuesFleet.Peer to : fleetPeers) {
                    if (from != to) {
                        topology.append(' ').append(to.name()).append('=').append(from.runtime().hopsTo(to.id()));
                    }
                }
                topology.append("; ");
            }
            System.out.println("topology: " + topology);
            assertThat(behindRelay).as("the curator and analyst-1 reach analyst-2 through the relay: " + topology).isTrue();

            Composition.Fleet fleet = new Composition.Fleet(curator, List.of(a1, a2), null, null);
            Composition.Report report = Composition.run(fleet, Composition.Sizes.bindings(), dir.resolve("work"), null, System.out);
            System.out.print(report.summary());

            assertThat(curator.runtime().hopsTo(a2.id())).as("the path stayed pinned: one relay between").isEqualTo(1);
            String kind = "PIPE_DATA:" + Content.TYPE;
            long relayed = relay.node().relayBytesByKind(relay.runtime().id()).getOrDefault(kind, 0L);
            System.out.printf("PD-Q-TST-03 on dtls: the relay carried %d bytes of tree exchange for analyst-2%n", relayed);
            assertThat(relayed).as("the tree exchange crossed the relay").isGreaterThan(report.factsBytes());

            // PD-Q-PRF-01, PD-Q-REL-02
            Composition.Update oneKey = report.update("1 facts one key");
            assertThat(oneKey.maxReaderBytes()).as("PD-Q-PRF-01 on dtls: " + oneKey).isEqualTo(oneKey.deltaBytes()).isLessThanOrEqualTo(8 * KIB);
            assertThat(oneKey.pipeNodes()).as("PD-Q-PRF-05 on dtls").isZero();
            assertThat(new HashSet<>(report.factsRoots().values())).as("PD-Q-REL-02: " + report.factsRoots()).hasSize(1);

            // PD-Q-PRF-02 to 04 on dtls
            Composition.Update edit = report.update("3 edit one byte");
            Matcher chunk = CHUNK.matcher(edit.note());
            assertThat(chunk.find()).as(edit.note()).isTrue();
            assertThat(edit.deltaBytes()).as("PD-Q-PRF-02: " + edit).isLessThanOrEqualTo(Long.parseLong(chunk.group(1)) + 16 * KIB);
            assertThat(edit.maxReaderBytes()).as("PD-Q-PRF-02 worst case: " + edit).isLessThanOrEqualTo(2 * 256 * KIB + 16 * KIB);
            for (Composition.Update append : report.updates("3 append")) {
                assertThat(append.maxReaderBytes()).as("PD-Q-PRF-04: " + append).isLessThanOrEqualTo(MIB + 256 * KIB + 16 * KIB);
            }
            for (Composition.Update insert : report.updates("3 insert")) {
                assertThat(insert.maxReaderBytes()).as("PD-Q-PRF-03 p99 bound: " + insert).isLessThanOrEqualTo(MIB);
            }
            for (Composition.Update u : report.updates("3 ")) {
                for (Composition.ReaderCost cost : u.readers()) {
                    assertThat(cost.fetched()).as(u.step() + ": " + cost.reader() + " fetched its delta once").isEqualTo(1);
                }
            }
        }
    }
}
