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
package ai.badmonkey.agentspaces.examples.intake;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.examples.intake.IntakeFleet.ExtractionCandidate;
import ai.badmonkey.agentspaces.examples.intake.IntakeFleet.FilingEntry;
import ai.badmonkey.agentspaces.examples.intake.IntakeFleet.Peer;
import ai.badmonkey.agentspaces.examples.intake.IntakeFleet.ScanEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** The extract-vote-file choreography over real TCP. */
class IntakeFleetFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(90)
    void theQuorumFilesTheCarefulReadingWithProvenance() throws Exception {
        int seedPort = freePort();
        Peer auditor = IntakeFleet.startPeer("auditor", seedPort, 0);
        Peer careful = IntakeFleet.startPeer("extract-careful", freePort(), seedPort);
        Peer fast = IntakeFleet.startPeer("extract-fast", freePort(), seedPort);
        List<Peer> fleet = List.of(auditor, careful, fast);
        try (AutoCloseable a = IntakeFleet.runExtractor(careful, 0.95);
             AutoCloseable b = IntakeFleet.runExtractor(fast, 0.6)) {
            Thread.sleep(1500);

            auditor.intake().write(new ScanEntry("scan-777", "smudged form"),
                    Lease.of(Duration.ofMinutes(30)));

            Optional<FilingEntry> filing = IntakeFleet.auditScan(auditor, "scan-777", 3);

            assertThat(filing).isPresent();
            assertThat(filing.get().extractor()).isEqualTo("extract-careful");
            assertThat(filing.get().taxpayerId()).isEqualTo("TIN-88-1234567");
            assertThat(filing.get().approvedBy()).contains("3-vote quorum");

            // The pipeline consumed its intermediates; the filing replicated.
            assertThat(auditor.intake().readAll(
                    Template.of(ExtractionCandidate.class), 10)).isEmpty();
            assertThat(careful.intake().read(Template.of(FilingEntry.class),
                    Duration.ofSeconds(20))).isPresent();
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
