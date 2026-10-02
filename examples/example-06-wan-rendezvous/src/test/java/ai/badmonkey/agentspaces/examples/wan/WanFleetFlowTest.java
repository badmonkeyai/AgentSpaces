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
package ai.badmonkey.agentspaces.examples.wan;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.examples.wan.WanFleet.Peer;
import ai.badmonkey.agentspaces.examples.wan.WanFleet.SiteResult;
import ai.badmonkey.agentspaces.examples.wan.WanFleet.SiteTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Two sites joined through one rendezvous address, over real TCP. */
class WanFleetFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(90)
    void sitesConvergeThroughTheRendezvousAndShareWork() throws Exception {
        int hqPort = freePort();
        Peer hq = WanFleet.startPeer("hq", hqPort, 0, true);
        Peer siteA = WanFleet.startPeer("siteA", freePort(), hqPort, false);
        Peer siteB = WanFleet.startPeer("siteB", freePort(), hqPort, false);
        List<Peer> fleet = List.of(hq, siteA, siteB);
        try {
            // Everyone learns everyone through the one configured address.
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (siteB.runtime().membership().allMembers().size() < 2
                    && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertThat(siteB.runtime().membership().allMembers()).hasSize(2);
            assertThat(siteB.runtime().membership()
                    .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS))
                    .containsExactly(hq.node().peerId());

            // Work written at one site completes at the other.
            siteB.tasks().write(new SiteTask("cross-site", "site-B"),
                    Lease.of(Duration.ofMinutes(10)));
            var taken = siteA.tasks().take(Template.of(SiteTask.class),
                    Lease.of(Duration.ofMinutes(5)), Duration.ofSeconds(20));
            assertThat(taken).isPresent();
            siteA.tasks().complete(taken.get(),
                    new SiteResult("cross-site", "site-B", "site-A"),
                    Lease.of(Duration.ofHours(1)));

            assertThat(siteB.tasks().read(Template.of(SiteResult.class),
                    Duration.ofSeconds(20)))
                    .hasValueSatisfying(r -> assertThat(r.completedBy()).isEqualTo("site-A"));
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
