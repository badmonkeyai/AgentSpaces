/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Closing a node closes every link it attached, the ones peers dialed to it as well as its own (2026-11-19). A link a
 * peer dialed, left open by a closed node, kept that peer sending into a reader nobody served; when the same identity
 * came back from a restart on a new port, the peer's replies went into the dead link and a reconcile never got an
 * answer (example 34's store restart).
 */
class CloseEveryLinkTest {

    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zCloseLinks",
            PeerIdentity.generate().peerId(), GroupId.of("zCloseLinks"), Instant.EPOCH, Duration.ofDays(1), "links",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private static int freePort() throws Exception {
        // below the ephemeral range, so no other process takes it before the test binds it (parallel forks)
        return ai.badmonkey.agentspaces.test.Ports.free();
    }

    private void tick(int times) throws InterruptedException {
        for (int i = 0; i < times; i++) {
            nodes.forEach(PeerNode::tick);
            Thread.sleep(50);
        }
    }

    @Test
    void aClosedNodeClosesTheLinksItsPeersDialedSoTheyForgetThem() throws Exception {
        int pa = freePort();
        int pb = freePort();
        PeerNode a = PeerNode.builder(PeerIdentity.generate()).build();
        PeerNode b = PeerNode.builder(PeerIdentity.generate()).build();
        nodes.add(a);
        nodes.add(b);
        a.listen(new TcpTransport(), "127.0.0.1:" + pa);
        b.listen(new TcpTransport(), "127.0.0.1:" + pb);
        a.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + pb, 0)));
        b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + pa, 0)));
        tick(40);
        assertThat(b.connectionSchemes()).as("b holds a link to a").containsKey(a.peerId());

        a.close();
        nodes.remove(a);
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (b.connectionSchemes().containsKey(a.peerId()) && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(b.connectionSchemes()).as("every link to the closed node was closed, so b forgot it at once")
                .doesNotContainKey(a.peerId());
    }
}
