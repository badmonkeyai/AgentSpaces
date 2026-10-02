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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole fabric over real TLS with channel authentication on: two nodes in
 * ATTESTED mode converge membership over loopback TLS, and after the
 * CHANNEL_HELLO exchange their frames travel without envelope signatures.
 */
class TlsFabricTest {

    @Test
    @Timeout(60)
    void twoAttestedNodesConvergeOverBareFrames() throws Exception {
        int portA = freePort();
        int portB = freePort();
        GroupId groupId = GroupId.fromFounding("tls-fabric".getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(1), "tls-fabric",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());

        PeerIdentity identityA = PeerIdentity.generate();
        PeerIdentity identityB = PeerIdentity.generate();
        PeerNode a = PeerNode.builder(identityA)
                .channelAuth(PeerNode.ChannelAuth.ATTESTED).build();
        PeerNode b = PeerNode.builder(identityB)
                .channelAuth(PeerNode.ChannelAuth.ATTESTED).build();
        try {
            a.listen(new TlsTcpTransport(identityA), "127.0.0.1:" + portA);
            b.listen(new TlsTcpTransport(identityB), "127.0.0.1:" + portB);

            GroupRuntime runtimeA = a.joinGroup(groupAd,
                    new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                    List.of());
            GroupRuntime runtimeB = b.joinGroup(groupAd,
                    new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                    List.of(new PeerAdvertisement.Endpoint("tls", "127.0.0.1:" + portA, 0)));
            a.startTicking(Duration.ofMillis(200));
            b.startTicking(Duration.ofMillis(200));

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            boolean converged = false;
            while (!converged && System.nanoTime() < deadline) {
                boolean aSeesB = runtimeA.membership().allMembers().stream()
                        .anyMatch(member -> member.id().equals(b.peerId()));
                boolean bSeesA = runtimeB.membership().allMembers().stream()
                        .anyMatch(member -> member.id().equals(a.peerId()));
                converged = aSeesB && bSeesA
                        && a.bareFramesSent() > 0 && b.bareFramesSent() > 0;
                if (!converged) {
                    Thread.sleep(100);
                }
            }

            assertThat(converged)
                    .as("membership converged and both nodes sent bare frames")
                    .isTrue();
            assertThat(a.channelModes()).containsEntry(b.peerId(), "attested");
            assertThat(b.channelModes()).containsEntry(a.peerId(), "attested");
        } finally {
            a.close();
            b.close();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
