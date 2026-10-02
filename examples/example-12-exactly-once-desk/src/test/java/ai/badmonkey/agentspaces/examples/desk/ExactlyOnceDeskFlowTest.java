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
package ai.badmonkey.agentspaces.examples.desk;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.examples.desk.ExactlyOnceDesk.PaymentOrder;
import ai.badmonkey.agentspaces.examples.desk.ExactlyOnceDesk.PaymentReceipt;
import ai.badmonkey.agentspaces.examples.desk.ExactlyOnceDesk.Peer;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Example 12 end to end over TCP: the leader is discoverable, every order is
 * confirmed exactly once, and the orders drain from the desk's replica — the
 * one that holds no claim and learns of each completion only by gossip.
 */
class ExactlyOnceDeskFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(120)
    void everyOrderIsConfirmedOnceAndDrainsEverywhere() throws Exception {
        List<PeerIdentity> ids = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> members = ids.stream().map(PeerIdentity::peerId).toList();
        int seedPort = freePort();
        Peer a = ExactlyOnceDesk.startClerk(ids.get(0), "clerk-a", seedPort, 0, members, 11);
        Peer b = ExactlyOnceDesk.startClerk(ids.get(1), "clerk-b", freePort(), seedPort, members, 12);
        Peer c = ExactlyOnceDesk.startClerk(ids.get(2), "clerk-c", freePort(), seedPort, members, 13);
        Peer desk = ExactlyOnceDesk.startDesk("desk", freePort(), seedPort);
        List<Peer> fleet = List.of(a, b, c, desk);
        List<AutoCloseable> clerks = List.of(ExactlyOnceDesk.runClerk(a),
                ExactlyOnceDesk.runClerk(b), ExactlyOnceDesk.runClerk(c));
        try {
            // SPEC §8: the leader lease is an advertisement. The desk, a non-member
            // of the log, finds the leader through discovery alone (QA4 A4-3).
            Optional<CapabilityAdvertisement> leader = await(
                    () -> ExactlyOnceDesk.leaderSeenBy(desk), Duration.ofSeconds(40));
            assertThat(leader).as("role=leader is discoverable by the desk; roles seen: "
                    + desk.discovery().find(CapabilityAdvertisement.class, ad -> true).stream()
                            .map(ad -> ad.parameters().getOrDefault("role", "?")).toList())
                    .isPresent();
            assertThat(members).contains(leader.orElseThrow().issuer());
            assertThat(leader.orElseThrow().parameters()).containsKey("leaseUntil");
            assertThat(fleet.stream().filter(Peer::leads).count())
                    .as("exactly one clerk leads").isEqualTo(1L);

            for (int i = 1; i <= 3; i++) {
                desk.payments().write(new PaymentOrder("ord-" + i, "payee-" + i, 1000L * i),
                        Lease.of(Duration.ofMinutes(10)));
            }

            // Exactly once: three receipts, one per order, whoever won each.
            List<PaymentReceipt> receipts = await(() -> {
                List<PaymentReceipt> all = desk.payments()
                        .readAll(Template.of(PaymentReceipt.class), 10);
                return all.size() >= 3 ? Optional.of(all) : Optional.<List<PaymentReceipt>>empty();
            }, Duration.ofSeconds(40)).orElseThrow();
            assertThat(receipts).extracting(PaymentReceipt::orderId)
                    .containsExactlyInAnyOrder("ord-1", "ord-2", "ord-3");
            assertThat(receipts).allSatisfy(r -> assertThat(r.clerk()).startsWith("clerk-"));

            // QA4 A4-5: the completed orders drain at the desk, which never held a
            // claim and authenticates each completion against the holder's own
            // signed claim carried by the log's decision. Before the fix the
            // orders stayed visible here for as long as the fleet ran.
            assertThat(await(() -> desk.payments().readAll(Template.of(PaymentOrder.class), 10)
                            .isEmpty() ? Optional.of(Boolean.TRUE) : Optional.<Boolean>empty(),
                    Duration.ofSeconds(30)))
                    .as("orders drained at the desk's replica").isPresent();
            for (Peer clerk : List.of(a, b, c)) {
                assertThat(clerk.payments().readAll(Template.of(PaymentOrder.class), 10))
                        .as("orders drained at " + clerk.name()).isEmpty();
            }
        } finally {
            for (AutoCloseable clerk : clerks) {
                clerk.close();
            }
            fleet.forEach(Peer::close);
        }
    }

    private static <T> Optional<T> await(Supplier<Optional<T>> probe, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<T> value = probe.get();
            if (value.isPresent()) {
                return value;
            }
            Thread.sleep(200);
        }
        return probe.get();
    }
}
