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
package ai.badmonkey.agentspaces.examples.quickstart;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.examples.quickstart.Quickstart.Order;
import ai.badmonkey.agentspaces.examples.quickstart.Quickstart.Progress;
import ai.badmonkey.agentspaces.examples.quickstart.Quickstart.Receipt;
import ai.badmonkey.agentspaces.examples.quickstart.Quickstart.Shipment;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The quickstart pipeline over one local space: worker, choreography, receipts. */
class QuickstartTest {

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace space = LocalSpace.builder("work", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50))
            .build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zQuick"),
            null, InstantSource.system());

    @AfterEach
    void tearDown() {
        binder.close();
        space.close();
    }

    @Test
    @Timeout(30)
    void ordersFlowToShipmentsToReceiptsWithNoSpaceNamesAnywhere() throws Exception {
        binder.space("work", space);
        binder.bind(new Quickstart.Fulfiller());
        binder.bind(new Quickstart.Auditor());

        space.write(new Order("ord-1", "kite"), Lease.of(Duration.ofMinutes(10)));
        space.write(new Order("ord-2", "compass"), Lease.of(Duration.ofMinutes(10)));

        List<Receipt> receipts = List.of();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (receipts.size() < 2 && System.nanoTime() < deadline) {
            receipts = space.readAll(Template.of(Receipt.class), 10);
            Thread.sleep(50);
        }

        // Orders were taken and shipped, shipments audited into receipts, and
        // the worker's @SpaceRef progress markers landed — all through the one
        // inferred space.
        assertThat(receipts).extracting(Receipt::summary).containsExactlyInAnyOrder(
                "kite shipped by fulfiller", "compass shipped by fulfiller");
        assertThat(space.readAll(Template.of(Shipment.class), 10)).hasSize(2);
        assertThat(space.readAll(Template.of(Progress.class), 10))
                .extracting(Progress::note)
                .containsExactlyInAnyOrder("picking kite", "picking compass");
        assertThat(space.readAll(Template.of(Order.class), 10)).isEmpty();
    }
}
