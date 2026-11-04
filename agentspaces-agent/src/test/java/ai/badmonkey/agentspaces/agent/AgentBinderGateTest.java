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

package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.constraints.Constraint;
import ai.badmonkey.agentspaces.constraints.ConstraintGate;
import ai.badmonkey.agentspaces.constraints.ConstraintViolated;
import ai.badmonkey.agentspaces.constraints.Vocabulary;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** ISSUE-Constraints FR-8, FR-10: the gated binder refuses a claim the marking does not enable, and a result write. */
class AgentBinderGateTest {

    public record HumanApproval(String claimId, String by) {
    }

    public record PaymentOrder(String claimId, long amount) {
    }

    public record Paid(String claimId, long amount) {
    }

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace payments = LocalSpace.builder("payments", identity.agent("host")).sweepEvery(Duration.ofMillis(50)).build();
    private final LocalSpace approvals = LocalSpace.builder("approvals", identity.agent("host")).sweepEvery(Duration.ofMillis(50)).build();
    private final LocalSpace constraints = LocalSpace.builder("constraints", identity.agent("host")).sweepEvery(Duration.ofMillis(50)).build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zGate"), null, InstantSource.system())
            .space("payments", payments).space("approvals", approvals).space("constraints", constraints);
    private ConstraintGate gate;

    @AfterEach
    void tearDown() {
        binder.close();
        if (gate != null) {
            gate.close();
        }
        payments.close();
        approvals.close();
        constraints.close();
    }

    @AgentSpec(name = "payer", description = "Pays orders")
    public static class Payer {
        final AtomicInteger paid = new AtomicInteger();

        @SpaceTake(space = "payments", lease = "2s", pollTimeout = "PT0.1S", resultLease = "1m")
        public Paid pay(PaymentOrder order) {
            paid.incrementAndGet();
            return new Paid(order.claimId(), order.amount());
        }
    }

    private void publish(Constraint... cs) {
        Vocabulary v = new Vocabulary(1, "rb", 1, "h1", Map.of());
        constraints.write(v, Lease.of(Duration.ofDays(1)), v.tags());
        for (Constraint c : cs) {
            constraints.write(c, Lease.of(Duration.ofDays(1)), c.tags("h1"));
        }
        gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "gate");
        gate.watch("payments", payments);
        gate.watch("approvals", approvals);
        binder.constraintGate(gate);
    }

    @Test
    void aPrecedenceHoldsTheClaimUntilTheApprovalArrives() throws Exception {
        publish(new Constraint(1, "rb", 1, "k1", Constraint.PRECEDENCE, "HumanApproval", List.of(), "PaymentOrder", List.of(),
                0, "", "field:claimId", List.of("gate"), Map.of()));
        Payer payer = new Payer();
        binder.bind(payer);
        payments.write(new PaymentOrder("c-1", 100), Lease.of(Duration.ofMinutes(5)));
        Thread.sleep(700);
        assertThat(payer.paid.get()).as("not taken before an approval").isZero();
        assertThat(payments.readAll(Template.of(PaymentOrder.class), 10)).as("the entry stays").hasSize(1);
        assertThat(constraints.readAll(Template.of(ConstraintViolated.class), 10)).singleElement()
                .satisfies(v -> assertThat(v.event()).isEqualTo(ConstraintViolated.CLAIM));
        approvals.write(new HumanApproval("c-1", "alice"), Lease.of(Duration.ofMinutes(5)));
        for (int i = 0; i < 50 && payer.paid.get() == 0; i++) {
            Thread.sleep(100);
        }
        assertThat(payer.paid.get()).isEqualTo(1);
        assertThat(payments.readAll(Template.of(Paid.class), 10)).hasSize(1);
        // another key's order is held while c-1's went through
        payments.write(new PaymentOrder("c-2", 100), Lease.of(Duration.ofMinutes(5)));
        Thread.sleep(500);
        assertThat(payer.paid.get()).isEqualTo(1);
    }

    @Test
    void aCardinalityRefusesTheSecondResultAndTheTakeLapses() throws Exception {
        publish(new Constraint(1, "rb", 1, "k2", Constraint.CARDINALITY, "Paid", List.of(), "", List.of(), 1, "",
                "field:claimId", List.of("gate"), Map.of()));
        Payer payer = new Payer();
        java.util.List<String> evts = new java.util.concurrent.CopyOnWriteArrayList<>();
        payments.notify(Template.of(Object.class), ev -> evts.add(ev.kind() + ":" + ev.entry().getClass().getSimpleName()), Lease.of(Duration.ofMinutes(5)));
        binder.bind(payer);
        payments.write(new PaymentOrder("c-7", 10), Lease.of(Duration.ofMinutes(5)));
        for (int i = 0; i < 50 && payments.readAll(Template.of(Paid.class), 10).isEmpty(); i++) {
            Thread.sleep(100);
        }
        assertThat(payments.readAll(Template.of(Paid.class), 10)).hasSize(1);
        payments.write(new PaymentOrder("c-7", 11), Lease.of(Duration.ofMinutes(5)));
        for (int i = 0; i < 50 && constraints.readAll(Template.of(ConstraintViolated.class), 10).isEmpty(); i++) {
            Thread.sleep(100);
        }
        assertThat(constraints.readAll(Template.of(ConstraintViolated.class), 10)).anySatisfy(v -> {
            assertThat(v.event()).isEqualTo(ConstraintViolated.RESULT);
            assertThat(v.unit()).isEqualTo("k2");
        });
        assertThat(payments.readAll(Template.of(Paid.class), 10)).as("the second result was not written").hasSize(1);
        assertThat(evts).contains("WRITTEN:Paid").doesNotContain("COMPLETED:Paid");
        // the take lease lapses and the order reappears for a peer whose marking admits it (this worker would
        // take and refuse it again, so it stops first)
        binder.close();
        Thread.sleep(2500);
        assertThat(payments.readAll(Template.of(PaymentOrder.class), 10)).hasSize(1);
    }
}
