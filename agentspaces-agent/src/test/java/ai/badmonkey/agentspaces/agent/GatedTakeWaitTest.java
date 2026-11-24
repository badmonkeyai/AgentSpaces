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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.constraints.Constraint;
import ai.badmonkey.agentspaces.constraints.ConstraintGate;
import ai.badmonkey.agentspaces.constraints.Vocabulary;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * srad/agentspaces-event-loop.md EL-W-07: a gated take waits for a change of its space or of the gate (a lane's
 * event, a version, a jury decision), or the gate's own next due, under the one 30 s backstop. It used to look every
 * second. On virtual time: no sleeps, and a wait that only a change can end is seen as such.
 */
class GatedTakeWaitTest {

    public record HumanApproval(String claimId, String by) {
    }

    public record PaymentOrder(String claimId, long amount) {
    }

    public record Paid(String claimId, long amount) {
    }

    @AgentSpec(name = "payer", description = "Pays orders")
    public static class Payer {
        final CountDownLatch paid = new CountDownLatch(1);

        @SpaceTake(space = "payments", lease = "2s", pollTimeout = "PT0.1S", resultLease = "1m")
        public Paid pay(PaymentOrder order) {
            paid.countDown();
            return new Paid(order.claimId(), order.amount());
        }
    }

    private final VirtualTime time = VirtualTime.create();
    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace payments = LocalSpace.builder("payments", identity.agent("host")).clock(time).build();
    private final LocalSpace approvals = LocalSpace.builder("approvals", identity.agent("host")).clock(time).build();
    private final LocalSpace constraints = LocalSpace.builder("constraints", identity.agent("host")).clock(time).build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zGateWait"), null, time)
            .space("payments", payments).space("approvals", approvals).space("constraints", constraints);
    /** One permit each time the take's thread starts a wait on the time. */
    private final Semaphore parks = new Semaphore(0);
    private volatile Thread taker;
    private final AutoCloseable parkWatch = time.onOutsideWait(() -> {
        if (Thread.currentThread().getName().startsWith("space-take-")) {
            taker = Thread.currentThread();
            parks.release();
        }
    });
    private ConstraintGate gate;

    @AfterEach
    void tearDown() throws Exception {
        binder.close();
        if (gate != null) {
            gate.close();
        }
        parkWatch.close();
        payments.close();
        approvals.close();
        constraints.close();
    }

    private void publishPrecedence() {
        Vocabulary v = new Vocabulary(1, "rb", 1, "h1", Map.of());
        constraints.write(v, Lease.of(Duration.ofDays(1)), v.tags());
        Constraint c = new Constraint(1, "rb", 1, "k1", Constraint.PRECEDENCE, "HumanApproval", List.of(), "PaymentOrder",
                List.of(), 0, "", "field:claimId", List.of("gate"), Map.of());
        constraints.write(c, Lease.of(Duration.ofDays(1)), c.tags("h1"));
        gate = ConstraintGate.attach(constraints, null, "constraints", time, "gate");
        gate.watch("payments", payments);
        gate.watch("approvals", approvals);
        binder.constraintGate(gate);
    }

    /** Binds the payer with an order the precedence refuses, and returns once its take waits, refused. */
    private Payer bindRefused() throws Exception {
        publishPrecedence();
        Payer payer = new Payer();
        binder.bind(payer);
        payments.write(new PaymentOrder("c-1", 100), Lease.of(Duration.ofHours(2)));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (gate.claimJudgements() == 0 || parks.availablePermits() == 0) {
            assertThat(parks.tryAcquire(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                    .as("the take looked and waits").isTrue();
            parks.release();
        }
        parks.drainPermits();
        assertThat(payer.paid.getCount()).as("refused before an approval").isEqualTo(1);
        return payer;
    }

    /**
     * Returns once the take's thread waits on the time again with a deadline armed: a wait the advance released has
     * looked and parked again (its timer task ran and a new one is queued), and one it did not release still waits.
     */
    private void waitsAgain() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!(time.waitingOutside(taker) && time.pending() > 0)) {
            assertThat(System.nanoTime() - deadline).as("the take waits again (pending %d)", time.pending()).isNegative();
            Thread.onSpinWait();
        }
    }

    @Test
    void aLaneEventEnablesTheWaitingTakeWithNoTimePassing() throws Exception {
        Payer payer = bindRefused();
        long monoBefore = time.monoNanos();
        approvals.write(new HumanApproval("c-1", "alice"), Lease.of(Duration.ofHours(2)));
        assertThat(payer.paid.await(5, TimeUnit.SECONDS))
                .as("the approval in another space woke the gated take; no virtual time passed").isTrue();
        assertThat(time.monoNanos()).isEqualTo(monoBefore);
    }

    @Test
    void anIdleRefusedTakeLooksAtTheBackstopNotEverySecond() throws Exception {
        Payer payer = bindRefused();
        long before = gate.claimJudgements();
        for (int s = 0; s < 120; s++) {
            time.advance(Duration.ofSeconds(1));
            waitsAgain();
        }
        long looks = gate.claimJudgements() - before;
        // 120 s at the 30 s backstop: four looks (each judges the one order); a look a second read 120
        assertThat(looks).as("claim judgements in two idle minutes").isBetween(3L, 5L);
        assertThat(payer.paid.getCount()).isEqualTo(1);
        assertThat(gate.nextClaimDue()).as("no claim judgement moves with time alone").isEmpty();
    }
}
