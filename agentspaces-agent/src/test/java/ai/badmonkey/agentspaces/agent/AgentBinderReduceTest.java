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
package ai.badmonkey.agentspaces.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Part;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.agent.reduce.Reductions;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.TestClock;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** ISSUE-SpaceReduce §14: the persistent accumulator over a {@link LocalSpace}. */
class AgentBinderReduceTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final PeerIdentity identity = PeerIdentity.generate();
    private final TestClock clock = TestClock.create();
    private final LocalSpace ledger = LocalSpace.builder("ledger", identity.agent("host")).clock(clock).build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zReduce"), null, clock)
            .space("ledger", ledger);

    @AfterEach
    void tearDown() {
        binder.close();
        ledger.close();
    }

    public record Payment(String account, long cents) {
    }

    public record Balance(String account, long cents, int payments) {
    }

    public record Alert(String account, long cents) {
    }

    @AgentSpec(name = "teller", description = "Keeps a running balance per account", goals = {"balance"})
    public static class Teller {
        final List<Balance> seen = new CopyOnWriteArrayList<>();
        /** The accumulator each step was handed, as key:payments; a lost update shows as a duplicate. */
        final List<String> handed = new CopyOnWriteArrayList<>();
        final long stepMillis;

        Teller() {
            this(0);
        }

        Teller(long stepMillis) {
            this.stepMillis = stepMillis;
        }

        @SpaceReduce(space = "ledger", key = "account", lease = "2s", pollTimeout = "100ms", accumulatorLease = "1h")
        public Balance apply(Balance balance, Payment payment) {
            seen.add(balance);
            handed.add(payment.account() + ":" + (balance == null ? 0 : balance.payments()));
            if (stepMillis > 0) {
                try {
                    Thread.sleep(stepMillis); // widen the read-fold-complete window: without the lock, two reducers collide
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            long cents = (balance == null ? 0 : balance.cents()) + payment.cents();
            int count = (balance == null ? 0 : balance.payments()) + 1;
            return new Balance(payment.account(), cents, count);
        }
    }

    @Test
    void aRunningTotalPerKeyFoldsEveryElementOnceAndLeavesOneAccumulatorPerKey() throws Exception {
        Teller teller = new Teller();
        AgentBinder.Bound bound = binder.bind(teller);
        for (int i = 1; i <= 5; i++) {
            ledger.write(new Payment("acct-a", i), HOUR);
            ledger.write(new Payment("acct-b", 10L * i), HOUR);
        }
        await(() -> ledger.readAll(Template.of(Payment.class), 20).isEmpty(), Duration.ofSeconds(10));
        // An element is invisible while claimed, so wait for the accumulators to say five.
        await(() -> List.of("acct-a", "acct-b").stream().allMatch(k -> Reductions
                .current(ledger, Balance.class, "teller.apply", k).map(e -> e.value().payments() == 5).orElse(false)),
                Duration.ofSeconds(10));
        Balance a = Reductions.current(ledger, Balance.class, "teller.apply", "acct-a").orElseThrow().value();
        Balance b = Reductions.current(ledger, Balance.class, "teller.apply", "acct-b").orElseThrow().value();
        assertThat(a).isEqualTo(new Balance("acct-a", 15, 5));
        assertThat(b).isEqualTo(new Balance("acct-b", 150, 5));
        assertThat(Reductions.steps(Reductions.current(ledger, Balance.class, "teller.apply", "acct-a").get()))
                .isEqualTo(5);
        assertThat(teller.seen.get(0)).as("the first step sees null").isNull();
        assertThat(ledger.readAll(Template.of(Balance.class), 20)).as("retired predecessors are gone").hasSize(2);
        await(() -> ledger.readAll(Template.of(JoinTicket.class), 20).isEmpty(), Duration.ofSeconds(5));
        assertThat(bound.card().actions()).anySatisfy(action -> {
            assertThat(action.kind()).isEqualTo(CardAction.REDUCE);
            assertThat(action.consumes()).containsExactly(Payment.class.getName() + "#v1");
            assertThat(action.produces()).containsExactly(Balance.class.getName() + "#v1");
        });
    }

    @AgentSpec(name = "picky", description = "Skips refunds", goals = {"balance"})
    public static class Picky {
        @SpaceReduce(space = "ledger", key = "account", lease = "2s", pollTimeout = "100ms")
        public Balance apply(Balance balance, Payment payment) {
            if (payment.cents() < 0) {
                return null;                    // consumed, nothing changes
            }
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents(),
                    (balance == null ? 0 : balance.payments()) + 1);
        }
    }

    @Test
    void aNullReturnConsumesTheElementAndLeavesTheAccumulator() throws Exception {
        binder.bind(new Picky());
        ledger.write(new Payment("acct-c", 7), HOUR);
        ledger.write(new Payment("acct-c", -3), HOUR);
        await(() -> ledger.readAll(Template.of(Payment.class), 20).isEmpty(), Duration.ofSeconds(10));
        assertThat(Reductions.current(ledger, Balance.class, "picky.apply", "acct-c").orElseThrow().value())
                .isEqualTo(new Balance("acct-c", 7, 1));
    }

    @AgentSpec(name = "alerting", description = "Balances with an alert over a limit", goals = {"balance"})
    public static class Alerting {
        @SpaceReduce(space = "ledger", key = "account", lease = "2s", pollTimeout = "100ms",
                produces = Alert.class)
        public Entries apply(Balance balance, Payment payment) {
            Balance next = new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents(),
                    (balance == null ? 0 : balance.payments()) + 1);
            return next.cents() > 100 ? Entries.of(Tagged.of(next, "flag", "hot"), new Alert(next.account(), next.cents()))
                    : Entries.of(next);
        }
    }

    @Test
    void aForkCarriesTheAccumulatorAtomicallyAndTheSideOutputsAfterIt() throws Exception {
        AgentBinder.Bound bound = binder.bind(new Alerting());
        ledger.write(new Payment("acct-d", 60), HOUR);
        ledger.write(new Payment("acct-d", 60), HOUR);
        await(() -> !ledger.readAll(Template.of(Alert.class), 20).isEmpty(), Duration.ofSeconds(10));
        assertThat(ledger.readAll(Template.of(Alert.class), 20)).containsExactly(new Alert("acct-d", 120));
        Space.Entry<Balance> current = Reductions.current(ledger, Balance.class, "alerting.apply", "acct-d").orElseThrow();
        assertThat(current.value()).isEqualTo(new Balance("acct-d", 120, 2));
        assertThat(current.tags()).as("the Tagged tags merge beneath the binder's")
                .containsEntry("flag", "hot").containsEntry(Reductions.REDUCE_TAG, "alerting.apply")
                .containsEntry(Reductions.KEY_TAG, "acct-d").containsEntry(Reductions.STEPS_TAG, "2");
        assertThat(bound.card().actions().get(0).produces())
                .containsExactly(Balance.class.getName() + "#v1", Alert.class.getName() + "#v1");
    }

    @AgentSpec(name = "flaky", description = "Throws once", goals = {"balance"})
    public static class Flaky {
        final AtomicInteger calls = new AtomicInteger();

        @SpaceReduce(space = "ledger", key = "account", lease = "300ms", pollTimeout = "100ms")
        public Balance apply(Balance balance, Payment payment) {
            if (calls.getAndIncrement() == 0) {
                throw new IllegalStateException("first call fails after nothing was written");
            }
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents(),
                    (balance == null ? 0 : balance.payments()) + 1);
        }
    }

    @Test
    void aStepThatThrowsLetsTheElementReappearAndItIsFoldedOnce() throws Exception {
        LocalSpace sweeping = LocalSpace.builder("sweeping", identity.agent("host"))
                .sweepEvery(Duration.ofMillis(50)).build();
        AgentBinder own = new AgentBinder(identity, GroupId.of("zReduce"), null, InstantSource.system())
                .space("ledger", sweeping);
        try {
            Flaky flaky = new Flaky();
            own.bind(flaky);
            sweeping.write(new Payment("acct-e", 5), HOUR);
            await(() -> sweeping.readAll(Template.of(Payment.class), 20).isEmpty()
                    && Reductions.current(sweeping, Balance.class, "flaky.apply", "acct-e").isPresent(),
                    Duration.ofSeconds(10));
            assertThat(Reductions.current(sweeping, Balance.class, "flaky.apply", "acct-e").get().value())
                    .isEqualTo(new Balance("acct-e", 5, 1));
            assertThat(flaky.calls.get()).isEqualTo(2);
        } finally {
            own.close();
            sweeping.close();
        }
    }

    @Test
    void elementsWrittenBeforeBindAreFolded() throws Exception {
        ledger.write(new Payment("acct-f", 1), HOUR);
        ledger.write(new Payment("acct-f", 2), HOUR);
        binder.bind(new Teller());
        await(() -> Reductions.current(ledger, Balance.class, "teller.apply", "acct-f")
                .map(e -> e.value().payments() == 2).orElse(false), Duration.ofSeconds(10));
        assertThat(Reductions.current(ledger, Balance.class, "teller.apply", "acct-f").orElseThrow().value())
                .isEqualTo(new Balance("acct-f", 3, 2));
    }

    @Test
    void twoReducersOverOneSpaceFoldEveryElementExactlyOnce() throws Exception {
        AgentBinder other = new AgentBinder(identity, GroupId.of("zReduce"), null, clock).space("ledger", ledger);
        try {
            Teller one = new Teller(15);
            Teller two = new Teller(15);
            binder.bind(one);
            other.bind(two);
            long[] expected = new long[2];
            for (int i = 0; i < 60; i++) {
                int account = i % 2;
                ledger.write(new Payment("acct-" + account, i), HOUR);
                expected[account] += i;
            }
            await(() -> ledger.readAll(Template.of(Payment.class), 300).isEmpty(), Duration.ofSeconds(60));
            await(() -> {
                for (int account = 0; account < 2; account++) {
                    var current = Reductions.current(ledger, Balance.class, "teller.apply", "acct-" + account);
                    if (current.isEmpty() || current.get().value().payments() != 30) {
                        return false;
                    }
                }
                return true;
            }, Duration.ofSeconds(10));
            for (int account = 0; account < 2; account++) {
                Balance balance = Reductions.current(ledger, Balance.class, "teller.apply", "acct-" + account)
                        .orElseThrow().value();
                assertThat(balance.cents()).as("acct-" + account).isEqualTo(expected[account]);
                assertThat(balance.payments()).isEqualTo(30);
            }
            // The lock: every step saw its predecessor's result, so no two steps of one
            // key were handed the same accumulator (which is what a lost update looks like).
            List<String> handed = new java.util.ArrayList<>(one.handed);
            handed.addAll(two.handed);
            assertThat(handed).as("each (key, predecessor) handed to exactly one step").doesNotHaveDuplicates();
            assertThat(one.handed).as("both reducers took part").isNotEmpty();
            assertThat(two.handed).isNotEmpty();
        } finally {
            other.close();
        }
    }

    @AgentSpec(name = "watcher", description = "Hears a key go quiet", goals = {"watch"})
    public static class Watcher {
        final List<Balance> lapsed = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "ledger", on = SpaceEvent.Kind.EXPIRED)
        public Balance quiet(Balance balance) {
            lapsed.add(balance);
            return null;
        }
    }

    @AgentSpec(name = "short", description = "A short idle timeout", goals = {"balance"})
    public static class ShortLived {
        @SpaceReduce(space = "ledger", key = "account", lease = "1s", pollTimeout = "100ms", accumulatorLease = "2s")
        public Balance apply(Balance balance, Payment payment) {
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents(),
                    (balance == null ? 0 : balance.payments()) + 1);
        }
    }

    @Test
    void anIdleKeyLapsesWithItsTicketAndStartsAgainFromNull() throws Exception {
        Watcher watcher = new Watcher();
        binder.bind(watcher);
        binder.bind(new ShortLived());
        ledger.write(new Payment("acct-g", 4), HOUR);
        await(() -> Reductions.current(ledger, Balance.class, "short.apply", "acct-g").isPresent(),
                Duration.ofSeconds(10));
        clock.advance(Duration.ofSeconds(3));
        ledger.sweepNow();
        await(() -> !watcher.lapsed.isEmpty(), Duration.ofSeconds(5));
        assertThat(watcher.lapsed).containsExactly(new Balance("acct-g", 4, 1));
        assertThat(ledger.readAll(Template.of(JoinTicket.class), 20)).as("no ticket outlives a drain").isEmpty();
        ledger.write(new Payment("acct-g", 6), HOUR);
        await(() -> Reductions.current(ledger, Balance.class, "short.apply", "acct-g")
                .map(e -> e.value().cents() == 6).orElse(false), Duration.ofSeconds(10));
        assertThat(Reductions.current(ledger, Balance.class, "short.apply", "acct-g").get().value())
                .isEqualTo(new Balance("acct-g", 6, 1));
    }

    // ------------------------------------------------------------------ refusals

    @AgentSpec(name = "bad-shape", description = "Wrong signature", goals = {"x"})
    public static class WrongShape {
        @SpaceReduce(space = "ledger", key = "account")
        public Balance apply(Payment payment) {
            return null;
        }
    }

    @AgentSpec(name = "bad-return", description = "Wrong return", goals = {"x"})
    public static class WrongReturn {
        @SpaceReduce(space = "ledger", key = "account")
        public Alert apply(Balance balance, Payment payment) {
            return null;
        }
    }

    @AgentSpec(name = "bad-key", description = "Unknown key", goals = {"x"})
    public static class UnknownKey {
        @SpaceReduce(space = "ledger", key = "nope")
        public Balance apply(Balance balance, Payment payment) {
            return null;
        }
    }

    @AgentSpec(name = "ordered", description = "No coordinator", goals = {"x"})
    public static class NoCoordinator {
        @SpaceReduce(space = "ledger", key = "account", mode = SpaceReduce.Mode.ORDERED)
        public Balance apply(Balance balance, Payment payment) {
            return null;
        }
    }

    @AgentSpec(name = "clash", description = "A join and a reduce with one name", goals = {"x"})
    public static class NameClash {
        @SpaceJoin(space = "ledger", key = "account", name = "shared", parts = {@Part(Payment.class), @Part(Balance.class)})
        public Alert join(Joined j) {
            return null;
        }

        @SpaceReduce(space = "ledger", key = "account", name = "shared")
        public Balance apply(Balance balance, Payment payment) {
            return null;
        }
    }

    @AgentSpec(name = "short-lease", description = "accumulator lease below take lease", goals = {"x"})
    public static class LeaseTooShort {
        @SpaceReduce(space = "ledger", key = "account", lease = "1m", accumulatorLease = "10s")
        public Balance apply(Balance balance, Payment payment) {
            return null;
        }
    }

    @Test
    void refusalsAtBindTimeNameTheMethod() {
        assertThatThrownBy(() -> binder.bind(new WrongShape())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("accumulator and the element");
        assertThatThrownBy(() -> binder.bind(new WrongReturn())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("returns A, Tagged<A>, or Entries");
        assertThatThrownBy(() -> binder.bind(new UnknownKey())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nope");
        assertThatThrownBy(() -> binder.bind(new NoCoordinator())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ordered-log coordinator");
        assertThatThrownBy(() -> binder.bind(new NameClash())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shared");
        assertThatThrownBy(() -> binder.bind(new LeaseTooShort())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shorter than the take lease");
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(20);
        }
    }
}
