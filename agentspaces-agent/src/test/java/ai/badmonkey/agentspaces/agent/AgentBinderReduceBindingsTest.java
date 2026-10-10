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

import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A class whose only binding is a {@code @SpaceReduce} has bindings
 * (found by the Micronaut enroller, 2026-10-09): {@code hasBindings} says
 * so, and the facade binds it into the group that registers its space.
 */
class AgentBinderReduceBindingsTest {

    public record Payment(String account, long cents) {
    }

    public record Balance(String account, long cents) {
    }

    /** No {@code @AgentSpec}: the reduce is the class's only fleet annotation. */
    public static class Tally {
        @SpaceReduce(space = "ledger", key = "account", lease = "2s", pollTimeout = "100ms")
        public Balance fold(Balance balance, Payment payment) {
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents());
        }
    }

    @Test
    void aReduceOnlyClassHasBindingsAndBindsThroughTheFacade() throws Exception {
        assertThat(AgentBinder.hasBindings(Tally.class)).isTrue();
        TestClock clock = TestClock.create();
        PeerIdentity identity = PeerIdentity.generate();
        try (LocalSpace ledger = LocalSpace.builder("ledger", identity.agent("ledger")).clock(clock).build();
             AgentSpaces spaces = new AgentSpaces(identity, clock)) {
            spaces.register("books", GroupId.of("zBooks"), null, null).space("ledger", ledger);
            List<AgentBinder.Bound> bound = spaces.bind(new Tally());
            assertThat(bound).hasSize(1);
            ledger.write(new Payment("acme", 5), Lease.of(Duration.ofMinutes(10)));
            ledger.write(new Payment("acme", 7), Lease.of(Duration.ofMinutes(10)));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline && ledger.readAll(Template.of(Balance.class), 10).stream()
                    .noneMatch(b -> b.cents() == 12)) {
                Thread.sleep(100);
            }
            assertThat(ledger.readAll(Template.of(Balance.class), 10)).anyMatch(b -> b.cents() == 12);
            bound.forEach(AgentBinder.Bound::close);
        }
    }
}
