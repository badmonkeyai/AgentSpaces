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
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The gather (ISSUE-WorkflowVerbs): several entries of one kind for one key,
 * counted by {@code atLeast}, by a count another part declares, or ended by
 * a quiet period.
 */
class AgentBinderGatherTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace quotes = LocalSpace.builder("quotes", identity.agent("host")).build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zGather"), null,
            InstantSource.system()).space("quotes", quotes);

    @AfterEach
    void tearDown() {
        binder.close();
        quotes.close();
    }

    public record QuoteRequest(String claimId, int expected) {
    }

    public record Quote(String claimId, String shop, double amount) {
    }

    public record Best(String claimId, String shop, double amount, int considered) {
    }

    @AgentSpec(name = "two-quotes", description = "Picks the best of at least two", goals = {"gather"})
    public static class AtLeastTwo {
        final List<Joined> fired = new CopyOnWriteArrayList<>();

        @SpaceJoin(space = "quotes", key = "claimId", parts = {@Part(value = Quote.class, atLeast = 2)})
        public Best pick(Joined j) {
            fired.add(j);
            Quote best = j.all(Quote.class).stream().min((a, b) -> Double.compare(a.amount(), b.amount())).orElseThrow();
            return new Best(j.key(), best.shop(), best.amount(), j.all(Quote.class).size());
        }
    }

    @Test
    void atLeastFiresWhenEnoughOfOneKindArePresentAndHandsThemAllOver() throws Exception {
        AtLeastTwo gather = new AtLeastTwo();
        binder.bind(gather);
        quotes.write(new Quote("c1", "north", 900), HOUR);
        Thread.sleep(150);
        assertThat(gather.fired).as("one quote is not enough").isEmpty();
        quotes.write(new Quote("c1", "east", 700), HOUR);
        await(() -> !quotes.readAll(Template.of(Best.class), 10).isEmpty(), Duration.ofSeconds(5));
        Best best = quotes.readAll(Template.of(Best.class), 10).get(0);
        assertThat(best.shop()).isEqualTo("east");
        assertThat(best.considered()).isEqualTo(2);
        quotes.write(new Quote("c1", "south", 500), HOUR);
        Thread.sleep(150);
        assertThat(gather.fired).as("LOCAL: once per key").hasSize(1);
    }

    @AgentSpec(name = "all-quotes", description = "Waits for as many as were asked for", goals = {"gather"})
    public static class CountedByRequest {
        final List<Joined> fired = new CopyOnWriteArrayList<>();

        @SpaceJoin(space = "quotes", key = "claimId",
                parts = {@Part(QuoteRequest.class),
                         @Part(value = Quote.class, countedBy = "QuoteRequest.expected")})
        public Best pick(Joined j) {
            fired.add(j);
            Quote best = j.all(Quote.class).stream().min((a, b) -> Double.compare(a.amount(), b.amount())).orElseThrow();
            return new Best(j.key(), best.shop(), best.amount(), j.all(Quote.class).size());
        }
    }

    @Test
    void countedByWaitsForTheNumberAnotherPartDeclares() throws Exception {
        CountedByRequest gather = new CountedByRequest();
        binder.bind(gather);
        quotes.write(new QuoteRequest("c2", 3), HOUR);
        quotes.write(new Quote("c2", "north", 900), HOUR);
        quotes.write(new Quote("c2", "east", 700), HOUR);
        Thread.sleep(200);
        assertThat(gather.fired).as("two of three asked for").isEmpty();
        quotes.write(new Quote("c2", "south", 800), HOUR);
        await(() -> !quotes.readAll(Template.of(Best.class), 10).isEmpty(), Duration.ofSeconds(5));
        assertThat(quotes.readAll(Template.of(Best.class), 10).get(0).considered()).isEqualTo(3);
        // The request may land after the quotes: the count is read whenever it arrives.
        quotes.write(new Quote("c3", "north", 10), HOUR);
        quotes.write(new Quote("c3", "east", 20), HOUR);
        Thread.sleep(150);
        assertThat(gather.fired).hasSize(1);
        quotes.write(new QuoteRequest("c3", 2), HOUR);
        await(() -> quotes.readAll(Template.of(Best.class), 10).size() == 2, Duration.ofSeconds(5));
    }

    @AgentSpec(name = "quiet-quotes", description = "Takes whatever came before it went quiet", goals = {"gather"})
    public static class Settled {
        final List<Joined> fired = new CopyOnWriteArrayList<>();

        @SpaceJoin(space = "quotes", key = "claimId", settle = "300ms",
                parts = {@Part(value = Quote.class, atLeast = 1)})
        public Best pick(Joined j) {
            fired.add(j);
            Quote best = j.all(Quote.class).stream().min((a, b) -> Double.compare(a.amount(), b.amount())).orElseThrow();
            return new Best(j.key(), best.shop(), best.amount(), j.all(Quote.class).size());
        }
    }

    @Test
    void settleFiresOnceTheKeyHasBeenQuietAndTakesEverythingThatArrived() throws Exception {
        Settled gather = new Settled();
        binder.bind(gather);
        quotes.write(new Quote("c4", "north", 900), HOUR);
        Thread.sleep(120);
        quotes.write(new Quote("c4", "east", 700), HOUR);
        Thread.sleep(120);
        quotes.write(new Quote("c4", "south", 800), HOUR);
        assertThat(gather.fired).as("still arriving").isEmpty();
        await(() -> !quotes.readAll(Template.of(Best.class), 10).isEmpty(), Duration.ofSeconds(5));
        Best best = quotes.readAll(Template.of(Best.class), 10).get(0);
        assertThat(best.considered()).as("all three, gathered while quiet").isEqualTo(3);
        assertThat(best.shop()).isEqualTo("east");
        Thread.sleep(400);
        assertThat(gather.fired).hasSize(1);
    }

    public static class BadCount {
        @SpaceJoin(space = "quotes", key = "claimId",
                parts = {@Part(QuoteRequest.class), @Part(value = Quote.class, countedBy = "expected")})
        public void pick(Joined j) {
        }
    }

    public static class NoSuchCounter {
        @SpaceJoin(space = "quotes", key = "claimId",
                parts = {@Part(QuoteRequest.class), @Part(value = Quote.class, countedBy = "Order.expected")})
        public void pick(Joined j) {
        }
    }

    @Test
    void gatherAttributesAreValidatedAtBindTime() {
        assertThatThrownBy(() -> binder.bind(new BadCount()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("SimpleTypeName.field");
        assertThatThrownBy(() -> binder.bind(new NoSuchCounter()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Order");
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
