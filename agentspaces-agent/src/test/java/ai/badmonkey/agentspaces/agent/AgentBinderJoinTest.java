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
import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code @SpaceJoin} (SPEC §10.3, issue #16) over a {@code LocalSpace}: the
 * local mode's once-per-key rule, the seed, the forgotten key, the optional
 * and tag-keyed parts, the card, the bind-time refusals, and the leased mode's
 * ticket.
 */
class AgentBinderJoinTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace intake = LocalSpace.builder("intake", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50))
            .build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zJoin"),
            null, InstantSource.system()).space("intake", intake);

    @AfterEach
    void tearDown() {
        binder.close();
        intake.close();
    }

    public record Fraud(String claimId, int score) {
    }

    public record Coverage(String claimId, boolean covered) {
    }

    /** Keyed by a tag, not a component: the ontology-instance shape. */
    public record Damage(String id, double estimate) {
    }

    public record Note(String claimId, String text) {
    }

    public record Assessment(String claimId, int score, boolean covered, double estimate, String note) {
    }

    @AgentSpec(name = "assembler", description = "Joins the findings", goals = {"assemble"})
    public static class Assembler {
        final List<Joined> fired = new CopyOnWriteArrayList<>();

        @SpaceJoin(space = "intake", key = "claimId",
                parts = {@Part(Fraud.class), @Part(Coverage.class),
                         @Part(value = Damage.class, keyTag = "claimId"),
                         @Part(value = Note.class, optional = true)})
        public Assessment assemble(Joined j) {
            fired.add(j);
            return new Assessment(j.key(), j.get(Fraud.class).score(), j.get(Coverage.class).covered(),
                    j.get(Damage.class).estimate(), j.find(Note.class).map(Note::text).orElse(""));
        }
    }

    private void writeParts(String claimId, int score) {
        intake.write(new Fraud(claimId, score), HOUR);
        intake.write(new Damage("dmg-" + claimId, 100.0 * score), HOUR, Map.of("claimId", claimId));
        intake.write(new Coverage(claimId, true), HOUR);
    }

    private List<Assessment> assessments() {
        return intake.readAll(Template.of(Assessment.class), 10);
    }

    @Test
    void firesOncePerKeyWithTheNewestPartOfEachTypeWhenTheLastPartLands() throws Exception {
        Assembler assembler = new Assembler();
        binder.bind(assembler);
        intake.write(new Fraud("c1", 10), HOUR);
        intake.write(new Coverage("c1", true), HOUR);
        Thread.sleep(100);
        assertThat(assembler.fired).as("two of three parts do not fire").isEmpty();
        intake.write(new Fraud("c1", 20), HOUR);                 // a newer finding, still incomplete
        intake.write(new Damage("dmg-c1", 500.0), HOUR, Map.of("claimId", "c1"));
        await(() -> assessments().size() == 1, Duration.ofSeconds(5));
        Assessment assessment = assessments().get(0);
        assertThat(assessment.claimId()).isEqualTo("c1");
        assertThat(assessment.score()).as("the newest Fraud wins").isEqualTo(20);
        assertThat(assessment.estimate()).isEqualTo(500.0);
        assertThat(assessment.note()).as("the optional part was absent").isEmpty();
        assertThat(assembler.fired.get(0).all(Fraud.class)).hasSize(2);
        // Another part for a key that fired does not fire it again in LOCAL mode.
        intake.write(new Fraud("c1", 30), HOUR);
        Thread.sleep(200);
        assertThat(assembler.fired).hasSize(1);
        // A second key fires on its own.
        writeParts("c2", 7);
        await(() -> assessments().size() == 2, Duration.ofSeconds(5));
        assertThat(assembler.fired).hasSize(2);
    }

    @Test
    void anOptionalPartIsIncludedWhenItArrivedBeforeTheKeyCompleted() throws Exception {
        Assembler assembler = new Assembler();
        binder.bind(assembler);
        intake.write(new Note("c1", "late shipment"), HOUR);
        writeParts("c1", 3);
        await(() -> assessments().size() == 1, Duration.ofSeconds(5));
        assertThat(assessments().get(0).note()).isEqualTo("late shipment");
        assertThat(assembler.fired.get(0).has(Note.class)).isTrue();
        assertThat(assembler.fired.get(0).entry(Note.class).tags()).isEmpty();
    }

    @Test
    void aJoinerBoundAfterThePartsLandedFiresFromTheSeed() throws Exception {
        writeParts("c1", 4);
        writeParts("c2", 5);
        intake.write(new Fraud("c3", 6), HOUR);                   // incomplete: never fires
        Assembler assembler = new Assembler();
        binder.bind(assembler);
        await(() -> assessments().size() == 2, Duration.ofSeconds(5));
        Thread.sleep(200);
        assertThat(assembler.fired).extracting(Joined::key).containsExactlyInAnyOrder("c1", "c2");
    }

    @AgentSpec(name = "forgetful", description = "Forgets quickly", goals = {"assemble"})
    public static class Forgetful {
        final List<String> fired = new CopyOnWriteArrayList<>();

        @SpaceJoin(space = "intake", key = "claimId", within = "200ms", maxOpen = 2,
                parts = {@Part(Fraud.class), @Part(Coverage.class)})
        public Assessment assemble(Joined j) {
            fired.add(j.key());
            return new Assessment(j.key(), j.get(Fraud.class).score(), j.get(Coverage.class).covered(), 0, "");
        }
    }

    @Test
    void aForgottenOrEvictedKeyIsFoundAgainInTheSpaceWhenItsLastPartArrives() throws Exception {
        Forgetful forgetful = new Forgetful();
        binder.bind(forgetful);
        intake.write(new Fraud("slow", 1), HOUR);
        Thread.sleep(600);                                         // past `within`: the key is forgotten
        intake.write(new Fraud("e1", 1), HOUR);
        intake.write(new Fraud("e2", 1), HOUR);
        intake.write(new Fraud("e3", 1), HOUR);                   // beyond maxOpen: the eldest is evicted
        intake.write(new Coverage("slow", true), HOUR);           // the forgotten key's last part
        intake.write(new Coverage("e1", true), HOUR);             // the evicted key's last part
        await(() -> forgetful.fired.size() == 2, Duration.ofSeconds(5));
        assertThat(forgetful.fired).containsExactlyInAnyOrder("slow", "e1");
    }

    @Test
    void theCardDeclaresOneJoinActionOverEveryPart() {
        AgentBinder.Bound bound = binder.bind(new Assembler());
        List<CardAction> actions = bound.card().actions();
        assertThat(actions).hasSize(1);
        CardAction join = actions.get(0);
        assertThat(join.kind()).isEqualTo(CardAction.JOIN);
        assertThat(join.invocable()).isFalse();
        assertThat(join.consumes()).containsExactlyInAnyOrder(
                Fraud.class.getName() + "#v1", Coverage.class.getName() + "#v1",
                Damage.class.getName() + "#v1", Note.class.getName() + "#v1");
        assertThat(join.produces()).containsExactly(Assessment.class.getName() + "#v1");
        assertThat(join.space()).isEqualTo("intake");
        assertThat(bound.card().consumes()).contains(Fraud.class.getName() + "#v1");
    }

    public static class OnePart {
        @SpaceJoin(space = "intake", key = "claimId", parts = {@Part(Fraud.class)})
        public void join(Joined j) {
        }
    }

    public static class NotABag {
        @SpaceJoin(space = "intake", key = "claimId", parts = {@Part(Fraud.class), @Part(Coverage.class)})
        public void join(Fraud f) {
        }
    }

    public static class NoSuchKey {
        @SpaceJoin(space = "intake", key = "caseId", parts = {@Part(Fraud.class), @Part(Coverage.class)})
        public void join(Joined j) {
        }
    }

    public static class BothKeys {
        @SpaceJoin(space = "intake", key = "claimId",
                parts = {@Part(Fraud.class), @Part(value = Damage.class, key = "id", keyTag = "claimId")})
        public void join(Joined j) {
        }
    }

    public static class Ordered {
        @SpaceJoin(space = "intake", key = "claimId", mode = SpaceJoin.Mode.ORDERED,
                parts = {@Part(Fraud.class), @Part(Coverage.class)})
        public void join(Joined j) {
        }
    }

    public static class BadFilter {
        @SpaceJoin(space = "intake", key = "claimId",
                parts = {@Part(value = Fraud.class, tags = "=eu"), @Part(Coverage.class)})
        public void join(Joined j) {
        }
    }

    @Test
    void joinsAreValidatedAtBindTime() {
        assertThatThrownBy(() -> binder.bind(new OnePart()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("two parts");
        assertThatThrownBy(() -> binder.bind(new NotABag()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Joined");
        assertThatThrownBy(() -> binder.bind(new NoSuchKey()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("caseId");
        assertThatThrownBy(() -> binder.bind(new BothKeys()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keyTag");
        assertThatThrownBy(() -> binder.bind(new Ordered()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ordered");
        assertThatThrownBy(() -> binder.bind(new BadFilter()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("=eu");
    }

    public record Brief(String claimId, String text) {
    }

    @AgentSpec(name = "cross", description = "Joins across spaces", goals = {"assemble"})
    public static class CrossSpace {
        final List<Joined> fired = new CopyOnWriteArrayList<>();

        @SpaceJoin(space = "intake", key = "claimId", resultSpace = "briefs",
                parts = {@Part(Fraud.class), @Part(value = Brief.class, space = "briefs")})
        public Assessment assemble(Joined j) {
            fired.add(j);
            return new Assessment(j.key(), j.get(Fraud.class).score(), false, 0, j.get(Brief.class).text());
        }
    }

    @Test
    void aPartMayLiveInAnotherSpaceAndTheResultGoesWhereTheJoinSays() throws Exception {
        LocalSpace briefs = LocalSpace.builder("briefs", identity.agent("host")).build();
        try {
            binder.space("briefs", briefs);
            CrossSpace cross = new CrossSpace();
            binder.bind(cross);
            briefs.write(new Brief("c1", "hail"), HOUR);
            intake.write(new Fraud("c1", 2), HOUR);
            await(() -> !briefs.readAll(Template.of(Assessment.class), 10).isEmpty(), Duration.ofSeconds(5));
            assertThat(briefs.readAll(Template.of(Assessment.class), 10).get(0).note()).isEqualTo("hail");
            assertThat(cross.fired.get(0).entry(Brief.class).issuer()).isEqualTo(identity.agent("host"));
        } finally {
            briefs.close();
        }
    }

    // ------------------------------------------------------------------ LEASED

    @AgentSpec(name = "leased", description = "Joins through a ticket", goals = {"assemble"})
    public static class Leased {
        final AtomicInteger invocations = new AtomicInteger();
        final AtomicInteger crashesLeft;

        Leased(int crashes) {
            this.crashesLeft = new AtomicInteger(crashes);
        }

        @SpaceJoin(space = "intake", key = "claimId", mode = SpaceJoin.Mode.LEASED,
                takeLease = "300ms", pollTimeout = "100ms",
                parts = {@Part(Fraud.class), @Part(Coverage.class)})
        public Assessment assemble(Joined j) {
            invocations.incrementAndGet();
            if (crashesLeft.getAndDecrement() > 0) {
                throw new IllegalStateException("simulated crash");
            }
            return new Assessment(j.key(), j.get(Fraud.class).score(), j.get(Coverage.class).covered(), 0, "");
        }
    }

    @Test
    void twoLeasedJoinersFireOnceThroughOneTicketAndACrashedFiringRetries() throws Exception {
        AgentBinder other = new AgentBinder(identity, GroupId.of("zJoin"), null, InstantSource.system())
                .space("intake", intake);
        Leased first = new Leased(0);
        Leased second = new Leased(0);
        AgentBinder.Bound firstBound = binder.bind(first);
        try {
            other.bind(second);
            intake.write(new Fraud("c1", 1), HOUR);
            intake.write(new Coverage("c1", true), HOUR);
            await(() -> !assessments().isEmpty(), Duration.ofSeconds(5));
            Thread.sleep(500);
            // Both joiners saw the key complete; the later ticket withdrew, so one
            // firing is the rule and a second one needs a partition (SPEC §10.3:
            // LEASED is once per lease race per ticket, at least once fleet-wide).
            int firings = first.invocations.get() + second.invocations.get();
            assertThat(firings).as("at least once, and once absent a partition").isBetween(1, 2);
            assertThat(assessments()).hasSize(firings);
            await(() -> intake.readAll(Template.of(JoinTicket.class), 10).isEmpty(), Duration.ofSeconds(5));
        } finally {
            other.close();
            firstBound.close();                    // only the crasher below may answer c2
        }
        // A firing that throws leaves the ticket to lapse and reappear: the join retries.
        AgentBinder crashy = new AgentBinder(identity, GroupId.of("zJoin"), null, InstantSource.system())
                .space("intake", intake);
        Leased crasher = new Leased(1);
        try {
            crashy.bind(crasher, "crasher");
            intake.write(new Fraud("c2", 2), HOUR);
            intake.write(new Coverage("c2", true), HOUR);
            await(() -> assessments().stream().anyMatch(x -> x.claimId().equals("c2")), Duration.ofSeconds(10));
            assertThat(crasher.invocations.get()).as("crashed once, then retried from the reappeared ticket")
                    .isEqualTo(2);
        } finally {
            crashy.close();
        }
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
