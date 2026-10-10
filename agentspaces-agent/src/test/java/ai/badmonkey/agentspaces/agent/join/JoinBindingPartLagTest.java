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
package ai.badmonkey.agentspaces.agent.join;

import static org.assertj.core.api.Assertions.assertThat;

import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The lost key of the ordered join cluster stress runs: a ticket is written by
 * a member that saw every part, and taken by a member that does not hold one
 * of them yet. The taker must not mark the key fired and complete the ticket;
 * it waits for the part, and otherwise releases the ticket by its lease.
 * Modelled with a space that hides one part type from reads until released.
 */
class JoinBindingPartLagTest {

    public record Left(String caseId, int value) {
    }

    public record Right(String caseId, String note) {
    }

    public record Result(String caseId, int value, String note) {
    }

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace real = LocalSpace.builder("intake", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50)).build();
    private final AtomicBoolean rightHidden = new AtomicBoolean(true);
    /**
     * The real space with {@code Right} invisible to every read while {@code rightHidden},
     * and never delivered to a subscription: a part that has not replicated here, so this
     * member neither sees the key complete nor writes a ticket of its own. The only
     * ticket is the one another member wrote.
     */
    private final Space lagging = (Space) Proxy.newProxyInstance(Space.class.getClassLoader(), new Class<?>[]{Space.class},
            (proxy, method, args) -> {
                boolean right = args != null && args.length > 0 && args[0] instanceof Template<?> template
                        && template.type() == Right.class;
                if (right && method.getName().equals("notify")) {
                    return new ai.badmonkey.agentspaces.api.space.Subscription() {
                        @Override public void renew(Duration extension) { }
                        @Override public void close() { }
                    };
                }
                if (rightHidden.get() && right
                        && (method.getName().equals("readAllEntries") || method.getName().equals("readAll")
                                || method.getName().equals("read"))) {
                    return method.getName().equals("read") ? java.util.Optional.empty() : List.of();
                }
                try {
                    return method.invoke(real, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    private final List<String> fired = new CopyOnWriteArrayList<>();
    private JoinBinding binding;

    static {
        // The join's decisions at DEBUG on the console, so a failure reads as a timeline.
        java.util.logging.Logger join = java.util.logging.Logger.getLogger("ai.badmonkey.agentspaces.agent.join");
        join.setLevel(java.util.logging.Level.FINE);
        java.util.logging.ConsoleHandler console = new java.util.logging.ConsoleHandler();
        console.setLevel(java.util.logging.Level.FINE);
        join.addHandler(console);
    }

    @AfterEach
    void tearDown() {
        if (binding != null) {
            binding.close();
        }
        real.close();
    }

    private JoinBinding binding(Duration takeLease) {
        List<JoinBinding.PartSpec> parts = List.of(
                new JoinBinding.PartSpec(Left.class, Template.of(Left.class), lagging, "caseId", null, false),
                new JoinBinding.PartSpec(Right.class, Template.of(Right.class), lagging, "caseId", null, false));
        JoinBinding.Config config = new JoinBinding.Config("lag", SpaceJoin.Mode.LEASED, lagging, parts,
                Duration.ofHours(1), 100, Lease.of(Duration.ofHours(1)), Lease.of(Duration.ofMinutes(10)),
                Lease.of(takeLease), Duration.ofMillis(100), null);
        JoinBinding b = new JoinBinding(config, joined -> {
            fired.add(joined.key());
            return new Result(joined.key(), joined.get(Left.class).value(), joined.get(Right.class).note());
        }, result -> real.write(result, Lease.of(Duration.ofHours(1))), InstantSource.system());
        b.start();
        return b;
    }

    @Test
    void aTicketTakenBeforeAPartIsReadableFiresOnceThePartArrives() throws Exception {
        binding = binding(Duration.ofSeconds(30));
        // Another member saw both parts and wrote the ticket; here, Right has not landed.
        real.write(new Left("k1", 1), Lease.of(Duration.ofHours(1)));
        real.write(new Right("k1", "n1"), Lease.of(Duration.ofHours(1)));
        real.write(new JoinTicket("lag", "k1"), Lease.of(Duration.ofMinutes(10)),
                Map.of(JoinTicket.JOIN_TAG, "lag", JoinTicket.KEY_TAG, "k1"));
        Thread.sleep(400);
        assertThat(fired).as("nothing fires while a required part is not readable").isEmpty();
        rightHidden.set(false);                            // the part replicates
        await(() -> fired.size() == 1, Duration.ofSeconds(10));
        await(() -> !real.readAll(Template.of(Result.class), 5).isEmpty(), Duration.ofSeconds(5));
        await(() -> real.readAll(Template.of(JoinTicket.class), 5).isEmpty(), Duration.ofSeconds(5));
        Thread.sleep(300);
        assertThat(fired).as("once").containsExactly("k1");
    }

    @Test
    void aTicketWhosePartNeverArrivesIsReleasedByItsLeaseNotCompleted() throws Exception {
        binding = binding(Duration.ofMillis(400));         // a short lease: the wait stays inside it (200 ms)
        real.write(new Left("k2", 2), Lease.of(Duration.ofHours(1)));
        real.write(new Right("k2", "n2"), Lease.of(Duration.ofHours(1)));
        real.write(new JoinTicket("lag", "k2"), Lease.of(Duration.ofMinutes(10)),
                Map.of(JoinTicket.JOIN_TAG, "lag", JoinTicket.KEY_TAG, "k2"));
        Thread.sleep(2500);                                // several lease lapses and retakes
        assertThat(fired).as("nothing fires while the part is missing").isEmpty();
        // The ticket was released by its lease every time, never completed: once the
        // part lands it is taken again and the key fires. Had the taker completed it
        // without firing, there would be no ticket left and the key would be lost.
        rightHidden.set(false);
        await(() -> !fired.isEmpty(), Duration.ofSeconds(10));
        await(() -> real.readAll(Template.of(JoinTicket.class), 5).isEmpty(), Duration.ofSeconds(5));
        Thread.sleep(500);
        assertThat(fired).as("once").containsExactly("k2");
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
