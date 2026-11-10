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

import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.InstantSource;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A take loop that keeps failing must neither spin nor flood the log. A space
 * that has closed under a still-bound agent ends its loop; any other failure
 * backs off and logs the first of a run, and the loop recovers at full speed
 * once the space answers again.
 */
class AgentBinderTakeFailuresTest {

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50))
            .build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zFail"),
            null, InstantSource.system());
    private final CopyOnWriteArrayList<LogRecord> warnings = new CopyOnWriteArrayList<>();
    private final Logger jul = Logger.getLogger(AgentBinder.class.getName());
    private final Handler capture = new Handler() {
        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                warnings.add(record);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    @BeforeEach
    void listen() {
        jul.addHandler(capture);
    }

    @AfterEach
    void tearDown() {
        jul.removeHandler(capture);
        binder.close();
        tasks.close();
    }

    public static class Worker {
        final CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();

        @SpaceTake(space = "tasks", lease = "PT10M", pollTimeout = "PT0.2S")
        public FindingEntry research(TaskEntry task) {
            seen.add(task.topic());
            return new FindingEntry(task.topic(), "done: " + task.topic());
        }
    }

    /** A space closed under a bound agent ends the take loop: no more takes, no warning flood. */
    @Test
    @Timeout(30)
    void aClosedSpaceEndsTheTakeLoop() throws Exception {
        AtomicInteger takes = new AtomicInteger();
        binder.space("tasks", counting(tasks, takes, new AtomicBoolean(false)));
        binder.bind(new Worker());
        awaitTrue(() -> takes.get() >= 1, Duration.ofSeconds(5));

        tasks.close();
        Thread.sleep(500);
        int settled = takes.get();
        Thread.sleep(1_000);

        assertThat(takes.get() - settled).as("takes after the space closed and the loop settled").isZero();
        assertThat(warnings).as("warnings logged for the closed space").hasSizeLessThanOrEqualTo(1);
    }

    /** A space that keeps refusing is polled with a growing pause, and only the first refusal of the run warns. */
    @Test
    @Timeout(30)
    void aFailingSpaceIsRetriedWithBackoffAndWarnsOnce() throws Exception {
        AtomicInteger takes = new AtomicInteger();
        binder.space("tasks", counting(tasks, takes, new AtomicBoolean(true)));
        binder.bind(new Worker());
        awaitTrue(() -> takes.get() >= 1, Duration.ofSeconds(5));

        Thread.sleep(1_500);

        assertThat(takes.get()).as("take attempts in about 1.5 s of failures").isLessThanOrEqualTo(25);
        assertThat(warnings).as("warnings in one run of failures").hasSize(1);
    }

    /** Once the space answers again the loop drains at once: the pause resets on the first good poll. */
    @Test
    @Timeout(30)
    void theLoopRecoversWhenTheSpaceAnswersAgain() throws Exception {
        AtomicInteger takes = new AtomicInteger();
        AtomicBoolean failing = new AtomicBoolean(true);
        binder.space("tasks", counting(tasks, takes, failing));
        Worker worker = new Worker();
        binder.bind(worker);
        awaitTrue(() -> takes.get() >= 3, Duration.ofSeconds(5));

        failing.set(false);
        for (int i = 0; i < 3; i++) {
            tasks.write(new TaskEntry("topic-" + i, i), Lease.of(Duration.ofMinutes(10)));
        }

        awaitTrue(() -> worker.seen.size() == 3, Duration.ofSeconds(5));
        assertThat(tasks.readAll(Template.of(FindingEntry.class), 10)).hasSize(3);
    }

    /** Counts take calls and, while {@code failing} holds, refuses each with a transient fault. */
    private static Space counting(Space target, AtomicInteger takes, AtomicBoolean failing) {
        return (Space) Proxy.newProxyInstance(Space.class.getClassLoader(),
                new Class<?>[] {Space.class}, (proxy, method, args) -> {
                    if (method.getName().equals("take")) {
                        takes.incrementAndGet();
                        if (failing.get()) {
                            throw new IllegalStateException("simulated transport fault");
                        }
                    }
                    try {
                        return method.invoke(target, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(20);
        }
    }
}
