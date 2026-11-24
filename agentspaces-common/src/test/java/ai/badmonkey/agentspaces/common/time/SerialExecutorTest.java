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
package ai.badmonkey.agentspaces.common.time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SerialExecutor}: one task at a time, in order, with exclusive callers serial with the drains
 * (srad/agentspaces-event-loop.md §3.5; the ground NL-2 stands on). The class-level timeout is a hang guard only.
 */
@Timeout(60)
class SerialExecutorTest {

    private static SerialExecutor virtualDrained(String name) {
        return new SerialExecutor(name, task -> Thread.ofVirtual().start(task));
    }

    @Test
    void tasksRunOneAtATimeAndInEachProducersOrder() throws Exception {
        SerialExecutor serial = virtualDrained("order");
        int producers = 8;
        int each = 2_000;
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger most = new AtomicInteger();
        int[] lastSeen = new int[producers];
        java.util.Arrays.fill(lastSeen, -1);
        List<String> disorder = new java.util.concurrent.CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(producers * each);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int producer = p;
            threads.add(Thread.ofVirtual().start(() -> {
                for (int i = 0; i < each; i++) {
                    int seq = i;
                    serial.execute(() -> {
                        most.accumulateAndGet(inside.incrementAndGet(), Math::max);
                        if (lastSeen[producer] != seq - 1) { // unsynchronized on purpose: serial runs need none
                            disorder.add(producer + ":" + seq);
                        }
                        lastSeen[producer] = seq;
                        inside.decrementAndGet();
                        done.countDown();
                    });
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        done.await();
        assertThat(most.get()).as("never two tasks at once").isEqualTo(1);
        assertThat(disorder).as("each producer's tasks run in its order").isEmpty();
    }

    @Test
    void exclusiveCallersAreSerialWithDrains() throws Exception {
        SerialExecutor serial = virtualDrained("exclusive");
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger most = new AtomicInteger();
        AtomicInteger runs = new AtomicInteger();
        Runnable body = () -> {
            most.accumulateAndGet(inside.incrementAndGet(), Math::max);
            Thread.onSpinWait();
            inside.decrementAndGet();
            runs.incrementAndGet();
        };
        int threads = 8;
        int each = 1_000;
        CountDownLatch queued = new CountDownLatch(threads * each / 2);
        List<Thread> started = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            boolean exclusive = t % 2 == 0;
            started.add(Thread.ofVirtual().start(() -> {
                for (int i = 0; i < each; i++) {
                    if (exclusive) {
                        serial.runExclusive(body);
                    } else {
                        serial.execute(() -> {
                            body.run();
                            queued.countDown();
                        });
                    }
                }
            }));
        }
        for (Thread t : started) {
            t.join();
        }
        queued.await();
        assertThat(most.get()).as("an exclusive caller never overlaps a drain or another caller").isEqualTo(1);
        assertThat(runs.get()).isEqualTo(threads * each);
    }

    @Test
    void anExclusiveCallFromInsideATaskRunsInline() throws Exception {
        SerialExecutor serial = virtualDrained("inline");
        CountDownLatch done = new CountDownLatch(1);
        List<String> order = new java.util.concurrent.CopyOnWriteArrayList<>();
        serial.execute(() -> {
            order.add("outer");
            assertThat(serial.ownedByCurrentThread()).isTrue();
            serial.runExclusive(() -> order.add("inner"));
            order.add("after");
            done.countDown();
        });
        done.await();
        assertThat(order).containsExactly("outer", "inner", "after");
        assertThat(serial.ownedByCurrentThread()).isFalse();
    }

    @Test
    void aThrowingTaskDoesNotStopTheDrain() throws Exception {
        SerialExecutor serial = virtualDrained("throws");
        CountDownLatch after = new CountDownLatch(1);
        serial.execute(() -> {
            throw new IllegalStateException("expected by the test");
        });
        serial.execute(after::countDown);
        after.await();
        assertThat(serial.queued()).isZero();
    }

    @Test
    void tasksQueuedWhileAnExclusiveCallerHoldsItRunAfterIt() throws Exception {
        SerialExecutor serial = virtualDrained("handoff");
        CountDownLatch ran = new CountDownLatch(1);
        List<String> order = new java.util.concurrent.CopyOnWriteArrayList<>();
        serial.runExclusive(() -> {
            serial.execute(() -> {
                order.add("queued");
                ran.countDown();
            });
            order.add("exclusive");
        });
        ran.await();
        assertThat(order).containsExactly("exclusive", "queued");
    }
}
