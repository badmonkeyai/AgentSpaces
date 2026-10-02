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
package ai.badmonkey.agentspaces.space.local;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.test.Fixtures;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The replicated-worker idiom under real concurrency: N virtual-thread workers
 * drain a task space, and every task is completed exactly once. This is the
 * in-process form of the M1 kill-tolerance demo.
 */
class LocalSpaceConcurrencyTest {

    @Test
    void manyWorkersDrainTheSpaceWithExactlyOneCompletionPerTask() throws Exception {
        int tasks = 200;
        int workers = 16;
        try (LocalSpace space = LocalSpace.builder("tasks", Fixtures.agent("coordinator")).build()) {
            for (int i = 0; i < tasks; i++) {
                space.write(new TaskEntry("task-" + i, i % 10), Lease.of(Duration.ofMinutes(10)));
            }

            ConcurrentHashMap<String, AtomicInteger> completions = new ConcurrentHashMap<>();
            CountDownLatch done = new CountDownLatch(workers);
            for (int w = 0; w < workers; w++) {
                Thread.ofVirtual().start(() -> {
                    try {
                        while (true) {
                            Optional<TakenEntry<TaskEntry>> taken = space.take(
                                    Template.of(TaskEntry.class),
                                    Lease.of(Duration.ofMinutes(1)), Duration.ofMillis(200));
                            if (taken.isEmpty()) {
                                return;
                            }
                            completions
                                    .computeIfAbsent(taken.get().entry().topic(),
                                            k -> new AtomicInteger())
                                    .incrementAndGet();
                            space.complete(taken.get());
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            done.await();

            assertThat(completions).hasSize(tasks);
            List<String> doubled = completions.entrySet().stream()
                    .filter(e -> e.getValue().get() != 1)
                    .map(java.util.Map.Entry::getKey)
                    .toList();
            assertThat(doubled).isEmpty();
            assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
        }
    }
}
