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

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.InstantSource;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link TakeContext}: present inside a worker method, absent everywhere else. */
class TakeContextTest {

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50))
            .build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zCtx"),
            null, InstantSource.system()).space("tasks", tasks);

    @AfterEach
    void tearDown() {
        binder.close();
        tasks.close();
    }

    /** A worker that records the take it runs under, and renews it once. */
    @AgentSpec(name = "recorder", description = "Records its take")
    public static class Recorder {
        final AtomicReference<TakeContext> seen = new AtomicReference<>();

        @SpaceTake(space = "tasks", lease = "2m", pollTimeout = "PT0.1S")
        public FindingEntry work(TaskEntry task) {
            TakeContext context = TakeContext.current().orElseThrow();
            context.renew();
            seen.set(context);
            return new FindingEntry(task.topic(), "done");
        }
    }

    @Test
    @Timeout(20)
    void theWorkerMethodSeesItsTakeAndNobodyElseDoes() throws Exception {
        Recorder recorder = new Recorder();
        binder.bind(recorder);
        tasks.write(new TaskEntry("context", 1), Lease.of(Duration.ofMinutes(5)));

        Optional<FindingEntry> finding = tasks.read(Template.of(FindingEntry.class),
                Duration.ofSeconds(10));
        assertThat(finding).isPresent();

        TakeContext context = recorder.seen.get();
        assertThat(context).isNotNull();
        assertThat(context.lease()).isEqualTo(Duration.ofMinutes(2));
        assertThat(context.agent().localName()).isEqualTo("recorder");
        assertThat(context.spaceName()).isEqualTo("tasks");
        assertThat(context.entryId()).isEqualTo(context.taken().entryId());
        assertThat(TakeContext.current()).as("the test thread holds no take").isEmpty();
    }

    @Test
    void bindAndUnbindCarryTheContextForPropagationLibraries() {
        assertThat(TakeContext.peek()).isNull();
        TakeContext context = new TakeContext(new ai.badmonkey.agentspaces.api.space.TakenEntry<>() {
            @Override
            public Object entry() {
                return "x";
            }

            @Override
            public ai.badmonkey.agentspaces.api.entry.EntryId entryId() {
                return ai.badmonkey.agentspaces.api.entry.EntryId.newId();
            }

            @Override
            public void renew(Duration extension) {
            }
        }, Duration.ofSeconds(30), identity.agent("a"), "tasks");
        TakeContext.bind(context);
        try {
            assertThat(TakeContext.peek()).isSameAs(context);
            assertThat(TakeContext.current()).contains(context);
        } finally {
            TakeContext.unbind();
        }
        assertThat(TakeContext.current()).isEmpty();
    }
}
