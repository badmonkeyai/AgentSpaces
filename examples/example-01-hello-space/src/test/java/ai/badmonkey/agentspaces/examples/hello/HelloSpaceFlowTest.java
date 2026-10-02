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
package ai.badmonkey.agentspaces.examples.hello;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.examples.hello.HelloSpace.Finding;
import ai.badmonkey.agentspaces.examples.hello.HelloSpace.ResearchTask;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static ai.badmonkey.agentspaces.api.space.Matchers.gte;
import static org.assertj.core.api.Assertions.assertThat;

/** The demo's flow as a deterministic test, driven by a TestClock instead of sleeps. */
class HelloSpaceFlowTest {

    @Test
    void crashedWorkersTaskReappearsAndCompletesElsewhere() {
        TestClock clock = TestClock.create();
        PeerIdentity identity = PeerIdentity.generate();
        try (LocalSpace space = LocalSpace.builder("tasks", identity.agent("coordinator"))
                .clock(clock)
                .build()) {

            space.write(new ResearchTask("agentic memory", 3), Lease.of(Duration.ofMinutes(30)));
            space.write(new ResearchTask("tuple spaces", 7), Lease.of(Duration.ofMinutes(30)));

            TakenEntry<ResearchTask> doomed = space.take(
                    Template.of(ResearchTask.class).where("priority", gte(5)),
                    Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
            assertThat(doomed.entry().topic()).isEqualTo("tuple spaces");
            // Worker A "crashes": no complete, the lease simply lapses.
            clock.advance(Duration.ofMinutes(2));

            TakenEntry<ResearchTask> retry = space.take(
                    Template.of(ResearchTask.class).where("priority", gte(5)),
                    Lease.of(Duration.ofMinutes(10)), Duration.ZERO).orElseThrow();
            assertThat(retry.entryId()).isEqualTo(doomed.entryId());

            space.complete(retry, new Finding("tuple spaces", "done"), Lease.of(Duration.ofHours(1)));

            assertThat(space.read(Template.of(Finding.class)))
                    .contains(new Finding("tuple spaces", "done"));
            assertThat(space.readAll(Template.of(ResearchTask.class), 10)).hasSize(1);
        }
    }
}
