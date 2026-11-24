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
import ai.badmonkey.agentspaces.api.ttl.TtlOverride;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.time.TtlFloor;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The TTL floor at bind time (srad/agentspaces-event-loop.md EL-E-03, EL-E-04): a verb annotation's lease under 60 s
 * warns, or refuses to bind under a strict floor; {@code @TtlOverride} beside it, or an expert override keyed
 * {@code binder.<AgentClass>.<method>.<member>}, passes it; the annotation's own default passes silently.
 */
class AgentBinderTtlFloorTest {

    public record Task(String topic) {
    }

    public static class Hasty {
        @SpaceTake(space = "tasks", lease = "20s", pollTimeout = "100ms")
        public void work(Task task) {
        }
    }

    public static class Excused {
        @SpaceTake(space = "tasks", lease = "20s", pollTimeout = "100ms")
        @TtlOverride(reason = "a 1 s handler on a test fleet")
        public void work(Task task) {
        }
    }

    public static class Plain {
        @SpaceTake(space = "tasks", pollTimeout = "100ms")
        public void work(Task task) {
        }
    }

    private final PeerIdentity identity = PeerIdentity.generate();
    private final TestClock clock = TestClock.create();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host")).clock(clock).build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zTtl"), null, clock).space("tasks", tasks);

    @AfterEach
    void tearDown() {
        binder.close();
        tasks.close();
    }

    @Test
    void aLeaseUnderTheFloorWarnsWhenLenient() {
        TtlFloor floor = TtlFloor.lenient();
        binder.ttlFloor(floor).bind(new Hasty()).close();
        assertThat(floor.findings()).singleElement().satisfies(f -> {
            assertThat(f.key()).isEqualTo("binder.Hasty.work.lease");
            assertThat(f.value()).isEqualTo(Duration.ofSeconds(20));
            assertThat(f.verdict()).isEqualTo(TtlFloor.Verdict.WARNED);
        });
    }

    @Test
    void aLeaseUnderTheFloorIsRefusedWhenStrict() {
        binder.ttlFloor(TtlFloor.of(true, Map.of()));
        assertThatThrownBy(() -> binder.bind(new Hasty())).isInstanceOf(TtlFloor.TtlFloorException.class)
                .hasMessageContaining("binder.Hasty.work.lease=PT20S");
    }

    @Test
    void aTtlOverrideOrAnExpertOverridePassesAndTheDefaultIsSilent() {
        TtlFloor strict = TtlFloor.of(true, Map.of("binder.Hasty.work.lease", "a test fleet's 1 s handler"));
        binder.ttlFloor(strict);
        binder.bind(new Excused()).close();
        binder.bind(new Hasty()).close();
        binder.bind(new Plain()).close(); // @SpaceTake's own 10-minute default
        assertThat(strict.findings()).extracting(TtlFloor.Finding::verdict)
                .containsExactly(TtlFloor.Verdict.OVERRIDDEN, TtlFloor.Verdict.OVERRIDDEN);
        assertThat(strict.findings()).extracting(TtlFloor.Finding::reason)
                .containsExactly("a 1 s handler on a test fleet", "a test fleet's 1 s handler");
    }
}
