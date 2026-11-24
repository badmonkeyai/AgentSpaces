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

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The TTL floor (srad/agentspaces-event-loop.md EL-E-03, EL-E-04, decision 18). */
class TtlFloorTest {

    @Test
    void aRegisteredDefaultPassesSilentlyEvenUnderTheFloor() {
        TtlFloor floor = TtlFloor.of(true, Map.of());
        floor.check("binder.Clerk.confirm.lease", Duration.ofSeconds(30), Duration.ofSeconds(30));
        floor.check("agentspaces.membership.member-ttl", Duration.ofSeconds(45), Duration.ofSeconds(45));
        assertThat(floor.findings()).isEmpty();
    }

    @Test
    void aValueUnderTheFloorWarnsOnceWhenLenient() {
        TtlFloor floor = TtlFloor.lenient();
        floor.check("agentspaces.relay.path-ttl", Duration.ofSeconds(20), Duration.ofMinutes(10));
        floor.check("agentspaces.relay.path-ttl", Duration.ofSeconds(20), Duration.ofMinutes(10));
        floor.check("agentspaces.content.holding-lease", Duration.ofMinutes(2), Duration.ofMinutes(5));
        floor.check("agentspaces.a2a.task-lease-millis", null, Duration.ofMinutes(10));
        assertThat(floor.findings()).singleElement().satisfies(f -> {
            assertThat(f.key()).isEqualTo("agentspaces.relay.path-ttl");
            assertThat(f.value()).isEqualTo(Duration.ofSeconds(20));
            assertThat(f.verdict()).isEqualTo(TtlFloor.Verdict.WARNED);
        });
    }

    @Test
    void aValueUnderTheFloorIsRefusedWhenStrictNamingTheKeyAndThePrinciple() {
        TtlFloor floor = TtlFloor.of(true, Map.of());
        assertThatThrownBy(() -> floor.check("agentspaces.embabel.action-lease", Duration.ofSeconds(30),
                Duration.ofMinutes(1)))
                .isInstanceOf(TtlFloor.TtlFloorException.class)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.embabel.action-lease=PT30S")
                .hasMessageContaining("Principle 8")
                .hasMessageContaining("expert-overrides");
        assertThat(floor.findings()).singleElement().extracting(TtlFloor.Finding::verdict)
                .isEqualTo(TtlFloor.Verdict.REFUSED);
    }

    @Test
    void anExpertOverridePassesItsKeyAloneAndIsLoggedOnceWithItsReason() {
        TtlFloor floor = TtlFloor.of(true, Map.of("agentspaces.embabel.action-lease", "a 10 s action, renewed at 15 s"));
        floor.check("agentspaces.embabel.action-lease", Duration.ofSeconds(30), Duration.ofMinutes(1));
        floor.check("agentspaces.embabel.action-lease", Duration.ofSeconds(30), Duration.ofMinutes(1));
        floor.check("binder.Clerk.confirm.lease", Duration.ofSeconds(20), Duration.ofSeconds(30),
                "a 2 s handler, renewed every 10 s");
        assertThat(floor.findings()).extracting(TtlFloor.Finding::verdict)
                .containsExactly(TtlFloor.Verdict.OVERRIDDEN, TtlFloor.Verdict.OVERRIDDEN);
        assertThat(floor.findings().get(0).reason()).isEqualTo("a 10 s action, renewed at 15 s");
        assertThatThrownBy(() -> floor.check("agentspaces.relay.path-ttl", Duration.ofSeconds(20),
                Duration.ofMinutes(10))).as("the override passes its own key only")
                .isInstanceOf(TtlFloor.TtlFloorException.class);
    }

    @Test
    void anOverrideWithNoReasonIsRefused() {
        assertThatThrownBy(() -> TtlFloor.of(false, Map.of("agentspaces.relay.path-ttl", " ")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("names no reason");
    }
}
