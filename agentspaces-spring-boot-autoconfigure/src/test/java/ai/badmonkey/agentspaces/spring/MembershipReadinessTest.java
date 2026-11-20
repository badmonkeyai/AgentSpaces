/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Readiness refuses traffic while any group is isolated and accepts again once none is; it never touches liveness. */
class MembershipReadinessTest {

    private final List<Object> published = new ArrayList<>();
    private final MembershipReadiness readiness = new MembershipReadiness(published::add);

    private static PeerNode.MembershipStatus status(String group, PeerNode.MembershipState state) {
        return new PeerNode.MembershipStatus(GroupId.of(group), state, 0, 0, List.of(), List.of(), true, false);
    }

    private List<Object> states() {
        return published.stream().map(e -> ((AvailabilityChangeEvent<?>) e).getState()).map(Object.class::cast)
                .toList();
    }

    @Test
    void readinessRefusesWhileAnyGroupIsIsolatedAndAcceptsOnceNoneIs() {
        readiness.onStatus(status("zA", PeerNode.MembershipState.CONNECTED));
        readiness.onStatus(status("zA", PeerNode.MembershipState.RECONNECTING));
        assertThat(published).as("reconnecting is not out of service yet").isEmpty();

        readiness.onStatus(status("zA", PeerNode.MembershipState.ISOLATED));
        readiness.onStatus(status("zB", PeerNode.MembershipState.ISOLATED));
        assertThat(states()).containsExactly(ReadinessState.REFUSING_TRAFFIC);
        assertThat(readiness.refusing()).isTrue();

        readiness.onStatus(status("zA", PeerNode.MembershipState.CONNECTED));
        assertThat(states()).as("zB still isolated").containsExactly(ReadinessState.REFUSING_TRAFFIC);
        readiness.onStatus(status("zB", PeerNode.MembershipState.DEGRADED));
        assertThat(states()).containsExactly(ReadinessState.REFUSING_TRAFFIC, ReadinessState.ACCEPTING_TRAFFIC);
        assertThat(readiness.isolatedGroups()).isEmpty();
    }

    @Test
    void aNodeAloneByDesignStaysInService() {
        readiness.onStatus(status("zA", PeerNode.MembershipState.ALONE));
        readiness.onStatus(status("zA", PeerNode.MembershipState.LAST_RESORT));
        assertThat(published).isEmpty();
    }
}
