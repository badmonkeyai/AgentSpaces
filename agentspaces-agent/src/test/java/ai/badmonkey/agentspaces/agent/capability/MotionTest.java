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
package ai.badmonkey.agentspaces.agent.capability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.badmonkey.agentspaces.api.space.Lease;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** {@link Motion} (ISSUE-Motion FR-1): the factories, the copied options, the checks `propose` makes. */
class MotionTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    @Test
    void ofInfersTheSpaceAndInNamesIt() {
        Motion inferred = Motion.of("rel:1", "ship?", List.of("yes", "no"), 2, HOUR);
        assertThat(inferred.space()).isEmpty();
        assertThat(inferred.proposalId()).isEqualTo("rel:1");
        assertThat(inferred.options()).containsExactly("yes", "no");
        assertThat(inferred.quorum()).isEqualTo(2);
        assertThat(inferred.lease()).isEqualTo(HOUR);
        assertThat(Motion.in("votes", "rel:1", "ship?", List.of("yes", "no"), 2, HOUR).space())
                .isEqualTo("votes");
    }

    @Test
    void theOptionsAreCopiedSoALaterChangeToTheListDoesNotReachTheMotion() {
        List<String> options = new ArrayList<>(List.of("yes", "no"));
        Motion motion = Motion.of("rel:1", "ship?", options, 1, HOUR);
        options.add("maybe");
        assertThat(motion.options()).containsExactly("yes", "no");
    }

    @Test
    void constructionAppliesProposeOwnChecks() {
        assertThatThrownBy(() -> Motion.of("rel:1", "ship?", List.of("yes"), 1, HOUR))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("two options");
        assertThatThrownBy(() -> Motion.of("rel:1", "ship?", List.of("yes", "no"), 0, HOUR))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("quorum");
        assertThatThrownBy(() -> Motion.of(null, "ship?", List.of("yes", "no"), 1, HOUR))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Motion.of("rel:1", null, List.of("yes", "no"), 1, HOUR))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Motion.of("rel:1", "ship?", List.of("yes", "no"), 1, null))
                .isInstanceOf(NullPointerException.class);
        assertThat(new Motion(null, "rel:1", "ship?", List.of("yes", "no"), 1, HOUR).space())
                .as("a null space reads as inferred").isEmpty();
    }
}
