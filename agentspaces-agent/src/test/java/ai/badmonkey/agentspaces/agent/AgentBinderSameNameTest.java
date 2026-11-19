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
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Two agents under one name on one peer (reviews/embabel-agentspaces-review-2026-11-12.md §10.2, deviation 2):
 * a different class is refused; a second instance of the same class is allowed. Whether the two instances'
 * cards are the same card depends on the agent identity factory, which this test pins.
 */
class AgentBinderSameNameTest {

    @AgentSpec(name = "twin", description = "One of two")
    public static class Twin {
    }

    @AgentSpec(name = "twin", description = "An impostor")
    public static class Impostor {
    }

    private final TestClock clock = TestClock.create();
    private final PeerIdentity identity = PeerIdentity.generate();
    private final GroupId group = GroupId.of("zTwins");

    @Test
    void aDifferentClassUnderTheSameNameIsRefused() {
        try (AgentBinder binder = new AgentBinder(identity, group, null, clock, "twins")) {
            binder.bind(new Twin());
            assertThatThrownBy(() -> binder.bind(new Impostor()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(Impostor.class.getName());
        }
    }

    @Test
    void peerSignedTwinsPublishTheSameCard() {
        try (AgentBinder binder = new AgentBinder(identity, group, null, clock, "twins")) {
            AgentCard first = binder.bind(new Twin()).card();
            AgentCard second = binder.bind(new Twin()).card();
            assertThat(second.id()).isEqualTo(first.id());
            assertThat(second.agentPublicKey()).isNull();
            assertThat(first.agentPublicKey()).isNull();
            // Everything but the issue time is the same, so a replacement in an ad-cache changes nothing a reader sees.
            assertThat(second.agent()).isEqualTo(first.agent());
            assertThat(second.description()).isEqualTo(first.description());
            assertThat(second.consumes()).isEqualTo(first.consumes());
            assertThat(second.produces()).isEqualTo(first.produces());
            assertThat(second.actions()).isEqualTo(first.actions());
            assertThat(second.agentCertificate()).isNull();
        }
    }

    /**
     * Under {@code agent-keys: subordinate} without an agent keystore, the factory makes a fresh key for every
     * bind, so two instances of one class publish one card id under two agent keys: the ad-cache keeps the newer,
     * and the card's key alternates on every refresh. The same-class allowance does not hold here.
     */
    @Test
    void subordinateTwinsPublishOneCardIdUnderTwoKeys() {
        try (AgentBinder binder = new AgentBinder(identity, group, null, clock, "twins",
                name -> identity.renewingSubordinate(name, Duration.ofHours(1), clock))) {
            AgentCard first = binder.bind(new Twin()).card();
            AgentCard second = binder.bind(new Twin()).card();
            assertThat(second.id()).isEqualTo(first.id());
            assertThat(first.agentPublicKey()).isNotNull();
            assertThat(second.agentPublicKey()).isNotEqualTo(first.agentPublicKey());
        }
    }
}
