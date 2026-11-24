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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.common.time.TtlFloor;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The starter's TTL floor at start (srad/agentspaces-event-loop.md EL-E-03, EL-E-04, decision 18): a configured value
 * under 60 s warns, or refuses to start under {@code agentspaces.ttl.strict=true}; an expert override passes its key.
 * The dotted keys' binding, in the properties form and YAML's bracket form, is {@code TtlBindingTest} in
 * {@code integration-tests/spring-boot-it}, which has Spring Boot's binder.
 */
class TtlFloorAutoConfigurationTest {

    @Test
    void theDefaultsPassSilentlyUnderAStrictFloor() {
        AgentSpacesProperties p = new AgentSpacesProperties();
        p.getTtl().setStrict(true);
        AgentSpacesAutoConfiguration.checkTtls(p);
        assertThat(AgentSpacesAutoConfiguration.ttlFloor(p).findings()).isEmpty();
    }

    @Test
    void aStrictFloorRefusesAThirtySecondActionLeaseAtStartNamingTheKey() {
        AgentSpacesProperties p = new AgentSpacesProperties();
        p.getTtl().setStrict(true);
        p.getEmbabel().setActionLease("30s");
        assertThatThrownBy(() -> new AgentSpacesAutoConfiguration().buildNode(p, PeerIdentity.generate(), List.of(),
                List.of(), List.of(), null))
                .isInstanceOf(TtlFloor.TtlFloorException.class)
                .hasMessageContaining("agentspaces.embabel.action-lease=PT30S");
    }

    @Test
    void aLenientFloorWarnsAndStarts() {
        AgentSpacesProperties p = new AgentSpacesProperties();
        p.getRelay().setPathTtl(Duration.ofSeconds(20));
        AgentSpacesAutoConfiguration.checkTtls(p);
        assertThat(AgentSpacesAutoConfiguration.ttlFloor(p).findings()).singleElement().satisfies(f -> {
            assertThat(f.key()).isEqualTo("agentspaces.relay.path-ttl");
            assertThat(f.verdict()).isEqualTo(TtlFloor.Verdict.WARNED);
        });
    }

    @Test
    void anExpertOverrideWithADottedKeyPasses() {
        AgentSpacesProperties p = new AgentSpacesProperties();
        p.getTtl().setStrict(true);
        p.getEmbabel().setActionLease("30s");
        p.getTtl().getExpertOverrides().put("agentspaces.embabel.action-lease", "10 s actions, renewed at 15 s");
        AgentSpacesAutoConfiguration.checkTtls(p);
        assertThat(AgentSpacesAutoConfiguration.ttlFloor(p).findings()).singleElement().satisfies(f -> {
            assertThat(f.verdict()).isEqualTo(TtlFloor.Verdict.OVERRIDDEN);
            assertThat(f.reason()).isEqualTo("10 s actions, renewed at 15 s");
        });
    }
}
