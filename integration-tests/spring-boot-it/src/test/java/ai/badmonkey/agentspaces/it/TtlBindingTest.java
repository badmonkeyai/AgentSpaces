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
package ai.badmonkey.agentspaces.it;

import ai.badmonkey.agentspaces.common.time.TtlFloor;
import ai.badmonkey.agentspaces.spring.AgentSpacesAutoConfiguration;
import ai.badmonkey.agentspaces.spring.AgentSpacesProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ByteArrayResource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code agentspaces.ttl.*} through Spring Boot's own binder (srad/agentspaces-event-loop.md EL-E-04): an expert
 * override's key is dotted, so it is written in brackets, in the properties form and in YAML, and the map keeps it
 * whole.
 */
class TtlBindingTest {

    private static AgentSpacesProperties bind(Map<String, String> values) {
        return new Binder(new MapConfigurationPropertySource(values))
                .bind("agentspaces", AgentSpacesProperties.class).orElseGet(AgentSpacesProperties::new);
    }

    @Test
    void aDottedOverrideKeyBindsWholeInThePropertiesForm() {
        AgentSpacesProperties p = bind(Map.of("agentspaces.ttl.strict", "true",
                "agentspaces.embabel.action-lease", "30s",
                "agentspaces.ttl.expert-overrides[agentspaces.embabel.action-lease]", "10 s actions, renewed at 15 s"));
        assertThat(p.getTtl().isStrict()).isTrue();
        assertThat(p.getTtl().getExpertOverrides()).containsOnlyKeys("agentspaces.embabel.action-lease");
        AgentSpacesAutoConfiguration.checkTtls(p);
        assertThat(AgentSpacesAutoConfiguration.ttlFloor(p).findings()).singleElement()
                .extracting(TtlFloor.Finding::verdict).isEqualTo(TtlFloor.Verdict.OVERRIDDEN);
    }

    @Test
    void aDottedOverrideKeyBindsWholeInYamlBracketForm() throws Exception {
        String yaml = """
                agentspaces:
                  ttl:
                    strict: true
                    expert-overrides:
                      "[agentspaces.relay.path-ttl]": "a lab fleet whose paths change every 10 s"
                      "[binder.Clerk.confirm.lease]": "a 2 s handler"
                  relay:
                    path-ttl: 20s
                """;
        List<PropertySource<?>> sources = new YamlPropertySourceLoader().load("ttl",
                new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)));
        Map<String, String> flat = new LinkedHashMap<>();
        for (PropertySource<?> source : sources) {
            if (source instanceof EnumerablePropertySource<?> e) {
                for (String name : e.getPropertyNames()) {
                    flat.put(name, String.valueOf(e.getProperty(name)));
                }
            }
        }
        AgentSpacesProperties p = bind(flat);
        assertThat(p.getTtl().getExpertOverrides())
                .containsOnlyKeys("agentspaces.relay.path-ttl", "binder.Clerk.confirm.lease");
        assertThat(p.getRelay().getPathTtl()).isEqualTo(Duration.ofSeconds(20));
        AgentSpacesAutoConfiguration.checkTtls(p);
        assertThat(AgentSpacesAutoConfiguration.ttlFloor(p).findings()).extracting(TtlFloor.Finding::verdict)
                .containsExactly(TtlFloor.Verdict.OVERRIDDEN);
    }
}
