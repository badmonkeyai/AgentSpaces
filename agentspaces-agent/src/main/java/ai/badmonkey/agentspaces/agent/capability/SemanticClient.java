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

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.capabilities.semantic.SemanticDiscovery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The typed client for {@code aspace:cap/semantic-discovery} (SPEC §8): find
 * advertisements by meaning, locally or across the fleet. Resolved through
 * {@code group.capability(SemanticClient.class)} or injected with
 * {@code @CapabilityRef}.
 */
public final class SemanticClient {

    private final SemanticDiscovery semantic;
    private final AgentSpaces.GroupContext group;

    SemanticClient(SemanticDiscovery semantic, AgentSpaces.GroupContext group) {
        this.semantic = Objects.requireNonNull(semantic, "semantic");
        this.group = Objects.requireNonNull(group, "group");
    }

    /** Ranks this peer's cached advertisements against a free-text query. */
    public List<SemanticDiscovery.Match> query(String text, int limit) {
        return semantic.query(text, limit);
    }

    /** Asks the fleet's semantic providers and merges their answers, within the timeout. */
    public List<SemanticDiscovery.Match> remoteQuery(String text, int limit, Duration timeout) {
        return semantic.remoteQuery(text, limit, timeout);
    }

    /** The semantic-discovery providers currently advertised in the group. */
    public List<CapabilityAdvertisement> providers() {
        return group.discovery().find(CapabilityAdvertisement.class,
                ad -> SemanticDiscovery.TYPE.equals(ad.capabilityType()));
    }

    /** The underlying capability. */
    public SemanticDiscovery capability() {
        return semantic;
    }

    /** Resolves the client from the group's locally provided semantic discovery. */
    public static final class Factory implements CapabilityClientFactory<SemanticClient> {
        @Override
        public Class<SemanticClient> clientType() {
            return SemanticClient.class;
        }

        @Override
        public SemanticClient create(AgentSpaces.GroupContext group) {
            Objects.requireNonNull(group, "group");
            Optional<SemanticDiscovery> local = group.provider(SemanticDiscovery.TYPE)
                    .filter(SemanticDiscovery.class::isInstance)
                    .map(SemanticDiscovery.class::cast);
            if (local.isPresent()) {
                return new SemanticClient(local.get(), group);
            }
            throw new IllegalStateException("group '" + group.name() + "' provides no "
                    + SemanticDiscovery.TYPE + "; register one with group.provide(new"
                    + " SemanticDiscovery(...)) or agentspaces.capabilities.semantic-discovery=true");
        }
    }
}
