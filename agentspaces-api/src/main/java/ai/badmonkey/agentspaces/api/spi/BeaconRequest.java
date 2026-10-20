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
package ai.badmonkey.agentspaces.api.spi;

import ai.badmonkey.agentspaces.common.id.GroupId;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * What the node knows when it asks a {@link BeaconLocator}.
 *
 * @param groupName     the group's configured name
 * @param groupId       the GroupId, when the group was founded locally or is
 *                      being joined by id; empty when only a name is known
 * @param defaultScheme the transport scheme a bare {@code host:port} takes
 *                      ({@code tls} or {@code tcp}, from the node's own
 *                      transports)
 * @param schemes       every scheme the node can dial, so a locator may skip
 *                      what the node cannot reach
 */
public record BeaconRequest(String groupName,
                            Optional<GroupId> groupId,
                            String defaultScheme,
                            Set<String> schemes) {

    public BeaconRequest {
        Objects.requireNonNull(groupName, "groupName");
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(defaultScheme, "defaultScheme");
        schemes = Set.copyOf(Objects.requireNonNull(schemes, "schemes"));
    }
}
