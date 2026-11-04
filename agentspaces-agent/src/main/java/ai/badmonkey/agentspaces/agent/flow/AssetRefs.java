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


package ai.badmonkey.agentspaces.agent.flow;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;

import java.util.Optional;

/** Resolves {@code asset:<source>/<asset>} (or a bare name) against the group's ad cache (FR-5). */
public final class AssetRefs {

    public static final String PREFIX = "asset:";

    private AssetRefs() {
    }

    public static String nameOf(String reference) {
        return reference.startsWith(PREFIX) ? reference.substring(PREFIX.length()) : reference;
    }

    public static boolean isReference(String value) {
        return value != null && value.startsWith(PREFIX);
    }

    public static Optional<AssetRef> resolve(DiscoveryService discovery, String reference) {
        if (discovery == null || reference == null || reference.isBlank()) {
            return Optional.empty();
        }
        String name = nameOf(reference);
        return discovery.find(AssetCard.class, c -> name.equals(c.asset())).stream().findFirst().map(AssetRef::of);
    }
}
