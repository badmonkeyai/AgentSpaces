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

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.common.id.GroupId;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What a {@link BeaconLocator} answers: endpoints to dial in priority order,
 * optionally the GroupId the locator believes the group has, optionally the
 * signed founding advertisement when the source carried one (the CBOR of the
 * peering layer's {@code SignedGroupAdvertisement}), parameters the source
 * carried (wire hints, a multicast group), how long the hint may be reused,
 * and the evidence a trace shows ({@code "SRV hq._agentspaces._tcp.corp.example"},
 * {@code "GET https://..."}, {@code "seeds[0]"}).
 *
 * <p>A GroupId or founding in a hint is a claim, not a fact: the node verifies
 * a founding against the id it was configured with (or against itself when it
 * has none) and accepts a GroupId claim only from locators the operator
 * named. Endpoints are only ever dialed.
 */
public record BeaconHint(List<PeerAdvertisement.Endpoint> endpoints,
                         Optional<GroupId> groupId,
                         Optional<byte[]> founding,
                         Map<String, String> parameters,
                         Duration ttl,
                         String evidence) {

    /** The default time to live of a hint whose source states none. */
    public static final Duration DEFAULT_TTL = Duration.ofMinutes(5);

    private static final BeaconHint NONE = new BeaconHint(List.of(), Optional.empty(),
            Optional.empty(), Map.of(), Duration.ZERO, "none");

    public BeaconHint {
        endpoints = List.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(founding, "founding");
        parameters = Map.copyOf(Objects.requireNonNull(parameters, "parameters"));
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(evidence, "evidence");
    }

    /** The empty hint: the locator had nothing. */
    public static BeaconHint none() {
        return NONE;
    }

    /**
     * A hint of endpoints only, with the default time to live.
     *
     * @param endpoints the endpoints, priority-ordered
     * @param evidence  what produced them
     * @return the hint
     */
    public static BeaconHint of(List<PeerAdvertisement.Endpoint> endpoints, String evidence) {
        return new BeaconHint(endpoints, Optional.empty(), Optional.empty(), Map.of(), DEFAULT_TTL, evidence);
    }

    /** Whether the hint carries neither endpoints nor a founding. */
    public boolean isEmpty() {
        return endpoints.isEmpty() && founding.isEmpty();
    }
}
