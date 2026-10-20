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
package ai.badmonkey.agentspaces.peering.bootstrap;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The configured seed list as a locator: {@code host:port} or
 * {@code scheme://host:port} strings, a bare one taking the request's default
 * scheme, priorities by position. Behaviour-identical to the seed list the
 * node took before locators existed; {@link #fixed(List)} wraps an already
 * parsed list for the overloads that still take one.
 */
public final class SeedsLocator implements BeaconLocator {

    /** The locator's name in traces and configuration. */
    public static final String NAME = "seeds";

    private final List<String> seeds;
    private final List<PeerAdvertisement.Endpoint> fixed;

    private SeedsLocator(List<String> seeds, List<PeerAdvertisement.Endpoint> fixed) {
        this.seeds = seeds;
        this.fixed = fixed;
    }

    /**
     * A locator over seed strings.
     *
     * @param seeds {@code host:port} or {@code scheme://host:port}, dialed in order
     * @return the locator
     */
    public static SeedsLocator of(List<String> seeds) {
        return new SeedsLocator(List.copyOf(Objects.requireNonNull(seeds, "seeds")), null);
    }

    /**
     * A locator over parsed endpoints, returned as they are.
     *
     * @param endpoints the endpoints
     * @return the locator
     */
    public static SeedsLocator fixed(List<PeerAdvertisement.Endpoint> endpoints) {
        return new SeedsLocator(null, List.copyOf(Objects.requireNonNull(endpoints, "endpoints")));
    }

    /**
     * Parses seed strings into endpoints.
     *
     * @param seeds         the strings
     * @param defaultScheme the scheme a bare {@code host:port} takes
     * @return the endpoints, priorities by position
     */
    public static List<PeerAdvertisement.Endpoint> parse(List<String> seeds, String defaultScheme) {
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < seeds.size(); i++) {
            String seed = seeds.get(i).trim();
            if (seed.isEmpty()) {
                continue;
            }
            String scheme = defaultScheme;
            int separator = seed.indexOf("://");
            if (separator > 0) {
                scheme = seed.substring(0, separator);
                seed = seed.substring(separator + 3);
            }
            endpoints.add(new PeerAdvertisement.Endpoint(scheme, seed, i));
        }
        return endpoints;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public BeaconHint locate(BeaconRequest request) {
        List<PeerAdvertisement.Endpoint> endpoints = fixed != null ? fixed
                : parse(seeds, request.defaultScheme());
        if (endpoints.isEmpty()) {
            return BeaconHint.none();
        }
        return new BeaconHint(endpoints, java.util.Optional.empty(), java.util.Optional.empty(),
                java.util.Map.of(), java.time.Duration.ofDays(365), "seeds[" + endpoints.size() + "]");
    }
}
