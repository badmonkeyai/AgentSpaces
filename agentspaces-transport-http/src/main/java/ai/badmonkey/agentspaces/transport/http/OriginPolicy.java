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
package ai.badmonkey.agentspaces.transport.http;

import java.net.URI;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * The browser-origin policy of an HTTP listener (ISSUE-Websocket SR-5, §9.7;
 * SRAD agentspaces-js JS-H-03): a request without an {@code Origin} header
 * (every non-browser peer) is allowed; a request with one is allowed only
 * when the origin is on the list. The default list is empty, so browsers are
 * refused until an operator names them. {@code *} admits any browser and is
 * for demonstrations only.
 */
public final class OriginPolicy {

    private static final OriginPolicy NONE = new OriginPolicy(Set.of(), false);

    private final Set<String> origins;
    private final boolean any;

    private OriginPolicy(Set<String> origins, boolean any) {
        this.origins = origins;
        this.any = any;
    }

    /** The default: no browser origin is allowed. */
    public static OriginPolicy none() {
        return NONE;
    }

    /**
     * A policy allowing the given serialised origins ({@code scheme://host[:port]},
     * RFC 6454 §6.1), compared case-insensitively on scheme and host and exactly on port.
     *
     * @param origins the allowed origins, or {@code *}
     * @return the policy
     * @throws IllegalArgumentException when an entry is not a serialised origin
     */
    public static OriginPolicy of(Collection<String> origins) {
        Set<String> normalized = new LinkedHashSet<>();
        boolean any = false;
        for (String origin : origins) {
            if (origin == null || origin.isBlank()) {
                continue;
            }
            if (origin.trim().equals("*")) {
                any = true;
                continue;
            }
            normalized.add(normalize(origin.trim()));
        }
        return new OriginPolicy(Set.copyOf(normalized), any);
    }

    /** Whether a request carrying this {@code Origin} header value (null when absent) is allowed. */
    public boolean allows(String origin) {
        if (origin == null) {
            return true;
        }
        if (any) {
            return true;
        }
        try {
            return origins.contains(normalize(origin.trim()));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** Whether any browser origin is allowed at all. */
    public boolean admitsBrowsers() {
        return any || !origins.isEmpty();
    }

    /** The allowed origins, normalized. */
    public Set<String> origins() {
        return origins;
    }

    static String normalize(String origin) {
        URI uri;
        try {
            uri = URI.create(origin);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("not a serialised origin: " + origin, e);
        }
        String scheme = uri.getScheme();
        String host = uri.getHost();
        if (scheme == null || host == null || uri.getRawPath() != null && !uri.getRawPath().isEmpty()
                || uri.getRawQuery() != null || uri.getRawFragment() != null || uri.getRawUserInfo() != null) {
            throw new IllegalArgumentException("not a serialised origin (scheme://host[:port]): " + origin);
        }
        String out = scheme.toLowerCase(Locale.ROOT) + "://" + host.toLowerCase(Locale.ROOT);
        return uri.getPort() >= 0 ? out + ":" + uri.getPort() : out;
    }

    @Override
    public String toString() {
        return any ? "OriginPolicy[*]" : "OriginPolicy" + origins;
    }
}
