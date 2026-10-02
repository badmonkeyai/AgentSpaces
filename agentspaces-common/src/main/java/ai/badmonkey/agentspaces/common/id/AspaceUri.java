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
package ai.badmonkey.agentspaces.common.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.Optional;

/**
 * The {@code aspace://} URI scheme (§4.4 of the spec):
 * {@code aspace://<groupId>/<spaceName>[#<entryId>]}. All AgentSpaces resources are
 * addressable under this common scheme.
 *
 * @param group     the group that scopes the space
 * @param spaceName the space's name within the group
 * @param fragment  optional entry identifier fragment; {@code null} when absent
 */
public record AspaceUri(GroupId group, String spaceName, String fragment) {

    /** The URI scheme name. */
    public static final String SCHEME = "aspace";

    private static final String PREFIX = SCHEME + "://";

    public AspaceUri {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(spaceName, "spaceName");
        if (spaceName.isEmpty() || spaceName.indexOf('/') >= 0 || spaceName.indexOf('#') >= 0) {
            throw new IllegalArgumentException(
                    "spaceName must be non-empty and contain no '/' or '#': " + spaceName);
        }
        if (fragment != null && fragment.isEmpty()) {
            throw new IllegalArgumentException("fragment must be null or non-empty");
        }
    }

    /**
     * Creates a URI for a space, without an entry fragment.
     *
     * @param group     the group
     * @param spaceName the space name
     * @return the URI
     */
    public static AspaceUri of(GroupId group, String spaceName) {
        return new AspaceUri(group, spaceName, null);
    }

    /**
     * Parses an {@code aspace://<groupId>/<spaceName>[#<entryId>]} string.
     *
     * @param uri the URI string
     * @return the parsed URI
     * @throws IllegalArgumentException if the input is not a valid aspace URI
     */
    @JsonCreator
    public static AspaceUri parse(String uri) {
        Objects.requireNonNull(uri, "uri");
        if (!uri.startsWith(PREFIX)) {
            throw new IllegalArgumentException("expected " + PREFIX + "…, got: " + uri);
        }
        String rest = uri.substring(PREFIX.length());
        String fragment = null;
        int hash = rest.indexOf('#');
        if (hash >= 0) {
            fragment = rest.substring(hash + 1);
            rest = rest.substring(0, hash);
        }
        int slash = rest.indexOf('/');
        if (slash <= 0 || slash == rest.length() - 1) {
            throw new IllegalArgumentException(
                    "expected " + PREFIX + "<groupId>/<spaceName>, got: " + uri);
        }
        return new AspaceUri(GroupId.of(rest.substring(0, slash)), rest.substring(slash + 1), fragment);
    }

    /** Returns the optional entry fragment. */
    public Optional<String> entryFragment() {
        return Optional.ofNullable(fragment);
    }

    /** Returns the full URI string; this is the serialized form. */
    @JsonValue
    public String encoded() {
        return PREFIX + group.value() + "/" + spaceName + (fragment == null ? "" : "#" + fragment);
    }

    @Override
    public String toString() {
        return encoded();
    }
}
