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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An entry returned from a bound method together with the tags it should be
 * written with (SPEC §10.3, issue #16). The binder unwraps it before any other
 * return handling: a same-space take result goes through
 * {@code complete(taken, entry, lease, tags)}, atomically with the completion,
 * and anything else through {@code write(entry, lease, tags)}. The sibling of
 * {@link ai.badmonkey.agentspaces.agent.capability.Contribution}: a return
 * value the binder recognises, so no annotation attribute has to fix at bind
 * time what usually depends on the entry.
 *
 * <pre>{@code
 * @SpaceNotify(space = "intake")
 * public Tagged<Instance> estimate(RegisteredClaim claim) {
 *     return Tagged.of(Instance.damageFinding(claim), "claimId", claim.claimId(),
 *             "rdf:type", Vocabulary.DAMAGE_FINDING);
 * }
 * }</pre>
 *
 * @param entry the entry to write
 * @param tags  the tags to write it with
 * @param <T>   the entry type
 */
public record Tagged<T>(T entry, Map<String, String> tags) {

    public Tagged {
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(tags, "tags");
        tags = Map.copyOf(tags);
    }

    /** Wraps an entry with its tags. */
    public static <T> Tagged<T> of(T entry, Map<String, String> tags) {
        return new Tagged<>(entry, tags);
    }

    /**
     * Wraps an entry with tags given as alternating keys and values.
     *
     * @throws IllegalArgumentException when the keys and values do not pair up
     */
    public static <T> Tagged<T> of(T entry, String... keysAndValues) {
        Objects.requireNonNull(keysAndValues, "keysAndValues");
        if (keysAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("tags come as key, value pairs; got "
                    + keysAndValues.length + " strings");
        }
        Map<String, String> tags = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            tags.put(Objects.requireNonNull(keysAndValues[i], "tag key"),
                    Objects.requireNonNull(keysAndValues[i + 1], "tag value"));
        }
        return new Tagged<>(entry, tags);
    }
}
