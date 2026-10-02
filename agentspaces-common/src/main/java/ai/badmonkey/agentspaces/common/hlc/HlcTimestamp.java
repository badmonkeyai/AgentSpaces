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
package ai.badmonkey.agentspaces.common.hlc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

/**
 * A hybrid logical clock (HLC) timestamp: a physical wall-clock component in epoch
 * milliseconds, a logical counter that disambiguates events within the same
 * millisecond, and the identifier of the node that issued the timestamp.
 *
 * <p>Timestamps order totally: first by physical time, then by logical counter, then
 * by node identifier. The node component makes the ordering deterministic across the
 * whole fleet, which the {@code LEASE_RACE} arbitration in the spec (§7.4) relies on.
 *
 * <p>The canonical string form is {@code physical:logical:node}, and that form is
 * used for serialization.
 *
 * @param physical wall-clock component, epoch milliseconds
 * @param logical  logical counter within the same physical millisecond
 * @param node     identifier of the issuing node; never {@code null}
 */
public record HlcTimestamp(long physical, int logical, String node)
        implements Comparable<HlcTimestamp> {

    public HlcTimestamp {
        Objects.requireNonNull(node, "node");
        if (physical < 0) {
            throw new IllegalArgumentException("physical must be >= 0: " + physical);
        }
        if (logical < 0) {
            throw new IllegalArgumentException("logical must be >= 0: " + logical);
        }
        if (node.isEmpty() || node.indexOf(':') >= 0) {
            throw new IllegalArgumentException("node must be non-empty and contain no ':': " + node);
        }
    }

    /**
     * Parses the canonical {@code physical:logical:node} form.
     *
     * @param encoded the canonical string form
     * @return the parsed timestamp
     * @throws IllegalArgumentException if the input does not match the canonical form
     */
    @JsonCreator
    public static HlcTimestamp parse(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        String[] parts = encoded.split(":", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("expected physical:logical:node, got: " + encoded);
        }
        try {
            return new HlcTimestamp(Long.parseLong(parts[0]), Integer.parseInt(parts[1]), parts[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("expected physical:logical:node, got: " + encoded, e);
        }
    }

    /** Returns the canonical {@code physical:logical:node} form. */
    @JsonValue
    public String encoded() {
        return physical + ":" + logical + ":" + node;
    }

    @Override
    public int compareTo(HlcTimestamp other) {
        int byPhysical = Long.compare(physical, other.physical);
        if (byPhysical != 0) {
            return byPhysical;
        }
        int byLogical = Integer.compare(logical, other.logical);
        if (byLogical != 0) {
            return byLogical;
        }
        return node.compareTo(other.node);
    }

    @Override
    public String toString() {
        return encoded();
    }
}
