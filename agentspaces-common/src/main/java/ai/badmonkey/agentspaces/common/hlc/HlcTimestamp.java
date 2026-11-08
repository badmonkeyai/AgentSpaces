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

    /**
     * The canonical constructor. Jackson decoded a map in place of the canonical string
     * through it and truncated a fractional counter (HLC logical counter, 2026-11-08, Rust
     * team report), so Jackson now decodes a stamp through {@link #parse(String)} alone.
     */
    @JsonCreator(mode = JsonCreator.Mode.DISABLED)
    public HlcTimestamp {
        Objects.requireNonNull(node, "node");
        if (physical < 0) {
            throw new IllegalArgumentException("physical must be >= 0: " + physical);
        }
        if (logical < 0) {
            throw new IllegalArgumentException("logical must be >= 0: " + logical);
        }
        if (!canonicalNode(node)) {
            throw new IllegalArgumentException(
                    "node must be non-empty and contain no ':' and no control character: " + node);
        }
    }

    /**
     * Whether a node is non-empty and holds no colon and no control character, U+0000 to U+001F
     * or U+007F (HLC canonical stamps, 2026-11-08). The constructor demands it, so every stamp
     * this process builds encodes to text that {@link #parse(String)} accepts.
     */
    private static boolean canonicalNode(String node) {
        if (node.isEmpty()) {
            return false;
        }
        for (int i = 0; i < node.length(); i++) {
            char c = node.charAt(i);
            if (c == ':' || c < 0x20 || c == 0x7f) {
                return false;
            }
        }
        return true;
    }

    /**
     * Parses the canonical {@code physical:logical:node} form, and refuses any other text (HLC
     * canonical stamps, 2026-11-08). The physical and logical parts are each {@code 0} or a
     * non-zero ASCII digit followed by ASCII digits: no leading zero, no sign, no fraction, no
     * exponent, no whitespace, no non-ASCII digit, never empty. The physical part fits a signed
     * 64-bit integer and the logical part an {@code int}. The node is non-empty and holds no
     * colon and no control character. A stamp therefore has exactly one text, and the bytes a
     * signer signed always equal {@link #encoded()}; Java once read {@code "007:00:n"} as
     * {@code 7:0:n} and verified signatures over the re-encoding while the clients verified over
     * the raw text, which split the claim lattice between them.
     *
     * <p>A counter from 65537 up to the {@code int} range parses, so the frame or state that
     * carries it decodes, its signature verifies, and the merge point that refuses it through
     * {@link HybridLogicalClock#isAcceptable(HlcTimestamp)} can strike its signer (HLC logical
     * counter, 2026-11-08, Rust team report). Every Java merge and ordering point refuses such
     * a stamp through that check.
     *
     * @param encoded the canonical string form
     * @return the parsed timestamp
     * @throws IllegalArgumentException if the input does not match the canonical form
     */
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static HlcTimestamp parse(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        String[] parts = encoded.split(":", 3);
        if (parts.length != 3 || !canonicalDigits(parts[0]) || !canonicalDigits(parts[1])) {
            throw new IllegalArgumentException("expected physical:logical:node, got: " + encoded);
        }
        try {
            return new HlcTimestamp(Long.parseLong(parts[0]), Integer.parseInt(parts[1]), parts[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("expected physical:logical:node, got: " + encoded, e);
        }
    }

    /**
     * Whether a part is {@code 0} or a non-zero ASCII digit followed by ASCII digits (HLC
     * canonical stamps, 2026-11-08). Long.parseLong also takes a sign, non-ASCII digits, and
     * leading zeros, none of which re-encode to the text that was signed.
     */
    private static boolean canonicalDigits(String part) {
        if (part.isEmpty() || (part.length() > 1 && part.charAt(0) == '0')) {
            return false;
        }
        for (int i = 0; i < part.length(); i++) {
            char c = part.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
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
