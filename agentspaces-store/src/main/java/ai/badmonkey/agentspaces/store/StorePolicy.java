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
package ai.badmonkey.agentspaces.store;

import ai.badmonkey.agentspaces.agent.content.StorePin;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What a content store accepts (PDT 2 phase 3, P2-S-05; srad/agentspaces-pdt2.md §3.2.3, appendix C). The
 * store refuses a pin it cannot honour: a kind it does not accept, more copies than {@link #copiesMax}, a
 * requester its {@link #acceptRequesters} rule refuses, a root above {@link #maxObjectBytes}, or a root that
 * would take it past {@link #usableBytes}. A store at capacity keeps serving what it holds and refuses new
 * pins with a reason (P2-Q-AVL-02). The swarm's own {@code ContentPolicy} still governs serving and budgets.
 *
 * @param capacityBytes     the bytes the store will hold ({@code agentspaces.store.capacity})
 * @param reserveBytes      the bytes kept free below the capacity ({@code reserve}, 5 % by default)
 * @param maxObjectBytes    the largest root accepted ({@code max-object})
 * @param leaseDefault      the lease of a pin asked by invoke without one ({@code lease-default})
 * @param leaseMax          the longest lease honoured; a longer pin is held this long ({@code lease-max})
 * @param copiesMax         the largest {@code copies} honoured ({@code copies-max})
 * @param acceptKinds       {@code tree}, {@code artifact}, or both ({@code accept-kinds})
 * @param acceptRequesters  {@link Accept#OPEN} or {@link Accept#GRANT} ({@code accept-requesters})
 * @param blind             whether the store holds without the content key ({@code blind}, P2-S-08)
 * @param tier              {@code hot} or {@code cold} ({@code tier})
 * @param pricePerGibMonth  the price hint on the card, 0 for none ({@code price-per-gib-month})
 */
public record StorePolicy(long capacityBytes, long reserveBytes, long maxObjectBytes, Duration leaseDefault, Duration leaseMax,
                          int copiesMax, Set<String> acceptKinds, Accept acceptRequesters, boolean blind, String tier,
                          long pricePerGibMonth) {

    /** Who may pin. */
    public enum Accept {
        /** Every member of the group. */
        OPEN,
        /**
         * A member the authorizer permits {@code CONTENT_FETCH} on the root (P2-Q-SEC-06). A store with this rule
         * and no authorizer takes no pins and says on its card that it waits for one (PDT 2 follow-ups,
         * 2026-11-09, F2).
         */
        GRANT
    }

    /** The hot tier. */
    public static final String HOT = "hot";

    /** The cold tier. */
    public static final String COLD = "cold";

    private static final Pattern SIZE = Pattern.compile("([0-9]{1,19})\\s*([KMGT]i?B|B)?", Pattern.CASE_INSENSITIVE);

    public StorePolicy {
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacity must be positive, got " + capacityBytes);
        }
        if (reserveBytes < 0 || reserveBytes >= capacityBytes) {
            throw new IllegalArgumentException("reserve must be in [0, capacity), got " + reserveBytes);
        }
        if (maxObjectBytes <= 0) {
            throw new IllegalArgumentException("max-object must be positive, got " + maxObjectBytes);
        }
        Objects.requireNonNull(leaseDefault, "leaseDefault");
        Objects.requireNonNull(leaseMax, "leaseMax");
        if (leaseDefault.toSeconds() < 1 || leaseDefault.compareTo(leaseMax) > 0) {
            throw new IllegalArgumentException("lease-default must be at least 1 s and at most lease-max, got "
                    + leaseDefault + " and " + leaseMax);
        }
        if (copiesMax < 1) {
            throw new IllegalArgumentException("copies-max must be at least 1, got " + copiesMax);
        }
        acceptKinds = Set.copyOf(Objects.requireNonNull(acceptKinds, "acceptKinds"));
        if (acceptKinds.isEmpty() || !Set.of(StorePin.KIND_TREE, StorePin.KIND_ARTIFACT).containsAll(acceptKinds)) {
            throw new IllegalArgumentException("accept-kinds names tree, artifact, or both, got " + acceptKinds);
        }
        Objects.requireNonNull(acceptRequesters, "acceptRequesters");
        if (!HOT.equals(tier) && !COLD.equals(tier)) {
            throw new IllegalArgumentException("tier is hot or cold, got " + tier);
        }
        if (pricePerGibMonth < 0) {
            throw new IllegalArgumentException("price-per-gib-month must not be negative, got " + pricePerGibMonth);
        }
    }

    /** The defaults of appendix C over a capacity: 5 % reserved, 64 GiB objects, 30 d and 365 d leases, 3 copies. */
    public static StorePolicy of(long capacityBytes) {
        return new StorePolicy(capacityBytes, capacityBytes / 20, 64L << 30, Duration.ofDays(30), Duration.ofDays(365), 3,
                Set.of(StorePin.KIND_TREE, StorePin.KIND_ARTIFACT), Accept.GRANT, false, HOT, 0);
    }

    /** The bytes a store fills before it refuses pins: the capacity less the reserve. */
    public long usableBytes() {
        return capacityBytes - reserveBytes;
    }

    public StorePolicy reserve(long bytes) {
        return new StorePolicy(capacityBytes, bytes, maxObjectBytes, leaseDefault, leaseMax, copiesMax, acceptKinds,
                acceptRequesters, blind, tier, pricePerGibMonth);
    }

    public StorePolicy maxObject(long bytes) {
        return new StorePolicy(capacityBytes, reserveBytes, bytes, leaseDefault, leaseMax, copiesMax, acceptKinds,
                acceptRequesters, blind, tier, pricePerGibMonth);
    }

    public StorePolicy leases(Duration defaultLease, Duration maxLease) {
        return new StorePolicy(capacityBytes, reserveBytes, maxObjectBytes, defaultLease, maxLease, copiesMax, acceptKinds,
                acceptRequesters, blind, tier, pricePerGibMonth);
    }

    public StorePolicy copiesMax(int value) {
        return new StorePolicy(capacityBytes, reserveBytes, maxObjectBytes, leaseDefault, leaseMax, value, acceptKinds,
                acceptRequesters, blind, tier, pricePerGibMonth);
    }

    public StorePolicy acceptKinds(Set<String> value) {
        return new StorePolicy(capacityBytes, reserveBytes, maxObjectBytes, leaseDefault, leaseMax, copiesMax, value,
                acceptRequesters, blind, tier, pricePerGibMonth);
    }

    public StorePolicy acceptRequesters(Accept value) {
        return new StorePolicy(capacityBytes, reserveBytes, maxObjectBytes, leaseDefault, leaseMax, copiesMax, acceptKinds,
                value, blind, tier, pricePerGibMonth);
    }

    public StorePolicy blind(boolean value) {
        return new StorePolicy(capacityBytes, reserveBytes, maxObjectBytes, leaseDefault, leaseMax, copiesMax, acceptKinds,
                acceptRequesters, value, tier, pricePerGibMonth);
    }

    public StorePolicy tier(String value) {
        return new StorePolicy(capacityBytes, reserveBytes, maxObjectBytes, leaseDefault, leaseMax, copiesMax, acceptKinds,
                acceptRequesters, blind, value, pricePerGibMonth);
    }

    public StorePolicy price(long perGibMonth) {
        return new StorePolicy(capacityBytes, reserveBytes, maxObjectBytes, leaseDefault, leaseMax, copiesMax, acceptKinds,
                acceptRequesters, blind, tier, perGibMonth);
    }

    /**
     * Reads a size: a count of bytes, or a count with a binary unit ({@code KiB}, {@code MiB}, {@code GiB},
     * {@code TiB}, or the same without the {@code i}, which this reader also takes as binary, as Spring's
     * {@code DataSize} does).
     *
     * @param text the size, such as {@code 2TiB}
     * @return the bytes
     * @throws IllegalArgumentException when the text is no size
     */
    public static long bytes(String text) {
        Matcher m = SIZE.matcher(text == null ? "" : text.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException("'" + text + "' is not a size such as 2TiB, 50GiB, or 1024");
        }
        long count = Long.parseLong(m.group(1));
        String unit = m.group(2) == null ? "B" : m.group(2).toUpperCase(Locale.ROOT);
        int shift = switch (unit.charAt(0)) {
            case 'K' -> 10;
            case 'M' -> 20;
            case 'G' -> 30;
            case 'T' -> 40;
            default -> 0;
        };
        if (shift > 0 && count > (Long.MAX_VALUE >> shift)) {
            throw new IllegalArgumentException("'" + text + "' is larger than a long");
        }
        return count << shift;
    }

    /**
     * Reads a reserve: a percentage of the capacity ({@code 5%}) or a size.
     *
     * @param text     the reserve
     * @param capacity the capacity in bytes
     * @return the bytes
     */
    public static long reserveOf(String text, long capacity) {
        String t = text == null ? "" : text.trim();
        if (t.endsWith("%")) {
            String number = t.substring(0, t.length() - 1).trim();
            if (!number.matches("[0-9]{1,2}")) {
                throw new IllegalArgumentException("'" + text + "' is not a percentage below 100");
            }
            return capacity / 100 * Integer.parseInt(number) + capacity % 100 * Integer.parseInt(number) / 100;
        }
        return bytes(t);
    }
}
