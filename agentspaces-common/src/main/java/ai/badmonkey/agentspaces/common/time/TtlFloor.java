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
package ai.badmonkey.agentspaces.common.time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The start-up check of configured TTLs, leases and expiries against principle 8 (srad/agentspaces-event-loop.md
 * §3.11, EL-E-03, EL-E-04, decision 18): "TTLs need to be generous; any TTL under 60 s is probably too fast, unless
 * someone really knows what they are doing."
 *
 * <ul>
 *   <li>A value equal to its registered default passes silently: the TTL register carries its reason (the member
 *   TTL's 45 s floor, the 30 s take leases).</li>
 *   <li>A key with an expert override passes, and the override is logged once with its reason.</li>
 *   <li>Any other value under 60 s logs a WARN naming the key, the value and the principle, or, under
 *   {@link #strict}, is refused with a {@link TtlFloorException}.</li>
 * </ul>
 *
 * <p>Thread-safe; each key warns or logs its override once.
 */
public final class TtlFloor {

    private static final System.Logger LOG = System.getLogger(TtlFloor.class.getName());

    /** Principle 8's presumption: a TTL, lease or expiry under this is too fast unless someone says why. */
    public static final Duration FLOOR = Duration.ofSeconds(60);

    /** What the check made of one key. */
    public enum Verdict {
        /** Under the floor with no override: warned (lenient). */
        WARNED,
        /** Under the floor with no override: refused (strict). */
        REFUSED,
        /** An expert override named the key: passed, logged with its reason. */
        OVERRIDDEN
    }

    /**
     * One finding.
     *
     * @param key     the configuration key or binder key
     * @param value   the configured value
     * @param verdict what the check did
     * @param reason  the override's reason, or empty
     */
    public record Finding(String key, Duration value, Verdict verdict, String reason) {
    }

    private final boolean strict;
    private final Map<String, String> overrides;
    private final List<Finding> findings = new ArrayList<>();
    private final Set<String> told = ConcurrentHashMap.newKeySet();

    private TtlFloor(boolean strict, Map<String, String> overrides) {
        this.strict = strict;
        this.overrides = Map.copyOf(overrides);
    }

    /**
     * A check that warns and never refuses, with no overrides: the default everywhere.
     *
     * @return the check
     */
    public static TtlFloor lenient() {
        return new TtlFloor(false, Map.of());
    }

    /**
     * A check with its mode and expert overrides ({@code agentspaces.ttl.strict}, {@code agentspaces.ttl.expert-overrides}).
     *
     * @param strict    whether a value under the floor with no override refuses to start
     * @param overrides key to reason; a key named here passes whatever its value
     * @return the check
     */
    public static TtlFloor of(boolean strict, Map<String, String> overrides) {
        Objects.requireNonNull(overrides, "overrides");
        for (Map.Entry<String, String> e : overrides.entrySet()) {
            if (e.getValue() == null || e.getValue().isBlank()) {
                throw new IllegalArgumentException("the expert override of " + e.getKey()
                        + " names no reason (principle 8: the reason is the point)");
            }
        }
        return new TtlFloor(strict, overrides);
    }

    /** Whether a value under the floor with no override refuses to start. */
    public boolean strict() {
        return strict;
    }

    /**
     * Checks one configured value.
     *
     * @param key               the key, as the operator would write it ({@code agentspaces.relay.path-ttl}) or the
     *                          binder's ({@code binder.<AgentClass>.<method>.<member>})
     * @param value             the configured value; null passes
     * @param registeredDefault the default the TTL register lists for this key, or null when it has none
     * @throws TtlFloorException under {@link #strict} for a value under the floor with no override
     */
    public void check(String key, Duration value, Duration registeredDefault) {
        check(key, value, registeredDefault, null);
    }

    /**
     * Checks one configured value with an override given beside it ({@code @TtlOverride} on a binder method).
     *
     * @param key               the key
     * @param value             the configured value; null passes
     * @param registeredDefault the registered default, or null
     * @param overrideReason    an override's reason given at the site, or null
     * @throws TtlFloorException under {@link #strict} for a value under the floor with no override
     */
    public void check(String key, Duration value, Duration registeredDefault, String overrideReason) {
        Objects.requireNonNull(key, "key");
        if (value == null || value.equals(registeredDefault)) {
            return;
        }
        String reason = overrideReason != null && !overrideReason.isBlank() ? overrideReason : overrides.get(key);
        if (reason != null) {
            if (told.add(key)) {
                LOG.log(System.Logger.Level.INFO, "TTL expert override: " + key + "=" + value + " (" + reason + ")");
                record(new Finding(key, value, Verdict.OVERRIDDEN, reason));
            }
            return;
        }
        if (value.compareTo(FLOOR) >= 0) {
            return;
        }
        String message = key + "=" + value + " is under 60 s. Principle 8 (srad/agentspaces-event-loop.md §3.11):"
                + " TTLs need to be generous, and one under 60 s is probably too fast unless someone really knows what"
                + " they are doing; name the reason in agentspaces.ttl.expert-overrides[" + key + "] (or @TtlOverride"
                + " beside a binder annotation)";
        if (strict) {
            record(new Finding(key, value, Verdict.REFUSED, ""));
            throw new TtlFloorException(message + ", or turn agentspaces.ttl.strict off");
        }
        if (told.add(key)) {
            LOG.log(System.Logger.Level.WARNING, message);
            record(new Finding(key, value, Verdict.WARNED, ""));
        }
    }

    private synchronized void record(Finding finding) {
        findings.add(finding);
    }

    /**
     * What the check found so far, oldest first; for tests and status.
     *
     * @return the findings
     */
    public synchronized List<Finding> findings() {
        return List.copyOf(findings);
    }

    /** A configured TTL under the floor with no override, refused under {@code agentspaces.ttl.strict=true}. */
    public static final class TtlFloorException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        TtlFloorException(String message) {
            super(message);
        }
    }
}
