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
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The one duration parser (review-110226-deadcode-parity D11): {@code 500ms},
 * {@code 30s}, {@code 10m}, {@code 2h}, {@code 7d}, or ISO-8601
 * ({@code PT10M}). A bare number is milliseconds only where the caller says
 * so; the annotation parsers reject it, the bootstrap tree accepts it.
 */
public final class Durations {

    private static final Pattern SIMPLE = Pattern.compile("([+]?\\d+)\\s*(ms|s|m|h|d)", Pattern.CASE_INSENSITIVE);

    private Durations() {
    }

    /**
     * Parses a duration.
     *
     * @param value      the text
     * @param bareMillis whether a number with no unit means milliseconds
     * @return the duration
     * @throws IllegalArgumentException when the text is not a duration
     */
    public static Duration parse(String value, boolean bareMillis) {
        String trimmed = Objects.requireNonNull(value, "value").trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("empty duration");
        }
        Matcher simple = SIMPLE.matcher(trimmed);
        if (simple.matches()) {
            long amount = Long.parseLong(simple.group(1));
            return switch (simple.group(2).toLowerCase(Locale.ROOT)) {
                case "ms" -> Duration.ofMillis(amount);
                case "s" -> Duration.ofSeconds(amount);
                case "m" -> Duration.ofMinutes(amount);
                case "h" -> Duration.ofHours(amount);
                default -> Duration.ofDays(amount);
            };
        }
        if (bareMillis && trimmed.matches("[+]?\\d+")) {
            return Duration.ofMillis(Long.parseLong(trimmed));
        }
        try {
            return Duration.parse(trimmed.toUpperCase(Locale.ROOT));
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("not a duration (try \"10m\", \"500ms\", \"30s\", or ISO-8601"
                    + " \"PT10M\"): '" + value + "'", e);
        }
    }
}
