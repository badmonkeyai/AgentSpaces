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
package ai.badmonkey.agentspaces.agent.annotation;

import java.time.Duration;

/**
 * Parses the duration strings the annotations accept: the Spring-style simple
 * form ({@code 10m}, {@code 500ms}, {@code 2h}, {@code 30s}, {@code 1d}) and
 * ISO-8601 ({@code PT10M}), so annotation values read the way Spring Boot
 * properties do while older ISO values keep working. A bare number has no unit
 * and is refused. The parsing is the shared
 * {@link ai.badmonkey.agentspaces.common.time.Durations} with bare milliseconds
 * off; this class is the annotations' name for it.
 */
public final class Durations {

    private Durations() {
    }

    /**
     * Parses a duration.
     *
     * @param value the duration string, simple-style or ISO-8601
     * @return the duration
     * @throws IllegalArgumentException when the value parses as neither form
     */
    public static Duration parse(String value) {
        return ai.badmonkey.agentspaces.common.time.Durations.parse(value, false);
    }
}
