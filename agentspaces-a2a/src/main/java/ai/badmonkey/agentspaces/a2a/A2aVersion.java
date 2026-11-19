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
package ai.badmonkey.agentspaces.a2a;

import java.util.Optional;

/**
 * The A2A protocol versions this module speaks. A2A 1.0 (§3.6) negotiates the
 * version per request with the {@code A2A-Version} service parameter, as
 * {@code Major.Minor}. An empty value means 0.3, so a client that predates the
 * header keeps the 0.3 method names and shapes. A patch number is ignored.
 */
public enum A2aVersion {

    /** A2A 0.3: {@code message/send}, {@code kind} discriminators, lower-case states. */
    V0_3("0.3"),
    /** A2A 1.0: {@code SendMessage}, member-based polymorphism, {@code TASK_STATE_*} states. */
    V1_0("1.0");

    /** The service parameter that names the version, as a header or a query parameter. */
    public static final String HEADER = "A2A-Version";

    private final String wire;

    A2aVersion(String wire) {
        this.wire = wire;
    }

    /** The {@code Major.Minor} form a card and the header carry. */
    public String wire() {
        return wire;
    }

    /**
     * The version a request asks for (A2A 1.0 §3.6.2): blank is 0.3, and
     * {@code 1.0}, {@code 1.0.1} and {@code 0.3.0} name their {@code Major.Minor}.
     *
     * @param requested the {@code A2A-Version} value, or null when absent
     * @return the version, or empty when this module does not speak it
     */
    public static Optional<A2aVersion> negotiate(String requested) {
        if (requested == null || requested.isBlank()) {
            return Optional.of(V0_3);
        }
        return of(requested);
    }

    /**
     * The version a card's interface declares, compared on {@code Major.Minor}.
     *
     * @param declared a version such as {@code 1.0} or {@code 0.3.0}
     * @return the version, or empty when this module does not speak it
     */
    public static Optional<A2aVersion> of(String declared) {
        if (declared == null) {
            return Optional.empty();
        }
        String[] parts = declared.strip().split("\\.");
        if (parts.length < 2) {
            return Optional.empty();
        }
        String majorMinor = parts[0] + "." + parts[1];
        for (A2aVersion v : values()) {
            if (v.wire.equals(majorMinor)) {
                return Optional.of(v);
            }
        }
        return Optional.empty();
    }
}
