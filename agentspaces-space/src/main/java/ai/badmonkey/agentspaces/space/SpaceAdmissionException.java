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
package ai.badmonkey.agentspaces.space;

import ai.badmonkey.agentspaces.api.error.AgentSpacesException;

/**
 * Thrown when an agent the space does not admit attempts a write, take, or
 * completion (spec §7.5): under {@code Admission.ALLOWLIST} only the listed
 * agents may mutate the space; everyone in the group may still read.
 */
public class SpaceAdmissionException extends AgentSpacesException {

    /**
     * Creates the exception.
     *
     * @param message what was refused and for whom
     */
    public SpaceAdmissionException(String message) {
        super(message);
    }
}
