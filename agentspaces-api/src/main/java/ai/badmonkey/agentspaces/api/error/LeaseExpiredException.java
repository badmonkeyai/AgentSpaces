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
package ai.badmonkey.agentspaces.api.error;

/**
 * Thrown when an operation requires a live lease and the lease has lapsed: renewing
 * an expired handle, completing a take whose lease already expired, and so on.
 * Lease lapse is the normal failure signal in AgentSpaces (spec P2), so callers
 * should treat this as an expected coordination outcome, log it, and retry the work
 * through the space.
 */
public class LeaseExpiredException extends AgentSpacesException {

    /**
     * Creates the exception with a message.
     *
     * @param message the detail message
     */
    public LeaseExpiredException(String message) {
        super(message);
    }
}
