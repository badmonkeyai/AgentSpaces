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
package ai.badmonkey.agentspaces.embabel;

import java.time.Duration;

/**
 * A generated fleet action produced no result in time (reviews/embabel-agentspaces-review-2026-11-12.md
 * §7.5): names the action, its space, the timeout, and whether the input was withdrawn. Embabel's planner
 * treats it as the step failing; the generated action fires once, so this is the step's one failure.
 */
public final class RemoteActionTimeoutException extends IllegalStateException {

    private final String action;
    private final String space;
    private final Duration timeout;
    private final boolean withdrawn;

    public RemoteActionTimeoutException(String action, String outputType, String space, Duration timeout,
                                        boolean withdrawn) {
        super("the fleet produced no " + outputType + " for " + action + " in " + space + " within " + timeout
                + (withdrawn ? "; the input was withdrawn" : "; the input was left for a late worker"));
        this.action = action;
        this.space = space;
        this.timeout = timeout;
        this.withdrawn = withdrawn;
    }

    public String action() {
        return action;
    }

    public String space() {
        return space;
    }

    public Duration timeout() {
        return timeout;
    }

    public boolean withdrawn() {
        return withdrawn;
    }
}
