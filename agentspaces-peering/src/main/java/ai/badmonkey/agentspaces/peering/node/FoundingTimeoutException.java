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
package ai.badmonkey.agentspaces.peering.node;

import java.util.concurrent.TimeoutException;

/**
 * A join by GroupId ran to its timeout without a verified founding
 * (ISSUE-BootstrapRobustness FR-6). The message carries the facts; the
 * accessors let a host add advice: when {@link #connected()} is above zero
 * the peers that were reached sent nothing, which is what a group started
 * from a {@code founding:} text does, because it has no signed founding.
 */
public class FoundingTimeoutException extends TimeoutException {

    private static final long serialVersionUID = 1L;

    private final int locates;
    private final int dialed;
    private final int connected;

    /**
     * @param message   the report
     * @param locates   how many times the locator was asked
     * @param dialed    how many distinct endpoints were dialed
     * @param connected how many of them accepted a connection
     */
    public FoundingTimeoutException(String message, int locates, int dialed, int connected) {
        super(message);
        this.locates = locates;
        this.dialed = dialed;
        this.connected = connected;
    }

    /** How many times the locator was asked during the join. */
    public int locates() {
        return locates;
    }

    /** How many distinct endpoints were dialed. */
    public int dialed() {
        return dialed;
    }

    /** How many dialed endpoints accepted a connection. */
    public int connected() {
        return connected;
    }
}
