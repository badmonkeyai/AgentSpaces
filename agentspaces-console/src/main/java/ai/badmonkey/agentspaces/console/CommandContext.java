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
package ai.badmonkey.agentspaces.console;

import java.time.Duration;

/**
 * What a {@link ConsoleCommand} may do to the fleet, all under the console
 * peer's signed identity. A command never touches the HTTP layer or the fabric
 * wiring directly; it expresses intent through this context, which the
 * {@link FleetCommander} carries out and records in the C2 audit log and the
 * activity stream.
 */
public interface CommandContext {

    /**
     * The operator the command is attributed to (authenticated at the HTTP
     * edge). Carried as metadata on everything the command writes.
     *
     * @return the operator name
     */
    String operator();

    /**
     * Dispatches an entry into a named space and tracks it so it can be
     * cancelled later ({@link #cancel}). The entry is written under the console
     * peer's identity, signed and leased.
     *
     * @param spaceName the target space, resolved on the console peer
     * @param entry     the entry to write
     * @param lease     the write lease
     * @return the new entry's id
     */
    String dispatch(String spaceName, Object entry, Duration lease);

    /**
     * Cancels an entry the console previously dispatched. The console can only
     * cancel entries it wrote itself: removal is issuer-signed (SPEC §11a.4), so
     * a foreign entry cannot be withdrawn from here.
     *
     * @param entryId the id returned by an earlier {@link #dispatch}
     * @return {@code true} if a tracked entry was cancelled
     */
    boolean cancel(String entryId);

    /**
     * Broadcasts a control directive to the fleet's workers.
     *
     * @param action the directive verb ({@link Directive#PAUSE} and friends)
     * @param target the worker name, or {@link Directive#ALL}
     */
    void broadcast(String action, String target);

    /**
     * Records a free-form C2 audit entry (beyond the automatic dispatch,
     * cancel, and directive records) in the durable audit log and the activity
     * stream.
     *
     * @param action a short verb for the action
     * @param detail a human-readable detail line
     */
    void audit(String action, String detail);
}
