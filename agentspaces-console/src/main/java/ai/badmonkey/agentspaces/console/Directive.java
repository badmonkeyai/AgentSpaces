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

/**
 * A command-and-control directive to the fleet's workers: an entry the
 * commander writes into the control space and workers honor through a
 * {@link DirectiveGate}. Directives are ordinary signed, leased space entries,
 * so they carry the console peer's cryptographic authorship, replicate to
 * every worker, and age out on their lease like all shared state (P2); the
 * {@code operator} names, as attribution metadata, the human the console
 * executed them on behalf of.
 *
 * @param action       the directive verb: {@link #PAUSE}, {@link #RESUME}, or
 *                     {@link #DRAIN}
 * @param target       the worker name the directive addresses, or {@link #ALL}
 *                     for every worker
 * @param operator     the operator the command is attributed to
 * @param issuedMillis when the directive was issued, epoch milliseconds, so a
 *                     worker honors the most recent directive for its name
 */
public record Directive(String action, String target, String operator, long issuedMillis) {

    /** Stop taking new work (current work finishes). */
    public static final String PAUSE = "PAUSE";
    /** Resume taking work after a pause. */
    public static final String RESUME = "RESUME";
    /** Finish current work and stop for good. */
    public static final String DRAIN = "DRAIN";
    /** The wildcard target: every worker. */
    public static final String ALL = "*";

    /** Whether this directive addresses the named worker (directly or via {@link #ALL}). */
    public boolean addresses(String worker) {
        return ALL.equals(target) || target.equals(worker);
    }
}
