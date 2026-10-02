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

import java.time.Instant;

/**
 * One observed fleet activity, as streamed over {@code /api/v1/events}.
 * Sequence numbers increase monotonically per console and double as SSE event
 * ids, which is what makes the stream resumable: a reconnecting
 * {@code EventSource} presents the last id it saw in the standard
 * {@code Last-Event-ID} header and replay starts after it, as far back as the
 * console's ring buffer still reaches.
 *
 * @param seq       the monotone sequence number, also the SSE event id
 * @param at        when the console observed the activity
 * @param space     the console-local name of the space involved
 * @param kind      the activity kind: {@code written}, {@code taken}, or
 *                  {@code completed}
 * @param entryType the simple class name of the entry involved
 * @param worker    the attributed worker, or the empty string when the
 *                  activity carries no attribution
 */
public record ConsoleEvent(long seq, Instant at, String space, String kind,
                           String entryType, String worker) {
}
