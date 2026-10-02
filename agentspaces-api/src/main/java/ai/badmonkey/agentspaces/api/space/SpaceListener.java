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
package ai.badmonkey.agentspaces.api.space;

/**
 * Receives {@link SpaceEvent}s for a {@link Subscription}. Delivery is
 * at-least-once; implementations should be idempotent per
 * {@code (entryId, kind)}. Listeners must return promptly; long work belongs on the
 * listener's own executor or, better, in a take-based worker.
 *
 * @param <T> the entry type
 */
@FunctionalInterface
public interface SpaceListener<T> {

    /**
     * Handles one event.
     *
     * @param event the event
     */
    void onEvent(SpaceEvent<T> event);
}
