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

import java.time.Duration;

/**
 * A leased event subscription created by
 * {@link Space#notify(Template, SpaceListener, Lease)}. Like all shared state, a
 * subscription vanishes unless renewed; close it to stop delivery immediately.
 */
public interface Subscription extends AutoCloseable {

    /**
     * Extends the subscription lease to at least the given duration from now. It never shortens the lease: the new
     * expiry is the later of the current one and now plus {@code extension}. {@link #close() Close} the
     * subscription to stop it sooner.
     *
     * @param extension how long from now the subscription should live at least
     * @throws ai.badmonkey.agentspaces.api.error.LeaseExpiredException if the lease already lapsed
     */
    void renew(Duration extension);

    /** Cancels the subscription. Idempotent. */
    @Override
    void close();
}
