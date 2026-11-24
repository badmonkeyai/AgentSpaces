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
package ai.badmonkey.agentspaces.identity;

import java.util.function.Supplier;

/**
 * A channel-trust supplier that tells its listeners when the trust it yields changes (srad/agentspaces-event-loop.md
 * EL-L-14): a node given one re-judges its live channels on the change, at once, and does not look at the supplier on a
 * timer. A plain {@link Supplier} still works: the node then compares the supplier's value once a second.
 */
public interface SignallingChannelTrust extends Supplier<ChannelTrust> {

    /**
     * Registers a listener run right after the yielded trust changes. It must only wake (a node's
     * {@code trustChanged()}): it must not block and must not call back into this supplier.
     *
     * @param listener the listener
     * @return closes the registration
     */
    AutoCloseable onChange(Runnable listener);
}
