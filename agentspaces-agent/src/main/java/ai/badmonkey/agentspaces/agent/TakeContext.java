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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.api.entry.EntryId;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The take an annotated worker is running right now. The {@code @SpaceTake} and
 * {@code @OrderedTake} loops set it around each method call, so code inside the
 * method (a model call's advisor, a chat-memory repository, a usage meter) can
 * renew the take lease while long work progresses, key state by the task's entry
 * ID, or attribute work to the bound agent, without the method threading any of
 * it through its own signature.
 *
 * <p>The context lives on the loop's thread. Libraries that hop threads (Reactor,
 * executors) carry it with {@link #peek()}, {@link #bind(TakeContext)}, and
 * {@link #unbind()}, which is the shape Micrometer's context-propagation
 * {@code ThreadLocalAccessor} expects.
 *
 * @param taken     the in-flight take
 * @param lease     the binding's take lease, the natural renewal extension
 * @param agent     the bound agent running the method
 * @param spaceName the space the entry was taken from
 * @param renewable whether {@link #renew()} may still renew: false once the binder has stopped renewing the take
 *                  itself (the hold limit passed, or the agent was unbound), so a library renewing from inside the
 *                  method (Spring AI's take-lease advisor, LangChain4j's take-lease listener, Embabel's invoker)
 *                  cannot keep a hung handler's entry past the limit the binder enforces
 *                  (srad/agentspaces-event-loop.md §10.33, I-05)
 */
public record TakeContext(TakenEntry<?> taken, Duration lease, AgentId agent, String spaceName,
                          java.util.function.BooleanSupplier renewable) {

    private static final ThreadLocal<TakeContext> CURRENT = new ThreadLocal<>();

    /** Validates the components. */
    public TakeContext {
        Objects.requireNonNull(taken, "taken");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(spaceName, "spaceName");
        Objects.requireNonNull(renewable, "renewable");
    }

    /**
     * A take with no renewal limit of its own: {@link #renew()} always renews.
     *
     * @param taken     the in-flight take
     * @param lease     the binding's take lease
     * @param agent     the bound agent
     * @param spaceName the space the entry was taken from
     */
    public TakeContext(TakenEntry<?> taken, Duration lease, AgentId agent, String spaceName) {
        this(taken, lease, agent, spaceName, () -> true);
    }

    /** Returns the take running on this thread, if a worker method is running. */
    public static Optional<TakeContext> current() {
        return Optional.ofNullable(CURRENT.get());
    }

    /** Returns the take running on this thread, or {@code null}; for context-propagation libraries. */
    public static TakeContext peek() {
        return CURRENT.get();
    }

    /**
     * Binds a take to this thread; for the worker loops and for
     * context-propagation libraries restoring a captured context.
     *
     * @param context the take, not null
     */
    public static void bind(TakeContext context) {
        CURRENT.set(Objects.requireNonNull(context, "context"));
    }

    /** Clears this thread's take. */
    public static void unbind() {
        CURRENT.remove();
    }

    /** The taken entry's ID: a stable key for the task across workers and restarts. */
    public EntryId entryId() {
        return taken.entryId();
    }

    /**
     * Renews the take lease by the binding's lease duration, unless the binder has stopped renewing this take (its
     * hold limit passed, or its agent was unbound): then nothing is renewed and the lease lapses to another taker, as
     * the binder's own renewal would let it. Kept {@code void}, so integrations compiled against 0.6.0 still link.
     */
    public void renew() {
        if (renewable.getAsBoolean()) {
            taken.renew(lease);
        }
    }
}
