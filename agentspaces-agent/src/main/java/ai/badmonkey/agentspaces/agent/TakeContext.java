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
 */
public record TakeContext(TakenEntry<?> taken, Duration lease, AgentId agent, String spaceName) {

    private static final ThreadLocal<TakeContext> CURRENT = new ThreadLocal<>();

    /** Validates the components. */
    public TakeContext {
        Objects.requireNonNull(taken, "taken");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(spaceName, "spaceName");
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

    /** Renews the take lease by the binding's lease duration. */
    public void renew() {
        taken.renew(lease);
    }
}
