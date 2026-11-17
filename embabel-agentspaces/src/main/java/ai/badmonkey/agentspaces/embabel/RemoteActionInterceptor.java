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

import ai.badmonkey.agentspaces.agent.remote.RemoteAction;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The delegation target behind every generated {@code @Action} method
 * ({@link EmbabelRemoteActions}): looks the invoked method up by name and runs
 * its remote action as a space round-trip. Public because ByteBuddy binds the
 * generated methods to it across class loaders.
 *
 * <p>Review §7.5: a second call for the same input inside one Embabel process (a replan of the same step,
 * or retries re-enabled) re-awaits the entry already written instead of writing another; the process id comes
 * from the {@code OperationContext} parameter the generated method takes when Embabel is present. A timeout
 * throws {@link RemoteActionTimeoutException}, and the input is withdrawn or left as configured.
 */
public final class RemoteActionInterceptor {

    private final Map<String, RemoteAction> byMethodName;
    private final Duration timeout;
    private final RemoteAction.OnTimeout onTimeout;
    private final Map<String, CompletableFuture<Optional<Object>>> pending = new ConcurrentHashMap<>();

    /**
     * Creates the interceptor.
     *
     * @param byMethodName the remote action behind each generated method name
     * @param timeout      how long an invocation waits for its result
     */
    public RemoteActionInterceptor(Map<String, RemoteAction> byMethodName, Duration timeout) {
        this(byMethodName, timeout, RemoteAction.OnTimeout.WITHDRAW);
    }

    public RemoteActionInterceptor(Map<String, RemoteAction> byMethodName, Duration timeout,
                                   RemoteAction.OnTimeout onTimeout) {
        this.byMethodName = Map.copyOf(Objects.requireNonNull(byMethodName, "byMethodName"));
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.onTimeout = Objects.requireNonNull(onTimeout, "onTimeout");
    }

    /**
     * Runs the remote action behind a generated method.
     *
     * @param method    the generated method being invoked
     * @param arguments the action's input entry, then Embabel's {@code OperationContext} when the method takes one
     * @return the correlated result entry
     * @throws RemoteActionTimeoutException when no result arrives within the timeout
     */
    @RuntimeType
    public Object invoke(@Origin Method method, @AllArguments Object[] arguments) {
        RemoteAction action = byMethodName.get(method.getName());
        if (action == null) {
            throw new IllegalStateException("no remote action behind " + method.getName());
        }
        Object input = arguments.length > 0 ? arguments[0] : null;
        Objects.requireNonNull(input, "input");
        String processId = arguments.length > 1 ? processIdOf(arguments[1]) : null;
        Optional<Object> result;
        if (processId == null) {
            result = action.invoke(input, timeout, onTimeout);
        } else {
            String key = processId + "|" + method.getName() + "|" + input;
            CompletableFuture<Optional<Object>> mine = new CompletableFuture<>();
            CompletableFuture<Optional<Object>> first = pending.putIfAbsent(key, mine);
            if (first != null) {
                result = first.join(); // re-await the entry already written for this process and input
            } else {
                try {
                    result = action.invoke(input, timeout, onTimeout);
                    mine.complete(result);
                } catch (RuntimeException e) {
                    mine.completeExceptionally(e);
                    throw e;
                } finally {
                    pending.remove(key, mine);
                }
            }
        }
        return result.orElseThrow(() -> new RemoteActionTimeoutException(action.name(),
                action.outputType().getSimpleName(), action.taskSpace().name(), timeout,
                onTimeout == RemoteAction.OnTimeout.WITHDRAW));
    }

    /** The Embabel process id behind an {@code OperationContext}, read reflectively; null when unknown. */
    static String processIdOf(Object context) {
        if (context == null) {
            return null;
        }
        try {
            Object process = context.getClass().getMethod("getAgentProcess").invoke(context);
            if (process == null) {
                return null;
            }
            Object id = process.getClass().getMethod("getId").invoke(process);
            return id == null ? null : id.toString();
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }
}
