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
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * The delegation target behind every generated {@code @Action} method
 * ({@link EmbabelRemoteActions}): looks the invoked method up by name and runs
 * its remote action as a space round-trip. Public because ByteBuddy binds the
 * generated methods to it across class loaders.
 */
public final class RemoteActionInterceptor {

    private final Map<String, RemoteAction> byMethodName;
    private final Duration timeout;

    /**
     * Creates the interceptor.
     *
     * @param byMethodName the remote action behind each generated method name
     * @param timeout      how long an invocation waits for its result
     */
    public RemoteActionInterceptor(Map<String, RemoteAction> byMethodName, Duration timeout) {
        this.byMethodName = Map.copyOf(Objects.requireNonNull(byMethodName, "byMethodName"));
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    /**
     * Runs the remote action behind a generated method.
     *
     * @param method the generated method being invoked
     * @param input  the action's input entry
     * @return the correlated result entry
     * @throws IllegalStateException when no result arrives within the timeout
     */
    @RuntimeType
    public Object invoke(@Origin Method method, @Argument(0) Object input) {
        RemoteAction action = byMethodName.get(method.getName());
        if (action == null) {
            throw new IllegalStateException("no remote action behind " + method.getName());
        }
        return action.invoke(input, timeout).orElseThrow(() -> new IllegalStateException(
                "the fleet produced no " + action.outputType().getSimpleName()
                        + " for " + action.name() + " within " + timeout));
    }
}
