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
package ai.badmonkey.agentspaces.agent.spi;

import ai.badmonkey.agentspaces.agent.TakeContext;

import java.lang.reflect.Method;

/**
 * Runs a taken entry through a host's own runtime instead of a direct method call
 * (reviews/embabel-agentspaces-review-2026-11-12.md §7.8): an Embabel worker action that takes
 * {@code OperationContext} beside its entry runs as an Embabel process. The binder asks every registered
 * invoker whether it {@linkplain #claims claims} a method; a claimed method may declare parameters after
 * the entry, which the invoker owns, and the binder hands each taken entry to the invoker instead of
 * reflection. The crash semantics are unchanged: a result is written and the take completed by the
 * binder; an exception lets the lease lapse; a null result completes the take with nothing written.
 *
 * <p>Registered on the facade ({@code AgentSpaces.takeInvokers(List)}) or statically
 * ({@code AgentBinder.registerTakeInvoker}), never through {@code ServiceLoader}: an invoker needs the
 * host's runtime.
 */
public interface TakeInvoker {

    /**
     * Whether this invoker runs the method. A claimed method's parameters after the entry are the
     * invoker's to resolve.
     *
     * @param bean   the bound bean
     * @param method the bound method
     * @return true to run it
     */
    boolean claims(Object bean, Method method);

    /**
     * Checks a claimed method at bind time.
     *
     * @param bean   the bound bean
     * @param method the bound method
     * @throws IllegalArgumentException naming the method and the problem
     */
    default void validate(Object bean, Method method) {
    }

    /**
     * Runs one taken entry.
     *
     * @param bean    the bound bean
     * @param method  the bound method
     * @param entry   the taken entry
     * @param context the take context: the taken entry, its lease (renew it for long work), the agent id
     * @return the result to write, or null for none
     * @throws Exception to let the lease lapse so another worker takes the entry
     */
    Object invoke(Object bean, Method method, Object entry, TakeContext context) throws Exception;
}
