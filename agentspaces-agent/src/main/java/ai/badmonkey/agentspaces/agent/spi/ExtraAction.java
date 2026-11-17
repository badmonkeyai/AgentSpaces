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

import ai.badmonkey.agentspaces.api.ad.CardAction;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * An action a declaration reader advertises without the binder binding it (review §7.3: an Embabel
 * {@code @Action} that is not a map of 1). The binder names its consumed and produced schemas through the
 * group's schema registry, as it does for every bound method, and drops it when it bound the method itself.
 *
 * @param method      the method; its entry parameters are consumed, its entry result produced
 * @param kind        the card kind, {@code local} for an action that is never invocable
 * @param description the action's description
 * @param contract    the action's contract, or null
 */
public record ExtraAction(Method method, String kind, String description, CardAction.Contract contract) {

    public ExtraAction {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(kind, "kind");
        description = description == null ? "" : description;
    }
}
