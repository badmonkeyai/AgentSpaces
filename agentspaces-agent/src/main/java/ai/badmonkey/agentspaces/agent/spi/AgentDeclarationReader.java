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

import java.util.Optional;

/**
 * Reads an agent's declaration from a framework's own annotations, so the core binder makes the
 * <em>one</em> card for a bean that is both a fleet worker and, say, an Embabel {@code @Agent}
 * (reviews/embabel-agentspaces-review-2026-11-12.md §7.2). The binder consults readers after
 * {@code @AgentSpec} and before the class-name rule: a reader supplies the card's name, description
 * and goals, the bindings it implies for methods that carry no space annotation (§7.3, the "map of
 * 1"), and the actions it advertises without binding.
 *
 * <p>Readers are found with {@link java.util.ServiceLoader} on the binder's class loader and the
 * thread's context loader, and a host may register configured instances directly through
 * {@code AgentSpaces.declarationReaders(List)} (or {@code AgentBinder.registerDeclarationReader}).
 * A registered reader replaces a loaded one of the same class. Readers are tried in {@code order},
 * lowest first; the first that answers wins.
 */
public interface AgentDeclarationReader {

    /**
     * Reads one class.
     *
     * @param userClass the bean's user class (a proxy already unwrapped to the class it proxies)
     * @return the declaration, or empty when this reader does not recognise the class
     */
    Optional<AgentDeclaration> read(Class<?> userClass);

    /**
     * The reader's precedence among readers; lower first.
     *
     * @return the order, default 0
     */
    default int order() {
        return 0;
    }
}
