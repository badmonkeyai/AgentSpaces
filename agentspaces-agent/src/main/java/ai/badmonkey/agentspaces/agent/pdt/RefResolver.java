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
package ai.badmonkey.agentspaces.agent.pdt;

import java.lang.reflect.Type;

/**
 * Builds the handle a {@link ai.badmonkey.agentspaces.agent.annotation.PdtRef} site receives
 * (PDT 2 phase 7, P2-X-03, PD-R-11). This module does not depend on {@code agentspaces-pdt},
 * so a host hands each group's binder a resolver over that group's facade
 * ({@code binder.refs(pdt::refFor)}); the binder calls it once per site at bind time.
 */
@FunctionalInterface
public interface RefResolver {

    /**
     * The handle of a ref for a site's declared type.
     *
     * @param name the ref name the annotation gives
     * @param type the site's generic type, for example {@code Ref<PMap<String, Fact>>}
     * @return the handle, an instance of the site's raw type
     * @throws IllegalArgumentException when the type names no ref the resolver can build, saying why
     * @throws IllegalStateException    when the group's facade cannot make refs yet, saying why
     */
    Object resolve(String name, Type type);
}
