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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What an {@link AgentDeclarationReader} found on a class.
 *
 * @param name         the card name, or empty for the class-name rule
 * @param description  the card's description
 * @param goals        the card's goals
 * @param meta         cost hints the card carries (a framework name, say); never null
 * @param implied      bindings the reader implies for methods that carry no space annotation
 * @param extraActions actions advertised but never bound by the binder; one whose method the binder
 *                     bound is dropped, because the worker wins
 */
public record AgentDeclaration(String name, String description, List<String> goals,
                               Map<String, String> meta, List<ImpliedBinding> implied,
                               List<ExtraAction> extraActions) {

    public AgentDeclaration {
        name = name == null ? "" : name;
        description = description == null ? "" : description;
        goals = goals == null ? List.of() : List.copyOf(goals);
        meta = meta == null ? Map.of() : Map.copyOf(meta);
        implied = implied == null ? List.of() : List.copyOf(implied);
        extraActions = extraActions == null ? List.of() : List.copyOf(extraActions);
        Objects.requireNonNull(name, "name");
    }
}
