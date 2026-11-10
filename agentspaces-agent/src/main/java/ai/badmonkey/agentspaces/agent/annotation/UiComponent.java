/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.agent.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares an A2UI component this agent serves (SPEC §6.1a.3, v0.1.26; ISSUE-UiComponents). The binder derives the
 * card's {@code ui} group from every such annotation on the agent class or its bound methods: the component, its
 * catalog, the standard entry behind it, the controls a person may trigger, and the role the component plays in an
 * application a console drafts from the group's cards. A control must name an action the card declares. An agent with no annotation has no
 * {@code ui}; a card without one is byte-identical to v0.1.25.
 *
 * <pre>{@code
 * @SpaceAgent(name = "notify")
 * @UiComponent(catalog = "example-widgets", component = "ItemPanel", entry = SharedItem.class, controls = {"open", "share"})
 * @UiComponent(catalog = "example-widgets", component = "MessageInput", entry = Message.class, role = "control", writes = true)
 * public class Notify { ... }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Repeatable(UiComponents.class)
public @interface UiComponent {

    /** The A2UI catalog id the component comes from. */
    String catalog();

    /** The component name in that catalog. */
    String component();

    /** The component's id within the card; the component name when empty (one component per name then). */
    String id() default "";

    /** The standard entry type behind the component; {@code Void} for none. Its IRI is {@link #entryIri()} when given, else the class name. */
    Class<?> entry() default Void.class;

    /** The entry type's IRI, when the class name is not the IRI the fleet speaks. */
    String entryIri() default "";

    /** The action names a person may trigger on the component; each must be an action of the card. */
    String[] controls() default {};

    /** {@code panel}, {@code control}, or {@code row}. */
    String role() default "panel";

    /** Whether the agent reads the entry through this component (fills {@code ui.entries.reads}). */
    boolean reads() default true;

    /** Whether the agent writes the entry through this component (fills {@code ui.entries.writes}). */
    boolean writes() default false;
}
