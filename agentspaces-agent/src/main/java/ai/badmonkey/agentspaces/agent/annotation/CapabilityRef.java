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
package ai.badmonkey.agentspaces.agent.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Injects a typed capability client into a field at bind time, the sibling of
 * {@link SpaceRef} for Layer 4 (SPEC §10.3, §10.5): {@code VoteClient},
 * {@code AggregateClient}, {@code SemanticClient}, or any client type the
 * binder or its group can resolve. Binding fails fast when no client of the
 * field's type is available, naming the registration that would supply it.
 *
 * <pre>{@code
 * @CapabilityRef VoteClient votes;
 * @SpaceRef("trip") Space trip;
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CapabilityRef {
    /** The group whose client to inject; empty means any binding group (the last one binding wins). */
    String group() default "";
}
