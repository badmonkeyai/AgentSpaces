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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * One entry type a {@link SpaceJoin} waits for, and where its key is. A part's
 * key is read from a field (the join's {@link SpaceJoin#key()} by default, or
 * this part's own {@link #key()}) or from a tag ({@link #keyTag()}), which is
 * how a type that does not carry the key as a component, an ontology instance
 * for one, still joins. Tag filters narrow the part the way the {@code tags}
 * attribute narrows a {@code @SpaceTake}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Part {

    /** The part's entry type. */
    Class<?> value();

    /** The space this part lives in; empty means the join's {@link SpaceJoin#space()}. */
    String space() default "";

    /** The field carrying the key; empty means the join's {@link SpaceJoin#key()}. */
    String key() default "";

    /** The tag carrying the key instead of a field; exclusive with {@link #key()}. */
    String keyTag() default "";

    /** Tag filters, {@code "key=value"} or {@code "key"}, all of which an entry must satisfy. */
    String[] tags() default {};

    /** Field filters, {@code "field=value"} or {@code "field!=value"}, all of which an entry must satisfy. */
    String[] where() default {};

    /**
     * How many entries of this part a key needs before it is complete; one by
     * default. A gather of several of one kind (ISSUE-WorkflowVerbs): the
     * method reads them with {@code Joined.all(type)}.
     */
    int atLeast() default 1;

    /**
     * Take the count a key needs from a field of another part present for the
     * key, written {@code "SimpleTypeName.field"}, so a fork can declare how
     * many it emitted and the gather waits for exactly that many; overrides
     * {@link #atLeast()} once that part is present.
     */
    String countedBy() default "";

    /** Whether the join fires without this part; an optional part is included when present. */
    boolean optional() default false;
}
