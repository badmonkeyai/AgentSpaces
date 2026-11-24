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
package ai.badmonkey.agentspaces.api.ttl;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An expert override of the TTL floor beside a binder annotation (srad/agentspaces-event-loop.md EL-E-04, decision
 * 18): the lease and TTL members of the method's verb annotation may be under 60 s, and this says why. The binder logs
 * the reason once and checks nothing else for that method; without it, a lease under 60 s logs a WARN, or refuses to
 * bind under {@code agentspaces.ttl.strict=true}.
 *
 * <pre>{@code
 * @OrderedTake(space = "orders", lease = "20s")
 * @TtlOverride(reason = "a 2 s handler on a local test fleet; the binder renews it every 10 s")
 * public Receipt confirm(Order order) { ... }
 * }</pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TtlOverride {

    /** Why the lease or TTL is under 60 s: the cycle that renews or bounds it. Required and never blank. */
    String reason();
}
