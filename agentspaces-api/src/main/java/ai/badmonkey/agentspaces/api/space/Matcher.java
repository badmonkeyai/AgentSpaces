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
package ai.badmonkey.agentspaces.api.space;

/**
 * A predicate over a single field value in a {@link Template} condition. Obtain
 * instances from {@link Matchers}; the interface is public so applications can
 * supply their own.
 */
@FunctionalInterface
public interface Matcher {

    /**
     * Tests a field value.
     *
     * @param actual the field value read from a candidate entry; may be {@code null}
     * @return {@code true} when the value satisfies this matcher
     */
    boolean matches(Object actual);
}
