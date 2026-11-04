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


package ai.badmonkey.agentspaces.guardrail;

import java.util.Map;

/**
 * A check over content. {@code kind} names what to look for ({@code prompt-injection}, {@code personal-data},
 * {@code terms}, {@code domains}, {@code schema}, {@code content-safety}, or a provider's own), {@code content} is
 * the text or the entry's JSON, and {@code context} carries the check's options from the policy plus
 * {@code where} and {@code subject}. A provider sees only the content the policy routes to it (SR-2).
 */
public interface GuardrailProvider {

    /** The provider's name as the policy names it ({@code builtin}, a vendor's). */
    String name();

    /** The kinds this provider offers; the advertisement parameter {@code checks}. */
    java.util.List<String> kinds();

    Verdict check(String kind, String content, Map<String, String> context);
}
