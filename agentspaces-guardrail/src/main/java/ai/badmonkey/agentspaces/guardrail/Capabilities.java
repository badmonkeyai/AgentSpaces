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

/** The two capability names this revision adds to SPEC §8 (ISSUE-ModelCard FR-3, FR-4). */
public final class Capabilities {

    /** A peer that checks content: parameters {@code checks} (the kinds offered) and {@code provider}. */
    public static final String GUARDRAIL = "aspace:cap/guardrail";
    /**
     * A peer that routes model requests: parameters {@code tasks} (the kinds routed), {@code policyVersion}, and
     * {@code coordination} ({@code lease-race}, {@code auction}, {@code partition}). A {@code ModelRequest} tagged
     * {@code task=<kind>} or addressed to the gateway's advertised model name is the gateway's.
     */
    public static final String LLM_GATEWAY = "aspace:cap/llm-gateway";
    /** The tag a request carries to name its task kind. */
    public static final String TASK_TAG = "task";

    private Capabilities() {
    }
}
