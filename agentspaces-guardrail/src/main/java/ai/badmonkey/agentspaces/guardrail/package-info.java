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


/**
 * Guardrails (SPEC §8 {@code aspace:cap/guardrail}, v0.1.24; ISSUE-ModelCard FR-4, FR-5): the {@link
 * ai.badmonkey.agentspaces.guardrail.GuardrailProvider} SPI, the {@link ai.badmonkey.agentspaces.guardrail.Verdict}
 * a check returns, the {@link ai.badmonkey.agentspaces.guardrail.GuardrailPolicy} and {@link
 * ai.badmonkey.agentspaces.guardrail.GuardrailFinding} records, the {@link
 * ai.badmonkey.agentspaces.guardrail.BuiltinProvider}, and {@link ai.badmonkey.agentspaces.guardrail.Guardrails#apply},
 * the one evaluation path every caller uses. A gateway applies a policy to prompts and completions; a door applies
 * it to entries written in named spaces; both record findings.
 */
package ai.badmonkey.agentspaces.guardrail;
