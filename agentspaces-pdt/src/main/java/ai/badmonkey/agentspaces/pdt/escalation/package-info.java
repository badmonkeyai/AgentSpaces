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
/**
 * Escalation of a space's large payloads into persistent trees
 * (srad/agentspaces-pdt.md §3.7, group E): {@link ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads}
 * is the {@code BulkPayloads} seam a space opts into, so a payload above its
 * threshold travels as the nodes a reader lacks.
 */
package ai.badmonkey.agentspaces.pdt.escalation;
