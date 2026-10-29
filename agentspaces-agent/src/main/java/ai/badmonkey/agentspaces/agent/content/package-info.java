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
 * The reserved entry records of the content swarm (srad/large-networks-bulk-data.md §3):
 * a {@link ai.badmonkey.agentspaces.agent.content.Manifest} names a large artifact by the
 * root of its pieces and a {@link ai.badmonkey.agentspaces.agent.content.Holding} says which
 * pieces one member holds. Both are plain records named
 * {@code ai.badmonkey.agentspaces.agent.content.<Name>#v1} on the wire, as
 * {@code JoinTicket} is, and pinned by golden vectors. Stability: API.
 */
package ai.badmonkey.agentspaces.agent.content;
