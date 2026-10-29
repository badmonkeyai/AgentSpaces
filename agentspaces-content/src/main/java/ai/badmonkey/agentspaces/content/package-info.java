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
 * The content swarm, {@code aspace:cap/content-swarm} (srad/large-networks-bulk-data.md):
 * {@link ai.badmonkey.agentspaces.content.Content} publishes a large artifact as a
 * signed manifest in the group's content space, advertises what it holds as leased
 * holdings, and fetches pieces from every reachable holder at once over capability
 * pipes on whatever transport sessions exist, verifying each piece against the
 * manifest and storing it through the {@link ai.badmonkey.agentspaces.content.PieceStore}
 * SPI. Stability: API for {@code Content}, {@code PieceStore}, {@code ContentPolicy},
 * and the options; internal for the session and messages.
 */
package ai.badmonkey.agentspaces.content;
