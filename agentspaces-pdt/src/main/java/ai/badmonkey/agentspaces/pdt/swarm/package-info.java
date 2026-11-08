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
 * The bridge from persistent data trees to the content swarm's tree exchange
 * (srad/agentspaces-pdt.md §3.5, §6.9): {@link ai.badmonkey.agentspaces.pdt.swarm.NodeStoreTreeStore}
 * shows a {@code NodeStore} to {@code agentspaces-content} as its {@code TreeStore},
 * {@link ai.badmonkey.agentspaces.pdt.swarm.SwarmFetcher} is the production
 * {@code Fetcher} of PD-V-06 over {@code Content.fetchNodes} and
 * {@code Content.fetchTree}, and {@link ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm}
 * attaches a store to a swarm with hold, release, fetch, and a loader. The
 * dependency runs one way: content knows the {@code TreeStore} interface and
 * nothing of pdt. Stability: API for every public type in this package.
 */
package ai.badmonkey.agentspaces.pdt.swarm;
