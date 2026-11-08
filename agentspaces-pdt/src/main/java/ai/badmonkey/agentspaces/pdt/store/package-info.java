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
 * The node store's durable half (srad/agentspaces-pdt.md §3.4, group S):
 * {@link ai.badmonkey.agentspaces.pdt.store.PackNodeStore} keeps nodes in pack
 * files (PD-S-02), {@link ai.badmonkey.agentspaces.pdt.store.Retention} keeps the
 * delta bases per ref (PD-S-03), {@link ai.badmonkey.agentspaces.pdt.store.NodeCollector}
 * marks from pins, heads, and retained versions and sweeps the rest a bounded tick at
 * a time (PD-S-04, PD-Q-REL-05), {@link ai.badmonkey.agentspaces.pdt.store.QuotaNodeStore}
 * refuses writes past the quota and evicts nothing (PD-S-05), and
 * {@link ai.badmonkey.agentspaces.pdt.store.StoreThenFetchLoader} loads a node from
 * the store or a {@link ai.badmonkey.agentspaces.pdt.store.Fetcher} within a deadline
 * (PD-V-06). Stability: API for every public type in this package.
 */
package ai.badmonkey.agentspaces.pdt.store;
