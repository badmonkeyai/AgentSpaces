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
 * The persistent values of srad/agentspaces-pdt.md §3.2 (group V, PD-V-01 to
 * PD-V-09) and the spill rules of PD-N-05. {@link ai.badmonkey.agentspaces.pdt.values.PMap}
 * and {@link ai.badmonkey.agentspaces.pdt.values.PSet} are CHAMP tries over the
 * canonical node layout, {@link ai.badmonkey.agentspaces.pdt.values.PVector} is
 * Clojure's tailed trie, {@link ai.badmonkey.agentspaces.pdt.values.Editor} and its
 * siblings are transients, {@link ai.badmonkey.agentspaces.pdt.values.Diff} and
 * {@link ai.badmonkey.agentspaces.pdt.values.Merge} compare and merge maps by
 * descending only into subtrees whose CIDs differ, and
 * {@link ai.badmonkey.agentspaces.pdt.values.Spill} holds large values out of line.
 * Every value loads nodes on demand through a
 * {@link ai.badmonkey.agentspaces.pdt.values.NodeLoader}, keeps new nodes unhashed
 * until {@code root()} needs the CID, and then writes them to the node store in one
 * batch (PD-V-08). Stability: API for the public types; the node classes are internal.
 */
package ai.badmonkey.agentspaces.pdt.values;
