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
 * The typed readers and writers of the seven level-2 node kinds of PDT 2 (PDT 2 phase 0;
 * srad/agentspaces-pdt2.md §3 and §6.10.3): {@link ai.badmonkey.agentspaces.pdt.kinds.DbRoot} and
 * {@link ai.badmonkey.agentspaces.pdt.kinds.DnNode} (P2-D-01), {@link ai.badmonkey.agentspaces.pdt.kinds.RrRoot}
 * and {@link ai.badmonkey.agentspaces.pdt.kinds.RvNode} (P2-R-01), {@link ai.badmonkey.agentspaces.pdt.kinds.TextLeaf}
 * (P2-T-01), {@link ai.badmonkey.agentspaces.pdt.kinds.VgNode} (P2-V-01), and
 * {@link ai.badmonkey.agentspaces.pdt.kinds.BoNode} (P2-B-02). {@code NodeCodec} checks each
 * field's type; each reader here checks the rules that span fields and refuses a node that breaks
 * one with a {@code NodeFormatException} naming the rule. None of these kinds reaches a level-1
 * reader: the first five live under a {@code PRef.v} 2 head, {@code vg} behind the attribute
 * {@code pdt.ancestry}, and {@code bo} behind the attribute {@code pdt.bao}.
 */
package ai.badmonkey.agentspaces.pdt.kinds;
