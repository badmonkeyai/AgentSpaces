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
 * Persistent data trees (srad/agentspaces-pdt.md §3, §6.2): the shared node layer
 * every persistent value builds on. {@link ai.badmonkey.agentspaces.content.tree.Cids} names
 * a node by its CIDv1 (PD-N-02), {@link ai.badmonkey.agentspaces.pdt.NodeCodec}
 * encodes and decodes the canonical DAG-CBOR node of §3.1 under the bounds of
 * PD-N-04, {@link ai.badmonkey.agentspaces.pdt.Kind} lists the node kinds, and
 * {@link ai.badmonkey.agentspaces.pdt.NodeStore} keeps nodes by CID over the
 * shipped {@code BlockStore} SPI with pins and completeness (PD-S-01, PD-S-06).
 * Stability: API for every public type in this package.
 */
package ai.badmonkey.agentspaces.pdt;
