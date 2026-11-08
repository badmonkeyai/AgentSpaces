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
 * The sealed-nodes mode of srad/agentspaces-pdt.md §3.8 (PD-P-05): {@link
 * ai.badmonkey.agentspaces.pdt.seal.NodeSealer} is the pinned construction
 * (HKDF per-epoch key, AES-256-GCM-SIV with a digest-derived nonce, the {@code sn}
 * envelope with links in the clear) and {@link
 * ai.badmonkey.agentspaces.pdt.seal.SealedNodeStore} is the {@link
 * ai.badmonkey.agentspaces.pdt.NodeStore} that seals on write, opens on read, and
 * serves envelopes to keyless members. {@code Pdt.Builder.sealing("sealed-nodes")}
 * with {@code Pdt.Builder.keyRing} wires it.
 */
package ai.badmonkey.agentspaces.pdt.seal;
