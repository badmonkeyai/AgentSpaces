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
 * {@code JoinTicket} is, and pinned by golden vectors.
 *
 * <p>PDT 2 phase 0 (P2-S-02; srad/agentspaces-pdt2.md §3.2.2) adds the content store's two
 * entries: a {@link ai.badmonkey.agentspaces.agent.content.StorePin} asks a store to hold a root
 * and a {@link ai.badmonkey.agentspaces.agent.content.StoreReceipt} says a store holds it. Both
 * are protocol records read from their own wire encoding ({@code @WireRecord}), pinned by the
 * golden vectors {@code pdt_store_pin_cbor} and {@code pdt_store_receipt_cbor}, and change shape
 * only under a new type name (P2-F-08). Since the PDT 2 follow-ups of 2026-11-09 (F1), the pin entry a store
 * takes is a {@link ai.badmonkey.agentspaces.agent.content.SignedPin}: the requester's {@code StorePin} under the
 * requester's signature, pinned by {@code pdt_signed_pin_cbor}, so a store charges only the member who signed
 * the pin. Stability: API.
 */
package ai.badmonkey.agentspaces.agent.content;
