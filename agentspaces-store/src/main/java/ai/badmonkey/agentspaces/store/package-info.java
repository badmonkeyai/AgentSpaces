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
 * The content store peer (PDT 2 phase 3, group S; srad/agentspaces-pdt2.md §3.2): {@link
 * ai.badmonkey.agentspaces.store.ContentStore} takes {@code SignedPin} entries that carry their requester's
 * signature, holds their roots under leases,
 * completes them with {@code StoreReceipt} entries, advertises a {@code content-store} card through the connector
 * SDK, and holds sealed trees blind; {@link ai.badmonkey.agentspaces.store.StorePolicy} says what it accepts and
 * {@link ai.badmonkey.agentspaces.store.StoreLedger} what it holds for whom. The module opens no socket and adds no
 * peer role.
 */
package ai.badmonkey.agentspaces.store;
