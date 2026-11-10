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
 * The requester's side of the content store peer (PDT 2 phase 3, srad/agentspaces-pdt2.md §3.2): choosing
 * stores by their cards, writing the {@code SignedPin} this peer signs as requester, challenging a store's possession, striking a store
 * that fails twice, and counting only the receipts of stores that pass. The store's side lives in
 * {@code agentspaces-store}.
 */
package ai.badmonkey.agentspaces.pdt.stores;
