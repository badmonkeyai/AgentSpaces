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
 * Zero-configuration bootstrap (spec §10.1, roadmap M1): the opt-in
 * {@link ai.badmonkey.agentspaces.peering.bootstrap.MulticastBeacon} announces a
 * node's signed self-advertisement on a LAN multicast group so peers with no
 * configured seeds find one another. Every datagram is verified exactly like a
 * bootstrap rumor before anything acts on it.
 */
package ai.badmonkey.agentspaces.peering.bootstrap;
