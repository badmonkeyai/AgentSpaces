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
 * Testing utilities for AgentSpaces modules and applications: a manually advanced
 * clock, the SimNetwork deterministic transport fabric with partition control, and
 * shared fixtures. Stability: API (for tests).
 *
 * <p>The link filters ({@link ai.badmonkey.agentspaces.test.LinkFilter}, with the
 * {@link ai.badmonkey.agentspaces.test.FrameDecoder} and {@link ai.badmonkey.agentspaces.test.ItemDecoder}
 * contracts) and the minimal fleet oracle ({@link ai.badmonkey.agentspaces.test.FleetInvariants} over a
 * {@link ai.badmonkey.agentspaces.test.FleetProbe}) are the in-process half of the spoiler peers of
 * srad/spoiler-peers.md: they act on the {@code mem} fabric only. Everything that acts on a real
 * transport or re-signs lives in the unpublished {@code agentspaces-spoilers} module.
 */
package ai.badmonkey.agentspaces.test;
