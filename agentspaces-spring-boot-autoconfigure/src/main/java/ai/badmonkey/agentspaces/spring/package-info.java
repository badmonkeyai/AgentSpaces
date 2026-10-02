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
 * Spring Boot autoconfiguration for AgentSpaces (spec §10.1): properties-driven
 * peer, group, and space wiring behind the fluent facade, with lifecycle-driven
 * ticking and annotation-driven bean enrollment. Compiled against the
 * provided-scope stub surface in {@code agentspaces-spring-stubs}; the
 * application's real Spring Boot classes take over at runtime, and the
 * integration test against real Spring runs in a Maven-Central-capable
 * environment (see docs/BUILD-ENVIRONMENTS.md). Stability: EXPERIMENTAL.
 */
package ai.badmonkey.agentspaces.spring;
