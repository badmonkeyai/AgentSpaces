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
 * The planner (ISSUE-CardContract, SPEC §6.1b): the {@link ai.badmonkey.agentspaces.planner.Planner} SPI over
 * {@link ai.badmonkey.agentspaces.planner.PlanAction}s read from agent cards and
 * {@link ai.badmonkey.agentspaces.planner.PlanningState}s read from a key's events and marking; the
 * enablement and search planners; a plan as {@link ai.badmonkey.agentspaces.planner.PlannedStep}s a peer
 * writes as {@code PlanStep} entries workers take.
 */
package ai.badmonkey.agentspaces.planner;
