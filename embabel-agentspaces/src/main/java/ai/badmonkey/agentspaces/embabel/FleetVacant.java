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
package ai.badmonkey.agentspaces.embabel;

/**
 * The input and output of the one inert action a vacant fleet generation carries
 * (reviews/embabel-agentspaces-review-2026-11-12.md §7.9): when every fleet action has lapsed, the bridge
 * deploys a generation whose only action maps this type to itself, so the previous generation's dead actions
 * leave the platform. Nothing constructs a {@code FleetVacant}, so the planner can never satisfy that action.
 */
public final class FleetVacant {

    private FleetVacant() {
    }
}
