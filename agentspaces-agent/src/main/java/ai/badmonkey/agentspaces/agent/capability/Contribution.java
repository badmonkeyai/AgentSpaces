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
package ai.badmonkey.agentspaces.agent.capability;

import java.util.Objects;

/**
 * A push-sum contribution returned from a {@code @SpaceNotify} or
 * {@code @SpaceTake} method: instead of writing an entry, the binder starts (or
 * joins) the aggregate epoch {@code epochId} with {@code value} as this peer's
 * share (SPEC §8 aggregate). The epoch id is usually a function of the cue —
 * the day, the trip, the topic — which is why this is a return value and not an
 * annotation attribute.
 *
 * <pre>{@code
 * @SpaceNotify(space = "trip")
 * public Contribution feel(TripDay day) {
 *     return Contribution.to(energyEpoch(day), energyOn(day));
 * }
 * }</pre>
 *
 * @param epochId the aggregate epoch
 * @param value   this peer's contribution
 */
public record Contribution(String epochId, double value) {

    public Contribution {
        Objects.requireNonNull(epochId, "epochId");
    }

    /** A contribution of {@code value} to {@code epochId}. */
    public static Contribution to(String epochId, double value) {
        return new Contribution(epochId, value);
    }
}
