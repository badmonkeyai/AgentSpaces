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
package ai.badmonkey.agentspaces.spoilers;

/**
 * The bound on a hold (srad/spoiler-peers.md §3.2): fleet-clock milliseconds or gossip periods on the
 * in-process fabric, never ticks, since the fleets tick at different periods. A hold with a release that
 * passes its ceiling fails the scenario; a hold with no release ends at it.
 *
 * @param millis  the bound in fleet-clock milliseconds, or -1
 * @param periods the bound in gossip periods, or -1
 */
public record Ceiling(long millis, int periods) {

    public static Ceiling millis(long value) {
        return new Ceiling(value, -1);
    }

    public static Ceiling periods(int value) {
        return new Ceiling(-1, value);
    }

    long toMillis(long periodMillis) {
        return millis >= 0 ? millis : periods * periodMillis;
    }

    @Override
    public String toString() {
        return millis >= 0 ? millis + " ms" : periods + " periods";
    }
}
