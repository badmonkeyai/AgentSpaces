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
package ai.badmonkey.agentspaces.agent.reduce;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The reader's side of a {@code @SpaceReduce} (ISSUE-SpaceReduce §9.6). An
 * accumulator is an entry of the application's type tagged with the reduce's
 * name, its key, and its step count. Between a successor's write and the
 * predecessor's retirement, normally microseconds and after a crash until the
 * next drain, two are visible, and a plain {@code read} returns the older;
 * {@link #current} returns the newest.
 */
public final class Reductions {

    /** The tag naming the reduce an accumulator belongs to. */
    public static final String REDUCE_TAG = "reduce";
    /** The tag carrying the accumulator's key. */
    public static final String KEY_TAG = "key";
    /** The tag carrying the accumulator's step count. */
    public static final String STEPS_TAG = "steps";
    /** The key of a reduce with neither {@code key} nor {@code keyTag}: one accumulator for the type. */
    public static final String GLOBAL_KEY = "*";

    private static final int READ_LIMIT = 8;

    private Reductions() {
    }

    /** The template of a reduce's accumulators of one key. */
    public static <A> Template<A> template(Class<A> type, String name, String key) {
        return Template.of(type).whereTag(REDUCE_TAG, eq(name)).whereTag(KEY_TAG, eq(key));
    }

    /**
     * The key's newest accumulator: the one with the highest step count among
     * those visible.
     *
     * @param space the space the reduce runs on
     * @param type  the accumulator type
     * @param name  the reduce's name
     * @param key   the key
     * @param <A>   the accumulator type
     * @return the newest accumulator, or empty when the key has none
     */
    public static <A> Optional<Space.Entry<A>> current(Space space, Class<A> type, String name, String key) {
        Objects.requireNonNull(space, "space");
        List<Space.Entry<A>> candidates = space.readAllEntries(template(type, name, key), READ_LIMIT);
        Space.Entry<A> newest = null;
        for (Space.Entry<A> candidate : candidates) {
            if (newest == null || steps(candidate) > steps(newest)) {
                newest = candidate;
            }
        }
        return Optional.ofNullable(newest);
    }

    /** The step count an accumulator carries; zero when it carries none. */
    public static long steps(Space.Entry<?> accumulator) {
        String steps = accumulator.tags().get(STEPS_TAG);
        if (steps == null) {
            return 0;
        }
        try {
            return Long.parseLong(steps);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
