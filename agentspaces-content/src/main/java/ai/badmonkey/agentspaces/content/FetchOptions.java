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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.util.Set;

/**
 * Options of {@link Content#fetch}.
 *
 * @param timeout      how long a fetch waits for a source before it reports waiting; zero means forever
 * @param prefer       sources to try first, or empty
 * @param avoid        sources never used, or empty
 * @param expectedSize the artifact's length the caller expects, or 0 for any: a manifest of another length fails the
 *                     fetch with an {@link IllegalArgumentException} before it asks for a piece (AppCard origin and
 *                     formats, 2026-11-09)
 */
public record FetchOptions(Duration timeout, Set<PeerId> prefer, Set<PeerId> avoid, long expectedSize) {

    /** No timeout, no preference, any length. */
    public static FetchOptions defaults() {
        return new FetchOptions(Duration.ZERO, Set.of(), Set.of(), 0);
    }

    public FetchOptions {
        prefer = prefer == null ? Set.of() : Set.copyOf(prefer);
        avoid = avoid == null ? Set.of() : Set.copyOf(avoid);
        if (expectedSize < 0) {
            throw new IllegalArgumentException("the expected size is a length or 0: " + expectedSize);
        }
    }

    /** The options before the expected size: any length. */
    public FetchOptions(Duration timeout, Set<PeerId> prefer, Set<PeerId> avoid) {
        this(timeout, prefer, avoid, 0);
    }

    public FetchOptions withTimeout(Duration value) {
        return new FetchOptions(value, prefer, avoid, expectedSize);
    }

    public FetchOptions prefer(PeerId... peers) {
        return new FetchOptions(timeout, Set.of(peers), avoid, expectedSize);
    }

    public FetchOptions avoid(PeerId... peers) {
        return new FetchOptions(timeout, prefer, Set.of(peers), expectedSize);
    }

    /**
     * The options with the length the caller expects, so a manifest of another length stops the fetch at once.
     *
     * @param bytes the artifact's length, or 0 for any
     * @return the options
     */
    public FetchOptions withExpectedSize(long bytes) {
        return new FetchOptions(timeout, prefer, avoid, bytes);
    }
}
