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
 * @param timeout how long a fetch waits for a source before it reports waiting; zero means forever
 * @param prefer  sources to try first, or empty
 * @param avoid   sources never used, or empty
 */
public record FetchOptions(Duration timeout, Set<PeerId> prefer, Set<PeerId> avoid) {

    /** No timeout, no preference. */
    public static FetchOptions defaults() {
        return new FetchOptions(Duration.ZERO, Set.of(), Set.of());
    }

    public FetchOptions {
        prefer = prefer == null ? Set.of() : Set.copyOf(prefer);
        avoid = avoid == null ? Set.of() : Set.copyOf(avoid);
    }

    public FetchOptions withTimeout(Duration value) {
        return new FetchOptions(value, prefer, avoid);
    }

    public FetchOptions prefer(PeerId... peers) {
        return new FetchOptions(timeout, Set.of(peers), avoid);
    }

    public FetchOptions avoid(PeerId... peers) {
        return new FetchOptions(timeout, prefer, Set.of(peers));
    }
}
