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
package ai.badmonkey.agentspaces.transport.http;

import java.time.Duration;
import java.util.Objects;

/**
 * The tunables of an HTTP binding (SRAD agentspaces-js §3.3, appendix C;
 * ISSUE-Websocket PR-2), each defaulted. None caps the number of connections
 * or sessions (JS-N-08); the per-source rate bounds abuse only.
 *
 * @param path                 the WebSocket upgrade path (default {@code /aspace})
 * @param exchangePath         the HTTP exchange base path (default {@code /aspace/h})
 * @param origins              the browser-origin policy (default: no browser)
 * @param idleTimeout          a connection or session silent this long closes (default 60 s)
 * @param pingInterval         a WebSocket idle this long for writing sends a ping (default 30 s)
 * @param longPoll             how long a download request is held with nothing to send (default 25 s)
 * @param opensPerSourcePerSecond exchange sessions one source address may open per second (default 4)
 */
public record HttpOptions(String path, String exchangePath, OriginPolicy origins, Duration idleTimeout,
                          Duration pingInterval, Duration longPoll, int opensPerSourcePerSecond) {

    /** The largest frame any binding carries (8 MiB, SPEC §9). */
    public static final int MAX_FRAME = 8 * 1024 * 1024;
    /** The largest exchange body: 64 frames and 9 MiB (appendix C). */
    public static final int MAX_BATCH_FRAMES = 64;
    public static final int MAX_BODY = 9 * 1024 * 1024;

    public HttpOptions {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(exchangePath, "exchangePath");
        Objects.requireNonNull(origins, "origins");
        Objects.requireNonNull(idleTimeout, "idleTimeout");
        Objects.requireNonNull(pingInterval, "pingInterval");
        Objects.requireNonNull(longPoll, "longPoll");
        if (!path.startsWith("/") || !exchangePath.startsWith("/")) {
            throw new IllegalArgumentException("paths must be absolute: " + path + ", " + exchangePath);
        }
        if (idleTimeout.compareTo(Duration.ofSeconds(5)) < 0 || pingInterval.isNegative() || pingInterval.isZero()
                || longPoll.isNegative() || longPoll.isZero() || longPoll.compareTo(idleTimeout) >= 0) {
            throw new IllegalArgumentException("idle-timeout must be at least 5 s and exceed long-poll;"
                    + " ping-interval and long-poll must be positive");
        }
        if (opensPerSourcePerSecond < 1) {
            throw new IllegalArgumentException("opens-per-source-per-second must be positive");
        }
    }

    /** The defaults. */
    public static HttpOptions defaults() {
        return new HttpOptions("/aspace", "/aspace/h", OriginPolicy.none(), Duration.ofSeconds(60),
                Duration.ofSeconds(30), Duration.ofSeconds(25), 4);
    }

    public HttpOptions withPath(String value) {
        return new HttpOptions(value, exchangePath, origins, idleTimeout, pingInterval, longPoll,
                opensPerSourcePerSecond);
    }

    public HttpOptions withExchangePath(String value) {
        return new HttpOptions(path, value, origins, idleTimeout, pingInterval, longPoll, opensPerSourcePerSecond);
    }

    public HttpOptions withOrigins(OriginPolicy value) {
        return new HttpOptions(path, exchangePath, value, idleTimeout, pingInterval, longPoll,
                opensPerSourcePerSecond);
    }

    public HttpOptions withIdleTimeout(Duration value) {
        return new HttpOptions(path, exchangePath, origins, value, pingInterval, longPoll, opensPerSourcePerSecond);
    }

    public HttpOptions withPingInterval(Duration value) {
        return new HttpOptions(path, exchangePath, origins, idleTimeout, value, longPoll, opensPerSourcePerSecond);
    }

    public HttpOptions withLongPoll(Duration value) {
        return new HttpOptions(path, exchangePath, origins, idleTimeout, pingInterval, value, opensPerSourcePerSecond);
    }

    public HttpOptions withOpensPerSourcePerSecond(int value) {
        return new HttpOptions(path, exchangePath, origins, idleTimeout, pingInterval, longPoll, value);
    }
}
