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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Operational counters of one HTTP binding (SRAD agentspaces-js JS-H-12), as
 * {@code long} accessors with no metrics dependency: upgrades accepted and
 * refused by reason, exchanges opened, long-polls served, frames and bytes in
 * and out, and closes by WebSocket status code.
 */
public final class HttpCounters {

    final AtomicLong upgradesAccepted = new AtomicLong();
    final AtomicLong exchangesOpened = new AtomicLong();
    final AtomicLong longPolls = new AtomicLong();
    final AtomicLong framesIn = new AtomicLong();
    final AtomicLong framesOut = new AtomicLong();
    final AtomicLong bytesIn = new AtomicLong();
    final AtomicLong bytesOut = new AtomicLong();
    private final Map<String, AtomicLong> refused = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> closes = new ConcurrentHashMap<>();

    public long upgradesAccepted() {
        return upgradesAccepted.get();
    }

    public long exchangesOpened() {
        return exchangesOpened.get();
    }

    public long longPolls() {
        return longPolls.get();
    }

    public long framesIn() {
        return framesIn.get();
    }

    public long framesOut() {
        return framesOut.get();
    }

    public long bytesIn() {
        return bytesIn.get();
    }

    public long bytesOut() {
        return bytesOut.get();
    }

    /** Requests refused, by reason ({@code origin}, {@code path}, {@code subprotocol}, {@code rate}, ...). */
    public Map<String, Long> refused() {
        Map<String, Long> out = new TreeMap<>();
        refused.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    /** WebSocket closes by status code. */
    public Map<Integer, Long> closes() {
        Map<Integer, Long> out = new TreeMap<>();
        closes.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    void refuse(String reason) {
        refused.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
    }

    void closed(int status) {
        closes.computeIfAbsent(status, s -> new AtomicLong()).incrementAndGet();
    }

    void in(int bytes) {
        framesIn.incrementAndGet();
        bytesIn.addAndGet(bytes);
    }

    void out(int bytes) {
        framesOut.incrementAndGet();
        bytesOut.addAndGet(bytes);
    }
}
