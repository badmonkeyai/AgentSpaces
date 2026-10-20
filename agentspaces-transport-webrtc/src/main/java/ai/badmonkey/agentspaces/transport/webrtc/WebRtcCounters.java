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

package ai.badmonkey.agentspaces.transport.webrtc;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The binding's counters (SRAD srad/agentspaces-js.md JS-W-14, JS-Q-MNT-05): sessions offered,
 * answered, opened, failed by reason; signals dropped by reason; chunks and bytes each way; the
 * selected candidate type of each opened connection. Read by the console and the tests; never
 * reset.
 */
public final class WebRtcCounters {

    final AtomicLong sessionsOffered = new AtomicLong();
    final AtomicLong sessionsAnswered = new AtomicLong();
    final AtomicLong sessionsOpened = new AtomicLong();
    final AtomicLong chunksSent = new AtomicLong();
    final AtomicLong chunksReceived = new AtomicLong();
    final AtomicLong bytesSent = new AtomicLong();
    final AtomicLong bytesReceived = new AtomicLong();
    private final Map<String, AtomicLong> failed = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> dropped = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> candidates = new ConcurrentHashMap<>();

    public long sessionsOffered() {
        return sessionsOffered.get();
    }

    public long sessionsAnswered() {
        return sessionsAnswered.get();
    }

    public long sessionsOpened() {
        return sessionsOpened.get();
    }

    public long chunksSent() {
        return chunksSent.get();
    }

    public long chunksReceived() {
        return chunksReceived.get();
    }

    public long bytesSent() {
        return bytesSent.get();
    }

    public long bytesReceived() {
        return bytesReceived.get();
    }

    /**
     * Sessions that ended without opening or failed after, by reason: {@code timeout},
     * {@code refused}, {@code fingerprint}, {@code engine}, {@code failed}, {@code protocol},
     * {@code closed}, {@code blocked}.
     */
    public Map<String, Long> failed() {
        return snapshot(failed);
    }

    /** The total of {@link #failed()}. */
    public long failedTotal() {
        return failed.values().stream().mapToLong(AtomicLong::get).sum();
    }

    /**
     * Signals dropped, by reason: {@code size}, {@code malformed}, {@code rate}, {@code non-member},
     * {@code no-acceptor}, {@code unknown-session}, {@code wrong-origin}, {@code candidates},
     * {@code tie}, {@code closed}.
     */
    public Map<String, Long> signalsDropped() {
        return snapshot(dropped);
    }

    /** The total of {@link #signalsDropped()}. */
    public long signalsDroppedTotal() {
        return dropped.values().stream().mapToLong(AtomicLong::get).sum();
    }

    /** Opened connections by the selected local candidate type ({@code host}, {@code srflx}, {@code relay}, {@code unknown}). */
    public Map<String, Long> selectedCandidates() {
        return snapshot(candidates);
    }

    void failed(String reason) {
        failed.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
    }

    void dropped(String reason) {
        dropped.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
    }

    void opened(String candidateType) {
        sessionsOpened.incrementAndGet();
        candidates.computeIfAbsent(candidateType, t -> new AtomicLong()).incrementAndGet();
    }

    void sent(int bytes) {
        chunksSent.incrementAndGet();
        bytesSent.addAndGet(bytes);
    }

    void received(int bytes) {
        chunksReceived.incrementAndGet();
        bytesReceived.addAndGet(bytes);
    }

    private static Map<String, Long> snapshot(Map<String, AtomicLong> source) {
        Map<String, Long> out = new TreeMap<>();
        source.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    @Override
    public String toString() {
        return "webrtc{offered=" + sessionsOffered() + ", answered=" + sessionsAnswered() + ", opened="
                + sessionsOpened() + ", failed=" + failed() + ", dropped=" + signalsDropped() + ", chunks="
                + chunksSent() + "/" + chunksReceived() + ", bytes=" + bytesSent() + "/" + bytesReceived()
                + ", candidates=" + selectedCandidates() + "}";
    }
}
