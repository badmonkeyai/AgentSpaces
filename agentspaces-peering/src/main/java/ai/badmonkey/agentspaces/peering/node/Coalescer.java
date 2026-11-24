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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.common.time.Watched;
import ai.badmonkey.agentspaces.peering.wire.Envelope;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A node's outboxes and its one flush timer (srad/agentspaces-event-loop.md §3.4, group B), in place of the
 * fixed-rate {@code peer-flush} executor. Frames to one peer in one group wait in that pair's outbox; the first
 * enqueue into an idle node arms one timer at its time plus the window, and the timer flushes every non-empty outbox.
 * An idle node arms nothing.
 *
 * <ul>
 *   <li>FT-1: one flush timer per node, armed only when needed.</li>
 *   <li>FT-2: a frame waits at most the window from its own enqueue.</li>
 *   <li>FT-3: no frame is lost or sent twice; per-outbox order holds; a size flush and the timer may race and either
 *       may find the outbox empty. Each outbox's take-and-send is one critical section, so two flushes of one outbox
 *       never overtake each other.</li>
 *   <li>FT-4: each frame keeps its kind and body beside its encoded bytes, so a single-frame flush sends it plain
 *       without decoding.</li>
 *   <li>FT-5: the timer clears {@code armed} before it walks; an enqueue tests {@code armed} only after its frame is
 *       in the outbox.</li>
 *   <li>FT-6: once {@link #close()} has set its stop flag, {@link #stopped()} is true and the node sends directly; a
 *       frame that raced into an outbox past the stop is flushed by its own enqueue.</li>
 * </ul>
 *
 * <p>The flush timer is {@link Watched} ({@code flush:<peer>}, srad/agentspaces-event-loop.md EL-H-02): a timer armed
 * and never fired, or a flush whose thread died, is re-initialised by flushing every non-empty outbox at once on a
 * fresh thread (EL-H-05).
 */
final class Coalescer implements Watched {

    /** Frames per BATCH before a size flush (unchanged, PERF2 §3.3). */
    static final int BATCH_MAX_FRAMES = 64;
    /** Bytes per BATCH before a size flush (unchanged). */
    static final int BATCH_MAX_BYTES = 48 * 1024;

    /** Where flushed frames go: the node's direct send. */
    interface Sink {
        /**
         * Sends one frame plain.
         *
         * @param group the group
         * @param to    the destination
         * @param kind  the frame's kind
         * @param body  its encoded body
         */
        void single(GroupId group, PeerId to, Envelope.Kind kind, byte[] body);

        /**
         * Sends frames as one BATCH.
         *
         * @param group  the group
         * @param to     the destination
         * @param frames the encoded inner envelopes, in enqueue order
         */
        void batch(GroupId group, PeerId to, List<byte[]> frames);

        /**
         * Told just before the frames of an outbox are taken to be sent, so the gossip bus can mark what this flush
         * carries (srad/agentspaces-event-loop.md EL-T-15). Never told for an outbox found empty.
         *
         * @param group the group
         * @param to    the destination
         */
        default void taking(GroupId group, PeerId to) {
        }
    }

    private record Key(PeerId to, GroupId group) {
    }

    private record Frame(Envelope.Kind kind, byte[] body, byte[] inner) {
    }

    /** One destination's frames. The frame list is guarded by the outbox's monitor; sends by {@link #sending}. */
    private static final class Outbox {
        final List<Frame> frames = new ArrayList<>();
        final ReentrantLock sending = new ReentrantLock();
        int bytes;
        long firstEnqueuedNanos;
    }

    private final TimeSource time;
    private final long windowNanos;
    private final Sink sink;
    private final LoopMeter meter;
    private final Map<Key, Outbox> outboxes = new ConcurrentHashMap<>();
    private final AtomicBoolean armed = new AtomicBoolean();
    private volatile TimeSource.Scheduled timer;
    private volatile boolean stopped;
    private final String name;
    // The heartbeat the watchdog reads (NL-9's shape).
    private volatile long armedAt = Long.MAX_VALUE;
    private volatile long flushStarted;
    private volatile Thread flushThread;
    private final AtomicLong flushes = new AtomicLong();
    private final AtomicLong generation = new AtomicLong();

    Coalescer(TimeSource time, Duration window, Sink sink, LoopMeter meter) {
        this("flush", time, window, sink, meter);
    }

    Coalescer(String name, TimeSource time, Duration window, Sink sink, LoopMeter meter) {
        this.name = Objects.requireNonNull(name, "name");
        this.time = Objects.requireNonNull(time, "time");
        if (window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("a coalescing window must be positive: " + window);
        }
        this.windowNanos = window.toNanos();
        this.sink = Objects.requireNonNull(sink, "sink");
        this.meter = Objects.requireNonNull(meter, "meter");
    }

    /**
     * Whether {@link #close()} has set the stop flag; the node then sends directly (FT-6).
     *
     * @return whether stopped
     */
    boolean stopped() {
        return stopped;
    }

    /**
     * Whether a flush timer is armed now.
     *
     * @return whether armed
     */
    boolean armed() {
        return armed.get();
    }

    /**
     * Adds a frame to the outbox toward {@code to} in {@code group}: a size flush at once when the outbox is full,
     * else the node's flush timer armed if none is.
     *
     * @param group the group
     * @param to    the destination
     * @param kind  the frame's kind
     * @param body  its encoded body
     * @param inner the encoded inner envelope
     */
    void enqueue(GroupId group, PeerId to, Envelope.Kind kind, byte[] body, byte[] inner) {
        Outbox outbox = outboxes.computeIfAbsent(new Key(to, group), k -> new Outbox());
        boolean full;
        synchronized (outbox) {
            if (outbox.frames.isEmpty()) {
                outbox.firstEnqueuedNanos = time.monoNanos();
            }
            outbox.frames.add(new Frame(kind, body, inner));
            outbox.bytes += inner.length;
            full = outbox.frames.size() >= BATCH_MAX_FRAMES || outbox.bytes >= BATCH_MAX_BYTES;
        }
        if (full || stopped) {
            flush(new Key(to, group), outbox); // a size flush (unchanged), or a frame that raced past the stop (FT-6)
            return;
        }
        if (armed.compareAndSet(false, true)) { // FT-5: tested only after the frame is in the outbox
            long at = time.monoNanos() + windowNanos;
            armedAt = at;
            timer = time.scheduleOnLoop(at, this::flushOutboxes); // on the loop pool, off the carriers (§3.5)
            if (stopped) {
                timer.cancel();
            }
        }
    }

    /**
     * Test hook (srad/agentspaces-event-loop.md EL-A-02, EL-A-03, §10.33): cancels the armed flush timer and leaves it
     * marked armed, as a fire lost by the timer would; the watchdog's missed-fire check must find it and re-initialise.
     */
    void dropArmedTimer() {
        TimeSource.Scheduled t = timer;
        if (t != null) {
            t.cancel();
        }
    }

    /**
     * The flush timer's run. The method keeps the name of the fixed-rate flusher it replaces, which the rig's CPU
     * probe classifies as loop work.
     */
    void flushOutboxes() {
        long now = time.monoNanos();
        flushThread = Thread.currentThread();
        flushStarted = now == 0 ? 1 : now;
        armedAt = Long.MAX_VALUE;
        armed.set(false); // FT-5: before the walk
        boolean found = false;
        boolean fatal = false;
        try {
            for (Map.Entry<Key, Outbox> e : outboxes.entrySet()) {
                found |= flush(e.getKey(), e.getValue());
            }
            meter.flushed(found);
        } catch (VirtualMachineError e) {
            fatal = true; // left marked started: the watchdog's dead check finds it
            throw e;
        } finally {
            if (!fatal) {
                flushStarted = 0;
                flushThread = null;
                flushes.incrementAndGet();
            }
        }
    }

    // Watched (§3.9): volatile reads, no lock.

    @Override
    public String name() {
        return name;
    }

    @Override
    public long runs() {
        return flushes.get();
    }

    @Override
    public long runStartedNanos() {
        return flushStarted;
    }

    @Override
    public String runningIn() {
        return flushStarted == 0 ? "" : "flush";
    }

    @Override
    public long armedNanos() {
        return armed.get() ? armedAt : Long.MAX_VALUE;
    }

    @Override
    public Thread thread() {
        return flushThread;
    }

    @Override
    public int queued() {
        return 0; // frames waiting are the armed timer's work, judged as a missed fire
    }

    @Override
    public boolean dispatched() {
        TimeSource.Scheduled t = timer;
        return t != null && t.dispatched() && flushStarted == 0 && armed.get(); // fired, waiting for a carrier
    }

    @Override
    public long generation() {
        return generation.get();
    }

    /**
     * Re-initialises the flush timer (EL-H-05): the armed timer is cancelled and every non-empty outbox is flushed
     * at once on a fresh thread, never on the watchdog's (EL-H-13).
     *
     * @param reason why
     */
    @Override
    public void reinitialise(Reason reason) {
        generation.incrementAndGet();
        TimeSource.Scheduled t = timer;
        if (t != null) {
            t.cancel();
        }
        flushStarted = 0;
        flushThread = null;
        ai.badmonkey.agentspaces.common.time.LoopPool pool = time.loopPool();
        if (pool != null) {
            pool.execute(this::flushOutboxes);
        } else {
            time.schedule(Duration.ZERO, this::flushOutboxes);
        }
    }

    /**
     * Sends every non-empty outbox now, on the caller's thread, leaving the flush timer as it is: a stretch's refresh
     * actions are on the wire before the stretch applies (EL-O-05). Take and send stay one section per outbox, so it
     * may race the timer's walk (FT-3).
     */
    void flushAllNow() {
        for (Map.Entry<Key, Outbox> e : outboxes.entrySet()) {
            flush(e.getKey(), e.getValue());
        }
    }

    /**
     * Discards a group's frames that have not been flushed, when the node leaves it; they are never sent.
     *
     * @param group the group
     */
    void discard(GroupId group) {
        for (Map.Entry<Key, Outbox> entry : List.copyOf(outboxes.entrySet())) {
            if (group.equals(entry.getKey().group())) {
                Outbox outbox = entry.getValue();
                synchronized (outbox) {
                    outbox.frames.clear();
                    outbox.bytes = 0;
                }
                outboxes.remove(entry.getKey(), outbox);
            }
        }
    }

    /** Sets the stop flag, cancels the timer and flushes every outbox once (EL-B-03, FT-6). */
    void close() {
        stopped = true;
        TimeSource.Scheduled current = timer;
        if (current != null) {
            current.cancel();
        }
        for (Map.Entry<Key, Outbox> e : outboxes.entrySet()) {
            try {
                flush(e.getKey(), e.getValue());
            } catch (RuntimeException ex) {
                // closing anyway
            }
        }
    }

    /**
     * Whether the outbox toward {@code to} in {@code group} holds frames not yet flushed (EL-T-15).
     *
     * @param group the group
     * @param to    the destination
     * @return whether it holds any
     */
    boolean holds(GroupId group, PeerId to) {
        Outbox outbox = outboxes.get(new Key(to, group));
        if (outbox == null) {
            return false;
        }
        synchronized (outbox) {
            return !outbox.frames.isEmpty();
        }
    }

    /**
     * Frames waiting in every outbox.
     *
     * @return how many
     */
    int waiting() {
        int n = 0;
        for (Outbox outbox : outboxes.values()) {
            synchronized (outbox) {
                n += outbox.frames.size();
            }
        }
        return n;
    }

    /** Sends what the outbox holds; false when it held nothing. Take and send are one section per outbox (FT-3). */
    private boolean flush(Key key, Outbox outbox) {
        outbox.sending.lock();
        try {
            List<Frame> frames;
            long first;
            synchronized (outbox) {
                if (outbox.frames.isEmpty()) {
                    return false;
                }
            }
            // Before the take: every item the bus stamped before this mark was enqueued before the take (EL-T-15).
            sink.taking(key.group(), key.to());
            synchronized (outbox) {
                if (outbox.frames.isEmpty()) {
                    return false;
                }
                frames = List.copyOf(outbox.frames);
                first = outbox.firstEnqueuedNanos;
                outbox.frames.clear();
                outbox.bytes = 0;
            }
            meter.flushWait(time.monoNanos() - first);
            if (frames.size() == 1) {
                // Nothing to coalesce: the frame goes as itself, from the kind and body kept beside it (FT-4).
                Frame only = frames.get(0);
                sink.single(key.group(), key.to(), only.kind(), only.body());
                return true;
            }
            List<byte[]> inner = new ArrayList<>(frames.size());
            for (Frame f : frames) {
                inner.add(f.inner());
            }
            sink.batch(key.group(), key.to(), inner);
            return true;
        } finally {
            outbox.sending.unlock();
        }
    }
}
