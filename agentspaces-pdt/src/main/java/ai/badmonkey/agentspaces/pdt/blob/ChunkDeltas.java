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
package ai.badmonkey.agentspaces.pdt.blob;

import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Dcz;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * The writer's side table of intra-chunk deltas (PDT 2 phase 4, P2-Z-02; srad/agentspaces-pdt2.md §3.8, decision 13):
 * when an edit replaces a chunk, the writer holds both chunks, computes the {@code dcz} frame of the new chunk over
 * the old one ({@link Dcz#encode}), and keeps it here keyed by {@code (base, cid)} for {@link #retention()} (one
 * hour by default). It keeps a frame only when the frame is under half its chunk, so a frame always saves at least
 * half. The tree exchange reads the table through {@code TreeStore.deltas} and sends a frame to a level-2 asker
 * whose haves hold the base.
 *
 * <p>Every {@link ai.badmonkey.agentspaces.pdt.NodeStore} owns one table, and a sibling store of the inline-delta
 * recording shares it, so the frames of a swap land where the holder serves them. The table holds at most
 * {@link #MAX_BYTES} of frames, oldest out first. A sealed store keeps none, since a sealed chunk's CID names its
 * envelope, which no frame over plaintext rebuilds. {@link #enabled(boolean)} turns the table off, the builder
 * option {@code deltas} of P2-Q-MNT-01.
 *
 * <p>An edit keeps frames only while its {@link #audience} says some member could receive one (PDT 2 phase 4
 * measures, owner default of 2026-11-09, §10.14): {@code PdtSwarm.attach} installs the swarm's answer, this peer and
 * another member at level 2, and a table no swarm attached records nothing. A fleet of level-1 members thus neither
 * compresses frames nor loads {@code zstd-jni}.
 */
public final class ChunkDeltas {

    /** How long a frame stays (§3.8: {@code deltaRetention}, one hour). */
    public static final Duration DEFAULT_RETENTION = Duration.ofHours(1);

    /** The most frame bytes the table holds; past it the oldest frames go. */
    public static final long MAX_BYTES = 16L * 1024 * 1024;

    /** The most new chunks of one edit that get a frame: about 1 MiB of changed content. */
    static final int MAX_FRAMES_PER_EDIT = 16;

    private record Kept(Messages.NodeDelta delta, long atMillis) {
    }

    private final InstantSource clock;
    /** Frames by {@code cid + " " + base}, oldest first. */
    private final LinkedHashMap<String, Kept> frames = new LinkedHashMap<>();
    /** The same frames by the chunk they rebuild, oldest first, so a holder's lookup per chunk costs no scan. */
    private final Map<String, List<Kept>> byCid = new HashMap<>();
    private volatile boolean enabled;
    private volatile BooleanSupplier audience = () -> false;
    private volatile Duration retention = DEFAULT_RETENTION;
    private long bytes;

    /**
     * A table.
     *
     * @param clock   the clock retention is judged by
     * @param enabled whether edits record frames
     */
    public ChunkDeltas(InstantSource clock, boolean enabled) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.enabled = enabled;
    }

    /** Whether edits record frames. */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Turns recording on or off; frames already kept stay until they lapse.
     *
     * @param value whether edits record frames
     * @return this table
     */
    public ChunkDeltas enabled(boolean value) {
        this.enabled = value;
        return this;
    }

    /**
     * Sets whether some member could receive a frame now, which recording asks at each edit.
     *
     * @param value the answer, for example the content swarm's {@code framesReachAMember}
     * @return this table
     */
    public ChunkDeltas audience(BooleanSupplier value) {
        this.audience = Objects.requireNonNull(value, "audience");
        return this;
    }

    /** Whether an edit keeps frames now: the table is enabled and some member could receive a frame. */
    public boolean recording() {
        return enabled && audience.getAsBoolean();
    }

    /** How long a frame stays. */
    public Duration retention() {
        return retention;
    }

    /**
     * Sets how long a frame stays.
     *
     * @param value a positive duration
     * @return this table
     */
    public ChunkDeltas retention(Duration value) {
        if (value == null || value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException("deltaRetention is a positive duration, got " + value);
        }
        this.retention = value;
        return this;
    }

    /**
     * Computes and keeps the frame of a new chunk over the chunk it replaced, when {@link #recording()} holds and the
     * frame is under half the chunk.
     *
     * @param baseCid the replaced chunk's CID
     * @param base    the replaced chunk
     * @param cid     the new chunk's CID
     * @param chunk   the new chunk
     * @return whether a frame was kept
     */
    public boolean record(String baseCid, byte[] base, String cid, byte[] chunk) {
        if (baseCid.equals(cid) || !recording()) {
            return false;
        }
        Optional<byte[]> frame = Dcz.encode(base, chunk);
        if (frame.isEmpty() || 2L * frame.get().length >= chunk.length) {
            return false;
        }
        Messages.NodeDelta delta = new Messages.NodeDelta(cid, baseCid, frame.get());
        synchronized (this) {
            Kept previous = frames.remove(cid + " " + baseCid);
            if (previous != null) {
                forget(previous);
            }
            Kept kept = new Kept(delta, clock.millis());
            frames.put(cid + " " + baseCid, kept);
            byCid.computeIfAbsent(cid, c -> new ArrayList<>(1)).add(kept);
            bytes += delta.dcz().length;
            lapse();
        }
        return true;
    }

    /**
     * The frames kept that rebuild a chunk, newest first.
     *
     * @param cid the chunk's CID
     * @return the frames; empty when none is kept
     */
    public synchronized List<Messages.NodeDelta> deltas(String cid) {
        lapse();
        List<Kept> kept = byCid.get(cid);
        if (kept == null) {
            return List.of();
        }
        List<Messages.NodeDelta> out = new ArrayList<>(kept.size());
        for (int i = kept.size() - 1; i >= 0; i--) {
            out.add(kept.get(i).delta);
        }
        return out;
    }

    /** The frames kept. */
    public synchronized int size() {
        lapse();
        return frames.size();
    }

    /** The bytes of the frames kept. */
    public synchronized long bytes() {
        return bytes;
    }

    /** Drops the frames past their retention and, past {@link #MAX_BYTES}, the oldest; under this table's lock. */
    private void lapse() {
        long oldest = clock.millis() - retention.toMillis();
        for (Iterator<Map.Entry<String, Kept>> it = frames.entrySet().iterator(); it.hasNext(); ) {
            Kept kept = it.next().getValue();
            if (kept.atMillis >= oldest && bytes <= MAX_BYTES) {
                break;
            }
            it.remove();
            forget(kept);
        }
    }

    private void forget(Kept kept) {
        bytes -= kept.delta.dcz().length;
        List<Kept> same = byCid.get(kept.delta.cid());
        same.remove(kept);
        if (same.isEmpty()) {
            byCid.remove(kept.delta.cid());
        }
    }
}
