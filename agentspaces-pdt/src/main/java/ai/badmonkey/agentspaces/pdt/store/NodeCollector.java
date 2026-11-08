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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Mark-and-sweep collection of a {@link NodeStore} (srad/agentspaces-pdt.md §3.4;
 * PD-S-04, PD-Q-REL-05). A cycle marks every node reachable from three sources,
 * the pinned roots of the store, the live heads a supplier reports, and the
 * retained versions of a {@link Retention}, and then sweeps every node the store
 * held when the cycle began that the mark did not reach. Nodes stored after the
 * cycle began are never candidates. The collector re-reads the mark roots each
 * time the frontier empties, before each sweep tick, and, inside a sweep tick,
 * before every removal whenever {@link NodeStore#pinGeneration()} moved since it
 * last read them; so a root pinned at any moment of a cycle, including while a
 * sweep tick runs on another thread, is marked before anything under it is removed.
 * Live heads and retained versions are re-read at the same points except the
 * per-removal check, which only the pin counter makes cheap.
 *
 * <p>A tick visits at most {@code visitsPerTick} nodes (10,000 by default), counting
 * each node read during marking and each candidate examined during sweeping, and
 * returns; the next tick resumes where the last one stopped. Call {@link #tick()}
 * from a scheduler, or {@link #collect()} to run a whole cycle at once.
 *
 * <p>Safety rests on two rules: a node is removed only when the cycle's mark did not
 * reach it, and the mark follows the links of nodes as held at the time, so a node
 * reachable from any mark root when the sweep examines it survives. The store's
 * backend handles a crash during its own compaction (see {@link PackNodeStore}),
 * so a removal here never costs a reachable node across a crash either. A node the
 * walk cannot decode is kept and its links are unknown; it is reported in the tick.
 *
 * <p>Threading: ticks run one at a time; call them from one thread or serialize them.
 * Puts and pins may proceed concurrently from other threads. A root pinned after the
 * sweep already removed part of its subtree stays incomplete; nothing brings those
 * nodes back.
 */
public final class NodeCollector {

    /** The default visit budget per tick (PD-S-04). */
    public static final int DEFAULT_VISITS_PER_TICK = 10_000;

    /** Where a cycle stands. */
    public enum Phase {
        /** Between cycles: the next tick starts a new one. */
        IDLE,
        /** Marking from the roots. */
        MARK,
        /** Sweeping unmarked candidates. */
        SWEEP
    }

    /**
     * What one tick did.
     *
     * @param phase        the phase the collector is in after the tick
     * @param visited      nodes read or examined during the tick
     * @param removed      nodes removed during the tick
     * @param bytesFreed   bytes those removals freed
     * @param undecodable  nodes the mark could not decode and kept
     * @param cycleComplete whether the tick finished a cycle
     */
    public record Tick(Phase phase, int visited, int removed, long bytesFreed, int undecodable, boolean cycleComplete) { }

    /**
     * What a whole cycle did.
     *
     * @param ticks      ticks run
     * @param marked     nodes marked
     * @param removed    nodes removed
     * @param bytesFreed bytes freed
     */
    public record Cycle(int ticks, int marked, int removed, long bytesFreed) { }

    private final NodeStore store;
    private final Supplier<Set<String>> liveHeads;
    private final Retention retention;
    private final int visitsPerTick;

    private Phase phase = Phase.IDLE;
    private Set<String> marked = new HashSet<>();
    private final Deque<String> frontier = new ArrayDeque<>();
    private final Deque<Integer> depths = new ArrayDeque<>();
    private Iterator<String> candidates;
    /** The store's pin generation when the roots were last read. */
    private long rootsGeneration = -1;
    private int cycleTicks;
    private int cycleRemoved;
    private long cycleBytesFreed;

    /**
     * A collector with the default budget.
     *
     * @param store     the store to collect
     * @param liveHeads the live heads' roots, read whenever the collector needs roots
     * @param retention the retained versions, or null when none are kept
     */
    public NodeCollector(NodeStore store, Supplier<Set<String>> liveHeads, Retention retention) {
        this(store, liveHeads, retention, DEFAULT_VISITS_PER_TICK);
    }

    /**
     * A collector.
     *
     * @param store         the store to collect
     * @param liveHeads     the live heads' roots, read whenever the collector needs roots
     * @param retention     the retained versions, or null when none are kept
     * @param visitsPerTick the visit budget per tick, at least 1
     */
    public NodeCollector(NodeStore store, Supplier<Set<String>> liveHeads, Retention retention, int visitsPerTick) {
        this.store = Objects.requireNonNull(store, "store");
        this.liveHeads = Objects.requireNonNull(liveHeads, "liveHeads");
        this.retention = retention;
        if (visitsPerTick < 1) {
            throw new IllegalArgumentException("visitsPerTick must be at least 1");
        }
        this.visitsPerTick = visitsPerTick;
    }

    /** The visit budget per tick. */
    public int visitsPerTick() {
        return visitsPerTick;
    }

    /** The phase the collector is in. */
    public Phase phase() {
        return phase;
    }

    /** The mark roots as of now: pins, live heads, and retained versions. */
    public Set<String> roots() {
        Set<String> roots = new LinkedHashSet<>(store.pinned());
        Set<String> heads = liveHeads.get();
        if (heads != null) {
            roots.addAll(heads);
        }
        if (retention != null) {
            roots.addAll(retention.retainedRoots());
        }
        return roots;
    }

    /**
     * Runs one tick of at most {@link #visitsPerTick()} visits.
     *
     * @return what the tick did
     */
    public synchronized Tick tick() {
        int budget = visitsPerTick;
        int visited = 0;
        int removed = 0;
        long freed = 0;
        int undecodable = 0;
        if (phase == Phase.IDLE) {
            begin();
        } else if (phase == Phase.SWEEP && seedRoots()) {
            phase = Phase.MARK;
        }
        cycleTicks++;
        while (visited < budget) {
            if (phase == Phase.MARK) {
                if (frontier.isEmpty()) {
                    if (!seedRoots()) {
                        phase = Phase.SWEEP;
                    }
                    continue;
                }
                String cid = frontier.poll();
                int depth = depths.poll();
                visited++;
                undecodable += mark(cid, depth);
            } else {
                if (store.pinGeneration() != rootsGeneration && seedRoots()) {
                    // A root was pinned since the roots were last read: mark from it before removing anything.
                    phase = Phase.MARK;
                    continue;
                }
                if (!candidates.hasNext()) {
                    candidates = null;
                    phase = Phase.IDLE;
                    frontier.clear();
                    depths.clear();
                    return new Tick(phase, visited, removed, freed, undecodable, true);
                }
                String cid = candidates.next();
                visited++;
                if (marked.contains(cid)) {
                    continue;
                }
                long bytes = store.backend().remove(cid);
                if (bytes >= 0) {
                    removed++;
                    freed += bytes;
                    cycleRemoved++;
                    cycleBytesFreed += bytes;
                }
            }
        }
        return new Tick(phase, visited, removed, freed, undecodable, false);
    }

    /**
     * Runs ticks until a cycle completes.
     *
     * @return what the cycle did
     */
    public Cycle collect() {
        if (phase != Phase.IDLE) {
            while (!tick().cycleComplete()) {
                // finish the cycle in progress first
            }
        }
        Tick last;
        do {
            last = tick();
        } while (!last.cycleComplete());
        return new Cycle(cycleTicks, marked.size(), cycleRemoved, cycleBytesFreed);
    }

    private void begin() {
        marked = new HashSet<>();
        frontier.clear();
        depths.clear();
        candidates = new ArrayList<>(store.backend().cids()).iterator();
        cycleTicks = 0;
        cycleRemoved = 0;
        cycleBytesFreed = 0;
        phase = Phase.MARK;
    }

    /** Queues every root not yet marked; returns whether any was. */
    private boolean seedRoots() {
        rootsGeneration = store.pinGeneration();
        boolean any = false;
        for (String root : roots()) {
            if (marked.add(root)) {
                frontier.add(root);
                depths.add(1);
                any = true;
            }
        }
        return any;
    }

    /** Reads a marked node and queues its unmarked children; returns 1 when the node could not be decoded. */
    private int mark(String cid, int depth) {
        Optional<byte[]> bytes;
        List<String> links;
        try {
            bytes = store.stored(cid);
            if (bytes.isEmpty()) {
                return 0;
            }
            links = NodeCodec.links(cid, bytes.get());
        } catch (NodeFormatException e) {
            return 1;
        }
        if (links.isEmpty() || depth >= NodeStore.MAX_DEPTH) {
            return 0;
        }
        for (String link : links) {
            if (marked.add(link)) {
                frontier.add(link);
                depths.add(depth + 1);
            }
        }
        return 0;
    }
}
