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
package ai.badmonkey.agentspaces.pdt.escalation;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.spi.BulkPayloads;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import ai.badmonkey.agentspaces.pdt.store.Retention;
import ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;
import ai.badmonkey.agentspaces.pdt.values.PayloadTree;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The persistent trees as a space's bulk-payload seam (srad/agentspaces-pdt.md
 * §3.7, §6.4 S6, group E). A space that opts in through
 * {@code ReplicatedSpace.Builder.bulk(pdt.payloads(), escalateAbove)} hands a
 * payload above its threshold here:
 *
 * <ul>
 *   <li>{@link #publish} spills the payload's CBOR into a tree by the rules of
 *       PD-E-02 ({@link PayloadTree}: maps and arrays above 4 KiB become sub-trees,
 *       byte strings above 4 KiB become blobs, the rest stays in an {@code x} node),
 *       pins the root for the entry's life (PD-E-05), and returns the root as the
 *       reference, a dag-cbor CID;</li>
 *   <li>{@link #local} reassembles the bytes the space gave {@link #publish}, byte
 *       for byte (PD-E-03), once every node is held here;</li>
 *   <li>{@link #ensure} starts one tree fetch for the root, preferring the entry's
 *       issuer and naming as {@code haves} the roots of this seam's live payloads
 *       held here, so a reader fetches only the delta against the versions it holds
 *       (PD-E-01), and runs the callback when the tree is complete, or the failure
 *       callback when the fetch gives up past the deadline, so the space retries it
 *       with the back-off of its block path (PD-Q-AVL-01);</li>
 *   <li>{@link #release} unpins a root once the space collected its entry (PD-E-05).</li>
 * </ul>
 *
 * <p>A reference published here is told from a flat content-swarm artifact by its
 * codec: {@link #accepts} takes dag-cbor roots only, so a space with this seam and a
 * flat fallback routes each reference to the seam that published it. Before a
 * write, the space asks {@link #unable} which members lack the content swarm's
 * {@code trees} parameter and falls back when any does (PD-E-06).
 *
 * <p>Nodes travel sealed per message in {@code group-key} mode and the space does
 * not pre-seal an escalated payload (PD-E-04); the record signature covers the
 * root, which names the whole tree.
 */
public final class PdtPayloads implements BulkPayloads {

    /** The retention name under which published and fetched roots are kept as delta bases. */
    public static final String RETENTION_NAME = "space-payloads";

    /** The most complete roots offered as {@code haves} on a fetch. */
    static final int MAX_HAVES = 8;

    /** The most recent roots examined for {@code haves}, newest first. */
    static final int HAVE_CANDIDATES = 16;

    /** The most recent roots remembered: published, asked for, or read here. */
    static final int RECENT_ROOTS = 64;

    /**
     * The seam's counters.
     *
     * @param published      payloads published here
     * @param bytesWritten   node bytes the store lacked across every publish (the deltas written)
     * @param fetched        tree fetches that completed
     * @param bytesReceived  node bytes those fetches received
     * @param pinned         roots this seam holds pinned
     * @param unusable       writes the space routed elsewhere because a member lacked the tree exchange (PD-E-06)
     * @param failed         tree fetches that gave up and were reported to the space for a retry (PD-Q-AVL-01)
     */
    public record Stats(long published, long bytesWritten, long fetched, long bytesReceived, int pinned, long unusable,
                        long failed) {
    }

    private final NodeStore store;
    private final PdtSwarm swarm;
    private final Retention retention;
    private final InstantSource clock;
    private final Duration deadline;
    private final Map<String, List<Runnable>> waiting = new ConcurrentHashMap<>();
    /** Roots pinned through this seam, with how many pins each holds. */
    private final Map<String, Integer> pins = new ConcurrentHashMap<>();
    /** The most recent roots seen here (published, asked for, read), oldest first; bounded by {@link #RECENT_ROOTS}. */
    private final Set<String> recent = new LinkedHashSet<>();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong bytesWritten = new AtomicLong();
    private final AtomicLong fetched = new AtomicLong();
    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicLong unusable = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    /**
     * A seam over a facade's parts.
     *
     * @param store     the node store
     * @param swarm     the attached swarm
     * @param retention the facade's retention, which keeps released roots as delta bases for a while
     * @param clock     the clock retention reads
     * @param deadline  how long a fetch waits for a source before it fails (PD-X-08)
     */
    public PdtPayloads(NodeStore store, PdtSwarm swarm, Retention retention, InstantSource clock, Duration deadline) {
        this.store = Objects.requireNonNull(store, "store");
        this.swarm = Objects.requireNonNull(swarm, "swarm");
        this.retention = Objects.requireNonNull(retention, "retention");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
    }

    @Override
    public String publish(byte[] payload, Lease lease) {
        Objects.requireNonNull(payload, "payload");
        PayloadTree.Written written = PayloadTree.write(payload, store);
        String root = written.root();
        pin(root);
        remember(root);
        published.incrementAndGet();
        bytesWritten.addAndGet(written.bytes());
        return root;
    }

    @Override
    public Optional<byte[]> local(String ref) {
        if (!accepts(ref)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = PayloadTree.read(ref, store, NodeLoader.of(store));
            remember(ref);
            return Optional.of(bytes);
        } catch (NodeUnavailableException e) {
            return Optional.empty();
        } catch (NodeFormatException e) {
            return Optional.empty();
        }
    }

    @Override
    public void ensure(String ref, PeerId issuer, Runnable onArrival) {
        ensure(ref, issuer, onArrival, () -> { });
    }

    @Override
    public void ensure(String ref, PeerId issuer, Runnable onArrival, Runnable onFailure) {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(onArrival, "onArrival");
        Objects.requireNonNull(onFailure, "onFailure");
        if (!accepts(ref)) {
            throw new IllegalArgumentException("not a payload root (a dag-cbor CID): " + ref);
        }
        waiting.computeIfAbsent(ref, r -> new CopyOnWriteArrayList<>()).add(onArrival);
        List<String> haves = haves(ref);
        remember(ref);
        FetchOptions options = FetchOptions.defaults().withTimeout(deadline);
        if (issuer != null) {
            options = options.prefer(issuer);
        }
        TreeFetch fetch = swarm.fetchTree(ref, haves, options);
        if (fetch.complete()) {
            arrived(ref, fetch);
            return;
        }
        swarm.content().onFetchDone(ref, () -> {
            if (fetch.complete()) {
                arrived(ref, fetch);
            } else if (waiting.remove(ref) != null) {
                // the fetch gave up (no source past the deadline): the space retries it later
                failed.incrementAndGet();
                onFailure.run();
            }
        });
    }

    @Override
    public void release(String ref) {
        if (ref == null) {
            return;
        }
        // a release before the tree arrived: the arrival pins nothing and runs no callback
        waiting.remove(ref);
        boolean[] last = {false};
        pins.computeIfPresent(ref, (r, n) -> {
            if (n <= 1) {
                last[0] = true;
                return null;
            }
            return n - 1;
        });
        if (last[0]) {
            // the last pin: the swarm withdraws the holding, and the collector may take the tree once retention lapses
            swarm.release(ref);
        }
    }

    @Override
    public boolean accepts(String ref) {
        if (!BulkPayloads.isBulk(ref)) {
            return false;
        }
        try {
            return Cids.parse(ref).codec() == Cids.DAG_CBOR;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public List<PeerId> unable(Collection<PeerId> members) {
        List<PeerId> lacking = new ArrayList<>();
        for (PeerId member : members) {
            if (!swarm.content().advertisesTrees(member)) {
                lacking.add(member);
            }
        }
        if (!lacking.isEmpty()) {
            unusable.incrementAndGet();
        }
        return lacking;
    }

    /** The counters. */
    public Stats stats() {
        return new Stats(published.get(), bytesWritten.get(), fetched.get(), bytesReceived.get(), pins.size(),
                unusable.get(), failed.get());
    }

    /** The roots this seam holds pinned. */
    public Set<String> pinned() {
        return Set.copyOf(pins.keySet());
    }

    /**
     * The roots a fetch names as {@code haves}: among the most recent roots seen here
     * (newest first, at most {@link #HAVE_CANDIDATES} examined), the ones the store holds
     * complete, at most {@link #MAX_HAVES}, other than the one asked for. A root whose
     * fetch is still running is incomplete and left out, so a burst of versions names the
     * last complete one and the holder prunes everything under it.
     */
    List<String> haves(String except) {
        List<String> newestFirst;
        synchronized (recent) {
            newestFirst = new ArrayList<>(recent);
        }
        java.util.Collections.reverse(newestFirst);
        List<String> out = new ArrayList<>();
        int examined = 0;
        for (String root : newestFirst) {
            if (out.size() >= MAX_HAVES || examined >= HAVE_CANDIDATES) {
                break;
            }
            if (root.equals(except)) {
                continue;
            }
            examined++;
            try {
                if (store.complete(root)) {
                    out.add(root);
                }
            } catch (NodeFormatException e) {
                // a malformed tree is no base
            }
        }
        return out;
    }

    private void pin(String root) {
        pins.merge(root, 1, Integer::sum);
        swarm.hold(root);
        retention.recordVersion(RETENTION_NAME, root, clock.millis());
    }

    private void remember(String root) {
        synchronized (recent) {
            recent.remove(root);
            recent.add(root);
            while (recent.size() > RECENT_ROOTS) {
                String eldest = recent.iterator().next();
                recent.remove(eldest);
            }
        }
    }

    private void arrived(String ref, TreeFetch fetch) {
        List<Runnable> callbacks = waiting.remove(ref);
        if (callbacks == null) {
            return;
        }
        fetched.incrementAndGet();
        bytesReceived.addAndGet(fetch.progress().bytesReceived());
        pin(ref);
        remember(ref);
        callbacks.forEach(Runnable::run);
    }

    @Override
    public String toString() {
        return "PdtPayloads(" + stats() + ")";
    }
}
