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
package ai.badmonkey.agentspaces.examples.pdt;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.reduce.Reductions;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.Peer;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.blob.BlobTree;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads;
import ai.badmonkey.agentspaces.pdt.refs.Heads;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.CODEC;
import static ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.LOAD_TIMEOUT;
import static ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.await;
import static ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.bytesNotIn;
import static ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.bytesUnder;
import static ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.fact;
import static ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.key;
import static ai.badmonkey.agentspaces.examples.pdt.PersistentValuesFleet.levelsBelowRoot;

/**
 * The worked composition of srad/agentspaces-pdt.md §7.3 as steps over a running
 * fleet, with a measure per update: the value's node bytes, the delta the writer
 * produced, the bytes each reader received with whether its delta came inline
 * on the head (PD-X-07) or by a tree fetch, the meter's round trips for the
 * same delta (PD-Q-PRF-06), and each reader's merge conflicts (§3.6.1).
 *
 * <ol>
 * <li>The curator builds the {@code facts} map (PD-Q-PRF-01 on the one-key change
 * that follows, PD-Q-PRF-05 on its pipe count) and later the {@code table} blob.</li>
 * <li>The analysts swap fact revisions concurrently in rounds; every reader, the
 * Python one included, derefs the same root after each round (PD-Q-REL-02, PD-Q-INT-01).</li>
 * <li>The curator edits one byte (PD-Q-PRF-02), appends 1 MiB (PD-Q-PRF-04), and
 * inserts 100 bytes (PD-Q-PRF-03) in the blob.</li>
 * <li>A late auditor fetches the table cold while an analyst dies (PD-Q-REL-04).</li>
 * <li>The {@code plans} space escalates a growing reduce accumulator by delta (PD-E-08).</li>
 * </ol>
 */
public final class Composition {

    /** The sizes of a run. */
    public record Sizes(int facts, int rounds, int swapsPerRound, long tableBytes, int appends, int inserts,
                        int planSteps, int planLinesPerStep) {

        /** The sizes of §7.3: 10,000 facts, 100 swaps per analyst, a 256 MiB table, ten appends and inserts, 50 steps. */
        public static Sizes full() {
            return new Sizes(10_000, 10, 10, 256L << 20, 10, 10, 50, 300);
        }

        /** The acceptance test's sizes: the same map, three rounds, a 32 MiB table, twelve steps. */
        public static Sizes quick() {
            return new Sizes(10_000, 3, 10, 32L << 20, 3, 5, 12, 300);
        }

        /** Steps 1 to 3 on a binding: two rounds, an 8 MiB table, two appends and inserts, no plans. */
        public static Sizes bindings() {
            return new Sizes(10_000, 2, 10, 8L << 20, 2, 2, 0, 0);
        }
    }

    /**
     * The running peers.
     *
     * @param curator  the writer of both refs and the reducer of step 5
     * @param analysts the readers that revise facts; the last one dies in step 4
     * @param meter    a peer without refs that fetches each delta explicitly to count round trips, or null
     * @param auditor  starts the late peer of step 4, or null to skip the step
     */
    public record Fleet(Peer curator, List<Peer> analysts, Peer meter, Callable<Peer> auditor) {
        List<Peer> all() {
            List<Peer> all = new ArrayList<>();
            all.add(curator);
            all.addAll(analysts);
            if (meter != null) {
                all.add(meter);
            }
            return all;
        }
    }

    /**
     * What one reader paid for one update: {@code bytes} is the growth of its node
     * store for a single-writer update (every node that landed, duplicates excluded)
     * and the inline bytes its ref engine accepted for a multi-writer round, where
     * the reader's own swaps also grow its store.
     */
    public record ReaderCost(String reader, long bytes, long inline, long fetched, long conflicts) {
        String how() {
            if (inline > 0 && fetched > 0) {
                return "inline+fetched";
            }
            return inline > 0 ? "inline" : fetched > 0 ? "fetched" : "held";
        }
    }

    /** The meter's explicit fetch of an update with the previous root held. */
    public record Meter(long bytes, int roundTrips, int requests, boolean complete) {
    }

    /**
     * One update.
     *
     * @param step       the step label
     * @param ref        the ref name
     * @param valueBytes the node bytes of the whole value
     * @param deltaBytes the bytes of the nodes the update created
     * @param readers    each reader's cost
     * @param meter      the meter's fetch, or null when the fleet has no meter
     * @param pipeNodes  tree nodes served or received across the fleet during the update, or -1 when not counted
     * @param note       a figure the row's bound reads against, for example the edited chunk's size
     */
    public record Update(String step, String ref, long valueBytes, long deltaBytes, List<ReaderCost> readers, Meter meter,
                         long pipeNodes, String note) {

        /** The most bytes any reader received. */
        public long maxReaderBytes() {
            return readers.stream().mapToLong(ReaderCost::bytes).max().orElse(0);
        }

        /** The fewest bytes any reader received. */
        public long minReaderBytes() {
            return readers.stream().mapToLong(ReaderCost::bytes).min().orElse(0);
        }
    }

    /** The late auditor's cold fetch of the table while an analyst dies (step 4). */
    public record ColdFetch(long blobBytes, long bytesReceived, int roundTrips, int depth, String killed, long bytesFromKilled,
                            boolean killedBeforeDone, boolean complete, Map<String, Long> bytesBySource) {
    }

    /** One step of the reduce accumulator (step 5): what the curator's seam wrote and how much each reader's store grew. */
    public record PlanStep(int step, long payloadBytes, long wrote, Map<String, Long> received) {
    }

    /** A root comparison between the Java readers and the Python reader after a round. */
    public record RootCheck(String when, int heads, String javaRoot, String pythonRoot) {
        /** Equal roots on both sides; a missing Python root is a failed check, never a pass. */
        public boolean equal() {
            return pythonRoot != null && pythonRoot.equals(javaRoot);
        }
    }

    /** The run's figures. */
    public record Report(long factsBytes, int factsDepth, List<Update> updates, Map<String, String> factsRoots,
                         List<RootCheck> rootChecks, Map<String, Long> conflicts, ColdFetch cold, List<String> plansUnable,
                         List<PlanStep> plans, String pythonNote) {

        /** The updates of one step prefix, for example {@code "3 insert"}. */
        public List<Update> updates(String prefix) {
            return updates.stream().filter(u -> u.step().startsWith(prefix)).toList();
        }

        /** One update by its exact step label. */
        public Update update(String step) {
            return updates.stream().filter(u -> u.step().equals(step)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("no update " + step));
        }

        public String summary() {
            StringBuilder out = new StringBuilder();
            out.append(String.format("facts: %d bytes of nodes, %d levels below the root%n", factsBytes, factsDepth));
            out.append(String.format("%-28s %-6s %12s %10s %10s %-16s %6s %6s %s%n", "update", "ref", "value", "delta",
                    "readers", "how", "rtrip", "pipe", "note"));
            for (Update u : updates) {
                String readers = u.readers().isEmpty() ? "-" : u.minReaderBytes() == u.maxReaderBytes()
                        ? Long.toString(u.maxReaderBytes()) : u.minReaderBytes() + ".." + u.maxReaderBytes();
                String how = u.readers().isEmpty() ? "-" : u.readers().get(0).how();
                String rtrip = u.meter() == null ? "-" : u.meter().complete() ? Integer.toString(u.meter().roundTrips()) : "fail";
                String pipe = u.pipeNodes() < 0 ? "-" : Long.toString(u.pipeNodes());
                out.append(String.format("%-28s %-6s %12d %10d %10s %-16s %6s %6s %s%n", u.step(), u.ref(), u.valueBytes(),
                        u.deltaBytes(), readers, how, rtrip, pipe, u.note() == null ? "" : u.note()));
            }
            out.append("facts roots after the final swap: ").append(factsRoots).append('\n');
            for (RootCheck check : rootChecks) {
                out.append(String.format("roots %s (%d heads): java %s python %s%s%n", check.when(), check.heads(), check.javaRoot(),
                        check.pythonRoot() == null ? "-" : check.pythonRoot(), check.equal() ? "" : " MISMATCH"));
            }
            if (pythonNote != null) {
                out.append("python reader: ").append(pythonNote).append('\n');
            }
            out.append("conflicts per reader: ").append(conflicts).append('\n');
            if (cold != null) {
                out.append(String.format("cold fetch of the table (%d bytes, depth %d) by the auditor: %d bytes received in %d round trips,"
                                + " %s killed %s, %d bytes from it, by source %s%n", cold.blobBytes(), cold.depth(), cold.bytesReceived(),
                        cold.roundTrips(), cold.killed(), cold.killedBeforeDone() ? "before the fetch finished" : "after it finished",
                        cold.bytesFromKilled(), cold.bytesBySource()));
            }
            if (!plans.isEmpty()) {
                out.append("plans: members without the tree exchange ").append(plansUnable).append('\n');
                for (PlanStep step : plans) {
                    out.append(String.format("  step %2d: accumulator %d bytes, curator wrote %d, readers received %s%n",
                            step.step(), step.payloadBytes(), step.wrote(), step.received()));
                }
            }
            return out.toString();
        }
    }

    /** A plan's accumulator: lines keyed by step and line, in canonical order so the map spills to a CHAMP sub-tree. */
    public record Plan(String plan, Map<String, String> lines, int steps) {
    }

    /** One step of a plan, the entry the reducer takes. */
    public record Step(String plan, int n) {
    }

    /** The reducer of step 5: every step adds a few hundred lines to the plan (PD-E-08). */
    @AgentSpec(name = "planner", description = "Accumulates plan lines per plan", goals = {"plan"})
    public static final class Planner {
        private final int linesPerStep;

        public Planner(int linesPerStep) {
            this.linesPerStep = linesPerStep;
        }

        @SpaceReduce(space = PersistentValuesFleet.PLANS_SPACE, key = "plan", lease = "5s", pollTimeout = "50ms",
                accumulatorLease = "1h")
        public Plan apply(Plan plan, Step step) {
            Map<String, String> lines = plan == null ? new TreeMap<>() : new TreeMap<>(plan.lines());
            Random random = new Random(step.n());
            for (int i = 0; i < linesPerStep; i++) {
                lines.put(String.format("step-%03d-line-%03d", step.n(), i), fact(i, random));
            }
            return new Plan(step.plan(), lines, (plan == null ? 0 : plan.steps()) + 1);
        }
    }

    private final Fleet fleet;
    private final Sizes sizes;
    private final Path work;
    private final PythonReader python;
    private final PrintStream out;
    private final List<Update> updates = new ArrayList<>();
    private final List<RootCheck> rootChecks = new ArrayList<>();
    private final Random random = new Random(33);
    private String pythonNote;

    private Composition(Fleet fleet, Sizes sizes, Path work, PythonReader python, PrintStream out) {
        this.fleet = fleet;
        this.sizes = sizes;
        this.work = work;
        this.python = python;
        this.out = out;
    }

    /**
     * Runs the composition.
     *
     * @param fleet  the running peers; the auditor it starts is closed here
     * @param sizes  the sizes
     * @param work   a directory for the generated table
     * @param python the Python reader, or null to run without one
     * @param out    where the per-update lines go
     * @return the figures
     * @throws Exception when a step fails
     */
    public static Report run(Fleet fleet, Sizes sizes, Path work, PythonReader python, PrintStream out) throws Exception {
        return new Composition(fleet, sizes, work, python, out).run();
    }

    private Report run() throws Exception {
        Files.createDirectories(work);
        List<Peer> all = fleet.all();
        for (Peer p : all) {
            require(PersistentValuesFleet.awaitMembers(p, all.size() - 1), p.name() + " sees the fleet");
        }
        require(PersistentValuesFleet.awaitTrees(all), "every member advertises the tree exchange");
        Peer curator = fleet.curator();

        // step 1: the facts map, then one key changed by one writer (PD-Q-PRF-01, PD-Q-PRF-05)
        Update v0 = update("1 facts v0", "facts", null, () -> curator.facts().swap(m -> {
            Random seeded = new Random(7);
            Editor<String, String> editor = m.editor();
            for (int i = 0; i < sizes.facts(); i++) {
                editor.assoc(key(i), fact(i, seeded));
            }
            return editor.done().value();
        }).root(), Peer::facts, true, false, null);
        long factsBytes = v0.valueBytes();
        int factsDepth = levelsBelowRoot(curator.store(), rootOf(curator.facts()));
        if (python != null) {
            try {
                python.start(PersistentValuesFleet.FOUNDING, "facts", PersistentValuesFleet.REF_SPACE, "content");
                pythonNote = "joined as " + python.peerId();
            } catch (Exception e) {
                pythonNote = "failed to start: " + e.getMessage();
                python.close();
            }
        }
        String root0 = rootOf(curator.facts());
        update("1 facts one key", "facts", root0, () -> curator.facts().swap(m -> m.assoc(key(1234), "changed")).root(),
                Peer::facts, true, true, null);

        // step 2: the analysts revise facts concurrently; every reader derefs the same root after each round
        List<Peer> readers = fleet.analysts();
        Set<String> previousHeads = headRoots(curator.facts());
        for (int round = 1; round <= sizes.rounds(); round++) {
            Map<String, Snap> before = snapshot(readers);
            List<Thread> writers = new ArrayList<>();
            for (int i = 0; i < readers.size(); i++) {
                Peer analyst = readers.get(i);
                long seed = 100L * round + i;
                writers.add(Thread.ofPlatform().name("swap-" + analyst.name()).start(() -> {
                    Random seeded = new Random(seed);
                    for (int s = 0; s < sizes.swapsPerRound(); s++) {
                        String revision = "rev-" + seed + "-" + s;
                        analyst.facts().swap(m -> m.assoc(key(seeded.nextInt(sizes.facts())), revision));
                    }
                }));
            }
            for (Thread writer : writers) {
                writer.join();
            }
            String label = "2 round " + round;
            require(awaitConverged(readers, Peer::facts), label + ": every reader holds the same heads");
            Set<String> heads = headRoots(curator.facts());
            String javaRoot = derefRoot(readers.get(0).facts());
            for (Peer reader : readers) {
                require(derefRoot(reader.facts()).equals(javaRoot), label + ": " + reader.name() + " derefs " + javaRoot);
            }
            rootChecks.add(new RootCheck("after round " + round, heads.size(), javaRoot, pythonRoot()));
            long valueBytes = bytesUnder(curator.store(), latestRoot(curator.facts()));
            long delta = bytesOfNewNodes(curator, heads, previousHeads);
            Meter meter = meterFetch(latestRoot(curator.facts()), previousHeads);
            record(new Update(label, "facts", valueBytes, delta, costs(before, readers, false), meter, -1,
                    heads.size() + " heads after " + readers.size() * sizes.swapsPerRound() + " swaps"));
            previousHeads = heads;
        }
        String beforeFold = latestRoot(curator.facts());
        update("2 curator folds heads", "facts", beforeFold, () -> curator.facts().swap(m -> m.assoc("curated", "done")).root(),
                Peer::facts, true, false, null);
        Map<String, String> factsRoots = new LinkedHashMap<>();
        for (Peer reader : readers) {
            factsRoots.put(reader.name(), derefRoot(reader.facts()));
        }
        factsRoots.put(curator.name(), derefRoot(curator.facts()));
        rootChecks.add(new RootCheck("after the fold", headRoots(curator.facts()).size(), derefRoot(curator.facts()), pythonRoot()));
        if (python != null) {
            python.close();
        }

        // step 3: the table blob and its edits
        Path rows = work.resolve("table.rows");
        PersistentValuesFleet.writeRows(rows, sizes.tableBytes(), 33);
        update("3 table v0", "table", null, () -> curator.table().swap(b -> PBlob.of(rows, b.store())).root(), Peer::table,
                true, false, null);
        long tableSize = sizes.tableBytes();
        long editOffset = Math.floorMod(random.nextLong(), tableSize);
        PBlob[] edited = new PBlob[1];
        update("3 edit one byte", "table", rootOf(curator.table()), () -> {
            edited[0] = curator.table().swap(b -> {
                byte old = b.read(editOffset, 1)[0];
                return b.splice(editOffset, 1, new byte[]{(byte) (old ^ 0x5a)});
            });
            return edited[0].root();
        }, Peer::table, true, false, null);
        BlobTree.Entry chunk = chunkAt(edited[0], editOffset);
        updates.set(updates.size() - 1, withNote(updates.get(updates.size() - 1),
                "edited chunk " + chunk.length() + " bytes at offset " + editOffset));
        for (int i = 1; i <= sizes.appends(); i++) {
            byte[] bytes = new byte[1 << 20];
            random.nextBytes(bytes);
            update("3 append " + i, "table", rootOf(curator.table()), () -> curator.table().swap(b -> b.append(bytes)).root(),
                    Peer::table, true, false, "1 MiB");
            tableSize += bytes.length;
        }
        for (int i = 1; i <= sizes.inserts(); i++) {
            byte[] bytes = new byte[100];
            random.nextBytes(bytes);
            long offset = Math.floorMod(random.nextLong(), tableSize);
            update("3 insert " + i, "table", rootOf(curator.table()), () -> curator.table().swap(b -> b.splice(offset, 0, bytes)).root(),
                    Peer::table, true, false, "100 bytes at " + offset);
            tableSize += bytes.length;
        }

        // step 4: a late auditor fetches the table cold while an analyst dies (PD-Q-REL-04)
        ColdFetch cold = null;
        Peer auditor = null;
        try {
            if (fleet.auditor() != null && readers.size() >= 2) {
                auditor = fleet.auditor().call();
                cold = coldFetch(auditor, readers.get(readers.size() - 1), rootOf(curator.table()), tableSize);
            }

            // step 5: the plans space escalates a growing accumulator by delta (PD-E-08)
            List<String> unable = new ArrayList<>();
            List<PlanStep> plans = new ArrayList<>();
            if (sizes.planSteps() > 0) {
                List<Peer> live = readers.stream().filter(p -> !p.closed()).toList();
                List<PeerId> view = curator.runtime().membership().allMembers().stream().map(m -> m.id()).toList();
                for (PeerId peer : curator.pdt().payloads().unable(view)) {
                    unable.add(peer.display());
                }
                GroupId groupId = GroupId.fromFounding(PersistentValuesFleet.FOUNDING.getBytes(StandardCharsets.UTF_8));
                AgentBinder binder = new AgentBinder(curator.identity(), groupId, null, InstantSource.system())
                        .space(PersistentValuesFleet.PLANS_SPACE, curator.plans());
                binder.bind(new Planner(sizes.planLinesPerStep()));
                curator.closeWith(binder);
                for (int step = 1; step <= sizes.planSteps(); step++) {
                    final int n = step;
                    PdtPayloads.Stats curatorBefore = curator.pdt().payloads().stats();
                    Map<String, Long> storeBefore = new LinkedHashMap<>();
                    Map<String, Long> fetchedBefore = new LinkedHashMap<>();
                    for (Peer reader : live) {
                        storeBefore.put(reader.name(), reader.store().bytesUsed());
                        fetchedBefore.put(reader.name(), reader.pdt().payloads().stats().fetched());
                    }
                    curator.plans().write(new Step("p1", n), Lease.of(Duration.ofHours(1)));
                    require(await(LOAD_TIMEOUT, () -> live.stream().allMatch(p -> planSteps(p) == n)),
                            "plans step " + n + ": every reader holds the accumulator");
                    Plan plan = Reductions.current(curator.plans(), Plan.class, "planner.apply", "p1").orElseThrow().value();
                    PdtPayloads.Stats curatorAfter = curator.pdt().payloads().stats();
                    long wrote = curatorAfter.bytesWritten() - curatorBefore.bytesWritten();
                    if (curatorAfter.published() > curatorBefore.published()) {
                        // the seam counts a fetch on its completion callback, after the nodes landed; give it a moment
                        await(Duration.ofSeconds(10), () -> live.stream().allMatch(p -> p.pdt().payloads().stats().fetched()
                                > fetchedBefore.get(p.name())));
                    }
                    Map<String, Long> received = new LinkedHashMap<>();
                    for (Peer reader : live) {
                        received.put(reader.name(), reader.store().bytesUsed() - storeBefore.get(reader.name()));
                    }
                    PlanStep row = new PlanStep(n, CODEC.toBytes(plan).length, wrote, received);
                    plans.add(row);
                    out.printf("plans step %2d: accumulator %d bytes, curator wrote %d bytes, readers received %s%n",
                            n, row.payloadBytes(), wrote, received);
                }
            }
            Map<String, Long> conflicts = new LinkedHashMap<>();
            for (Peer reader : readers) {
                if (!reader.closed()) {
                    conflicts.put(reader.name(), reader.pdt().stats().conflicts());
                }
            }
            conflicts.put(curator.name(), curator.pdt().stats().conflicts());
            return new Report(factsBytes, factsDepth, List.copyOf(updates), factsRoots, List.copyOf(rootChecks), conflicts, cold,
                    unable, plans, pythonNote);
        } finally {
            if (auditor != null) {
                auditor.close();
            }
        }
    }

    // ------------------------------------------------------------ one update

    /**
     * Applies one single-writer update and measures it: the readers' counters
     * before and after they hold the new root, the fleet's pipe count when asked
     * (PD-Q-PRF-05), and the meter's explicit fetch with the old root held.
     */
    private Update update(String step, String ref, String oldRoot, Supplier<String> write, Function<Peer, Ref<?>> refOf,
                          boolean meterFetch, boolean countPipe, String note) throws Exception {
        List<Peer> readers = fleet.analysts().stream().filter(p -> !p.closed()).toList();
        Map<String, Snap> before = snapshot(readers);
        long pipeBefore = countPipe ? pipeNodes() : -1;
        String newRoot = write.get();
        long valueBytes = bytesUnder(fleet.curator().store(), newRoot);
        long delta = bytesNotIn(fleet.curator().store(), newRoot, oldRoot);
        if (!await(LOAD_TIMEOUT, () -> readers.stream().allMatch(p -> holdsHead(p, refOf.apply(p), newRoot)))) {
            throw new IllegalStateException(step + ": every reader holds " + newRoot + ": did not happen in time; "
                    + diagnose(readers, refOf, newRoot));
        }
        long pipe = countPipe ? pipeNodes() - pipeBefore : -1;
        Meter meter = meterFetch ? meterFetch(newRoot, oldRoot == null ? Set.of() : Set.of(oldRoot)) : null;
        Update update = new Update(step, ref, valueBytes, delta, costs(before, readers, true), meter, pipe, note);
        record(update);
        return update;
    }

    private void record(Update u) {
        updates.add(u);
        StringBuilder readers = new StringBuilder();
        for (ReaderCost cost : u.readers()) {
            readers.append(String.format(" %s=%d(%s%s)", cost.reader(), cost.bytes(), cost.how(),
                    cost.conflicts() > 0 ? ", " + cost.conflicts() + " conflicts" : ""));
        }
        out.printf("%-28s %s: value %d bytes, delta %d bytes; readers%s; meter %s%s%s%n", u.step(), u.ref(), u.valueBytes(),
                u.deltaBytes(), readers, u.meter() == null ? "-" : u.meter().complete()
                        ? u.meter().bytes() + " bytes in " + u.meter().roundTrips() + " round trips (" + u.meter().requests() + " requests)"
                        : "FAILED", u.pipeNodes() < 0 ? "" : "; pipe nodes " + u.pipeNodes(), u.note() == null ? "" : "; " + u.note());
    }

    private static Update withNote(Update u, String note) {
        return new Update(u.step(), u.ref(), u.valueBytes(), u.deltaBytes(), u.readers(), u.meter(), u.pipeNodes(), note);
    }

    private Meter meterFetch(String root, Collection<String> haves) throws InterruptedException {
        Peer meter = fleet.meter();
        if (meter == null) {
            return null;
        }
        List<String> held = new ArrayList<>();
        for (String have : haves) {
            if (meter.store().complete(have)) {
                held.add(have);
            }
        }
        List<PeerId> prefer = new ArrayList<>();
        prefer.add(fleet.curator().id());
        for (Peer analyst : fleet.analysts()) {
            if (!analyst.closed()) {
                prefer.add(analyst.id());
            }
        }
        FetchOptions options = FetchOptions.defaults().withTimeout(LOAD_TIMEOUT).prefer(prefer.toArray(PeerId[]::new));
        TreeFetch fetch = meter.swarm().fetchTree(root, held, options);
        boolean done = fetch.await(LOAD_TIMEOUT);
        TreeFetch.Progress p = fetch.progress();
        if (!done || !fetch.complete()) {
            out.println("meter fetch of " + root + " failed: " + fetch.trace());
        }
        return new Meter(p.bytesReceived(), p.roundTrips(), p.requestsSent(), done && fetch.complete());
    }

    private ColdFetch coldFetch(Peer auditor, Peer victim, String root, long blobBytes) throws Exception {
        List<Peer> holders = new ArrayList<>();
        holders.add(fleet.curator());
        for (Peer analyst : fleet.analysts()) {
            if (!analyst.closed()) {
                holders.add(analyst);
            }
        }
        require(PersistentValuesFleet.awaitMembers(auditor, holders.size()), "the auditor sees the holders");
        require(await(Duration.ofSeconds(30), () -> holders.stream().allMatch(h -> auditor.content().advertisesTrees(h.id())
                && h.content().advertisesTrees(auditor.id()))), "the auditor and the holders advertise the tree exchange");
        int depth = levelsBelowRoot(fleet.curator().store(), root);
        FetchOptions options = FetchOptions.defaults().withTimeout(LOAD_TIMEOUT)
                .prefer(holders.stream().map(Peer::id).toArray(PeerId[]::new));
        TreeFetch fetch = auditor.swarm().fetchTree(root, List.of(), options);
        // the victim dies once it has served part of the tree, or once a tenth of the blob arrived from anyone
        long deadline = System.nanoTime() + LOAD_TIMEOUT.toNanos();
        while (!fetch.isDoneOrFailed() && System.nanoTime() < deadline
                && fetch.progress().bytesBySource().getOrDefault(victim.id(), 0L) == 0
                && fetch.progress().bytesReceived() < blobBytes / 10) {
            Thread.sleep(1);
        }
        boolean killedBeforeDone = !fetch.isDoneOrFailed();
        long fromVictimAtKill = fetch.progress().bytesBySource().getOrDefault(victim.id(), 0L);
        out.printf("step 4: %s killed with %d bytes served to the auditor, fetch %s%n", victim.name(), fromVictimAtKill,
                killedBeforeDone ? "in progress" : "already finished");
        victim.close();
        boolean done = fetch.await(LOAD_TIMEOUT);
        TreeFetch.Progress p = fetch.progress();
        boolean complete = done && fetch.complete() && auditor.store().complete(root);
        if (complete) {
            PBlob onAuditor = PBlob.load(auditor.store(), root);
            require(onAuditor.size() == blobBytes, "the auditor's table has " + blobBytes + " bytes");
            PBlob onCurator = PBlob.load(fleet.curator().store(), root);
            long sample = Math.floorMod(random.nextLong(), Math.max(1, blobBytes - 4096));
            require(java.util.Arrays.equals(onAuditor.read(sample, 4096), onCurator.read(sample, 4096)),
                    "the auditor's table matches the curator's at " + sample);
        } else {
            out.println("step 4: the cold fetch did not complete: " + fetch.trace());
        }
        Map<String, Long> bySource = new TreeMap<>();
        for (Map.Entry<PeerId, Long> e : p.bytesBySource().entrySet()) {
            String name = holders.stream().filter(h -> h.id().equals(e.getKey())).map(Peer::name).findFirst()
                    .orElse(e.getKey().display());
            bySource.put(name, e.getValue());
        }
        ColdFetch cold = new ColdFetch(blobBytes, p.bytesReceived(), p.roundTrips(), depth, victim.name(),
                p.bytesBySource().getOrDefault(victim.id(), 0L), killedBeforeDone, complete, bySource);
        out.printf("step 4: cold fetch of %d bytes (depth %d) %s with %d bytes received in %d round trips; by source %s%n",
                blobBytes, depth, complete ? "complete" : "INCOMPLETE", p.bytesReceived(), p.roundTrips(), bySource);
        return cold;
    }

    // ------------------------------------------------------------ readings

    /** A reader's counters and store size at one moment. */
    private record Snap(Pdt.Stats stats, long bytesUsed) {
        static Snap of(Peer reader) {
            return new Snap(reader.pdt().stats(), reader.store().bytesUsed());
        }

        long deltas() {
            return stats.deltasInline() + stats.deltasFetched();
        }
    }

    private static Map<String, Snap> snapshot(List<Peer> readers) {
        Map<String, Snap> before = new LinkedHashMap<>();
        for (Peer reader : readers) {
            before.put(reader.name(), Snap.of(reader));
        }
        return before;
    }

    /**
     * The readers' costs since a snapshot. The ref engine counts a fetched delta on
     * its tick after the nodes landed, so a reader whose counters have not moved
     * yet is given a moment before its inline or fetched flag is read; the bytes of
     * a single-writer update come from the store's growth, which is exact as soon
     * as the root is complete.
     */
    private static List<ReaderCost> costs(Map<String, Snap> before, List<Peer> readers, boolean byStoreGrowth)
            throws InterruptedException {
        if (byStoreGrowth) {
            await(Duration.ofSeconds(10), () -> readers.stream().allMatch(r -> Snap.of(r).deltas() > before.get(r.name()).deltas()));
        }
        List<ReaderCost> costs = new ArrayList<>();
        for (Peer reader : readers) {
            Snap was = before.get(reader.name());
            Snap now = Snap.of(reader);
            long bytes = byStoreGrowth ? now.bytesUsed() - was.bytesUsed() : now.stats().bytesReceived() - was.stats().bytesReceived();
            costs.add(new ReaderCost(reader.name(), bytes, now.stats().deltasInline() - was.stats().deltasInline(),
                    now.stats().deltasFetched() - was.stats().deltasFetched(), now.stats().conflicts() - was.stats().conflicts()));
        }
        return costs;
    }

    /** Tree nodes served plus received across every peer of the fleet: zero for an inline delta (PD-Q-PRF-05). */
    private long pipeNodes() {
        long total = 0;
        for (Peer p : fleet.all()) {
            if (!p.closed()) {
                Content.Stats stats = p.content().stats();
                total += stats.nodesServed() + stats.nodesReceived();
            }
        }
        return total;
    }

    private String diagnose(List<Peer> readers, Function<Peer, Ref<?>> refOf, String root) {
        StringBuilder out = new StringBuilder();
        out.append("curator ").append(fleet.curator().pdt().stats()).append(' ').append(fleet.curator().content().stats())
                .append(" heads ").append(refOf.apply(fleet.curator()).heads().roots()).append(" treeRoots ")
                .append(fleet.curator().content().treeRoots());
        for (Peer reader : readers) {
            out.append("; ").append(reader.name()).append(" complete=").append(reader.store().complete(root))
                    .append(" heads=").append(refOf.apply(reader).heads().roots()).append(' ').append(reader.pdt().stats())
                    .append(' ').append(reader.content().stats()).append(" inFlight=")
                    .append(reader.swarm().fetcher().inFlight().keySet()).append(" advertisesCurator=")
                    .append(reader.content().advertisesTrees(fleet.curator().id())).append(" entries=")
                    .append(reader.refSpace().readAllEntries(ai.badmonkey.agentspaces.api.space.Template.of(
                            ai.badmonkey.agentspaces.agent.pdt.PRef.class), 16).size());
            for (TreeFetch fetch : reader.swarm().fetcher().inFlight().values()) {
                out.append(" trace=").append(fetch.trace());
            }
        }
        return out.toString();
    }

    private static boolean holdsHead(Peer reader, Ref<?> ref, String root) {
        return reader.store().complete(root) && ref.heads().roots().contains(root);
    }

    private static String rootOf(Ref<?> ref) {
        return ref.heads().latest().root();
    }

    private static String latestRoot(Ref<?> ref) {
        Heads heads = ref.heads();
        Heads.Head latest = heads.latest();
        return latest.root();
    }

    private static Set<String> headRoots(Ref<?> ref) {
        return new HashSet<>(ref.heads().roots());
    }

    private static String derefRoot(Ref<PMap<String, String>> ref) {
        return ref.deref().root();
    }

    private static int planSteps(Peer reader) {
        return Reductions.current(reader.plans(), Plan.class, "planner.apply", "p1").map(e -> e.value().steps()).orElse(0);
    }

    /** Every reader holds the same head roots, all complete, and the view stays for half a second. */
    private static boolean awaitConverged(List<Peer> readers, Function<Peer, Ref<?>> refOf) throws InterruptedException {
        long deadline = System.nanoTime() + LOAD_TIMEOUT.toNanos();
        Set<String> stable = null;
        long stableSince = 0;
        while (System.nanoTime() < deadline) {
            Set<String> first = null;
            boolean same = true;
            for (Peer reader : readers) {
                Set<String> roots = headRoots(refOf.apply(reader));
                if (roots.isEmpty() || !roots.stream().allMatch(r -> reader.store().complete(r))) {
                    same = false;
                    break;
                }
                if (first == null) {
                    first = roots;
                } else if (!first.equals(roots)) {
                    same = false;
                    break;
                }
            }
            if (same) {
                if (first.equals(stable)) {
                    if (System.nanoTime() - stableSince > Duration.ofMillis(500).toNanos()) {
                        return true;
                    }
                } else {
                    stable = first;
                    stableSince = System.nanoTime();
                }
            } else {
                stable = null;
            }
            Thread.sleep(20);
        }
        return false;
    }

    /** The bytes of the nodes under the current heads that are under none of the previous heads. */
    private static long bytesOfNewNodes(Peer peer, Set<String> heads, Set<String> previous) {
        Set<String> old = new HashSet<>();
        for (String root : previous) {
            if (peer.store().complete(root)) {
                old.addAll(peer.store().reachable(root));
            }
        }
        Set<String> fresh = new HashSet<>();
        for (String root : heads) {
            for (String cid : peer.store().reachable(root)) {
                if (!old.contains(cid)) {
                    fresh.add(cid);
                }
            }
        }
        long total = 0;
        for (String cid : fresh) {
            total += peer.store().get(cid).orElseThrow().length;
        }
        return total;
    }

    private String pythonRoot() {
        if (python == null || !python.running()) {
            return null;
        }
        try {
            return python.derefRoot();
        } catch (Exception e) {
            pythonNote = "deref failed: " + e.getMessage();
            return null;
        }
    }

    private static BlobTree.Entry chunkAt(PBlob blob, long offset) {
        long start = 0;
        for (BlobTree.Entry entry : blob.chunks()) {
            if (offset < start + entry.length()) {
                return entry;
            }
            start += entry.length();
        }
        throw new IllegalArgumentException("offset " + offset + " past the end");
    }

    private static void require(boolean condition, String what) {
        if (!condition) {
            throw new IllegalStateException(what + ": did not happen in time");
        }
    }
}
