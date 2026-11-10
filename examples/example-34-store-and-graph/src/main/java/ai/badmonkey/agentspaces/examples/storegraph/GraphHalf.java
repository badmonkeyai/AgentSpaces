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
package ai.badmonkey.agentspaces.examples.storegraph;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.examples.storegraph.StoreGraphFleet.Peer;
import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.values.PDb;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

/**
 * The graph half of example 34 (srad/agentspaces-pdt2.md §7.3, P2-O-03; group D): the superfriends graph peer
 * writes its database on the {@code db} ref {@value #REF}, two analysts add findings about {@value SuperfriendsGraph#ACME}
 * at the same time, and the ref's linear mode makes them take turns, each swap over the head the one before it
 * wrote. A Python reader then joins over tcp, follows the ref space lazily, and answers the lookup of
 * {@code [:entity/key "company:Acme"]} and the findings about it from the few index nodes those reads walk.
 */
public final class GraphHalf {

    /** The graph group's founding text. */
    public static final String FOUNDING = "store-and-graph-example-graph-v1";

    /** The ref the database lives on, as in the superfriends graph peer (P2-D-07). */
    public static final String REF = "graph";

    private static final Duration CONVERGE = Duration.ofSeconds(60);

    /**
     * The sizes of a run.
     *
     * @param graph              the graph's entities
     * @param findingsPerAnalyst the findings each analyst adds, one swap each
     */
    public record Sizes(SuperfriendsGraph.Sizes graph, int findingsPerAnalyst) {

        /** The demo: 300 companies with tickers, 300 people, 900 findings, five findings per analyst. */
        public static Sizes full() {
            return new Sizes(new SuperfriendsGraph.Sizes(300, 300, 900), 5);
        }

        /** The acceptance test: 120 companies, 100 people, 400 findings, three findings per analyst. */
        public static Sizes quick() {
            return new Sizes(new SuperfriendsGraph.Sizes(120, 100, 400), 3);
        }
    }

    /**
     * One analyst's swap.
     *
     * @param analyst    who swapped
     * @param entity     the finding's entity id
     * @param base       the root the swap took
     * @param root       the root it wrote
     * @param deltaBytes the bytes of the nodes the swap added: the new root and the tail
     */
    public record Turn(String analyst, long entity, String base, String root, long deltaBytes) {
    }

    /**
     * The Python reader's answers.
     *
     * @param root  the root it derefs
     * @param e     the entity of {@code [:entity/key "company:Acme"]}
     * @param name  that entity's {@code :entity/name}
     * @param about the findings about it, from AVET and the tail
     * @param held  the nodes in its store after the reads: the nodes the reads walked and the heads' inline nodes
     */
    public record Lookup(String root, Long e, String name, List<Long> about, int held) {
    }

    /**
     * The run's figures.
     *
     * @param datoms     the graph's datoms in the first version
     * @param dbNodes    the nodes of the final database
     * @param dbBytes    their bytes
     * @param v0         the first version's root
     * @param turns      the analysts' swaps, in the order they wrote
     * @param finalRoot  the root after the last swap
     * @param javaRoots  the root each Java peer derefs at the end
     * @param acme       the entity of Acme in the graph
     * @param aboutAcme  the findings about Acme in the final database
     * @param python     the Python reader's answers, or null when it did not run
     * @param pythonNote how the Python reader ran
     */
    public record Report(long datoms, int dbNodes, long dbBytes, String v0, List<Turn> turns, String finalRoot,
                         Map<String, String> javaRoots, long acme, List<Long> aboutAcme, Lookup python, String pythonNote) {

        public String summary() {
            StringBuilder out = new StringBuilder();
            out.append(String.format("graph: %d datoms, %d dn nodes of %d bytes in the final version; v0 %s%n", datoms, dbNodes,
                    dbBytes, v0));
            for (Turn t : turns) {
                out.append(String.format("  %-10s finding %d over %s: delta %d bytes%n", t.analyst(), t.entity(), shortCid(t.base()),
                        t.deltaBytes()));
            }
            out.append(String.format("final root %s; Java roots %s%n", finalRoot, javaRoots));
            if (python != null) {
                out.append(String.format("python: root %s, lookup %s = %d (%s), %d findings about it, %d of %d nodes held%n",
                        python.root(), SuperfriendsGraph.ACME_KEY, python.e(), python.name(), python.about().size(), python.held(),
                        dbNodes));
            }
            out.append("python reader: ").append(pythonNote).append('\n');
            return out.toString();
        }
    }

    private GraphHalf() {
    }

    static String shortCid(String cid) {
        return cid == null ? "-" : cid.substring(Math.max(0, cid.length() - 8));
    }

    /**
     * Runs the graph half over loopback tcp.
     *
     * @param dir    a directory for the peers' stores
     * @param ports  a fresh tcp port per call
     * @param sizes  the sizes
     * @param python whether to run the Python reader
     * @param out    where the per-step lines go
     * @return the figures
     * @throws Exception when a step fails
     */
    public static Report run(Path dir, IntSupplier ports, Sizes sizes, boolean python, PrintStream out) throws Exception {
        int graphPort = ports.getAsInt();
        String seed = "127.0.0.1:" + graphPort;
        // the Python reader dials the graph peer alone, so the graph peer relays its frames to the analysts
        try (Peer graph = StoreGraphFleet.start("graph", StoreGraphFleet.Spec.member(FOUNDING, graphPort, List.of(), dir.resolve("graph"))
                .roles(Set.of(PeerAdvertisement.PeerRole.RELAY)));
             Peer a1 = StoreGraphFleet.start("analyst-1", StoreGraphFleet.Spec.member(FOUNDING, ports.getAsInt(), List.of(seed),
                     dir.resolve("analyst-1")));
             Peer a2 = StoreGraphFleet.start("analyst-2", StoreGraphFleet.Spec.member(FOUNDING, ports.getAsInt(), List.of(seed),
                     dir.resolve("analyst-2")))) {
            List<Peer> all = List.of(graph, a1, a2);
            require(StoreGraphFleet.awaitFleet(all, CONVERGE), "the graph group forms");

            // the graph peer folds the whole graph into the db ref's first version
            SuperfriendsGraph.Built built = SuperfriendsGraph.datoms(sizes.graph(), 34);
            Ref<PDb> onGraph = graph.pdt().dbRef(REF, null);
            PDb v0 = onGraph.swap(db -> SuperfriendsGraph.fold(db, built));
            out.printf("graph: v0 %s, %d datoms%n", v0.root(), built.datoms().size());
            List<Ref<PDb>> analysts = List.of(a1.pdt().dbRef(REF, null), a2.pdt().dbRef(REF, null));
            for (Ref<PDb> ref : analysts) {
                require(StoreGraphFleet.await(CONVERGE, () -> ref.heads().roots().equals(List.of(v0.root()))),
                        "every analyst hears v0");
            }

            // two analysts add findings at once; linear mode serializes the swaps (PD-R-04)
            long acme = built.entities().get(SuperfriendsGraph.ACME_KEY);
            long acmeTicker = built.entities().get("ticker:" + SuperfriendsGraph.ticker(0));
            List<Turn> turns = Collections.synchronizedList(new ArrayList<>());
            List<Thread> writers = new ArrayList<>();
            AtomicReference<Throwable> failed = new AtomicReference<>();
            for (int i = 0; i < analysts.size(); i++) {
                Peer analyst = List.of(a1, a2).get(i);
                Ref<PDb> ref = analysts.get(i);
                writers.add(Thread.ofPlatform().name("swap-" + analyst.name()).start(() -> {
                    try {
                        for (int k = 0; k < sizes.findingsPerAnalyst(); k++) {
                            String inquiry = "inq-" + analyst.name() + "-" + k;
                            String note = analyst.name() + " notes item " + k + " about " + SuperfriendsGraph.ACME;
                            String[] base = new String[1];
                            long[] entity = new long[1];
                            PDb next = ref.swap(db -> {
                                base[0] = db.root();
                                return SuperfriendsGraph.transact(db, (e, tx) -> {
                                    entity[0] = e;
                                    return SuperfriendsGraph.finding(e, acme, acmeTicker, SuperfriendsGraph.ticker(0), inquiry,
                                            "news", note, 0.9, tx);
                                });
                            });
                            long delta = bytesNotIn(analyst.store(), next.root(), base[0]);
                            Turn turn = new Turn(analyst.name(), entity[0], base[0], next.root(), delta);
                            turns.add(turn);
                            out.printf("  %s wrote finding %d over %s: %d bytes%n", analyst.name(), entity[0], shortCid(base[0]), delta);
                        }
                    } catch (Throwable t) {
                        failed.compareAndSet(null, t);
                    }
                }));
            }
            for (Thread writer : writers) {
                writer.join();
            }
            if (failed.get() != null) {
                throw new IllegalStateException("an analyst's swap failed", failed.get());
            }
            List<Turn> ordered = chainOrder(turns, v0.root());
            String finalRoot = ordered.get(ordered.size() - 1).root();
            for (Peer p : all) {
                Ref<PDb> ref = p.pdt().dbRef(REF, null);
                require(StoreGraphFleet.await(CONVERGE, () -> ref.heads().roots().equals(List.of(finalRoot))),
                        p.name() + " hears the last swap");
            }
            Map<String, String> javaRoots = new LinkedHashMap<>();
            for (Peer p : all) {
                javaRoots.put(p.name(), p.pdt().dbRef(REF, null).deref().root());
            }
            PDb last = onGraph.deref();
            List<Long> aboutAcme = about(built, last, acme);
            int dbNodes = graph.store().reachable(finalRoot).size();
            long dbBytes = bytesNotIn(graph.store(), finalRoot, null);

            Lookup lookup = null;
            String note;
            if (!python) {
                note = "not asked";
            } else if (!PythonDbReader.available()) {
                note = "skipped (" + PythonDbReader.whyUnavailable() + ")";
            } else {
                try (PythonDbReader reader = new PythonDbReader("127.0.0.1", graphPort)) {
                    reader.start(FOUNDING, REF);
                    JsonNode at = reader.await(finalRoot, CONVERGE);
                    JsonNode found = reader.lookup(":entity/key", SuperfriendsGraph.ACME_KEY);
                    Long e = found.path("e").isNull() ? null : found.path("e").asLong();
                    JsonNode findings = reader.datoms(DnNode.AVET, ":finding/about", e == null ? -1 : e);
                    List<Long> ids = new ArrayList<>();
                    for (JsonNode d : findings.path("datoms")) {
                        ids.add(d.get(0).asLong());
                    }
                    lookup = new Lookup(at.path("root").asText(), e, found.path("entity").path(":entity/name").asText(null),
                            ids, findings.path("held").asInt());
                    note = "read " + at.path("heads").asInt() + " head(s) with " + at.path("held").asInt()
                            + " nodes held before the reads";
                }
            }
            return new Report(built.datoms().size(), dbNodes, dbBytes, v0.root(), ordered, finalRoot, javaRoots, acme, aboutAcme,
                    lookup, note);
        }
    }

    /** The turns from v0 forward, each the swap over the root the one before it wrote. */
    static List<Turn> chainOrder(List<Turn> turns, String v0) {
        Map<String, Turn> byBase = new LinkedHashMap<>();
        for (Turn t : turns) {
            if (byBase.put(t.base(), t) != null) {
                throw new IllegalStateException("two swaps over one base " + t.base() + ": " + turns);
            }
        }
        List<Turn> out = new ArrayList<>();
        String at = v0;
        while (byBase.containsKey(at)) {
            Turn t = byBase.get(at);
            out.add(t);
            at = t.root();
        }
        if (out.size() != turns.size()) {
            throw new IllegalStateException("the swaps form no single chain from v0: " + turns);
        }
        return out;
    }

    /** The findings about an entity: the folded datoms of the first version and the tail of the last, ascending. */
    static List<Long> about(SuperfriendsGraph.Built built, PDb last, long entity) {
        Set<Long> out = new TreeSet<>();
        for (Datom d : built.datoms()) {
            if (d.a().equals(":finding/about") && Long.valueOf(entity).equals(d.v())) {
                out.add(d.e());
            }
        }
        for (Datom d : last.tail()) {
            if (d.a().equals(":finding/about") && Long.valueOf(entity).equals(d.v())) {
                out.add(d.e());
            }
        }
        return List.copyOf(out);
    }

    /** The bytes of the nodes under a root that are not under a base. */
    static long bytesNotIn(NodeStore store, String root, String base) {
        Set<String> shared = base == null ? Set.of() : store.reachable(base);
        long total = 0;
        for (String cid : store.reachable(root)) {
            if (!shared.contains(cid)) {
                total += store.stored(cid).map(b -> (long) b.length).orElse(0L);
            }
        }
        return total;
    }

    static void require(boolean condition, String what) {
        if (!condition) {
            throw new IllegalStateException("example 34: " + what + " did not happen in time");
        }
    }
}
