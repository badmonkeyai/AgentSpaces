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

import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.kinds.DbRoot;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;
import ai.badmonkey.agentspaces.pdt.values.DatomOrder;
import ai.badmonkey.agentspaces.pdt.values.PDb;
import ai.badmonkey.agentspaces.pdt.values.PSortedMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * The superfriends graph as a DataScript database on a {@code db} ref (P2-D-07; the graph peer of
 * {@code agentspaces-example-apps/superfriends/graph}): entities keyed by {@code :entity/key "kind:name"}, the
 * company to ticker and person to company relations, and findings linked to every entity they are about, under
 * the identities, refs, and indexed attributes of that peer's {@code resources/schema.edn}.
 *
 * <p>The build folds every datom into the three indexes ({@link PSortedMap#of}, branching factor 128), as a
 * DataScript fold does, and a transaction appends its datoms to the tail, which rides inline on the head
 * (§3.1.3). The first company is {@value #ACME}, the entity the readers look up.
 */
public final class SuperfriendsGraph {

    /** The company the readers look up. */
    public static final String ACME = "Acme";

    /** The lookup key of {@value #ACME}. */
    public static final String ACME_KEY = "company:" + ACME;

    private static final List<String> NAMES = List.of(ACME, "Bolt", "Cobalt", "Delta", "Ember", "Flint", "Garnet", "Harbor",
            "Iris", "Juniper", "Kestrel", "Lumen", "Maple", "Nimbus", "Onyx", "Pioneer", "Quarry", "Raven", "Summit",
            "Tundra", "Umber", "Vertex", "Willow", "Zephyr");
    private static final List<String> SERVICES = List.of("news", "filings", "prices");

    /** The subset of the superfriends schema the graph uses, attribute to its properties. */
    public static Map<String, Object> schema() {
        Map<String, Object> out = new TreeMap<>();
        out.put(":entity/key", Map.of(":db/unique", ":db.unique/identity"));
        out.put(":relation/key", Map.of(":db/unique", ":db.unique/identity"));
        out.put(":relation/subject", Map.of(":db/valueType", ":db.type/ref"));
        out.put(":relation/object", Map.of(":db/valueType", ":db.type/ref"));
        out.put(":relation/predicate", Map.of(":db/index", true));
        out.put(":finding/about", Map.of(":db/valueType", ":db.type/ref", ":db/cardinality", ":db.cardinality/many"));
        out.put(":finding/inquiry", Map.of(":db/index", true));
        out.put(":finding/service", Map.of(":db/index", true));
        out.put(":finding/ticker", Map.of(":db/index", true));
        return out;
    }

    /** The attributes DataScript puts in AVET: unique, ref, and {@code :db/index true}. */
    static final Set<String> INDEXED = Set.of(":entity/key", ":relation/key", ":relation/subject", ":relation/object",
            ":relation/predicate", ":finding/about", ":finding/inquiry", ":finding/service", ":finding/ticker");

    /**
     * The sizes of a graph.
     *
     * @param companies companies, each with a ticker and a company to ticker relation
     * @param people    people, each with a person to company relation
     * @param findings  findings, each about one company and its ticker
     */
    public record Sizes(int companies, int people, int findings) {
    }

    /** The graph as built: every datom in EAVT order, and the entity of each key. */
    public record Built(List<Datom> datoms, Map<String, Long> entities, long maxEid) {
    }

    private SuperfriendsGraph() {
    }

    /** The company of index i: the names in turn, then numbered. */
    static String company(int i) {
        String base = NAMES.get(i % NAMES.size());
        return i < NAMES.size() ? base : base + "-" + (i / NAMES.size());
    }

    /** The ticker of company i, unique: the names' first four letters differ. */
    static String ticker(int i) {
        String base = NAMES.get(i % NAMES.size()).toUpperCase(java.util.Locale.ROOT);
        return i < NAMES.size() ? base : base.substring(0, Math.min(4, base.length())) + (i / NAMES.size());
    }

    /**
     * The datoms of a graph, every one in the first transaction.
     *
     * @param sizes the sizes
     * @param seed  the seed of the findings
     * @return the datoms and the entity of each key
     */
    public static Built datoms(Sizes sizes, long seed) {
        long tx = PDb.TX0 + 1;
        Random random = new Random(seed);
        List<Datom> out = new ArrayList<>();
        Map<String, Long> entities = new LinkedHashMap<>();
        long[] next = {1};
        for (int i = 0; i < sizes.companies(); i++) {
            entity(out, entities, next, "company", company(i), tx);
        }
        for (int i = 0; i < sizes.companies(); i++) {
            long t = entity(out, entities, next, "ticker", ticker(i), tx);
            relation(out, next, "company:" + company(i), entities.get("company:" + company(i)), "ticker", "ticker:" + ticker(i),
                    t, tx);
        }
        for (int i = 0; i < sizes.people(); i++) {
            String person = "Analyst " + i;
            long p = entity(out, entities, next, "person", person, tx);
            int works = random.nextInt(sizes.companies());
            relation(out, next, "person:" + person, p, "works-at", "company:" + company(works),
                    entities.get("company:" + company(works)), tx);
        }
        for (int i = 0; i < sizes.findings(); i++) {
            // a third of the findings are about Acme, so its lookup reads leaves the tail and the folds both feed
            int about = random.nextInt(3) == 0 ? 0 : random.nextInt(sizes.companies());
            out.addAll(finding(next[0]++, entities.get("company:" + company(about)), entities.get("ticker:" + ticker(about)),
                    ticker(about), "inq-" + i, SERVICES.get(random.nextInt(SERVICES.size())), "fact " + i + ": " + text(random),
                    random.nextInt(100) / 100.0, tx));
        }
        out.sort(DatomOrder.EAVT);
        return new Built(List.copyOf(out), Map.copyOf(entities), next[0] - 1);
    }

    private static long entity(List<Datom> out, Map<String, Long> entities, long[] next, String kind, String name, long tx) {
        long e = next[0]++;
        String key = kind + ":" + name;
        entities.put(key, e);
        out.add(new Datom(e, ":entity/key", key, tx));
        out.add(new Datom(e, ":entity/kind", kind, tx));
        out.add(new Datom(e, ":entity/name", name, tx));
        return e;
    }

    private static void relation(List<Datom> out, long[] next, String subjectKey, long subject, String predicate, String objectKey,
                                 long object, long tx) {
        long e = next[0]++;
        out.add(new Datom(e, ":relation/key", subjectKey + "|" + predicate + "|" + objectKey, tx));
        out.add(new Datom(e, ":relation/subject", subject, tx));
        out.add(new Datom(e, ":relation/predicate", predicate, tx));
        out.add(new Datom(e, ":relation/object", object, tx));
        out.add(new Datom(e, ":relation/confidence", 1.0, tx));
        out.add(new Datom(e, ":relation/service", "graph", tx));
    }

    /** The datoms of one finding entity about a company and its ticker. */
    public static List<Datom> finding(long e, long company, long ticker, String symbol, String inquiry, String service, String text,
                                      double confidence, long tx) {
        return List.of(new Datom(e, ":finding/about", company, tx), new Datom(e, ":finding/about", ticker, tx),
                new Datom(e, ":finding/inquiry", inquiry, tx), new Datom(e, ":finding/service", service, tx),
                new Datom(e, ":finding/ticker", symbol, tx), new Datom(e, ":finding/kind", "fact", tx),
                new Datom(e, ":finding/text", text, tx), new Datom(e, ":finding/confidence", confidence, tx));
    }

    private static String text(Random random) {
        StringBuilder out = new StringBuilder();
        for (int w = 0; w < 12; w++) {
            out.append(w == 0 ? "" : " ");
            for (int c = 3 + random.nextInt(6); c > 0; c--) {
                out.append((char) ('a' + random.nextInt(26)));
            }
        }
        return out.toString();
    }

    /**
     * The database of a graph, every datom folded into the indexes: the value of the first swap.
     *
     * @param over  the empty database whose store and loader the nodes go to
     * @param built the datoms
     * @return the database
     */
    public static PDb fold(PDb over, Built built) {
        List<Datom> eavt = built.datoms();
        List<Datom> aevt = new ArrayList<>(eavt);
        aevt.sort(DatomOrder.AEVT);
        List<Datom> avet = new ArrayList<>(eavt.stream().filter(d -> INDEXED.contains(d.a())).toList());
        avet.sort(DatomOrder.AVET);
        int bf = (int) DbRoot.BRANCHING;
        String e = PSortedMap.of(over.store(), DnNode.EAVT, DatomOrder.EAVT, bf, eavt).root();
        String a = PSortedMap.of(over.store(), DnNode.AEVT, DatomOrder.AEVT, bf, aevt).root();
        String v = PSortedMap.of(over.store(), DnNode.AVET, DatomOrder.AVET, bf, avet).root();
        long maxTx = eavt.stream().mapToLong(Datom::tx).max().orElse(PDb.TX0);
        return PDb.writer(over.store(), over.loader()).root(StoreGraphFleet.CODEC.toBytes(schema()), built.maxEid(), maxTx, e,
                a, v, null);
    }

    /**
     * One transaction on a database: a new entity's datoms appended to the tail, the indexes unchanged, as a
     * DataScript transaction before the next fold.
     *
     * @param db      the database the swap hands over
     * @param datoms  the datoms of the new entity, given its id and the transaction id
     * @return the database after the transaction
     */
    public static PDb transact(PDb db, java.util.function.BiFunction<Long, Long, List<Datom>> datoms) {
        long e = db.node().maxEid() + 1;
        long tx = db.node().maxTx() + 1;
        List<Datom> tail = new ArrayList<>(db.tail());
        tail.addAll(datoms.apply(e, tx));
        PDb.Writer writer = PDb.writer(db.store(), db.loader());
        return writer.root(db.schema(), e, tx, db.node().eavt().cid(), db.node().aevt().cid(), db.node().avet().cid(),
                writer.tail(tail));
    }
}
