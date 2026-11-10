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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.kinds.DbRoot;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The value of a {@code db} ref (PDT 2 phase 2, P2-D-02 to P2-D-06; srad/agentspaces-pdt2.md §3.1):
 * a DataScript database as a {@code db} root over three {@code dn} indexes and an optional tail.
 * {@link #load} reads the root node and nothing else (P2-D-04); {@link #index} and {@link #read}
 * read further nodes on demand. The DataScript adapter of {@code agentspaces-datascript} writes a
 * fold's dirty paths, a tail, and a new root through one {@link Writer} batch; the datom merge
 * reads {@link #changesSince} a base, which follows the index diff and both tails.
 */
public final class PDb {

    /** DataScript's first transaction id: an empty database's max-tx. */
    public static final long TX0 = 536870912L;

    private final NodeStore store;
    private final NodeLoader loader;
    private final String root;
    private final DbRoot node;

    private PDb(NodeStore store, NodeLoader loader, String root, DbRoot node) {
        this.store = Objects.requireNonNull(store, "store");
        this.loader = Objects.requireNonNull(loader, "loader");
        this.root = Objects.requireNonNull(root, "root");
        this.node = Objects.requireNonNull(node, "node");
    }

    /**
     * A stored database: reads its root node through the loader and nothing else.
     *
     * @param store  the store a later write goes to
     * @param loader the loader of the root and of every node a read reaches
     * @param root   the {@code db} root CID
     * @return the database
     */
    public static PDb load(NodeStore store, NodeLoader loader, String root) {
        return new PDb(store, loader, root, DbRoot.decode(root, loader.load(root)));
    }

    /**
     * The empty database: an empty schema, max-eid 0, max-tx {@link #TX0}, three empty leaves, no
     * tail, written into the store at once so every read of it finds its four nodes.
     *
     * @param store  the store
     * @param loader the loader
     * @return the database
     */
    public static PDb empty(NodeStore store, NodeLoader loader) {
        Writer writer = new Writer(store, loader);
        String eavt = writer.node(DnNode.EAVT, 0, List.of(), List.of());
        String aevt = writer.node(DnNode.AEVT, 0, List.of(), List.of());
        String avet = writer.node(DnNode.AVET, 0, List.of(), List.of());
        return writer.root(CborCodec.defaultCodec().toBytes(Map.of()), 0, TX0, eavt, aevt, avet, null);
    }

    /** The writer of a database's nodes into a store, one batch per {@link Writer#root}. */
    public static Writer writer(NodeStore store, NodeLoader loader) {
        return new Writer(store, loader);
    }

    /** The root CID. */
    public String root() {
        return root;
    }

    /** The decoded root node. */
    public DbRoot node() {
        return node;
    }

    /** The store a write of this database's successor goes to. */
    public NodeStore store() {
        return store;
    }

    /** The loader of this database's nodes. */
    public NodeLoader loader() {
        return loader;
    }

    /** The size a head carries: the transactions since {@link #TX0}. */
    public long size() {
        return Math.max(0, node.maxTx() - TX0);
    }

    /** The schema's CBOR, read back from its spill node when it left the root. */
    public byte[] schema() {
        return Spill.resolve(Spill.refOf(node.schema().bytes()), PSortedMap.context(loader));
    }

    /**
     * One index as a sorted map.
     *
     * @param index {@code eavt}, {@code aevt}, or {@code avet}
     * @return the map
     */
    public PSortedMap index(String index) {
        Link link = switch (index) {
            case DnNode.EAVT -> node.eavt();
            case DnNode.AEVT -> node.aevt();
            case DnNode.AVET -> node.avet();
            default -> throw new IllegalArgumentException("a db root has the indexes eavt, aevt and avet, not '" + index + "'");
        };
        return PSortedMap.load(store, loader, index, DatomOrder.of(index), link.cid());
    }

    /**
     * The tail's datoms in transaction order, a retraction with a negative transaction; empty when
     * the root links no tail.
     *
     * @return the datoms
     */
    public List<Datom> tail() {
        return node.tail() == null ? List.of() : read(node.tail().cid()).datoms();
    }

    /**
     * A {@code dn} node of this database with its spilled values read back: what the adapter's
     * {@code restore} turns into a leaf or a branch of DataScript's sorted set.
     *
     * @param cid the node
     * @return the node
     */
    public DnNode read(String cid) {
        return PSortedMap.read(cid, loader);
    }

    // ------------------------------------------------------------------ changes

    /** Entity, attribute, value: a datom's identity in a database, whatever its transaction. */
    private static final Comparator<Datom> EAV = (x, y) -> {
        int c = Long.compare(x.e(), y.e());
        if (c == 0) {
            c = DatomOrder.compareAttributes(x.a(), y.a());
        }
        return c != 0 ? c : DatomOrder.compareValues(x.v(), y.v());
    };

    /**
     * The datoms a database asserts and retracts relative to a base, each by its entity,
     * attribute, and value.
     *
     * @param added     held here and not in the base, with the transaction that holds it here
     * @param retracted held in the base and not here, with the base's transaction negated
     */
    public record Changes(List<Datom> added, List<Datom> retracted) {
    }

    /**
     * What changed since a base (§3.1.4): the candidates are the datoms of the EAVT diff, which
     * prunes every shared subtree, and the datoms of both tails; each is then read in both
     * databases, a tail's last operation on it first and the index otherwise.
     *
     * @param base the base
     * @return the changes, in EAVT order
     */
    public Changes changesSince(PDb base) {
        PSortedMap.Delta delta = base.index(DnNode.EAVT).diff(index(DnNode.EAVT));
        TreeMap<Datom, Datom> candidates = new TreeMap<>(EAV);
        for (Datom d : delta.removed()) {
            candidates.putIfAbsent(d, d);
        }
        for (Datom d : delta.added()) {
            candidates.putIfAbsent(d, d);
        }
        TreeMap<Datom, Datom> mineTail = lastOps(tail());
        TreeMap<Datom, Datom> baseTail = lastOps(base.tail());
        mineTail.keySet().forEach(d -> candidates.putIfAbsent(d, d));
        baseTail.keySet().forEach(d -> candidates.putIfAbsent(d, d));
        List<Datom> added = new ArrayList<>();
        List<Datom> retracted = new ArrayList<>();
        for (Datom key : candidates.keySet()) {
            Datom here = held(key, mineTail);
            Datom there = base.held(key, baseTail);
            if (here != null && there == null) {
                added.add(here);
            } else if (here == null && there != null) {
                retracted.add(new Datom(there.e(), there.a(), there.v(), -Math.abs(there.tx())));
            }
        }
        return new Changes(Collections.unmodifiableList(added), Collections.unmodifiableList(retracted));
    }

    /** The last operation of a tail on each datom identity. */
    private static TreeMap<Datom, Datom> lastOps(List<Datom> tail) {
        TreeMap<Datom, Datom> out = new TreeMap<>(EAV);
        for (Datom d : tail) {
            out.remove(d);
            out.put(d, d);
        }
        return out;
    }

    /** The datom this database holds for an identity, or null: the tail's last operation decides, else the index. */
    private Datom held(Datom key, TreeMap<Datom, Datom> tailOps) {
        Datom op = tailOps.get(key);
        if (op != null) {
            return op.tx() > 0 ? op : null;
        }
        Iterator<Datom> it = index(DnNode.EAVT).seek(new Datom(key.e(), key.a(), key.v(), 1));
        if (it.hasNext()) {
            Datom found = it.next();
            if (EAV.compare(found, key) == 0) {
                return found;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ writing

    /**
     * Writes the nodes of a database in one batch (P2-D-02): the adapter's {@code store} of a dirty
     * node calls {@link #node}, a transaction's tail calls {@link #tail}, and {@link #root} writes
     * the batch and the root. A text or byte value above 1 KiB, and a schema above it, leave their
     * node as PDT 1 spills them.
     */
    public static final class Writer {
        private final NodeStore store;
        private final NodeLoader loader;
        private final Flusher flusher;
        private final Ctx ctx;

        private Writer(NodeStore store, NodeLoader loader) {
            this.store = Objects.requireNonNull(store, "store");
            this.loader = Objects.requireNonNull(loader, "loader");
            this.flusher = new Flusher(store);
            this.ctx = PSortedMap.context(loader);
        }

        /**
         * Queues an index node.
         *
         * @param index    {@code eavt}, {@code aevt}, or {@code avet}
         * @param level    0 for a leaf, the height for a branch
         * @param keys     the keys; a branch's are the largest key of each child
         * @param children one CID per key on a branch, empty on a leaf
         * @return the node's CID
         */
        public String node(String index, long level, List<Datom> keys, List<String> children) {
            List<Link> links = new ArrayList<>(children.size());
            for (String child : children) {
                links.add(new Link(child));
            }
            return flusher.put(new DnNode(level, index, PSortedMap.spilled(keys, flusher, ctx), links).encode(), Cids.DAG_CBOR);
        }

        /**
         * Queues a tail node.
         *
         * @param datoms the unfolded datoms in transaction order
         * @return the CID, or null for no datoms, which the root writes as a null {@code t}
         */
        public String tail(List<Datom> datoms) {
            if (datoms.isEmpty()) {
                return null;
            }
            return flusher.put(DnNode.tail(PSortedMap.spilled(datoms, flusher, ctx)).encode(), Cids.DAG_CBOR);
        }

        /**
         * Writes the queued nodes and the root in one batch.
         *
         * @param schema the schema's CBOR: a map with the attributes' print forms as keys
         * @param maxEid the largest entity id
         * @param maxTx  the largest transaction id
         * @param eavt   the EAVT root
         * @param aevt   the AEVT root
         * @param avet   the AVET root
         * @param tail   the tail, or null
         * @return the database
         */
        public PDb root(byte[] schema, long maxEid, long maxTx, String eavt, String aevt, String avet, String tail) {
            Embedded embedded = Spill.embed(Spill.spill(schema, ctx), flusher);
            DbRoot node = new DbRoot(embedded, maxEid, maxTx, DbRoot.BRANCHING, new Link(eavt), new Link(aevt),
                    new Link(avet), tail == null ? null : new Link(tail));
            String cid = flusher.put(node.encode(), Cids.DAG_CBOR);
            flusher.commit();
            return new PDb(store, loader, cid, node);
        }
    }
}
