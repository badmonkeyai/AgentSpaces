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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FilePieceStore;
import ai.badmonkey.agentspaces.content.SpacePayloads;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Example 33: values that change across a fleet (srad/agentspaces-pdt.md §7.3,
 * PD-O-04). Every peer is a member of one founded group with the content swarm
 * and the persistent trees; a peer with refs keeps a replica of the ref space
 * {@value #REF_SPACE} and a {@code Pdt} facade over a pack node store, and the
 * application space {@value #PLANS_SPACE} opts into tree escalation above
 * {@value #ESCALATE_ABOVE} bytes (PD-E-01). A peer without refs is a plain
 * holder and fetcher on the tree exchange, which the composition uses to
 * measure round trips (PD-Q-PRF-06) and the cold fetch of PD-Q-REL-04.
 *
 * <p>The demo runs every peer on {@code tcp}; {@link #start} takes a map of
 * listeners ({@code tcp}, {@code tls}, {@code dtls}) and seeds written as
 * {@code scheme://host:port}, as example 29 does.
 */
public final class PersistentValuesFleet {

    /** The fleet's group, founded by string so every member derives one id. */
    public static final String FOUNDING = "persistent-values-example-v1";

    /** The space that carries the ref heads ({@code agentspaces.pdt.space}). */
    public static final String REF_SPACE = "pdt";

    /** The application space that opts into tree escalation (step 5 of §7.3). */
    public static final String PLANS_SPACE = "plans";

    /** Payloads above this size escalate to a tree (PD-E-01). */
    public static final long ESCALATE_ABOVE = 64 * 1024;

    /** The codec of the fact keys and values. */
    public static final Codec<String> STR = Codec.of(String.class);

    static final CborCodec CODEC = CborCodec.defaultCodec();
    static final Duration LOAD_TIMEOUT = Duration.ofSeconds(120);

    private PersistentValuesFleet() {
    }

    /** One peer: its node, the group it joined, the content swarm, the node store, and the refs when it keeps them. */
    public static final class Peer implements AutoCloseable {
        private final String name;
        private final PeerIdentity identity;
        private final PeerNode node;
        private final GroupRuntime runtime;
        private final ReplicatedSpace contentSpace;
        private final ReplicatedSpace refSpace;
        private final ReplicatedSpace plans;
        private final CapabilityRuntime capabilities;
        private final Content content;
        private final PackNodeStore pack;
        private final NodeStore store;
        private final PdtSwarm swarm;
        private final Pdt pdt;
        private final ScheduledExecutorService ticker;
        private final List<AutoCloseable> transports;
        private final List<AutoCloseable> extras = new ArrayList<>();
        private volatile Ref<PMap<String, String>> facts;
        private volatile Ref<PBlob> table;
        private volatile boolean closed;

        private Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace contentSpace,
                     ReplicatedSpace refSpace, ReplicatedSpace plans, CapabilityRuntime capabilities, Content content,
                     PackNodeStore pack, NodeStore store, PdtSwarm swarm, Pdt pdt, ScheduledExecutorService ticker,
                     List<AutoCloseable> transports) {
            this.name = name;
            this.identity = identity;
            this.node = node;
            this.runtime = runtime;
            this.contentSpace = contentSpace;
            this.refSpace = refSpace;
            this.plans = plans;
            this.capabilities = capabilities;
            this.content = content;
            this.pack = pack;
            this.store = store;
            this.swarm = swarm;
            this.pdt = pdt;
            this.ticker = ticker;
            this.transports = transports;
        }

        public String name() {
            return name;
        }

        public PeerIdentity identity() {
            return identity;
        }

        public PeerId id() {
            return node.peerId();
        }

        public PeerNode node() {
            return node;
        }

        public GroupRuntime runtime() {
            return runtime;
        }

        public Content content() {
            return content;
        }

        public NodeStore store() {
            return store;
        }

        public PdtSwarm swarm() {
            return swarm;
        }

        /** Whether this peer keeps the ref space and a facade. */
        public boolean hasRefs() {
            return pdt != null;
        }

        /** The facade; a peer without refs has none. */
        public Pdt pdt() {
            if (pdt == null) {
                throw new IllegalStateException(name + " keeps no refs");
            }
            return pdt;
        }

        public ReplicatedSpace refSpace() {
            return refSpace;
        }

        /** The opted-in application space of step 5. */
        public ReplicatedSpace plans() {
            return plans;
        }

        /** The ref {@code facts}: a map of fact id to text. */
        public Ref<PMap<String, String>> facts() {
            Ref<PMap<String, String>> ref = facts;
            if (ref == null) {
                ref = pdt().mapRef("facts", STR, STR);
                facts = ref;
            }
            return ref;
        }

        /** The ref {@code table}: a blob of generated rows. */
        public Ref<PBlob> table() {
            Ref<PBlob> ref = table;
            if (ref == null) {
                ref = pdt().blobRef("table");
                table = ref;
            }
            return ref;
        }

        /** Something to close before the peer: an agent binder, a worker. */
        public void closeWith(AutoCloseable extra) {
            extras.add(extra);
        }

        public boolean closed() {
            return closed;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            ticker.shutdownNow();
            for (AutoCloseable extra : extras) {
                quietly(extra);
            }
            if (plans != null) {
                quietly(plans::close);
            }
            if (pdt != null) {
                quietly(pdt::close);
            }
            quietly(capabilities::close);
            quietly(content::close);
            if (refSpace != null) {
                quietly(refSpace::close);
            }
            quietly(contentSpace::close);
            quietly(node::close);
            for (AutoCloseable transport : transports) {
                quietly(transport);
            }
            quietly(pack::close);
        }

        private static void quietly(AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // closing something that is already down
            }
        }
    }

    /**
     * Starts a peer on loopback {@code tcp} that keeps refs.
     *
     * @param name  a label for the log
     * @param port  the TCP port to listen on
     * @param seeds {@code host:port} seeds, empty for the first peer
     * @param dir   the directory of the pack node store and the piece store
     * @return the running peer
     * @throws IOException when the port cannot be bound or the store cannot open
     */
    public static Peer start(String name, int port, List<String> seeds, Path dir) throws IOException {
        return start(name, Map.of("tcp", port), Set.of(), seeds, dir, true);
    }

    /**
     * Starts a peer listening on several bindings: {@code listeners} maps a scheme
     * ({@code tcp}, {@code tls}, {@code dtls}) to its port on loopback; a seed is
     * {@code host:port} for tcp or {@code scheme://host:port}. A peer with no
     * listeners is dial-only, as a kit behind a site's relay is.
     *
     * @param name      a label for the log
     * @param listeners the bindings to listen on
     * @param roles     the peer's roles, for example {@code RELAY}
     * @param seeds     the seeds
     * @param dir       the directory of the pack node store and the piece store
     * @param refs      whether the peer keeps the ref space and a facade; a peer
     *                  without refs is a holder and fetcher on the tree exchange only
     * @return the running peer
     * @throws IOException when a port cannot be bound or the store cannot open
     */
    public static Peer start(String name, Map<String, Integer> listeners, Set<PeerAdvertisement.PeerRole> roles,
                             List<String> seeds, Path dir, boolean refs) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).roles(roles).build();
        List<AutoCloseable> transports = new ArrayList<>();
        for (Map.Entry<String, Integer> listener : new TreeMap<>(listeners).entrySet()) {
            Transport transport = transportFor(listener.getKey(), identity);
            if (transport instanceof AutoCloseable closeable) {
                transports.add(closeable);
            }
            node.listen(transport, "127.0.0.1:" + listener.getValue());
        }
        Set<String> dialSchemes = new TreeSet<>();
        for (String seed : seeds) {
            int at = seed.indexOf("://");
            dialSchemes.add(at < 0 ? "tcp" : seed.substring(0, at));
        }
        dialSchemes.removeAll(listeners.keySet());
        for (String scheme : dialSchemes) {
            Transport transport = transportFor(scheme, identity);
            if (transport instanceof AutoCloseable closeable) {
                transports.add(closeable);
            }
            node.transport(transport);
        }
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement ad = new GroupAdvertisement("aspace://" + groupId.value(), identity.peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(365), FOUNDING, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            int at = seed.indexOf("://");
            endpoints.add(at < 0 ? new PeerAdvertisement.Endpoint("tcp", seed, 0)
                    : new PeerAdvertisement.Endpoint(seed.substring(0, at), seed.substring(at + 3), 0));
        }
        GroupRuntime runtime = node.joinGroup(ad, GroupMembership.Config.defaults(), endpoints);
        BlockExchange blocks = new BlockExchange(runtime, CODEC);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .blocks(blocks).build();
        Files.createDirectories(dir);
        PackNodeStore pack = PackNodeStore.open(dir.resolve("nodes"));
        NodeStore store = new NodeStore(pack, InstantSource.system());
        // the group's discovery cache carries each member's content-swarm advertisement, whose trees
        // parameter tells this peer who speaks the tree exchange (PD-X-01); the capability runtime
        // signs and publishes this peer's own and drives the swarm's tick
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(CODEC, InstantSource.system()), CODEC,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        Content.Builder contentBuilder = Content.builder(new CapabilityPipes(runtime, CODEC), runtime, contentSpace,
                        identity.peerId())
                .codec(CODEC).store(new FilePieceStore(dir.resolve("pieces"), CODEC, InstantSource.system(), 0))
                .blocks(blocks).random(new Random(name.hashCode())).discovery(discovery);
        ReplicatedSpace refSpace = null;
        Pdt pdt = null;
        if (refs) {
            refSpace = ReplicatedSpace.builder(runtime, REF_SPACE, identity, "refs").build();
            pdt = Pdt.builder(refSpace).store(store).loadTimeout(LOAD_TIMEOUT).collectEvery(Duration.ZERO).build();
            contentBuilder.treeStore(pdt.treeStore());
        } else {
            contentBuilder.treeStore(PdtSwarm.treeStore(store));
        }
        Content content = contentBuilder.build();
        capabilities.register(content);
        PdtSwarm swarm;
        ReplicatedSpace plans = null;
        if (refs) {
            pdt.attach(content).start();
            Pdt facade = pdt;
            runtime.onTick(facade::tick);
            swarm = pdt.swarm();
            // the opted-in space: trees above the threshold, the flat swarm when a member lacks the exchange (PD-E-06)
            plans = ReplicatedSpace.builder(runtime, PLANS_SPACE, identity, "plans")
                    .blocks(blocks).bulk(pdt.payloads(), ESCALATE_ABOVE).sealBeforeEscalate(false)
                    .bulkFallback(new SpacePayloads(content)).build();
        } else {
            swarm = PdtSwarm.attach(content, store, LOAD_TIMEOUT);
        }
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tick-" + name);
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(() -> {
            try {
                node.tick();
            } catch (RuntimeException e) {
                System.err.println(name + " tick failed: " + e.getMessage());
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
        capabilities.autoRefresh(Duration.ofSeconds(30), ticker);
        Peer peer = new Peer(name, identity, node, runtime, contentSpace, refSpace, plans, capabilities, content, pack, store,
                swarm, pdt, ticker, transports);
        if (refs) {
            // the handles exist before any head arrives, as an agent's would
            peer.facts();
            peer.table();
        }
        return peer;
    }

    /** The transport for a scheme; a binding with its own channel certificate is made for the peer's identity. */
    static Transport transportFor(String scheme, PeerIdentity identity) throws IOException {
        try {
            return switch (scheme) {
                case "tcp" -> new TcpTransport();
                case "tls" -> new ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport(identity);
                case "dtls" -> new ai.badmonkey.agentspaces.transport.dtls.DtlsTransport(identity);
                default -> throw new IllegalArgumentException("no binding " + scheme + " (tcp, tls, dtls)");
            };
        } catch (java.security.GeneralSecurityException e) {
            throw new IOException("cannot make the " + scheme + " binding: " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------ waits

    /** Polls until a condition holds or the time runs out. */
    public static boolean await(Duration max, BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    /** Waits until a peer's view holds {@code count} other members (the view leaves the peer itself out). */
    public static boolean awaitMembers(Peer peer, int count) throws InterruptedException {
        return await(Duration.ofSeconds(30), () -> peer.runtime().membership().allMembers().size() >= count);
    }

    /** Waits until every peer sees every other's content-swarm advertisement with the trees parameter (PD-X-01). */
    public static boolean awaitTrees(List<Peer> peers) throws InterruptedException {
        return await(Duration.ofSeconds(30), () -> {
            for (Peer from : peers) {
                for (Peer to : peers) {
                    if (from != to && !from.content().advertisesTrees(to.id())) {
                        return false;
                    }
                }
            }
            return true;
        });
    }

    // ------------------------------------------------------------ fixtures and measures

    static String key(int i) {
        return "fact-" + i;
    }

    /** A fact of about 190 characters, so 10,000 facts make about 2 MiB of nodes (PD-Q-PRF-01). */
    static String fact(int i, Random random) {
        char[] chars = new char[190];
        for (int c = 0; c < chars.length; c++) {
            chars[c] = (char) ('a' + random.nextInt(26));
        }
        return i + ":" + new String(chars);
    }

    /**
     * Writes generated table rows to a file: one row of about 200 bytes per line,
     * with a seeded random text column so the content-defined chunker finds cut points.
     */
    public static void writeRows(Path file, long bytes, long seed) throws IOException {
        Random random = new Random(seed);
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
        try (FileChannel out = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            long written = 0;
            long row = 0;
            while (written < bytes) {
                String line = String.format("row-%010d|%s%n", row++, fact((int) (row % 1000), random));
                byte[] encoded = line.getBytes(StandardCharsets.US_ASCII);
                int n = (int) Math.min(encoded.length, bytes - written);
                if (buffer.remaining() < n) {
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        out.write(buffer);
                    }
                    buffer.clear();
                }
                buffer.put(encoded, 0, n);
                written += n;
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                out.write(buffer);
            }
        }
    }

    /** The bytes of the nodes under a root that are not under a base: the delta a reader holding the base needs. */
    public static long bytesNotIn(NodeStore store, String root, String base) {
        Set<String> shared = base == null ? Set.of() : store.reachable(base);
        long total = 0;
        for (String cid : store.reachable(root)) {
            if (!shared.contains(cid)) {
                total += store.get(cid).orElseThrow().length;
            }
        }
        return total;
    }

    /** The bytes of every node under a root. */
    public static long bytesUnder(NodeStore store, String root) {
        return bytesNotIn(store, root, null);
    }

    /** Levels below the root: 0 for a single node. */
    public static int levelsBelowRoot(NodeStore store, String root) {
        Deque<String> queue = new ArrayDeque<>();
        Deque<Integer> depths = new ArrayDeque<>();
        Set<String> seen = new HashSet<>();
        queue.add(root);
        depths.add(0);
        seen.add(root);
        int deepest = 0;
        while (!queue.isEmpty()) {
            String cid = queue.poll();
            int depth = depths.poll();
            deepest = Math.max(deepest, depth);
            byte[] bytes = store.get(cid).orElseThrow();
            for (String link : NodeCodec.links(cid, bytes)) {
                if (seen.add(link)) {
                    queue.add(link);
                    depths.add(depth + 1);
                }
            }
        }
        return deepest;
    }

    // ------------------------------------------------------------ the demo

    /**
     * Runs the composition of §7.3 over loopback tcp: a curator, three analysts, a
     * meter that measures each update's round trips, a late auditor for the
     * fetch an analyst dies during, and the Python reader when
     * {@code agentspaces-python/tests/workers/pdt_worker.py} and its interpreter
     * are present. The first argument is the base port (7733 by default); the
     * second is {@code full} (the sizes of §7.3) or {@code quick}.
     */
    public static void main(String[] args) throws Exception {
        int base = args.length > 0 ? Integer.parseInt(args[0]) : 7733;
        Composition.Sizes sizes = args.length > 1 && args[1].equals("quick") ? Composition.Sizes.quick() : Composition.Sizes.full();
        Path root = Files.createTempDirectory("example-33");
        String seed = "127.0.0.1:" + base;
        try (Peer curator = start("curator", base, List.of(), root.resolve("curator"));
             Peer a1 = start("analyst-1", base + 1, List.of(seed), root.resolve("a1"));
             Peer a2 = start("analyst-2", base + 2, List.of(seed), root.resolve("a2"));
             Peer a3 = start("analyst-3", base + 3, List.of(seed), root.resolve("a3"));
             Peer meter = start("meter", Map.of("tcp", base + 4), Set.of(), List.of(seed), root.resolve("meter"), false)) {
            Composition.Fleet fleet = new Composition.Fleet(curator, List.of(a1, a2, a3), meter,
                    () -> start("auditor", Map.of("tcp", base + 5), Set.of(), List.of(seed), root.resolve("auditor"), false));
            PythonReader python = PythonReader.available() ? new PythonReader("127.0.0.1", base) : null;
            if (python == null) {
                System.out.println("python reader: skipped (" + PythonReader.whyUnavailable() + ")");
            }
            Composition.Report report = Composition.run(fleet, sizes, root.resolve("work"), python, System.out);
            System.out.print(report.summary());
        }
    }
}
