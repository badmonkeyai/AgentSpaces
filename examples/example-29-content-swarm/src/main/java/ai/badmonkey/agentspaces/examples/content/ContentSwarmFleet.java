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
package ai.badmonkey.agentspaces.examples.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.Fetch;
import ai.badmonkey.agentspaces.content.FilePieceStore;
import ai.badmonkey.agentspaces.content.PieceStore;
import ai.badmonkey.agentspaces.content.PublishOptions;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Example 29: a large artifact across a fleet (srad/large-networks-bulk-data.md
 * §7.3). Every peer runs on {@code tcp}; the content swarm never names a
 * binding, so the same code runs on any mix of them.
 */
public final class ContentSwarmFleet {

    /** The fleet's group, founded by string so every member derives one id. */
    public static final String FOUNDING = "content-swarm-example-v1";

    /** A second group the bridge shares into. */
    public static final String MISSION_FOUNDING = "content-swarm-mission-v1";

    /** Piece size for a quick run. */
    public static final int PIECE_SIZE = 256 * 1024;

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private ContentSwarmFleet() {
    }

    /** One peer: its node, the groups it joined, and the content capability per group. */
    public static final class Peer implements AutoCloseable {
        private final String name;
        private final PeerNode node;
        private final List<Joined> joined = new ArrayList<>();
        private final PieceStore store;
        private final ScheduledExecutorService ticker;

        private record Joined(String founding, GroupRuntime runtime, Content content) {
        }

        private final List<AutoCloseable> transports;
        private ai.badmonkey.agentspaces.content.ContentPolicy policy = ai.badmonkey.agentspaces.content.ContentPolicy.DEFAULT;

        private Peer(String name, PeerNode node, PieceStore store, ScheduledExecutorService ticker,
                     List<AutoCloseable> transports) {
            this.transports = transports;
            this.name = name;
            this.node = node;
            this.store = store;
            this.ticker = ticker;
        }

        public String name() {
            return name;
        }

        public PeerNode node() {
            return node;
        }

        public PieceStore store() {
            return store;
        }

        /** The content capability of the fleet's group. */
        public Content content() {
            return content(FOUNDING);
        }

        /** The content capability of a group this peer joined. */
        public Content content(String founding) {
            return joined.stream().filter(j -> j.founding().equals(founding)).findFirst()
                    .orElseThrow(() -> new IllegalStateException(name + " did not join " + founding)).content();
        }

        public GroupRuntime runtime() {
            return joined.get(0).runtime();
        }

        private void join(String founding, List<String> seeds, PeerIdentity identity) {
            GroupId groupId = GroupId.fromFounding(founding.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            GroupAdvertisement ad = new GroupAdvertisement("aspace://" + groupId.value(), identity.peerId(), groupId,
                    Instant.EPOCH, Duration.ofDays(365), founding, GroupAdvertisement.MembershipPolicy.OPEN,
                    ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
            List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
            for (String seed : seeds) {
                // "host:port" is tcp; "scheme://host:port" names the binding (tls, dtls, quic, ws, http)
                int at = seed.indexOf("://");
                endpoints.add(at < 0 ? new PeerAdvertisement.Endpoint("tcp", seed, 0)
                        : new PeerAdvertisement.Endpoint(seed.substring(0, at), seed.substring(at + 3), 0));
            }
            GroupRuntime runtime = node.joinGroup(ad, GroupMembership.Config.defaults(), endpoints);
            BlockExchange blocks = new BlockExchange(runtime, CODEC);
            ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                    .blocks(blocks).build();
            Content content = Content.builder(new CapabilityPipes(runtime, CODEC), runtime, contentSpace, identity.peerId())
                    .codec(CODEC).store(store).blocks(blocks).policy(policy).random(new Random(name.hashCode())).build();
            runtime.onTick(content::tick);
            content.start();
            joined.add(new Joined(founding, runtime, content));
        }

        @Override
        public void close() {
            ticker.shutdownNow();
            joined.forEach(j -> j.content().close());
            node.close();
            for (AutoCloseable transport : transports) {
                try {
                    transport.close();
                } catch (Exception ignored) {
                    // closing a transport that is already down
                }
            }
        }
    }

    /**
     * Starts a peer listening on loopback, joining the fleet's group and optionally the mission group.
     *
     * @param name         a label for the log
     * @param port         the TCP port to listen on
     * @param seeds        {@code host:port} seeds of the fleet's group, empty for the first peer
     * @param missionSeeds seeds of the mission group, or null not to join it
     * @param storeDir     the file store directory
     * @return the running peer
     * @throws IOException when the port cannot be bound
     */
    public static Peer start(String name, int port, List<String> seeds, List<String> missionSeeds, Path storeDir)
            throws IOException {
        return start(name, java.util.Map.of("tcp", port), java.util.Set.of(), seeds, missionSeeds, storeDir);
    }

    /**
     * Starts a peer listening on several bindings (LB-O-06): {@code listeners} maps a scheme
     * ({@code tcp}, {@code tls}, {@code dtls}, {@code quic}, {@code ws}, {@code http}) to its port on
     * loopback; a seed is {@code host:port} for tcp or {@code scheme://host:port}.
     */
    public static Peer start(String name, java.util.Map<String, Integer> listeners,
                             java.util.Set<PeerAdvertisement.PeerRole> roles, List<String> seeds,
                             List<String> missionSeeds, Path storeDir) throws IOException {
        return start(name, listeners, roles, seeds, missionSeeds, storeDir, ai.badmonkey.agentspaces.content.ContentPolicy.DEFAULT);
    }

    /**
     * Starts a peer with a content policy, for example a seeder with an upload budget so the workers
     * must complete each other from what they hold (LB-P rows).
     */
    public static Peer start(String name, java.util.Map<String, Integer> listeners,
                             java.util.Set<PeerAdvertisement.PeerRole> roles, List<String> seeds,
                             List<String> missionSeeds, Path storeDir,
                             ai.badmonkey.agentspaces.content.ContentPolicy policy) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).roles(roles).build();
        List<AutoCloseable> transports = new ArrayList<>();
        for (java.util.Map.Entry<String, Integer> listener : new java.util.TreeMap<>(listeners).entrySet()) {
            ai.badmonkey.agentspaces.api.spi.Transport transport = transportFor(listener.getKey(), identity);
            if (transport instanceof AutoCloseable closeable) {
                transports.add(closeable);
            }
            node.listen(transport, "127.0.0.1:" + listener.getValue());
        }
        // a seed on a scheme this peer does not listen on still needs a binding to dial with; a peer
        // with no listeners at all is dial-only, as a kit behind a site's NAT is
        java.util.Set<String> dialSchemes = new java.util.TreeSet<>();
        for (String seed : missionSeeds == null ? seeds : java.util.stream.Stream.concat(seeds.stream(),
                missionSeeds.stream()).toList()) {
            int at = seed.indexOf("://");
            dialSchemes.add(at < 0 ? "tcp" : seed.substring(0, at));
        }
        dialSchemes.removeAll(listeners.keySet());
        for (String scheme : dialSchemes) {
            ai.badmonkey.agentspaces.api.spi.Transport transport = transportFor(scheme, identity);
            if (transport instanceof AutoCloseable closeable) {
                transports.add(closeable);
            }
            node.transport(transport);
        }
        Files.createDirectories(storeDir);
        PieceStore store = new FilePieceStore(storeDir, CODEC, InstantSource.system(), 0);
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tick-" + name);
            t.setDaemon(true);
            return t;
        });
        Peer peer = new Peer(name, node, store, ticker, transports);
        peer.policy = policy;
        peer.join(FOUNDING, seeds, identity);
        if (missionSeeds != null) {
            peer.join(MISSION_FOUNDING, missionSeeds, identity);
        }
        ticker.scheduleAtFixedRate(() -> {
            try {
                node.tick();
            } catch (RuntimeException e) {
                System.err.println(name + " tick failed: " + e.getMessage());
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
        return peer;
    }

    /** The transport for a scheme; a binding with its own channel certificate is made for the peer's identity. */
    static ai.badmonkey.agentspaces.api.spi.Transport transportFor(String scheme, PeerIdentity identity) throws IOException {
        try {
            return switch (scheme) {
                case "tcp" -> new TcpTransport();
                case "tls" -> new ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport(identity);
                case "dtls" -> new ai.badmonkey.agentspaces.transport.dtls.DtlsTransport(identity);
                case "quic" -> new ai.badmonkey.agentspaces.transport.quic.QuicTransport(identity);
                case "ws" -> new ai.badmonkey.agentspaces.transport.http.WebSocketTransport();
                case "http" -> new ai.badmonkey.agentspaces.transport.http.HttpExchangeTransport();
                default -> throw new IllegalArgumentException("no binding " + scheme + " (tcp, tls, dtls, quic, ws, http)");
            };
        } catch (java.security.GeneralSecurityException e) {
            throw new IOException("cannot make the " + scheme + " binding: " + e.getMessage(), e);
        }
    }

    /** A deterministic artifact of the given size. */
    public static byte[] artifact(int bytes, long seed) {
        byte[] out = new byte[bytes];
        new Random(seed).nextBytes(out);
        return out;
    }

    public static void main(String[] args) throws Exception {
        int base = args.length > 0 ? Integer.parseInt(args[0]) : 7629;
        Path root = Files.createTempDirectory("example-29");
        try (Peer trainer = start("trainer", base, List.of(), null, root.resolve("trainer"));
             Peer w1 = start("worker-1", base + 1, List.of("127.0.0.1:" + base), null, root.resolve("w1"));
             Peer w2 = start("worker-2", base + 2, List.of("127.0.0.1:" + base), null, root.resolve("w2"));
             Peer bridge = start("bridge", base + 3, List.of("127.0.0.1:" + base), List.of(), root.resolve("bridge"));
             Peer mission = start("mission-peer", base + 4, List.of(),
                     List.of("127.0.0.1:" + (base + 3)), root.resolve("mission"))) {
            Thread.sleep(2_000);
            byte[] checkpoint = artifact(32 * 1024 * 1024, 29);
            Manifest manifest = trainer.content().publish(checkpoint,
                    PublishOptions.defaults().withPieceSize(PIECE_SIZE).withName("checkpoint.bin"));
            System.out.printf("trainer published %s (%d pieces of %d KiB)%n", manifest.root(), manifest.pieces(), PIECE_SIZE / 1024);
            List<Peer> workers = List.of(w1, w2, bridge);
            List<Fetch> fetches = new ArrayList<>();
            for (Peer w : workers) {
                fetches.add(w.content().fetch(manifest.root()));
            }
            boolean killed = false;
            while (fetches.stream().anyMatch(f -> !f.isDoneOrFailed())) {
                Thread.sleep(500);
                StringBuilder line = new StringBuilder();
                for (int i = 0; i < workers.size(); i++) {
                    Fetch.Progress p = fetches.get(i).progress();
                    line.append(String.format("%s %3d%% (%d sources)  ", workers.get(i).name(),
                            (int) (p.fraction() * 100), p.sourcesActive()));
                }
                System.out.println(line);
                if (!killed && fetches.stream().allMatch(f -> f.progress().fraction() > 0.3)) {
                    System.out.println("** trainer killed; the workers complete each other **");
                    trainer.close();
                    killed = true;
                }
            }
            for (int i = 0; i < workers.size(); i++) {
                System.out.printf("%s: %s, bytes by source %s%n", workers.get(i).name(),
                        fetches.get(i).complete() ? "complete" : "failed", fetches.get(i).progress().bytesBySource());
            }
            Manifest shared = bridge.content(MISSION_FOUNDING).share(manifest.root(), Lease.of(Duration.ofDays(1)));
            System.out.println("bridge shared " + shared.root() + " into the mission group");
            Fetch onMission = mission.content(MISSION_FOUNDING).fetch(manifest.root());
            onMission.await(Duration.ofMinutes(2));
            System.out.printf("mission peer: %s, bytes by source %s%n",
                    onMission.complete() ? "complete" : "failed", onMission.progress().bytesBySource());
        }
    }
}
