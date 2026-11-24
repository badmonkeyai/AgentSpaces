/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.it.idle;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
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
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One idle core peer of {@link IdleCostIT}, in its own JVM: the IDLE-LOOP harness of 2026-11-18
 * ({@code /tmp/claude-watch/idle-profile/src/IdleNode.java}) moved into the tree (srad/agentspaces-event-loop.md
 * EL-M-01) with the starter's defaults: coalescing at {@code agentspaces.coalesce-millis} (20 ms unless the shape says
 * otherwise), the 250 ms tick, SIGNED channel auth, and the card refresh every 5 minutes. With four spaces it holds
 * the content, pdt, plans and work spaces, one PDT map ref, two {@code @SpaceTake} bindings and one
 * {@code @SpaceNotify}; with one space, the work space and the bindings only. After a small set-up write by the first
 * member it does nothing: every cycle it spends afterwards is the idle loop.
 *
 * <p>Args: {@code name port seedPort(0 = none) dir founding spaces coalesceMillis watchViews(true|false)}. It answers
 * one command per line on stdin with one line on stdout: {@code stats} prints {@code STATS <json>}, {@code view} prints
 * {@code VIEW <peer,...>}, {@code quit} exits.
 */
public final class IdleCostNode {

    /** An order the worker ships. */
    public record Order(String id, String item) {
    }

    /** A ticket the worker handles. */
    public record Ticket(String id, String note) {
    }

    /** A note the watcher sees. */
    public record Note(String id, String text) {
    }

    /** What the worker writes for an order. */
    public record Shipment(String id) {
    }

    /** Two take bindings, as an agent holds. */
    public static class Worker {
        /** Ships an order. */
        @SpaceTake(lease = "10m")
        public Shipment ship(Order o) {
            return new Shipment(o.id());
        }

        /** Handles a ticket. */
        @SpaceTake(lease = "10m")
        public void handle(Ticket t) {
        }
    }

    /** One notify binding. */
    public static class Watcher {
        /** Sees a note. */
        @SpaceNotify
        public void seen(Note n) {
        }
    }

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final Codec<String> STR = Codec.of(String.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private IdleCostNode() {
    }

    /**
     * Runs one member until {@code quit} or the end of stdin.
     *
     * @param args see the class comment
     * @throws Exception when the member cannot start
     */
    public static void main(String[] args) throws Exception {
        String name = args[0];
        int port = Integer.parseInt(args[1]);
        int seed = Integer.parseInt(args[2]);
        Path dir = Path.of(args[3], name);
        String founding = args[4];
        int spaces = Integer.parseInt(args[5]);
        long coalesceMillis = Long.parseLong(args[6]);
        boolean watchViews = Boolean.parseBoolean(args[7]);
        Files.createDirectories(dir);
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode.Builder builder = PeerNode.builder(identity); // SIGNED channel auth, the starter's default
        if (coalesceMillis > 0) {
            builder.coalesce(Duration.ofMillis(coalesceMillis)); // AC:342-343
        }
        PeerNode node = builder.build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        GroupId groupId = GroupId.fromFounding(founding.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement ad = new GroupAdvertisement("aspace://" + groupId.value(), identity.peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(365), founding, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        List<PeerAdvertisement.Endpoint> seeds = seed == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seed, 0));
        GroupRuntime runtime = node.joinGroup(ad, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace work = ReplicatedSpace.builder(runtime, "work", identity, "host").build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), null, InstantSource.system());
        binder.space("work", work);
        Ref<PMap<String, String>> facts = null;
        ScheduledExecutorService refresher = null;
        if (spaces >= 4) {
            BlockExchange blocks = new BlockExchange(runtime, CODEC);
            ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                    .blocks(blocks).build();
            NodeStore store = new NodeStore(PackNodeStore.open(dir.resolve("nodes")), InstantSource.system());
            DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(CODEC, InstantSource.system()),
                    CODEC, identity.peerId());
            CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
            ReplicatedSpace refSpace = ReplicatedSpace.builder(runtime, "pdt", identity, "refs").build();
            Pdt pdt = Pdt.builder(refSpace).store(store).build();
            Content content = Content.builder(new CapabilityPipes(runtime, CODEC), runtime, contentSpace,
                            identity.peerId())
                    .codec(CODEC).store(new FilePieceStore(dir.resolve("pieces"), CODEC, InstantSource.system(), 0))
                    .blocks(blocks).random(new Random(name.hashCode())).discovery(discovery)
                    .treeStore(pdt.treeStore()).build();
            capabilities.register(content);
            pdt.attach(content).start();
            pdt.joinLoop(runtime); // srad/agentspaces-event-loop.md EL-T-08: its own deadlines, not every tick
            ReplicatedSpace.builder(runtime, "plans", identity, "plans")
                    .blocks(blocks).bulk(pdt.payloads(), 64 * 1024).sealBeforeEscalate(false)
                    .bulkFallback(new SpacePayloads(content)).build();
            refresher = Executors.newSingleThreadScheduledExecutor();
            capabilities.autoRefresh(Duration.ofMinutes(5), refresher); // the starter's card-refresh-millis
            facts = pdt.mapRef("facts", STR, STR);
        }
        node.startTicking(Duration.ofMillis(250)); // the starter's tick-millis
        binder.bind(new Worker());
        binder.bind(new Watcher());
        if (seed == 0) {
            Thread.sleep(3000);
            if (facts != null) {
                facts.swap(m -> m.assoc("a", "one").assoc("b", "two").assoc("c", "three"));
            }
            for (int i = 0; i < 20; i++) {
                work.write(new Note("n" + i, "note " + i), Lease.of(Duration.ofHours(2)));
            }
        }
        Map<String, AtomicLong> departures = new ConcurrentHashMap<>();
        if (watchViews) {
            Thread.ofVirtual().name("idle-view-watch").start(() -> watch(runtime, departures));
        }
        System.out.println("READY " + identity.peerId().value() + " " + port + " "
                + ProcessHandle.current().pid());
        System.out.flush();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            switch (line.trim()) {
                case "stats" -> System.out.println("STATS " + JSON.writeValueAsString(stats(node, runtime,
                        departures)));
                case "view" -> System.out.println("VIEW " + String.join(",", view(runtime)));
                case "quit" -> {
                    System.out.println("BYE");
                    System.out.flush();
                    Runtime.getRuntime().halt(0);
                }
                default -> System.out.println("UNKNOWN " + line);
            }
            System.out.flush();
        }
        Runtime.getRuntime().halt(0);
    }

    private static List<String> view(GroupRuntime runtime) {
        List<String> ids = new ArrayList<>();
        for (GroupMembership.Member member : runtime.membership().allMembers()) {
            ids.add(member.id().value());
        }
        return ids;
    }

    /** Counts each member's departures from this node's view, once a second (only when asked, the TS shape). */
    private static void watch(GroupRuntime runtime, Map<String, AtomicLong> departures) {
        Set<String> before = Set.of();
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
            Set<String> now = Set.copyOf(view(runtime));
            for (String gone : before) {
                if (!now.contains(gone)) {
                    departures.computeIfAbsent(gone, g -> new AtomicLong()).incrementAndGet();
                }
            }
            before = now;
        }
    }

    private static Map<String, Object> stats(PeerNode node, GroupRuntime runtime, Map<String, AtomicLong> departures) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("peer", node.peerId().value());
        out.put("monoNanos", System.nanoTime());
        out.put("cpuNanos", ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime());
        // the JIT compiler's elapsed compile time since the JVM started (-1 when the VM does not report it): IdleCostIT
        // reads it at the window's bounds to show the JIT's share in and out of the window (owner decision 2026-11-23)
        java.lang.management.CompilationMXBean jit = ManagementFactory.getCompilationMXBean();
        out.put("jitMillis", jit != null && jit.isCompilationTimeMonitoringSupported()
                ? jit.getTotalCompilationTime() : -1L);
        PeerNode.LoopStats loop = node.loopStats();
        Map<String, Object> l = new LinkedHashMap<>();
        l.put("runs", loop.runs());
        l.put("timerWakes", loop.timerWakes());
        l.put("eventWakes", loop.eventWakes());
        l.put("flushRuns", loop.flushRuns());
        l.put("flushesWithFrames", loop.flushesWithFrames());
        l.put("slowRuns", loop.slowRuns());
        l.put("failedRuns", loop.failedRuns());
        l.put("directRuns", loop.directRuns());
        l.put("backstopRuns", loop.backstopRuns());
        l.put("backstopOnlyRuns", loop.backstopOnlyRuns());
        l.put("backstopFoundWork", loop.backstopFoundWork());
        l.put("backstopFoundBy", loop.backstopFoundBy());
        l.put("overruns", loop.overruns());
        l.put("latenessSamples", loop.latenessSamples());
        l.put("latenessP50Nanos", loop.latenessP50Nanos());
        l.put("latenessP99Nanos", loop.latenessP99Nanos());
        l.put("latenessMaxNanos", loop.latenessMaxNanos());
        l.put("busyNanos", loop.busyNanos());
        l.put("flushWaitSamples", loop.flushWaitSamples());
        l.put("flushWaitP99Nanos", loop.flushWaitP99Nanos());
        l.put("flushWaitMaxNanos", loop.flushWaitMaxNanos());
        l.put("generation", loop.generation());
        l.put("health", node.loopHealth().state().name());
        // The per-participant wake counters (srad/agentspaces-event-loop.md §10.21): who a timer wake was due for,
        // who an event wake asked, and who raised each wake.
        l.put("dueRuns", loop.dueRuns());
        l.put("askedRuns", loop.askedRuns());
        l.put("wakeCalls", loop.wakeCalls());
        l.put("stretch", loop.stretch());
        l.put("poolThreads", loop.poolThreads());
        out.put("loop", l);
        ai.badmonkey.agentspaces.common.time.TimeSource system = ai.badmonkey.agentspaces.common.time.TimeSource.system();
        if (system instanceof ai.badmonkey.agentspaces.common.time.SystemTime st) {
            ai.badmonkey.agentspaces.common.time.LatencyRing.Figures d = st.dispatchLatency();
            ai.badmonkey.agentspaces.common.time.LoopPool.Stats p = st.loopPool().stats();
            out.put("timer", Map.of("dispatchSamples", d.samples(), "dispatchP99Nanos", d.p99Nanos(),
                    "dispatchMaxNanos", d.maxNanos(), "poolThreads", p.threads(), "poolPeak", p.peakThreads(),
                    "poolGrows", p.grows(), "poolPromptGrows", p.promptGrows(), "poolShrinks", p.shrinks()));
        }
        // The process watchdog (srad/agentspaces-event-loop.md §3.9): its ticks and findings in this JVM.
        ai.badmonkey.agentspaces.common.time.Watchdog.Stats wd =
                ai.badmonkey.agentspaces.common.time.Watchdog.system().stats();
        out.put("watchdog", Map.of("ticks", wd.ticks(), "detections", wd.detections(), "reinits", wd.reinits(),
                "slowRuns", wd.slowRuns(), "restartsStarved", wd.restartsStarved(),
                "restartsThrown", wd.restartsThrown()));
        PeerNode.KeepaliveStats keep = node.keepaliveStats();
        out.put("keepalive", Map.of("refreshes", keep.refreshes(), "refreshFrames", keep.refreshFrames(),
                "floods", keep.floods(), "introductions", keep.introductions()));
        Map<String, Long> sent = new TreeMap<>();
        node.traffic().sentByKind().forEach((k, v) -> sent.put(k.name(), v));
        out.put("sentByKind", sent);
        Map<String, Long> received = new TreeMap<>();
        node.traffic().receivedByKind().forEach((k, v) -> received.put(k.name(), v));
        out.put("receivedByKind", received);
        out.put("view", view(runtime));
        // the member TTL and the targeted relay (srad/agentspaces-event-loop.md EL-E-05, §3.12)
        GroupMembership membership = runtime.membership();
        GroupMembership.ProbeStats probe = membership.probeStats();
        Map<String, Long> byRelay = new TreeMap<>();
        probe.holderPingReqsByRelay().forEach((k, v) -> byRelay.put(k.value(), v));
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("memberTtlMillis", membership.memberTtlMillis());
        p.put("quietProbeCycleMillis", membership.quietProbeCycleMillis());
        p.put("currentProbeIntervalMillis", membership.currentProbeIntervalMillis());
        p.put("directPings", probe.directPings());
        p.put("holderRounds", probe.holderRounds());
        p.put("holderPingReqs", probe.holderPingReqs());
        p.put("escalations", probe.escalations());
        p.put("randomPingReqs", probe.randomPingReqs());
        p.put("dropsAfterHolderRound", probe.dropsAfterHolderRound());
        p.put("dropsAfterIndirectRound", probe.dropsAfterIndirectRound());
        p.put("holderPingReqsByRelay", byRelay);
        out.put("membership", p);
        out.put("newcomerRoundMillis", node.newcomerRoundMillis());
        Map<String, Long> gone = new TreeMap<>();
        departures.forEach((k, v) -> gone.put(k, v.get()));
        out.put("departures", gone);
        out.put("threads", Thread.activeCount());
        // The platform threads by name with their CPU (srad/agentspaces-event-loop.md §10.24): the watchdog's own cost
        // (core.watchdog.cpu), the timer's, and the loop pool's threads at idle (core.loop.pool-threads).
        java.lang.management.ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        Map<String, Long> platform = new TreeMap<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (!t.isVirtual()) {
                platform.merge(t.getName(), Math.max(0, mx.getThreadCpuTime(t.threadId())), Long::sum);
            }
        }
        out.put("platformThreadCpuNanos", platform);
        return out;
    }

    /** The id of a peer as {@link #view} prints it. */
    static String idOf(PeerId peer) {
        return peer.value();
    }
}
