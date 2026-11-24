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
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A released 0.6.0 Java member for EL-SP-30 (srad/agentspaces-event-loop.md §3.11, R16), compiled at test time against
 * the 0.6.0 jars and using only their API: PeerNode.builder, listen on TCP, joinGroup with the 0.6.0 defaults (a 30 s
 * member TTL, the probe order reshuffled every cycle, no targeted relay), startTicking at 250 ms. It speaks the same
 * stdin protocol as IdleCostNode: READY, then view, stats and quit.
 *
 *   java -cp <classes>:<0.6.0 classpath> LegacyIdleNode <name> <port> <seed port, 0 for none> <founding>
 */
public final class LegacyIdleNode {

    private LegacyIdleNode() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[1]);
        int seed = Integer.parseInt(args[2]);
        String founding = args[3];
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        GroupId groupId = GroupId.fromFounding(founding.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement ad = new GroupAdvertisement("aspace://" + groupId.value(), identity.peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(365), founding, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        List<PeerAdvertisement.Endpoint> seeds = seed == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seed, 0));
        GroupRuntime runtime = node.joinGroup(ad, GroupMembership.Config.defaults(), seeds);
        node.startTicking(Duration.ofMillis(250));
        Map<String, AtomicLong> departures = new ConcurrentHashMap<>();
        Thread watcher = new Thread(() -> watch(runtime, departures), "legacy-view-watch");
        watcher.setDaemon(true);
        watcher.start();
        ObjectMapper json = new ObjectMapper();
        System.out.println("READY " + identity.peerId().value() + " " + port + " " + ProcessHandle.current().pid());
        System.out.flush();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            switch (line.trim()) {
                case "stats" -> {
                    Map<String, Object> out = new LinkedHashMap<>();
                    out.put("peer", identity.peerId().value());
                    out.put("version", "0.6.0");
                    out.put("monoNanos", System.nanoTime());
                    out.put("memberTtlMillis", GroupMembership.Config.defaults().memberTtl().toMillis());
                    Map<String, Long> sent = new TreeMap<>();
                    node.traffic().sentByKind().forEach((k, v) -> sent.put(k.name(), v));
                    out.put("sentByKind", sent);
                    Map<String, Long> received = new TreeMap<>();
                    node.traffic().receivedByKind().forEach((k, v) -> received.put(k.name(), v));
                    out.put("receivedByKind", received);
                    out.put("view", view(runtime));
                    Map<String, Long> gone = new TreeMap<>();
                    departures.forEach((k, v) -> gone.put(k, v.get()));
                    out.put("departures", gone);
                    System.out.println("STATS " + json.writeValueAsString(out));
                }
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
}
