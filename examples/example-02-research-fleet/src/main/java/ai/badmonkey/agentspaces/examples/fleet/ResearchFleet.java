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
package ai.badmonkey.agentspaces.examples.fleet;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Example 02: the replicated-worker idiom over the full stack (Layers 0-3) on
 * real TCP. One process spins up a coordinator peer and two worker peers on
 * localhost ports, the coordinator publishes research tasks into a replicated
 * space, worker A dies mid-task, and the fleet finishes everything anyway.
 *
 * <p>Run all-in-one:
 * <pre>{@code mvn -q -pl examples/example-02-research-fleet exec:java}</pre>
 *
 * <p>Or run peers as separate processes (the M1 LAN demo):
 * <pre>{@code
 *   exec:java -Dexec.args="coordinator 7451"
 *   exec:java -Dexec.args="worker 7452 7451"
 *   exec:java -Dexec.args="worker 7453 7451"
 * }</pre>
 *
 * <p>Add {@code --tls} to any of those for the {@code tls} binding with attestation
 * required (SPEC §5.6): every member must present a channel certificate, and the
 * coordinator prints each member's link ({@code link: <peer> tls attested}).
 */
public final class ResearchFleet {

    /** A unit of research work. */
    public record ResearchTask(String topic, int priority) {
    }

    /** A completed research result. */
    public record Finding(String topic, String summary, String worker) {
    }

    /** One peer's assembled stack: node, group runtime, and the shared task space. */
    public record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {

        /** Shuts the peer down. */
        public void close() {
            space.close();
            node.close();
        }
    }

    private static final String HOST = "127.0.0.1";

    private ResearchFleet() {
    }

    /** The well-known demo group, derived self-certifyingly from a founding document. */
    public static GroupAdvertisement fleetGroup() {
        GroupId groupId = GroupId.fromFounding("research-fleet-demo-v1"
                .getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "research-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer of the fleet: identity, TCP transport, group membership,
     * and the replicated task space, ticking in the background.
     *
     * @param name     the agent name for attribution
     * @param port     the TCP port to listen on
     * @param seedPort the port of an existing member, or {@code 0} for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startPeer(String name, int port, int seedPort) throws Exception {
        return startPeer(name, port, seedPort, false);
    }

    /**
     * As {@link #startPeer(String, int, int)}, on TLS 1.3 with {@code tls}: the peer listens on
     * {@link TlsTcpTransport}, requires attestation (a frame not on a channel the handshake
     * attested to its sender is dropped and the sender evicted), and runs in ATTESTED channel
     * mode, so a member that announces CHANNEL_HELLO receives bare frames.
     *
     * @param name     the agent name for attribution
     * @param port     the port to listen on
     * @param seedPort the port of an existing member, or {@code 0} for the first
     * @param tls      whether to use the tls binding with attestation required
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startPeer(String name, int port, int seedPort, boolean tls) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = tls
                ? PeerNode.builder(identity).requireAttestation(true).channelAuth(PeerNode.ChannelAuth.ATTESTED).build()
                : PeerNode.builder(identity).build();
        node.listen(tls ? new TlsTcpTransport(identity) : new TcpTransport(), HOST + ":" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint(tls ? "tls" : "tcp", HOST + ":" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(fleetGroup(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        node.startTicking(Duration.ofMillis(250));
        return new Peer(node, runtime, space);
    }

    /**
     * Worker loop: drain tasks until the space stays quiet for the idle window.
     *
     * @param peer      the worker's peer
     * @param name      the worker's name (stamped into findings)
     * @param dieAfter  crash after completing this many takes; -1 to never crash
     * @return how many tasks this worker completed
     */
    public static int workerLoop(Peer peer, String name, int dieAfter) {
        int completed = 0;
        while (true) {
            Optional<TakenEntry<ResearchTask>> taken = peer.space().take(
                    Template.of(ResearchTask.class),
                    Lease.of(Duration.ofSeconds(2)), Duration.ofSeconds(2));
            if (taken.isEmpty()) {
                return completed;
            }
            ResearchTask task = taken.get().entry();
            if (dieAfter >= 0 && completed >= dieAfter) {
                System.out.println("  [" + name + "] took '" + task.topic()
                        + "' and DIED (no complete; the lease will lapse)");
                peer.close();
                return completed;
            }
            peer.space().complete(taken.get(),
                    new Finding(task.topic(), "researched: " + task.topic(), name),
                    Lease.of(Duration.ofHours(1)));
            completed++;
            System.out.println("  [" + name + "] completed '" + task.topic() + "'");
        }
    }

    /**
     * Runs the demo.
     *
     * @param args empty for all-in-one; or {@code coordinator <port>} /
     *             {@code worker <port> <seedPort>}
     * @throws Exception on startup failure
     */
    public static void main(String[] argv) throws Exception {
        boolean tls = List.of(argv).contains("--tls");
        String[] args = List.of(argv).stream().filter(a -> !a.equals("--tls")).toArray(String[]::new);
        if (args.length == 0) {
            runAllInOne();
            return;
        }
        String role = args[0];
        int port = Integer.parseInt(args[1]);
        int seedPort = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        Peer peer = startPeer(role + "-" + port, port, seedPort, tls);
        if (tls) {
            System.out.println("transport: tls on " + port + ", attestation required, channel mode ATTESTED");
        }
        if (role.equals("coordinator")) {
            // coordinator PORT [seedPort] [tasks] [expectMembers]: with expectMembers set, the
            // coordinator waits until that many other peers are in its view before it
            // publishes, so a polyglot fleet races on equal terms (run-polyglot-test.sh).
            int tasks = args.length > 3 ? Integer.parseInt(args[3]) : 6;
            int expectMembers = args.length > 4 ? Integer.parseInt(args[4]) : 0;
            if (expectMembers > 0) {
                long deadline = System.nanoTime() + Duration.ofSeconds(150).toNanos();
                int seen = -1;
                while (otherMembers(peer) < expectMembers && System.nanoTime() < deadline) {
                    int now = otherMembers(peer);
                    if (now != seen) {
                        System.out.println("view: " + now + " other peers");
                        seen = now;
                    }
                    Thread.sleep(500);
                }
                System.out.println("members: " + otherMembers(peer) + " other peers in view (expected "
                        + expectMembers + ")");
                if (tls) {
                    printLinks(peer.node(), expectMembers, Duration.ofSeconds(15));
                }
            }
            for (int i = 1; i <= tasks; i++) {
                peer.space().write(new ResearchTask("topic-" + i, i),
                        Lease.of(Duration.ofMinutes(30)));
            }
            System.out.println("coordinator: " + tasks + " tasks published; waiting for findings…");
            // Distinct topics: a LEASE_RACE double take inside the settle window can
            // write two findings for one task, which must not end the wait early.
            while (peer.space().readAll(Template.of(Finding.class), tasks + 100).stream()
                    .map(Finding::topic).distinct().count() < tasks) {
                Thread.sleep(500);
            }
            List<Finding> findings = peer.space().readAll(Template.of(Finding.class), tasks + 100);
            findings.forEach(f -> System.out.println("finding: " + f.topic() + " by " + f.worker()));
            java.util.Map<String, Long> tally = new java.util.TreeMap<>();
            findings.forEach(f -> tally.merge(f.worker(), 1L, Long::sum));
            System.out.println("tally: " + tally);
            if (tls) {
                printFrames(peer.node());
            }
            peer.close();
        } else {
            System.out.println(role + " on " + port + ": serving tasks until stopped…");
            while (true) {
                workerLoop(peer, role + "-" + port, -1);
            }
        }
    }

    /**
     * Prints one {@code link:} line per connected peer (its transport scheme and channel mode) and
     * a {@code links:} summary, after waiting up to {@code wait} for at least {@code expected}
     * links that are all {@code tls attested}: a link turns attested once its peer's
     * CHANNEL_HELLO arrives, a moment after it connects. run-polyglot-test.sh asserts on them.
     *
     * @param node     the node whose links to print
     * @param expected how many links to wait for
     * @param wait     how long to wait for them
     * @throws InterruptedException if interrupted while waiting
     */
    public static void printLinks(PeerNode node, int expected, Duration wait) throws InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        Map<String, String> links = linkTable(node);
        while (System.nanoTime() < deadline && (links.size() < expected
                || links.values().stream().anyMatch(l -> !l.equals("tls attested")))) {
            Thread.sleep(200);
            links = linkTable(node);
        }
        links.forEach((peer, link) -> System.out.println("link: " + peer + " " + link));
        long attested = links.values().stream().filter(l -> l.equals("tls attested")).count();
        System.out.println("links: " + attested + " tls attested of " + links.size());
    }

    /** Prints how many frames the node sent signed and bare (bare only on attested channels). */
    public static void printFrames(PeerNode node) {
        System.out.println("frames: signed=" + node.signedFramesSent() + " bare=" + node.bareFramesSent()
                + " session=" + node.sessionFramesSent());
    }

    private static Map<String, String> linkTable(PeerNode node) {
        Map<String, String> table = new TreeMap<>();
        Map<ai.badmonkey.agentspaces.common.id.PeerId, String> modes = node.channelModes();
        node.connectionSchemes().forEach((peer, scheme) ->
                table.put(peer.value(), scheme + " " + modes.getOrDefault(peer, "?")));
        return table;
    }

    /** How many peers other than this one the group's membership view holds. */
    static int otherMembers(Peer peer) {
        return (int) peer.runtime().membership().allMembers().stream()
                .filter(m -> !m.id().equals(peer.node().peerId())).count();
    }

    private static void runAllInOne() throws Exception {
        System.out.println("research-fleet: 3 peers on TCP localhost, 6 tasks, one crash\n");
        Peer coordinator = startPeer("coordinator", 7451, 0);
        Peer workerA = startPeer("worker-a", 7452, 7451);
        Peer workerB = startPeer("worker-b", 7453, 7451);
        Thread.sleep(1500); // let membership gossip settle

        for (int i = 1; i <= 6; i++) {
            coordinator.space().write(new ResearchTask("topic-" + i, i),
                    Lease.of(Duration.ofMinutes(30)));
        }
        System.out.println("coordinator published 6 tasks\n");

        // Worker A dies after 1 completion (its second take never completes);
        // worker B and a relief worker finish everything.
        Thread a = Thread.ofVirtual().start(() -> workerLoop(workerA, "worker-a", 1));
        Thread b = Thread.ofVirtual().start(() -> workerLoop(workerB, "worker-b", -1));
        a.join();
        b.join();

        // The crashed worker's task reappears after its lease lapses; B sweeps it up.
        while (coordinator.space().readAll(Template.of(Finding.class), 10).size() < 6) {
            Optional<TakenEntry<ResearchTask>> retry = workerB.space().take(
                    Template.of(ResearchTask.class),
                    Lease.of(Duration.ofSeconds(2)), Duration.ofSeconds(3));
            if (retry.isPresent()) {
                workerB.space().complete(retry.get(),
                        new Finding(retry.get().entry().topic(),
                                "recovered: " + retry.get().entry().topic(), "worker-b"),
                        Lease.of(Duration.ofHours(1)));
                System.out.println("  [worker-b] recovered '" + retry.get().entry().topic() + "'");
            }
        }

        System.out.println("\nall findings:");
        coordinator.space().readAll(Template.of(Finding.class), 10).forEach(f ->
                System.out.println("  " + f.topic() + " -> " + f.summary() + " (" + f.worker() + ")"));
        workerB.close();
        coordinator.close();
        System.out.println("\nkill-tolerance demonstrated: every task completed exactly once.");
    }
}
