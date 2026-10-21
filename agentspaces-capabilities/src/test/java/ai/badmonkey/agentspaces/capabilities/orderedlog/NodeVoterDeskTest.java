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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.space.replicated.TakeClaim;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * The cross-language ordered desk (srad/agentspaces-js.md JS-V-11; the port
 * of {@code OrderedTakesClusterTest.exactlyOnceSurvivesLeaderFailureMidTake}
 * across languages): two Java voters and one Node voter (the service host of
 * {@code @agentspaces/node} with the voter of {@code @agentspaces/ordered})
 * run the {@code jobs} log; a TypeScript clerk takes a job through it; then
 * the leader — whichever language it is in — is killed, the survivors elect,
 * and the clerk takes the next job through the new leader. Every job is
 * completed exactly once: each surviving voter's commit listener saw one
 * claim per job, and no job remains. Needs Node and the built packages;
 * skipped otherwise unless {@code -Dtsclient.required=true}.
 */
class NodeVoterDeskTest {

    public record Job(String jobId, String payload) {
    }

    public record Done(String jobId, String by) {
    }

    private record Member(PeerNode node, ReplicatedSpace space, RaftLog raft, OrderedTakes ordered,
                          List<TakeClaim> committed, int port) {
    }

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path NODE_DIR = ROOT.resolve("agentspaces-typescript");
    private static final Path VOTER = NODE_DIR.resolve("packages/node/build/test/workers/voter-worker.js");
    private static final Path CLERK = NODE_DIR.resolve("build/test/workers/ordered-worker.js");
    private static final String FOUNDING = "node-voter-desk-v1";
    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<Member> members = new ArrayList<>();
    private final List<Process> processes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Process process : processes) {
            process.destroy();
        }
        for (Member member : members) {
            member.space().close();
            member.node().close();
        }
    }

    private static void require(boolean condition, String why) {
        if (Boolean.getBoolean("tsclient.required")) {
            assertThat(condition).as(why).isTrue();
        }
        Assumptions.assumeTrue(condition, why);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** A process whose stdout lines are kept and echoed, with a waiter for a line matching a predicate. */
    private static final class Worker {
        final Process process;
        final List<String> lines = new CopyOnWriteArrayList<>();
        final String name;

        Worker(String name, Process process) {
            this.name = name;
            this.process = process;
            Thread.ofVirtual().start(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                        System.out.println("  [" + name + "] " + line);
                    }
                } catch (IOException ignored) {
                    // the worker ended
                }
            });
        }

        String await(Predicate<String> wanted, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (String line : lines) {
                    if (wanted.test(line)) {
                        return line;
                    }
                }
                if (!process.isAlive()) {
                    break;
                }
                Thread.sleep(100);
            }
            throw new AssertionError(name + " never printed what was awaited; output:\n" + String.join("\n", lines));
        }
    }

    @Test
    @Timeout(300)
    void aDeskOfTwoJavaVotersAndOneNodeVoterCompletesEveryJobExactlyOnceWhenTheLeaderDies() throws Exception {
        require(Files.exists(VOTER) && Files.exists(CLERK),
                "typescript packages built (npm run build:packages) at " + NODE_DIR);
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement group = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365), FOUNDING,
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        List<PeerIdentity> identities = List.of(PeerIdentity.generate(), PeerIdentity.generate());
        int[] ports = {freePort(), freePort()};
        int nodePort = freePort();
        // The Java nodes and their spaces first, so the Node voter has seeds; the logs once its PeerID is known.
        List<GroupRuntime> runtimes = new ArrayList<>();
        List<ReplicatedSpace> spaces = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            PeerIdentity identity = identities.get(i);
            PeerNode node = PeerNode.builder(identity).build();
            node.listen(new TcpTransport(), "127.0.0.1:" + ports[i]);
            GroupRuntime runtime = node.joinGroup(group,
                    new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                    i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + ports[0], 0)));
            ReplicatedSpace space = ReplicatedSpace.builder(runtime, "jobs", identity, "host")
                    .settleWindow(Duration.ofMillis(150)).build();
            node.startTicking(Duration.ofMillis(250));
            runtimes.add(runtime);
            spaces.add(space);
            members.add(new Member(node, space, null, null, new CopyOnWriteArrayList<>(), ports[i]));
        }
        String javaIds = identities.stream().map(id -> id.peerId().value()).reduce((a, b) -> a + "," + b).orElseThrow();
        Worker voter = new Worker("node voter", new ProcessBuilder("node", VOTER.toString(), "127.0.0.1",
                String.valueOf(ports[0]), String.valueOf(ports[1]), FOUNDING, String.valueOf(nodePort), javaIds)
                .directory(NODE_DIR.toFile()).redirectErrorStream(true).start());
        processes.add(voter.process);
        String up = voter.await(line -> line.contains("\"event\":\"up\""), Duration.ofSeconds(60));
        String nodeId = up.replaceAll(".*\"peerId\":\"([^\"]+)\".*", "$1");
        List<PeerId> memberIds = new ArrayList<>(identities.stream().map(PeerIdentity::peerId).toList());
        memberIds.add(PeerId.of(nodeId));
        for (int i = 0; i < 2; i++) {
            PeerIdentity identity = identities.get(i);
            Member plain = members.get(i);
            OrderedTakes[] holder = new OrderedTakes[1];
            RaftLog raft = new RaftLog(new CapabilityPipes(runtimes.get(i), codec), codec, identity.peerId(), memberIds,
                    Clock.systemUTC(), 7000 + i, (command, index) -> {
                        if (holder[0] != null) {
                            holder[0].onCommand(command, index);
                        }
                    });
            OrderedTakes ordered = new OrderedTakes(raft, spaces.get(i), identity, "host", codec, Clock.systemUTC());
            holder[0] = ordered;
            ordered.onCommitted((entryId, claim) -> plain.committed().add(claim));
            DiscoveryService discovery = new DiscoveryService(runtimes.get(i), new AdCache(codec, Clock.systemUTC()),
                    codec, identity.peerId());
            new CapabilityRuntime(runtimes.get(i), discovery, identity).register(raft);
            members.set(i, new Member(plain.node(), plain.space(), raft, ordered, plain.committed(), plain.port()));
        }
        Member seed = members.get(0);
        seed.space().write(new Job("job-1", "settle the invoice"), Lease.of(Duration.ofMinutes(10)));
        seed.space().write(new Job("job-2", "file the report"), Lease.of(Duration.ofMinutes(10)));

        // The first take: through whichever member leads.
        String leaderBefore = awaitLeader(voter, Duration.ofSeconds(90));
        Set<String> doneBy = runClerk(ports[0], Set.of(), Duration.ofSeconds(90));
        assertThat(doneBy).hasSize(1);
        String firstJob = doneBy.iterator().next();
        assertThat(firstJob).isIn("job-1", "job-2");

        // The leader dies mid-desk: a Java leader is closed, a Node leader is killed.
        String leaderNow = awaitLeader(voter, Duration.ofSeconds(30));
        boolean nodeLed = leaderNow.equals(nodeId);
        int clerkSeed;
        if (nodeLed) {
            voter.process.destroyForcibly();
            voter.process.waitFor(10, TimeUnit.SECONDS);
            clerkSeed = ports[0];
        } else {
            Member dead = members.stream().filter(m -> m.node().peerId().value().equals(leaderNow)).findFirst().orElseThrow();
            dead.space().close();
            dead.node().close();
            clerkSeed = dead == members.get(0) ? ports[1] : ports[0];
        }
        System.out.println("  killed leader " + leaderNow.substring(0, 8) + (nodeLed ? " (node)" : " (java)")
                + ", was " + leaderBefore.substring(0, 8));

        // The survivors elect and the next take commits through the new leader.
        Set<String> secondDone = runClerk(clerkSeed, doneBy, Duration.ofSeconds(150));
        assertThat(secondDone).hasSize(1).doesNotContain(firstJob);

        // Exactly once: each surviving voter's commit listener saw a claim for each job, and every
        // claim it saw for one job is the same claim (holder and epoch). A clerk whose leader died
        // before replying resends its command after its bounded attempt, so the log may carry the
        // same signed claim twice; OrderedTakes.onCommand installs it once and, by design, still
        // tells committed-order listeners of every authentic claim in log order. A second take
        // would show as a second holder or epoch, never as a repeat.
        Member survivor = members.stream().filter(m -> m.node().peerId().value().equals(leaderNow) == false
                && m.raft() != null).findFirst().orElseThrow();
        Map<String, Set<String>> perEntry = new ConcurrentHashMap<>();
        for (TakeClaim claim : survivor.committed()) {
            perEntry.computeIfAbsent(claim.entryId().value(), k -> new java.util.HashSet<>())
                    .add(claim.holder().toString() + "@" + claim.epoch());
        }
        assertThat(perEntry).as("a committed claim for each job at " + survivor.node().peerId()).hasSize(2);
        assertThat(perEntry.values()).as("one holder and epoch per job; a resent command repeats the same claim")
                .allMatch(holders -> holders.size() == 1);
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline && !survivor.space().read(Template.of(Job.class)).isEmpty()) {
            Thread.sleep(200);
        }
        assertThat(survivor.space().read(Template.of(Job.class))).as("every job was completed").isEmpty();
        if (!nodeLed) {
            // The same rule at the Node voter, whose listener also hears a resent claim again.
            Map<String, Set<String>> nodePerEntry = new ConcurrentHashMap<>();
            for (String line : voter.lines) {
                if (line.contains("\"event\":\"committed\"")) {
                    nodePerEntry.computeIfAbsent(line.replaceAll(".*\"entryId\":\"([^\"]+)\".*", "$1"),
                            k -> new java.util.HashSet<>()).add(line.replaceAll(".*\"holder\":\"([^\"]+)\".*", "$1")
                            + "@" + line.replaceAll(".*\"epoch\":(\\d+).*", "$1"));
                }
            }
            assertThat(nodePerEntry).as("the Node voter committed a claim for each job").hasSize(2);
            assertThat(nodePerEntry.values()).as("one holder and epoch per job at the Node voter")
                    .allMatch(holders -> holders.size() == 1);
            voter.process.getOutputStream().write("stop\n".getBytes(StandardCharsets.UTF_8));
            voter.process.getOutputStream().flush();
            assertThat(voter.process.waitFor(30, TimeUnit.SECONDS)).as("the Node voter drained on stop").isTrue();
        }
    }

    /** The current leader's PeerID: a Java member's when one leads, else what the Node voter's last state line says. */
    private String awaitLeader(Worker voter, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            for (Member member : members) {
                if (member.raft() != null && member.raft().isLeader()) {
                    return member.node().peerId().value();
                }
            }
            String state = voter.lines.stream().filter(l -> l.contains("\"event\":\"state\"")).reduce((a, b) -> b).orElse("");
            if (state.contains("\"role\":\"leader\"")) {
                return state.replaceAll(".*\"leader\":\"([^\"]+)\".*", "$1");
            }
            Thread.sleep(200);
        }
        throw new AssertionError("no leader elected; node voter said:\n" + String.join("\n", voter.lines));
    }

    /** Runs the TypeScript clerk once against a seed; returns the job ids of the Done entries it added. */
    private Set<String> runClerk(int seedPort, Set<String> already, Duration timeout) throws Exception {
        Worker clerk = new Worker("ts clerk", new ProcessBuilder("node", CLERK.toString(), "127.0.0.1",
                String.valueOf(seedPort), FOUNDING, Job.class.getName() + "#v1", Done.class.getName() + "#v1",
                String.valueOf(timeout.toSeconds())).directory(NODE_DIR.toFile()).redirectErrorStream(true).start());
        processes.add(clerk.process);
        assertThat(clerk.process.waitFor(timeout.toSeconds() + 30, TimeUnit.SECONDS)).as("the clerk finished").isTrue();
        assertThat(clerk.process.exitValue()).as(String.join("\n", clerk.lines)).isZero();
        Set<String> done = new java.util.HashSet<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && done.size() <= already.size()) {
            done.clear();
            for (Member member : members) {
                if (member.raft() == null) {
                    continue;
                }
                try {
                    for (Done d : member.space().readAll(Template.of(Done.class), 10)) {
                        done.add(d.jobId());
                    }
                } catch (RuntimeException closed) {
                    // a dead member's space
                }
            }
            Thread.sleep(200);
        }
        done.removeAll(already);
        return done;
    }
}
