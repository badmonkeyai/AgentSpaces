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
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.content.Fetch;
import ai.badmonkey.agentspaces.content.PublishOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The fleet of example 29 under the gate: real TCP on free loopback ports. */
class ContentSwarmFlowTest {

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static boolean unionCovers(List<ContentSwarmFleet.Peer> workers, Manifest manifest) {
        java.util.BitSet union = new java.util.BitSet();
        for (ContentSwarmFleet.Peer w : workers) {
            union.or(w.store().have(manifest.root()));
        }
        return union.cardinality() >= manifest.pieces();
    }

    private static byte[] readAll(Fetch fetch) throws Exception {
        try (SeekableByteChannel channel = fetch.open()) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            return buffer.array();
        }
    }

    @Test
    void workersCompleteEachOtherAfterTheTrainerDiesAndABridgeSharesIntoASecondGroup(@TempDir Path dir) throws Exception {
        int trainerPort = freePort();
        int bridgePort = freePort();
        int size = 6 * 1024 * 1024 + 321;
        // the trainer is a slow link: it answers one range at a time, 10 ms apart. Over loopback three
        // workers could otherwise each take the whole artifact from it in under a second, before it
        // dies; this way the swarm holds every piece after about one copy, and the rest moves between
        // the workers
        Object link = new Object();
        ai.badmonkey.agentspaces.content.ContentPolicy slowLink = new ai.badmonkey.agentspaces.content.ContentPolicy() {
            @Override
            public boolean mayServe(ai.badmonkey.agentspaces.common.id.GroupId group,
                                    ai.badmonkey.agentspaces.common.id.PeerId requester, Manifest manifest) {
                synchronized (link) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return true;
            }
        };
        ContentSwarmFleet.Peer trainer = ContentSwarmFleet.start("trainer", java.util.Map.of("tcp", trainerPort),
                java.util.Set.of(), List.of(), null, dir.resolve("t"), slowLink);
        try (ContentSwarmFleet.Peer w1 = ContentSwarmFleet.start("worker-1", freePort(),
                     List.of("127.0.0.1:" + trainerPort), null, dir.resolve("w1"));
             ContentSwarmFleet.Peer w2 = ContentSwarmFleet.start("worker-2", freePort(),
                     List.of("127.0.0.1:" + trainerPort), null, dir.resolve("w2"));
             ContentSwarmFleet.Peer bridge = ContentSwarmFleet.start("bridge", bridgePort,
                     List.of("127.0.0.1:" + trainerPort), List.of(), dir.resolve("b"));
             ContentSwarmFleet.Peer mission = ContentSwarmFleet.start("mission", freePort(), List.of(),
                     List.of("127.0.0.1:" + bridgePort), dir.resolve("m"))) {
            for (int i = 0; i < 100 && w1.runtime().membership().allMembers().size() < 3; i++) {
                Thread.sleep(100);
            }
            assertThat(w1.runtime().membership().allMembers()).hasSizeGreaterThanOrEqualTo(3);

            byte[] checkpoint = ContentSwarmFleet.artifact(size, 29);
            Manifest manifest = trainer.content().publish(checkpoint,
                    PublishOptions.defaults().withPieceSize(ContentSwarmFleet.PIECE_SIZE).withName("checkpoint.bin"));
            Fetch f1 = w1.content().fetch(manifest.root());
            Fetch f2 = w2.content().fetch(manifest.root());
            Fetch f3 = bridge.content().fetch(manifest.root());
            List<Fetch> fetches = List.of(f1, f2, f3);

            // kill the trainer once the workers together hold every piece (each holds a share), then let
            // them finish each other; a seeder that dies before the swarm holds a piece takes it with it
            List<ContentSwarmFleet.Peer> workers = List.of(w1, w2, bridge);
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline && !(unionCovers(workers, manifest)
                    && fetches.stream().allMatch(f -> f.progress().piecesVerified() >= 3))) {
                Thread.sleep(50);
            }
            assertThat(unionCovers(workers, manifest)).as("the swarm holds every piece somewhere").isTrue();
            trainer.close(); // over loopback the workers may already be done; the property holds either way
            for (Fetch f : fetches) {
                boolean done = f.await(Duration.ofSeconds(90));
                if (!done) {
                    for (ContentSwarmFleet.Peer p : List.of(w1, w2, bridge)) {
                        System.err.printf("%s: dropped=%d stale=%d replayed=%d received=%s sent=%s content=%s%n", p.name(),
                                p.node().framesDropped(), p.node().framesStale(), p.node().framesReplayed(),
                                p.node().traffic().receivedByKind(), p.node().traffic().sentByKind(), p.content().stats());
                    }
                    System.err.printf("trainer: dropped=%d stale=%d replayed=%d received=%s sent=%s content=%s%n",
                            trainer.node().framesDropped(), trainer.node().framesStale(), trainer.node().framesReplayed(),
                            trainer.node().traffic().receivedByKind(), trainer.node().traffic().sentByKind(), trainer.content().stats());
                }
                assertThat(done).as(f.trace().toString()).isTrue();
                assertThat(readAll(f)).isEqualTo(checkpoint);
            }
            long fromTrainer = fetches.stream()
                    .mapToLong(f -> f.progress().bytesBySource().getOrDefault(trainer.node().peerId(), 0L)).sum();
            assertThat(fromTrainer).as("some of the bytes came from the trainer before it died").isGreaterThan(0);
            assertThat(fromTrainer).as("and the rest from each other").isLessThan(3L * checkpoint.length);

            Manifest shared = bridge.content(ContentSwarmFleet.MISSION_FOUNDING)
                    .share(manifest.root(), Lease.of(Duration.ofHours(1)));
            assertThat(shared.root()).isEqualTo(manifest.root());
            Fetch onMission = mission.content(ContentSwarmFleet.MISSION_FOUNDING).fetch(manifest.root());
            assertThat(onMission.await(Duration.ofSeconds(90))).as(onMission.trace().toString()).isTrue();
            assertThat(readAll(onMission)).isEqualTo(checkpoint);
            assertThat(onMission.progress().bytesBySource()).containsOnlyKeys(bridge.node().peerId());
        }
    }
}
