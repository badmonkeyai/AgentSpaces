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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P28 (reviews/RBE-REGRESSION-MATRIX.md §3; srad/replication-by-entry.md RE-I-04, RE-I-05): a late joiner of a content
 * swarm whose content space arrives as many answers cut to a 4 KiB budget, newest first, so a manifest may land before
 * the holdings that name its sources, or after. Every root still fetches whole and verified, and the test prints how
 * long the join and the fetches took.
 */
class ContentBudgetedJoinTest {

    private static final int PIECE = 64 * 1024; // the smallest piece size a publish takes
    private static final long SMALL = 4096;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zContentBudget", PeerIdentity.generate().peerId(), GroupId.of("zContentBudget"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(String name, PeerNode node, GroupRuntime runtime, ReplicatedSpace space, Content content) {
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.content().close();
            p.node().close();
        });
    }

    private Peer newPeer(String address, long seed, long budget, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).auditBudget(budget).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        CapabilityPipes pipes = new CapabilityPipes(runtime, codec);
        Content content = Content.builder(pipes, runtime, space, identity.peerId()).codec(codec).clock(clock)
                .store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed)).build();
        runtime.onTick(content::tick);
        content.start();
        Peer peer = new Peer(address, node, runtime, space, content);
        peers.add(peer);
        return peer;
    }

    private void tickAll() {
        peers.forEach(p -> p.node().tick());
        clock.advance(Duration.ofMillis(250));
    }

    private boolean runUntil(Duration max, BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            tickAll();
            Thread.sleep(1);
        }
        return true;
    }

    private static byte[] readAll(Fetch fetch) throws IOException {
        try (SeekableByteChannel channel = fetch.open()) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            return buffer.array();
        }
    }

    @Test
    void aLateJoinerWhoseContentSpaceArrivesInBudgetedAnswersFetchesEveryRoot() throws Exception {
        Peer a = newPeer("a", 1, SMALL);
        Peer b = newPeer("b", 2, SMALL, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        List<byte[]> artifacts = new ArrayList<>();
        List<Manifest> manifests = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            byte[] artifact = new byte[2 * PIECE + 100 * i];
            new Random(i).nextBytes(artifact);
            artifacts.add(artifact);
            manifests.add(a.content().publish(artifact,
                    PublishOptions.defaults().withPieceSize(PIECE).withName("artifact-" + i + ".bin")));
        }
        // b holds a few, so the space carries holdings from two members beside the manifests.
        for (int i = 0; i < 4; i++) {
            Fetch fetch = b.content().fetch(manifests.get(i).root());
            assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).isTrue();
        }
        assertThat(runUntil(Duration.ofSeconds(20),
                () -> b.space().knownEntries() == a.space().knownEntries())).isTrue();
        long entries = a.space().knownEntries();

        long joining = System.nanoTime();
        Peer late = newPeer("l", 3, SMALL, "b");
        assertThat(runUntil(Duration.ofSeconds(60), () -> late.space().knownEntries() >= entries))
                .as("the late joiner's content space converges").isTrue();
        double joinSeconds = (System.nanoTime() - joining) / 1e9;
        GossipBus.AuditSignals signals = late.runtime().gossip().auditSignals();
        GossipBus.PacingSignals pacing = late.runtime().gossip().pacingSignals();
        assertThat(signals.remainderReceived()).as("the space came in answers cut to the budget").isPositive();

        long fetching = System.nanoTime();
        for (int i = 0; i < manifests.size(); i++) {
            Fetch fetch = late.content().fetch(manifests.get(i).root());
            assertThat(runUntil(Duration.ofSeconds(30), fetch::complete)).as(fetch.trace().toString()).isTrue();
            assertThat(readAll(fetch)).isEqualTo(artifacts.get(i));
        }
        double fetchSeconds = (System.nanoTime() - fetching) / 1e9;
        System.out.printf("content budgeted join (P28): %d content entries joined under a %,d-byte budget in %.2f s "
                        + "wall (%,d PULL_RESP, %,d delta bytes, %,d ids asked, %,d continued at once); %d roots "
                        + "fetched in %.2f s wall; the fabric's clock moves 250 ms a tick%n", entries, SMALL,
                joinSeconds, pacing.pullRespReceived(), pacing.pullRespBytesReceived(), signals.idsFetched(),
                signals.fetchContinued(), manifests.size(), fetchSeconds);
    }
}
