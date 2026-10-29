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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Group E: a space payload above the escalation threshold becomes an artifact
 * of the content swarm, the record carries its root, and a reader's space
 * acquires it through a fetch, fires WRITTEN on completion, and decodes it as
 * if it had been inline; a sealed space seals first and escalates the sealed
 * bytes, which the swarm never seals again.
 */
class EscalationTest {

    public record Scan(String name, byte[] pixels) {
    }

    private static final long THRESHOLD = 1024 * 1024;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zEscalate");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zEscalate", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "escalate",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, ReplicatedSpace data, Content content) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, GroupKey key, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks)
                .random(new Random(seed)).build();
        runtime.onTick(content::tick);
        content.start();
        ReplicatedSpace.Builder data = ReplicatedSpace.builder(runtime, "scans", identity, "scanner")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks)
                .bulk(new SpacePayloads(content), THRESHOLD);
        if (key != null) {
            data.groupKey(key);
        }
        nodes.add(node);
        return new Peer(node, data.build(), content);
    }

    private void tickAll() {
        nodes.forEach(PeerNode::tick);
        clock.advance(Duration.ofMillis(250));
    }

    private boolean runUntil(Duration max, java.util.function.BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            tickAll();
            Thread.sleep(2);
        }
        return true;
    }

    private static byte[] pixels(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    @Test
    void aPayloadAboveTheThresholdEscalatesAndAReaderAcquiresIt() throws Exception {
        Peer a = newPeer("a", 1, null);
        Peer b = newPeer("b", 2, null, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        List<String> written = new CopyOnWriteArrayList<>();
        b.data().notify(Template.of(Scan.class), event -> {
            if (event.kind() == SpaceEvent.Kind.WRITTEN) {
                written.add(event.entry().name());
            }
        }, Lease.of(Duration.ofHours(1)));

        byte[] big = pixels(3 * 1024 * 1024 + 123, 7);
        EntryHandle handle = a.data().write(new Scan("big", big), Lease.of(Duration.ofHours(1)));
        String ref = a.data().recordOf(handle.entryId()).orElseThrow().payloadRef();
        assertThat(ref).as("the record carries a content root").startsWith("bafkrei");
        assertThat(a.content().store().roots()).contains(ref);
        assertThat(a.data().read(Template.of(Scan.class))).as("the writer reads its own entry at once")
                .hasValueSatisfying(scan -> assertThat(scan.pixels()).isEqualTo(big));

        // small payloads keep the block path; an inline one stays inline
        EntryHandle small = a.data().write(new Scan("small", pixels(200_000, 8)), Lease.of(Duration.ofHours(1)));
        assertThat(a.data().recordOf(small.entryId()).orElseThrow().payloadRef()).startsWith("z");
        EntryHandle tiny = a.data().write(new Scan("tiny", pixels(1_000, 9)), Lease.of(Duration.ofHours(1)));
        assertThat(a.data().recordOf(tiny.entryId()).orElseThrow().payloadRef()).isNull();

        assertThat(runUntil(Duration.ofSeconds(30), () -> written.contains("big"))).as(written.toString()).isTrue();
        assertThat(b.data().read(Template.of(Scan.class).where("name", ai.badmonkey.agentspaces.api.space.Matchers.eq("big")),
                Duration.ofSeconds(5)))
                .hasValueSatisfying(scan -> assertThat(scan.pixels()).isEqualTo(big));
        assertThat(b.data().readAll(Template.of(Scan.class), 10)).hasSize(3);
        assertThat(b.content().store().have(ref).cardinality()).isEqualTo(4);
    }

    @Test
    void aSealedSpaceEscalatesSealedBytesAndOpensThemAfterTheSwarm() throws Exception {
        GroupKey key = GroupKey.generate();
        Peer a = newPeer("a", 1, key);
        Peer b = newPeer("b", 2, key, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] big = pixels(2 * 1024 * 1024 + 5, 11);
        EntryHandle handle = a.data().write(new Scan("sealed", big), Lease.of(Duration.ofHours(1)));
        String ref = a.data().recordOf(handle.entryId()).orElseThrow().payloadRef();
        assertThat(ref).startsWith("bafkrei");
        assertThat(a.content().store().manifest(ref).orElseThrow().sealing())
                .as("the space sealed; the swarm carries opaque bytes").isEqualTo("none");

        assertThat(runUntil(Duration.ofSeconds(30), () -> b.data().read(Template.of(Scan.class)).isPresent())).isTrue();
        assertThat(b.data().read(Template.of(Scan.class)))
                .hasValueSatisfying(scan -> assertThat(scan.pixels()).isEqualTo(big));
    }
}
