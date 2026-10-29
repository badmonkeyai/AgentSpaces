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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Content-addressed payloads (spec §7.1): the space replicates only the record,
 * and readers fetch the block from its writer on first touch.
 */
class LargePayloadTest {

    /** An entry whose payload exceeds the 64 KiB inline limit. */
    public record BigEntry(String name, byte[] data) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zBig");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zBig", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "big",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, ReplicatedSpace space, BlockExchange blocks) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, boolean withBlocks,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = withBlocks
                ? new BlockExchange(runtime, CborCodec.defaultCodec()) : null;
        ReplicatedSpace.Builder builder = ReplicatedSpace
                .builder(runtime, "bulk", identity, "writer")
                .clock(clock)
                .settleWindow(Duration.ZERO);
        if (blocks != null) {
            builder.blocks(blocks);
        }
        ReplicatedSpace space = builder.build();
        nodes.add(node);
        return new Peer(node, space, blocks);
    }

    /**
     * Written fails-first: a {@code notify} subscriber must see a content-addressed
     * entry without anyone reading it. The record merges before its block lands,
     * so the WRITTEN event could not be decoded and was silently dropped — and
     * nothing re-fired it when the block arrived, which is why every flagship
     * that watches for large entries polled with {@code readAll} instead of
     * subscribing. The replica now starts the block fetch itself and fires the
     * deferred event once the block is local.
     */
    @Test
    void notifySubscribersSeeLargeEntriesWithoutAnyRead() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        java.util.concurrent.CopyOnWriteArrayList<String> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        b.space().notify(Template.of(BigEntry.class), event -> {
            if (event.kind() == ai.badmonkey.agentspaces.api.space.SpaceEvent.Kind.WRITTEN) {
                seen.add(event.entry().name());
            }
        }, Lease.of(Duration.ofMinutes(30)));

        byte[] data = new byte[150_000];
        new Random(12).nextBytes(data);
        a.space().write(new BigEntry("scan", data), Lease.of(Duration.ofMinutes(30)));
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (seen.isEmpty() && System.nanoTime() < deadline) {
            nodes.forEach(PeerNode::tick);   // no read anywhere: only the fabric moves
            Thread.sleep(20);
        }
        assertThat(seen).as("the subscriber learned of the large entry with no read").containsExactly("scan");
        assertThat(b.blocks().size()).isEqualTo(1);
    }

    @Test
    void largePayloadsTravelByReferenceAndFetchOnRead() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }

        byte[] data = new byte[150_000];
        new Random(11).nextBytes(data);
        a.space().write(new BigEntry("weights", data), Lease.of(Duration.ofMinutes(30)));

        // B's replica has the record; the first read starts the block fetch from
        // A in the background (P7), and a blocking read sees it once it lands.
        assertThat(b.space().read(Template.of(BigEntry.class), Duration.ofSeconds(5)))
                .hasValueSatisfying(entry -> {
                    assertThat(entry.name()).isEqualTo("weights");
                    assertThat(entry.data()).isEqualTo(data);
                });
        assertThat(b.blocks().size()).isEqualTo(1); // cached for the next reader
    }

    /** Spec §7.2 / P7: readAll never waits on a block fetch, while a blocking read awaits the block within its own timeout. */
    @Test
    void readAllNeverBlocksOnAnUnfetchedBlockAndBlockingReadAwaitsIt() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        byte[] data = new byte[120_000];
        new Random(7).nextBytes(data);
        a.space().write(new BigEntry("unfetched", data), Lease.of(Duration.ofMinutes(30)));

        // B holds the record but cannot reach A for the block.
        network.partition("a", "b");
        long started = System.nanoTime();
        List<BigEntry> visible = b.space().readAll(Template.of(BigEntry.class), 10);
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
        assertThat(visible).as("an entry whose block has not arrived is skipped").isEmpty();
        assertThat(elapsedMillis)
                .as("readAll returned without waiting on the network")
                .isLessThan(ReplicatedSpace.BLOCK_FETCH_TIMEOUTS_MILLIS[0] / 2);
        assertThat(b.space().read(Template.of(BigEntry.class))).isEmpty();

        // The block becomes reachable; a blocking read sees the entry once it lands.
        network.heal();
        assertThat(b.space().read(Template.of(BigEntry.class), Duration.ofSeconds(5)))
                .hasValueSatisfying(entry -> {
                    assertThat(entry.name()).isEqualTo("unfetched");
                    assertThat(entry.data()).isEqualTo(data);
                });
        assertThat(b.space().readAll(Template.of(BigEntry.class), 10)).hasSize(1);
    }

    @Test
    void largeWritesWithoutABlockExchangeFailFast() throws Exception {
        Peer lonely = newPeer("solo", 3, false);

        assertThatThrownBy(() -> lonely.space().write(
                new BigEntry("too-big", new byte[100_000]), Lease.of(Duration.ofMinutes(1))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("inline limit");
    }

    // ---- srad/large-networks-bulk-data.md group B (LB-B-08) ----

    /** LB-B-02: a failed background fetch is retried from the group tick with back-off, not only by the next read. */
    @Test
    void blockFetchRetriesOnTheTickUntilTheHolderAnswers() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        byte[] data = new byte[120_000];
        new Random(31).nextBytes(data);
        ai.badmonkey.agentspaces.api.space.EntryHandle written =
                a.space().write(new BigEntry("retried", data), Lease.of(Duration.ofMinutes(30)));
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick); // the record replicates; the block waits for a read
        }
        assertThat(b.space().recordOf(written.entryId(), true)).as("the record replicated").isPresent();

        network.partition("a", "b");
        assertThat(b.space().read(Template.of(BigEntry.class))).isEmpty(); // starts attempt 0
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (b.space().pendingBlockRetries() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(b.space().pendingBlockRetries()).as("attempt 0 failed and queued a retry").isEqualTo(1);

        network.heal();
        nodes.forEach(PeerNode::tick);
        assertThat(b.space().pendingBlockRetries()).as("not due yet: 2 s of back-off").isEqualTo(1);
        clock.advance(Duration.ofMillis(2_100));
        nodes.forEach(PeerNode::tick); // the retry runs here, from the tick
        assertThat(b.space().read(Template.of(BigEntry.class), Duration.ofSeconds(5)))
                .hasValueSatisfying(entry -> assertThat(entry.data()).isEqualTo(data));
        deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (b.space().pendingBlockRetries() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(b.space().pendingBlockRetries()).isZero();
    }

    /** LB-B-06: a payload no frame could carry fails at the writer, with the way out named. */
    @Test
    void payloadAboveTheFrameCapIsRefusedAtWrite() throws Exception {
        Peer a = newPeer("a", 1, true);
        assertThatThrownBy(() -> a.space().write(
                new BigEntry("nine", new byte[9 * 1024 * 1024]), Lease.of(Duration.ofMinutes(1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("content swarm");
        assertThat(a.space().readAll(Template.of(BigEntry.class), 10)).isEmpty();
    }

    /** A 7 MiB payload still crosses: one block, one frame, under the cap. */
    @Test
    void sevenMebibytesTravelAsOneBlock() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        byte[] data = new byte[7 * 1024 * 1024];
        new Random(41).nextBytes(data);
        a.space().write(new BigEntry("seven", data), Lease.of(Duration.ofMinutes(30)));
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        assertThat(b.space().read(Template.of(BigEntry.class), Duration.ofSeconds(15)))
                .hasValueSatisfying(entry -> assertThat(entry.data()).isEqualTo(data));
    }

    /** LB-B-04: a live entry pins its block; collecting the entry unpins it. */
    @Test
    void collectedEntriesReleaseTheirBlocks() throws Exception {
        Peer a = newPeer("a", 1, true);
        ai.badmonkey.agentspaces.api.space.EntryHandle written =
                a.space().write(new BigEntry("short", new byte[100_000]), Lease.of(Duration.ofMinutes(1)));
        String cid = a.space().recordOf(written.entryId()).orElseThrow().payloadRef();
        assertThat(cid).isNotNull();
        assertThat(a.blocks().references(cid)).isEqualTo(1);

        clock.advance(Duration.ofMinutes(1).plusSeconds(1));
        a.space().sweepNow(); // expired, kept as a tombstone
        assertThat(a.blocks().references(cid)).isEqualTo(1);
        clock.advance(Duration.ofHours(3)); // past twice the maximum lease the collector reckons with
        a.space().sweepNow();
        assertThat(a.blocks().references(cid)).as("collected, so released").isZero();
    }
}
