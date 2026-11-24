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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The content swarm as due work on a started loop (srad/agentspaces-event-loop.md EL-T-08 to EL-T-10; owner answer
 * 5, 2026-11-22): an idle complete holder runs only at its holding's half-life, after the changes its own renewals make
 * in the content space, and at the 5-minute manifest scan, where the tick stepped it every 250 ms and renewed its
 * manifest every 30 s; a manifest the replica lost is rewritten in the run its change wakes.
 */
@Timeout(120)
class ContentDueTest {

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zContentDue");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zContentDue",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "content-due",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private PeerNode node;
    private Content content;
    private ReplicatedSpace space;

    @AfterEach
    void tearDown() {
        if (content != null) {
            content.close();
        }
        if (node != null) {
            node.close();
        }
    }

    private void start() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(time).time(time).randomSeed(3).build();
        node.listen(network.register("a"), "a");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        BlockExchange blocks = new BlockExchange(runtime, codec, time, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        space = ReplicatedSpace.builder(runtime, "content", identity, "content").clock(time)
                .settleWindow(Duration.ZERO).blocks(blocks).build();
        content = Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(time).store(new MemoryPieceStore(time, 0)).blocks(blocks).random(new Random(3))
                .build();
        content.joinLoop(runtime);
        content.start();
        node.startTicking(Duration.ofMillis(250));
        time.advance(Duration.ofSeconds(1));
    }

    private static byte[] artifact() {
        byte[] bytes = new byte[600 * 1024];
        new Random(11).nextBytes(bytes);
        return bytes;
    }

    @Test
    void anIdleCompleteHolderRunsOnlyAtItsDeadlines() throws Exception {
        start();
        Manifest manifest = content.publish(artifact(), PublishOptions.defaults().withPieceSize(256 * 1024));
        time.advance(Duration.ofMinutes(1));
        long before = content.dueWork();
        time.advance(Duration.ofMinutes(10));
        long runs = content.dueWork() - before;
        System.out.printf("[content-due] an idle complete holder: %d runs with work in 10 minutes (the tick: 2,400)%n",
                runs);
        // four renewals at the 5-minute holding's half-life, the repair look after each (its renewal moves the
        // content space), and two 5-minute scans
        assertThat(runs).isBetween(4L, 12L);
        assertThat(space.readAll(Template.of(Manifest.class), 8)).as("the manifest stays").hasSize(1);
        assertThat(new ContentSpaces(space).holdings(manifest.root())).as("the holding was renewed at its half-life,"
                + " so it is still held 11 minutes after a 5-minute lease").hasSize(1);
        assertThat(content.manifestsRepaired()).isZero();
        assertThat(node.loopStats().backstopFoundWork()).isZero();
        assertThat(node.loopStats().wireVisible()).as("EL-O-07, WD-4: the holdings' renewals are wire-visible")
                .anySatisfy(name -> assertThat(name).endsWith("/content"));
        assertThat(manifest.root()).isNotBlank();
    }

    @Test
    void aManifestTheReplicaLostIsRewrittenInTheRunItsChangeWakes() throws Exception {
        start();
        Manifest manifest = content.publish(artifact(), PublishOptions.defaults().withPieceSize(256 * 1024));
        time.advance(Duration.ofMinutes(1));
        Optional<TakenEntry<Manifest>> taken = space.take(Template.of(Manifest.class), Lease.of(Duration.ofMinutes(1)),
                Duration.ZERO);
        assertThat(taken).isPresent();
        space.complete(taken.get()); // the manifest leaves the replica, as a cancel or a lapse would take it
        time.advance(Duration.ofMillis(1));
        assertThat(content.manifestsRepaired()).as("rewritten in the run the change woke").isEqualTo(1);
        assertThat(space.readAll(Template.of(Manifest.class), 8)).extracting(Manifest::root)
                .containsExactly(manifest.root());
        assertThat(node.loopStats().backstopFoundWork()).isZero();
    }

    /**
     * EL-C-04, EL-C-05: the swarm's local periods are on the loop's monotonic base, so a wall step back of an hour
     * moves none of them. The 5-minute manifest scan still runs every 5 minutes of real time; on the wall clock it
     * waited for the wall to catch up, an hour and more.
     */
    @Test
    void aWallStepBackMovesNoLocalPeriod() throws Exception {
        start();
        content.publish(artifact(), PublishOptions.defaults().withPieceSize(256 * 1024));
        time.advance(Duration.ofMinutes(1));
        time.stepWall(Duration.ofHours(-1));
        time.advance(Duration.ofSeconds(1));
        long scans = content.manifestScans();
        time.advance(Duration.ofMinutes(11));
        long ran = content.manifestScans() - scans;
        System.out.printf("[content-due] manifest scans in 11 minutes after a wall step back of an hour: %d%n", ran);
        assertThat(ran).as("the 5-minute scan on the monotonic base").isGreaterThanOrEqualTo(2);
        assertThat(node.loopStats().backstopFoundWork()).isZero();
    }
}
