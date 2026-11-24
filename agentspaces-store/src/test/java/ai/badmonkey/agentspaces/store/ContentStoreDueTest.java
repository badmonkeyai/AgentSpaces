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

package ai.badmonkey.agentspaces.store;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The content store as due work on a started loop (srad/agentspaces-event-loop.md EL-T-08, EL-T-09):
 * {@link ContentStore#joinLoop} ticks an idle store only at its card refresh (half the card's TTL, owner answer 6 of
 * 2026-11-22) and its reconcile turn, where the {@code onTick} adapter ticked it every 250 ms and part 1 every 10 s
 * holding interval, and nothing it waits for is found by the loop's backstop.
 */
@Timeout(180)
class ContentStoreDueTest {

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zStoreDue");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zStoreDue",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "store-due",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closers = new ArrayList<>();

    private record Member(PeerNode node, GroupRuntime runtime, ContentStore store) {
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : closers.reversed()) {
            c.close();
        }
        nodes.forEach(PeerNode::close);
    }

    private Member member(String name, String seed, boolean store) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        nodes.add(node);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        BlockExchange blocks = new BlockExchange(runtime, codec, time, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, time), codec, identity.peerId());
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content").clock(time)
                .settleWindow(Duration.ZERO).blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(runtime, "refs", identity, "refs").clock(time)
                .settleWindow(Duration.ZERO).build();
        Pdt pdt = Pdt.builder(refSpace).store(new NodeStore(new MemoryBlockStore(), time)).clock(time)
                .loadTimeout(Duration.ofSeconds(30)).discovery(discovery).identity(identity).build();
        Content.Builder contentBuilder = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace,
                        identity.peerId()).codec(codec).clock(time).store(new MemoryPieceStore(time, 0)).blocks(blocks)
                .random(new Random(name.hashCode())).treeStore(pdt.treeStore());
        if (store) {
            contentBuilder.pdtLevel(Content.PDT_LEVEL);
        }
        Content content = contentBuilder.build();
        content.joinLoop(runtime);
        content.start();
        pdt.attach(content).start();
        pdt.joinLoop(runtime);
        closers.add(content);
        closers.add(pdt);
        ContentStore contentStore = null;
        if (store) {
            contentStore = ContentStore.builder(pdt, discovery, identity).name(name).policy(StorePolicy.of(1L << 30))
                    .clock(time).authorizer(StoreFleet.GRANT_ALL).build();
            contentStore.joinLoop(runtime);
            contentStore.start();
            closers.add(contentStore);
        }
        node.startTicking(Duration.ofMillis(250));
        return new Member(node, runtime, contentStore);
    }

    private void form(Member a, Member b) {
        long waited = 0;
        while (a.runtime().membership().allMembers().isEmpty() || b.runtime().membership().allMembers().isEmpty()) {
            time.advance(Duration.ofMillis(250));
            waited += 250;
            assertThat(waited).isLessThan(60_000);
        }
    }

    @Test
    void anIdleStoreTicksAtItsCardRefreshAndReconcileTurnOnlyAndTheBackstopFindsNothing() throws Exception {
        Member a = member("a", null, false);
        Member b = member("b", "a", true);
        form(a, b);
        time.advance(Duration.ofMinutes(1));

        long before = b.store().dueTicks();
        time.advance(Duration.ofMinutes(10));
        long idle = b.store().dueTicks() - before;
        System.out.printf("[store-due] idle 10 min: store ticks %d (the adapter: 2,400; one per %s holding interval: 60;"
                + " the card at half its %s TTL and the reconcile turn: 2)%n", idle,
                ContentStore.DEFAULT_HOLDING_INTERVAL, ContentStore.CARD_TTL);
        // owner answer 6 (2026-11-22): an idle store with no holding and no watched take publishes its card at half
        // its 15-minute TTL (7.5 min after start) and takes its reconcile turn (10 min after start); nothing else
        assertThat(idle).as("the card refresh and the reconcile turn").isEqualTo(2L);
        assertThat(b.node().loopStats().backstopFoundBy()).as("no missed wake on the store").isEmpty();
        assertThat(b.node().loopStats().wireVisible()).as("EL-O-07, WD-4: the store's card re-issue is wire-visible")
                .anySatisfy(name -> assertThat(name).endsWith("/store"));
        assertThat(a.node().loopStats().backstopFoundBy()).as("no missed wake on the member").isEmpty();
    }

    /**
     * EL-W-04 (srad/agentspaces-event-loop.md, §10.47): an idle store's take loop waits for a change of what it judges,
     * under the 30 s backstop, instead of scanning the pin space every 200 ms.
     */
    @Test
    void anIdleStoresTakeLoopScansAtTheBackstopNotFiveTimesASecond() throws Exception {
        Member a = member("a", null, false);
        Member b = member("b", "a", true);
        form(a, b);
        time.advance(Duration.ofMinutes(1));
        long before = b.store().scans();
        time.advance(Duration.ofMinutes(10));
        long idle = b.store().scans() - before;
        System.out.printf("[store-due] idle 10 min: pin scans %d (a 200 ms pause: 3,000; the 30 s backstop: 20)%n", idle);
        assertThat(idle).as("about one scan per backstop, plus the changes the space signalled").isLessThanOrEqualTo(40L);
    }
}
