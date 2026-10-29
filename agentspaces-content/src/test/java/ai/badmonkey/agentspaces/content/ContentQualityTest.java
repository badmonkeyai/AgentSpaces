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

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The measured quality rows of srad/large-networks-bulk-data.md §5 on the simulated network
 * (LB-Q-PRF-02, -03, -05, -06, -08; LB-Q-REL-03; LB-Q-AVL-03; scenario 3 for LB-Q-TST-01).
 * Each prints what it measured, so a regression shows the number as well as the failure.
 */
class ContentQualityTest {

    private static final int PIECE = 65_536;
    private static final Set<PeerAdvertisement.PeerRole> RELAY = Set.of(PeerAdvertisement.PeerRole.RELAY);

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<Content> contents = new ArrayList<>();
    private final Set<PeerNode> dead = new java.util.HashSet<>();

    private record Member(String address, PeerNode node, GroupRuntime runtime, ReplicatedSpace space, Content content) {
    }

    @AfterEach
    void tearDown() {
        contents.forEach(Content::close);
        nodes.forEach(PeerNode::close);
    }

    private static GroupAdvertisement group(String id, Duration gossip) {
        return new GroupAdvertisement("aspace://" + id, PeerIdentity.generate().peerId(), GroupId.of(id),
                Instant.EPOCH, Duration.ofDays(1), id, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, new GroupAdvertisement.GossipParameters(3, gossip));
    }

    private Member member(GroupAdvertisement ad, String address, boolean listening, Set<PeerAdvertisement.PeerRole> roles,
                          GroupMembership.Config config, PeerNode.ChannelAuth auth, ContentPolicy policy,
                          String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode.Builder builder = PeerNode.builder(identity).clock(clock).randomSeed(address.hashCode()).roles(roles);
        if (auth != null) {
            builder.channelAuth(auth);
        }
        PeerNode node = builder.build();
        if (listening) {
            node.listen(network.register(address), address);
        } else {
            node.transport(network.register(address));
        }
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", seed, 0));
        }
        GroupRuntime runtime = node.joinGroup(ad, config, endpoints);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        Content.Builder cb = Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks)
                .random(new Random(address.hashCode()));
        if (policy != null) {
            cb.policy(policy);
        }
        Content content = cb.build();
        runtime.onTick(content::tick);
        content.start();
        nodes.add(node);
        contents.add(content);
        return new Member(address, node, runtime, space, content);
    }

    private Member member(GroupAdvertisement ad, String address, String... seeds) throws IOException {
        return member(ad, address, true, Set.of(), GroupMembership.Config.defaults(), null, null, seeds);
    }

    private void tick(int rounds) {
        for (int i = 0; i < rounds; i++) {
            for (PeerNode node : nodes) {
                if (!dead.contains(node)) {
                    node.tick();
                }
            }
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private void kill(Member m) {
        dead.add(m.node());
        m.content().close();
        m.node().close();
    }

    private static byte[] bytes(int length, long seed) {
        byte[] out = new byte[length];
        new Random(seed).nextBytes(out);
        return out;
    }

    private boolean contentFrame(byte[] frame) {
        try {
            WireCodec.Signed signed = codec.fromBytes(frame, WireCodec.Signed.class);
            if (signed == null || signed.envelope() == null) {
                return false;
            }
            Envelope env = signed.envelope();
            if (env.kind() == Envelope.Kind.RELAY_FRAME) {
                return true; // a relayed frame to a bystander would be traffic too
            }
            if (env.kind() != Envelope.Kind.PIPE_DATA) {
                return false;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> pipe = codec.fromBytes(env.body(), Map.class);
            return Content.TYPE.equals(pipe.get("capability"));
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Test
    @Timeout(120)
    void holdingTrafficPerFetcherIsBoundedAndBystandersHearNoContentFrames() throws Exception {
        GroupAdvertisement ad = group("zQualityHoldings", Duration.ofSeconds(1));
        Member a = member(ad, "a");
        Member b = member(ad, "b", "a");
        Member c = member(ad, "c", "a");
        tick(6);
        int pieces = 40;
        Manifest manifest = a.content().publish(bytes(pieces * PIECE, 1), PublishOptions.defaults().withPieceSize(PIECE));
        AtomicInteger holdingWrites = new AtomicInteger();
        a.space().notify(Template.of(Holding.class), event -> {
            if (event.kind() == SpaceEvent.Kind.WRITTEN && event.entry().holder().equals(b.node().peerId())) {
                holdingWrites.incrementAndGet();
            }
        }, Lease.of(Duration.ofHours(1)));
        List<byte[]> toC = new CopyOnWriteArrayList<>();
        network.tap("a", "c", toC::add);
        network.tap("b", "c", toC::add);
        long started = clock.millis();
        Fetch fetch = b.content().fetch(manifest.root());
        for (int i = 0; i < 120 && !fetch.complete(); i++) {
            tick(1);
        }
        assertThat(fetch.complete()).as(fetch.trace().toString()).isTrue();
        tick(2);
        network.untap("a", "c");
        network.untap("b", "c");
        long seconds = Math.max(1, (clock.millis() - started) / 1000);
        long budget = (seconds + 9) / 10 + (pieces + 31) / 32 + 1;
        System.out.printf("LB-Q-PRF-02: %d holding writes by the fetcher over %d s and %d pieces (budget %d)%n",
                holdingWrites.get(), seconds, pieces, budget);
        assertThat(holdingWrites.get()).as("LB-Q-PRF-02").isLessThanOrEqualTo((int) budget);
        long contentToBystander = toC.stream().filter(this::contentFrame).count();
        System.out.printf("LB-Q-PRF-03: %d content frames reached the bystander%n", contentToBystander);
        assertThat(contentToBystander).as("LB-Q-PRF-03").isZero();
        assertThat(c.content().stats().bytesServed()).isZero();
    }

    @Test
    @Timeout(120)
    void aFetchHoldsAtMostSessionsTimesWindowRangesInFlight() throws Exception {
        GroupAdvertisement ad = group("zQualityMemory", Duration.ofSeconds(1));
        Member a = member(ad, "a");
        Member b = member(ad, "b", "a");
        Member c = member(ad, "c", "a");
        Member d = member(ad, "d", "a");
        tick(6);
        Manifest manifest = a.content().publish(bytes(48 * PIECE, 2), PublishOptions.defaults().withPieceSize(PIECE));
        for (Member m : List.of(b, c)) {
            Fetch f = m.content().fetch(manifest.root());
            for (int i = 0; i < 120 && !f.complete(); i++) {
                tick(1);
            }
            assertThat(f.complete()).isTrue();
        }
        network.hold("a", "d"); // answers queue so ranges stay in flight while the window opens
        network.hold("b", "d");
        network.hold("c", "d");
        FetchSession fetch = (FetchSession) d.content().fetch(manifest.root());
        int max = 0;
        for (int i = 0; i < 30; i++) {
            tick(1);
            max = Math.max(max, fetch.inFlightRanges());
            network.release("a", "d", SimNetwork.Order.FIFO, 1);
            network.release("b", "d", SimNetwork.Order.FIFO, 1);
            network.release("c", "d", SimNetwork.Order.FIFO, 1);
        }
        int bound = ContentPolicy.DEFAULT.maxSessions() * ContentPolicy.DEFAULT.maxWindow();
        System.out.printf("LB-Q-PRF-06: at most %d ranges in flight (bound %d sessions x window = %d; %d bytes per range at most)%n",
                max, ContentPolicy.DEFAULT.maxSessions(), bound, PIECE);
        assertThat(max).as("LB-Q-PRF-06").isPositive().isLessThanOrEqualTo(bound);
    }

    @Test
    @Timeout(180)
    void aFourMinutePartitionAfterALossyLinkCompletesWithinTheHoldingLeaseAfterHeal() throws Exception {
        GroupAdvertisement ad = group("zQualityPartition", Duration.ofSeconds(1));
        Member a = member(ad, "a");
        Member b = member(ad, "b", "a");
        tick(6);
        Manifest manifest = a.content().publish(bytes(24 * PIECE, 3), PublishOptions.defaults().withPieceSize(PIECE));
        // a lossy link: one second in ten fades out entirely (frames sent then are lost, not garbled)
        Random loss = new Random(7);
        Fetch fetch = b.content().fetch(manifest.root());
        for (int i = 0; i < 30 && fetch.progress().piecesVerified() < 4; i++) {
            boolean fade = loss.nextInt(10) == 0;
            if (fade) {
                network.partition("a", "b");
            }
            tick(1);
            if (fade) {
                network.heal();
            }
        }
        network.partition("a", "b");
        tick(240); // four minutes apart
        int during = fetch.progress().piecesVerified();
        network.heal();
        long healed = clock.millis();
        for (int i = 0; i < 300 && !fetch.complete(); i++) {
            tick(1);
        }
        long seconds = (clock.millis() - healed) / 1000;
        System.out.printf("LB-Q-REL-03: %d of %d pieces before the heal, complete %d s after it (lease 300 s)%n",
                during, manifest.pieces(), seconds);
        assertThat(fetch.complete()).as(fetch.trace().toString()).isTrue();
        assertThat(seconds).as("LB-Q-REL-03").isLessThanOrEqualTo(300);
    }

    @Test
    @Timeout(240)
    void aFetchCrossesThreeRelaysAndCarriesOnFromADirectSourceWhenARelayDies() throws Exception {
        GroupAdvertisement ad = group("zQualityRelays", Duration.ofSeconds(1));
        GroupMembership.Config config = GroupMembership.Config.defaults();
        Member hq = member(ad, "hq", true, RELAY, config, null, null);
        Member ra = member(ad, "ra", true, RELAY, config, null, null, "hq");
        Member rb = member(ad, "rb", true, RELAY, config, null, null, "hq");
        network.partition("ra", "rb");
        network.partition("a1", "rb");
        network.partition("a1", "hq");
        network.partition("a1", "b1");
        network.partition("b1", "ra");
        network.partition("b1", "hq");
        Member a1 = member(ad, "a1", false, Set.of(), config, null, null, "ra");
        Member b1 = member(ad, "b1", false, Set.of(), config, null, null, "rb");
        // the reach hints cross four hops before a path exists; the hop count says when
        for (int i = 0; i < 200 && a1.runtime().hopsTo(b1.node().peerId()) != 3; i++) {
            tick(1);
        }
        // scenario 3: two sites, three relays
        byte[] tiles = bytes(6 * PIECE + 11, 4);
        Manifest manifest = b1.content().publish(tiles, PublishOptions.defaults().withPieceSize(PIECE).withName("tiles"));
        for (int i = 0; i < 60 && b1.content().describe(manifest.root()).isEmpty(); i++) {
            tick(1);
        }
        Fetch across = a1.content().fetch(manifest.root());
        for (int i = 0; i < 180 && !across.complete(); i++) {
            tick(1);
        }
        assertThat(across.complete()).as(across.trace().toString()).isTrue();
        assertThat(a1.runtime().hopsTo(b1.node().peerId())).isEqualTo(3);
        assertThat(across.progress().bytesBySource()).containsOnlyKeys(b1.node().peerId());
        assertThat(hq.node().relayBytesByKind(ad.group())).containsKey("PIPE_DATA:" + Content.TYPE);
        System.out.printf("scenario 3: %d bytes across three relays; hq carried %s%n", tiles.length,
                hq.node().relayBytesByKind(ad.group()));

        // LB-Q-AVL-03: a direct holder on a1's side, then the far site's relay dies
        // c listens on a1's side: a1 dials it directly, and b1 reaches it too
        Member c = member(ad, "c", true, Set.of(), config, null, null, "ra");
        tick(10);
        byte[] maps = bytes(5 * PIECE + 3, 5);
        Manifest second = b1.content().publish(maps, PublishOptions.defaults().withPieceSize(PIECE).withName("maps"));
        Fetch onC = c.content().fetch(second.root());
        for (int i = 0; i < 180 && !onC.complete(); i++) {
            tick(1);
        }
        assertThat(onC.complete()).as(onC.trace().toString()).isTrue();
        kill(rb);
        tick(5);
        Fetch onA1 = a1.content().fetch(second.root());
        for (int i = 0; i < 180 && !onA1.complete(); i++) {
            tick(1);
        }
        assertThat(onA1.complete()).as(onA1.trace().toString()).isTrue();
        assertThat(onA1.progress().bytesBySource().getOrDefault(c.node().peerId(), 0L)).isEqualTo((long) maps.length);
        assertThat(onA1.progress().bytesBySource().getOrDefault(b1.node().peerId(), 0L)).isZero();
    }

    @Test
    @Timeout(240)
    void aCatalogueOfTenThousandManifestsStaysSmallAndACollectionPagesFortyThousandRowsQuickly() throws Exception {
        GroupAdvertisement ad = group("zQualityCatalogue", Duration.ofSeconds(1));
        ContentPolicy quiet = new ContentPolicy() {
            @Override
            public SeedMode seedMode() {
                return SeedMode.NEVER;
            }
        };
        Member a = member(ad, "a", true, Set.of(), GroupMembership.Config.defaults(), null, quiet);
        Member reader = member(ad, "reader", "a");
        tick(4);
        System.gc();
        long before = usedHeap();
        for (int i = 0; i < 10_000; i++) {
            byte[] data = new byte[64];
            data[0] = (byte) i;
            data[1] = (byte) (i >> 8);
            a.content().publish(data, PublishOptions.defaults().withPieceSize(PIECE).withName("tile-" + i)
                    .withAttrs(Map.of("sector", String.valueOf(i % 50))));
        }
        tick(2);
        assertThat(reader.content().list(20_000)).hasSize(10_000);
        System.gc();
        long cost = usedHeap() - before;
        System.out.printf("LB-Q-PRF-08: 10,000 manifests on two replicas cost %.1f MiB of heap%n", cost / 1024.0 / 1024.0);
        assertThat(cost).as("LB-Q-PRF-08 replica cost").isLessThan(2L * 100 * 1024 * 1024);

        for (int i = 10_000; i < 40_000; i++) {
            byte[] data = new byte[64];
            data[0] = (byte) i;
            data[1] = (byte) (i >> 8);
            data[2] = (byte) (i >> 16);
            a.content().publish(data, PublishOptions.defaults().withPieceSize(PIECE).withReplicate(false));
        }
        CollectionAssetProvider provider = new CollectionAssetProvider(a.content(),
                List.of(CollectionAssetProvider.Collection.all("tiles", "Map tiles.")));
        provider.query(CollectionAssetProvider.assetName("tiles"), Map.of(CollectionAssetProvider.PARAM_LIMIT, "100")); // warm
        long start = System.nanoTime();
        var page = provider.query(CollectionAssetProvider.assetName("tiles"), Map.of(CollectionAssetProvider.PARAM_LIMIT, "100"));
        long millis = (System.nanoTime() - start) / 1_000_000;
        System.out.printf("LB-Q-PRF-08: a page of %d rows over %d roots in %d ms%n", page.rows().size(),
                a.content().store().roots().size(), millis);
        assertThat(page.rows()).hasSize(100);
        assertThat(millis).as("LB-Q-PRF-08 query").isLessThan(200);
    }

    private static long usedHeap() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    @Test
    @Timeout(900)
    void fiftyIdleMembersWithTheSwarmOnCostWhatTheyCostWithoutIt() throws Exception {
        GroupAdvertisement ad = group("zQualityFifty", Duration.ofSeconds(1));
        GroupMembership.Config config = new GroupMembership.Config(Duration.ofSeconds(20), Duration.ofSeconds(2), 2);
        List<Member> members = new ArrayList<>();
        members.add(member(ad, "n0", true, Set.of(), config, PeerNode.ChannelAuth.SESSION, null));
        for (int i = 1; i < 50; i++) {
            members.add(member(ad, "n" + i, true, Set.of(), config, PeerNode.ChannelAuth.SESSION, null, "n0"));
        }
        int settled = -1;
        for (int t = 1; t <= 300; t++) {
            tick(1);
            if (members.stream().allMatch(m -> m.runtime().membership().allMembers().size() >= 49)) { // a view leaves self out
                settled = t;
                break;
            }
        }
        assertThat(settled).as("fifty admissions settle").isPositive();
        tick(40);
        long before = members.stream().mapToLong(m -> m.node().traffic().framesSent()).sum();
        tick(60);
        long after = members.stream().mapToLong(m -> m.node().traffic().framesSent()).sum();
        double perMemberPerTick = (after - before) / (50.0 * 60);
        double model = 49 / Math.min(30, config.memberTtl().toSeconds() / 2.0) + 1.0;
        System.out.printf("LB-Q-PRF-05: fifty idle members with the swarm on: %.2f frames per member per tick "
                + "(ROBUSTNESS.md 6b without it: 5.4; refresh model %.1f)%n", perMemberPerTick, model);
        assertThat(perMemberPerTick).as("LB-Q-PRF-05").isLessThan(model + 1.5);
    }
}
