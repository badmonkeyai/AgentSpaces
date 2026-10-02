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
package ai.badmonkey.agentspaces.capabilities.learn;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.PipeChannel;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/**
 * Gossip learning on a SimNetwork: pairwise push-pull averaging drives every
 * member's model to the fleet mean. Deterministic: seeded sampling, manual
 * ticks, synchronous delivery, so each exchange is an exact pairwise average.
 */
class GossipLearningTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zLearn");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zLearn", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "learn",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Learner(PeerNode node, GossipLearning learning) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Learner newLearner(String address, long seed, String... seedAddresses)
            throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        GossipLearning learning = new GossipLearning(
                new CapabilityPipes(runtime, codec), runtime.sampler(),
                identity.peerId(), codec, clock);
        nodes.add(node);
        return new Learner(node, learning);
    }

    private void tickAll(int rounds, List<Learner> learners) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
            learners.forEach(l -> l.learning().tick());
        }
    }

    @Test
    void modelsConvergeToTheFleetMean() throws Exception {
        Learner a = newLearner("a", 1);
        Learner b = newLearner("b", 2, "a");
        Learner c = newLearner("c", 3, "a");
        Learner d = newLearner("d", 4, "a");
        List<Learner> fleet = List.of(a, b, c, d);
        tickAll(4, List.of());

        a.learning().start("m", new double[]{0.0, 100.0});
        b.learning().start("m", new double[]{10.0, 80.0});
        c.learning().start("m", new double[]{20.0, 60.0});
        d.learning().start("m", new double[]{50.0, 40.0});
        // Fleet mean: {20.0, 70.0}. Pairwise averaging preserves it exactly.

        tickAll(60, fleet);

        for (Learner learner : fleet) {
            double[] model = learner.learning().model("m").orElseThrow();
            assertThat(model[0]).isCloseTo(20.0, offset(1e-3));
            assertThat(model[1]).isCloseTo(70.0, offset(1e-3));
            assertThat(learner.learning().round("m")).isPositive();
        }
    }

    @Test
    void nonParticipantsIgnoreExchangesAndParticipantsSkipThem() throws Exception {
        Learner a = newLearner("a", 1);
        Learner bystander = newLearner("b", 2, "a");
        tickAll(4, List.of());

        a.learning().start("m", new double[]{5.0});
        // The bystander never starts the model; pushes to it are ignored and it
        // never replies, so A's parameters stay untouched.
        tickAll(10, List.of(a));

        assertThat(a.learning().model("m").orElseThrow()[0]).isEqualTo(5.0);
        assertThat(bystander.learning().model("m")).isEmpty();
        assertThat(a.learning().round("m")).isZero();
    }

    @Test
    void dimensionMismatchesAreIgnored() throws Exception {
        Learner a = newLearner("a", 1);
        Learner b = newLearner("b", 2, "a");
        tickAll(4, List.of());

        a.learning().start("m", new double[]{1.0, 2.0});
        b.learning().start("m", new double[]{9.0});
        tickAll(10, List.of(a, b));

        // Different shapes never merge; both keep their own parameters.
        assertThat(a.learning().model("m").orElseThrow()).containsExactly(1.0, 2.0);
        assertThat(b.learning().model("m").orElseThrow()).containsExactly(9.0);
    }

    @Test
    void theLocalUpdateStepRunsAfterEveryMerge() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(9).build();
        node.listen(network.register("u"), "u");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of());
        nodes.add(node);
        GossipLearning clipped = new GossipLearning(
                new CapabilityPipes(runtime, codec), runtime.sampler(),
                identity.peerId(), codec, clock,
                params -> {
                    double[] out = params.clone();
                    for (int i = 0; i < out.length; i++) {
                        out[i] = Math.min(out[i], 1.0); // a clipping "trainer"
                    }
                    return out;
                });

        Learner peer = newLearner("p", 10, "u");
        peer.learning().start("m", new double[]{4.0});
        clipped.start("m", new double[]{2.0});
        tickAll(6, List.of(peer));
        nodes.forEach(PeerNode::tick);
        clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        clipped.tick();

        assertThat(clipped.model("m").orElseThrow()[0]).isLessThanOrEqualTo(1.0);
    }
    /** SPEC §8 gossip-learning: the capability type URI is {@code aspace:cap/gossip-learn} (the SPEC text aligns to the code). */
    @Test
    void theTypeUriIsGossipLearn() {
        assertThat(GossipLearning.TYPE).isEqualTo("aspace:cap/gossip-learn");
        assertThat(GossipLearner.TYPE).isEqualTo(GossipLearning.TYPE);
    }

    /** A pipe whose outgoing frames wait in a queue until the test delivers them, in whatever order it chooses. */
    private static final class QueuedPipe implements PipeChannel {
        record Frame(CapabilityPipes via, PeerId to, String type, byte[] payload) {
            void deliver() {
                via.send(to, type, payload);
            }
        }

        private final CapabilityPipes real;
        private final List<Frame> queue;

        QueuedPipe(CapabilityPipes real, List<Frame> queue) {
            this.real = real;
            this.queue = queue;
        }

        @Override
        public void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler) {
            real.onCapability(capabilityType, handler);
        }

        @Override
        public void send(PeerId to, String capabilityType, byte[] payload) {
            queue.add(new Frame(real, to, capabilityType, payload));
        }
    }

    private record Delayed(PeerNode node, GossipLearner<double[]> learner) {
    }

    private Delayed newDelayedLearner(String address, long seed, List<QueuedPipe.Frame> queue,
                                      String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        QueuedPipe pipe = new QueuedPipe(new CapabilityPipes(runtime, codec), queue);
        GossipLearner<double[]> learner = GossipLearner.builder(pipe, runtime.sampler(),
                identity.peerId(), codec, clock, WeightAveraging.INSTANCE).build();
        nodes.add(node);
        return new Delayed(node, learner);
    }

    private static double[] fleetSum(List<Delayed> fleet, String modelId) {
        double[] sum = null;
        for (Delayed d : fleet) {
            double[] m = d.learner().model(modelId).orElseThrow();
            if (sum == null) {
                sum = new double[m.length];
            }
            for (int i = 0; i < m.length; i++) {
                sum[i] += m[i];
            }
        }
        return sum;
    }

    /** Delivers the queued frames in a seeded random order; replies they provoke queue up behind them. */
    private static void deliverShuffled(List<QueuedPipe.Frame> queue, int count, Random random) {
        List<QueuedPipe.Frame> batch = new ArrayList<>(queue.subList(0, Math.min(count, queue.size())));
        queue.subList(0, batch.size()).clear();
        Collections.shuffle(batch, random);
        batch.forEach(QueuedPipe.Frame::deliver);
    }

    /** SPEC §8 gossip-learning: with frames delayed and delivered out of order — offers overtaking replies, replies arriving after a later tick — every completed exchange still merges one exact pair, so the fleet sum is conserved to 1e-6 and the models still converge to the fleet mean. */
    @Test
    void asynchronousInterleavedExchangesPreserveTheFleetMean() throws Exception {
        List<QueuedPipe.Frame> queue = new ArrayList<>();
        Delayed a = newDelayedLearner("a", 1, queue);
        Delayed b = newDelayedLearner("b", 2, queue, "a");
        Delayed c = newDelayedLearner("c", 3, queue, "a");
        Delayed d = newDelayedLearner("d", 4, queue, "a");
        List<Delayed> fleet = List.of(a, b, c, d);
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }

        a.learner().start("m", new double[]{0.0, 100.0});
        b.learner().start("m", new double[]{10.0, 80.0});
        c.learner().start("m", new double[]{20.0, 60.0});
        d.learner().start("m", new double[]{50.0, 40.0});
        double[] initial = fleetSum(fleet, "m"); // {80, 280}: mean {20, 70}

        Random random = new Random(7);
        long merges = 0;
        for (int round = 0; round < 80; round++) {
            // Everyone offers; only some frames get through before the next tick,
            // so replies to this round's offers interleave with next round's offers.
            fleet.forEach(l -> l.learner().tick());
            deliverShuffled(queue, Math.max(1, queue.size() / 2), random);
            fleet.forEach(l -> l.learner().tick());
            // Drain: nothing in flight, so every exchange is complete or aborted.
            while (!queue.isEmpty()) {
                deliverShuffled(queue, queue.size(), random);
            }
            double[] sum = fleetSum(fleet, "m");
            for (int i = 0; i < sum.length; i++) {
                assertThat(sum[i]).as("fleet sum, coordinate " + i + ", round " + round)
                        .isCloseTo(initial[i], offset(1e-6));
            }
            merges = fleet.stream().mapToLong(l -> l.learner().round("m")).sum();
        }

        assertThat(merges).as("exchanges actually happened").isGreaterThan(20);
        for (Delayed learner : fleet) {
            double[] model = learner.learner().model("m").orElseThrow();
            assertThat(model[0]).isCloseTo(20.0, offset(1e-3));
            assertThat(model[1]).isCloseTo(70.0, offset(1e-3));
        }
    }

    private record Metered(PeerNode node, GossipLearner<double[]> learner, ReplicatedSpace metrics) {
    }

    private Metered newMeteredLearner(String address, long seed, String... seedAddresses)
            throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace metrics = ReplicatedSpace.builder(runtime, "metrics", identity, "learner")
                .clock(clock).settleWindow(Duration.ZERO).build();
        GossipLearner<double[]> learner = GossipLearner.builder(
                new CapabilityPipes(runtime, codec), runtime.sampler(), identity.peerId(),
                codec, clock, WeightAveraging.INSTANCE)
                .metrics(metrics, Lease.of(Duration.ofHours(1)))
                .build();
        nodes.add(node);
        return new Metered(node, learner, metrics);
    }

    /** SPEC §8 gossip-learning epoch protocol: ending epoch k on four nodes writes four Evaluation{modelId, epoch, loss, contentId} entries, one per authenticated evaluator, visible on every replica of the metrics space. */
    @Test
    void epochEvaluationEntriesLandInTheMetricsSpace() throws Exception {
        Metered a = newMeteredLearner("a", 1);
        Metered b = newMeteredLearner("b", 2, "a");
        Metered c = newMeteredLearner("c", 3, "a");
        Metered d = newMeteredLearner("d", 4, "a");
        List<Metered> fleet = List.of(a, b, c, d);
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
        a.learner().start("m", new double[]{0.0, 100.0});
        b.learner().start("m", new double[]{10.0, 80.0});
        c.learner().start("m", new double[]{20.0, 60.0});
        d.learner().start("m", new double[]{50.0, 40.0});
        for (int i = 0; i < 10; i++) {
            fleet.forEach(l -> l.learner().tick());
        }

        int epoch = 3;
        for (Metered learner : fleet) {
            List<GossipLearner.Evaluation> written = learner.learner().endEpoch(epoch,
                    model -> model[0] * model[0] + model[1] * model[1]);
            assertThat(written).singleElement().satisfies(e -> {
                assertThat(e.modelId()).isEqualTo("m");
                assertThat(e.epoch()).isEqualTo(epoch);
                assertThat(e.contentId()).isEqualTo(
                        learner.learner().contentIdOf("m").orElseThrow());
            });
        }
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }

        Template<GossipLearner.Evaluation> thisEpoch = Template.of(GossipLearner.Evaluation.class)
                .where("epoch", eq(epoch));
        for (Metered replica : fleet) {
            List<Space.Issued<GossipLearner.Evaluation>> issued =
                    replica.metrics().readAllIssued(thisEpoch, 100);
            assertThat(issued).as("four evaluations on " + replica.node().peerId()).hasSize(4);
            assertThat(issued.stream().map(Space.Issued::issuer).distinct())
                    .as("one per evaluator").hasSize(4);
            assertThat(issued.stream().map(Space.Issued::issuer).map(AgentId::peer))
                    .containsExactlyInAnyOrderElementsOf(
                            fleet.stream().map(m -> m.node().peerId()).toList());
            assertThat(issued).allSatisfy(i -> {
                assertThat(i.entry().loss()).isPositive();
                assertThat(i.entry().contentId()).startsWith("z");
            });
        }
    }

    /** SPEC §8 gossip-learning content addressing: a model whose encoding exceeds the 64 KiB inline limit travels by CID through the block exchange and still averages; small models stay inline. */
    @Test
    void largeModelsTravelByContentIdThroughTheBlockExchange() throws Exception {
        PeerIdentity ida = PeerIdentity.generate();
        PeerNode na = PeerNode.builder(ida).clock(clock).randomSeed(21).build();
        na.listen(network.register("a"), "a");
        GroupRuntime ra = na.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        PeerIdentity idb = PeerIdentity.generate();
        PeerNode nb = PeerNode.builder(idb).clock(clock).randomSeed(22).build();
        nb.listen(network.register("b"), "b");
        GroupRuntime rb = nb.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        nodes.add(na);
        nodes.add(nb);
        BlockExchange blocksA = new BlockExchange(ra, codec);
        BlockExchange blocksB = new BlockExchange(rb, codec);
        GossipLearner<double[]> la = GossipLearner.builder(new CapabilityPipes(ra, codec),
                ra.sampler(), ida.peerId(), codec, clock, WeightAveraging.INSTANCE)
                .blocks(blocksA).build();
        GossipLearner<double[]> lb = GossipLearner.builder(new CapabilityPipes(rb, codec),
                rb.sampler(), idb.peerId(), codec, clock, WeightAveraging.INSTANCE)
                .blocks(blocksB).build();
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }

        int dims = 10_000; // 80 000 bytes encoded: over the 64 KiB inline limit
        double[] ones = new double[dims];
        double[] threes = new double[dims];
        java.util.Arrays.fill(ones, 1.0);
        java.util.Arrays.fill(threes, 3.0);
        la.start("big", ones);
        lb.start("big", threes);
        la.start("small", new double[]{1.0});
        lb.start("small", new double[]{3.0});
        assertThat(la.contentIdOf("big")).isNotEqualTo(lb.contentIdOf("big"));

        la.tick();

        assertThat(la.model("big").orElseThrow()).containsOnly(2.0);
        assertThat(lb.model("big").orElseThrow()).containsOnly(2.0);
        assertThat(la.model("small").orElseThrow()).containsExactly(2.0);
        assertThat(la.contentIdOf("big")).isEqualTo(lb.contentIdOf("big"));
        assertThat(blocksA.size()).as("A stored its offer and fetched B's reply").isGreaterThanOrEqualTo(2);
        assertThat(blocksB.size()).as("B fetched A's offer and stored its reply").isGreaterThanOrEqualTo(2);
        assertThat(la.describe(groupId).parameters()).containsEntry("content", "inline,cid")
                .containsEntry("merge", "weight-averaging");
    }
}
