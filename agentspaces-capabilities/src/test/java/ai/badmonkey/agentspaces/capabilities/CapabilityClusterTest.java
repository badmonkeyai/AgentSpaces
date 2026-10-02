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
package ai.badmonkey.agentspaces.capabilities;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate.Mode;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate.Quantile;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class CapabilityClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zCapGroup");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zCapGroup", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "caps",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Cap(PeerNode node, GroupRuntime runtime, DiscoveryService discovery,
                       CapabilityRuntime capabilities, PushSumAggregate aggregate) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Cap newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = new DiscoveryService(
                runtime, new AdCache(codec, clock), codec, identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        CapabilityPipes pipes = new CapabilityPipes(runtime, codec);
        PushSumAggregate aggregate = new PushSumAggregate(
                pipes, runtime.sampler(), identity.peerId(), codec, clock);
        nodes.add(node);
        return new Cap(node, runtime, discovery, capabilities, aggregate);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
    }

    @Test
    void registeredCapabilitiesAreDiscoverableAcrossTheGroup() throws Exception {
        Cap a = newPeer("a", 1);
        Cap b = newPeer("b", 2, "a");
        Cap c = newPeer("c", 3, "a");
        tickAll(4);

        a.capabilities().register(a.aggregate());

        List<CapabilityAdvertisement> found = b.capabilities().providersOf(PushSumAggregate.TYPE);
        assertThat(found).hasSize(1);
        assertThat(found.get(0).issuer()).isEqualTo(a.node().peerId());
        assertThat(found.get(0).binding()).isEqualTo("pipe");
        assertThat(c.capabilities().providersOf("aspace:cap/nonexistent")).isEmpty();
    }

    @Test
    void lapsedCapabilityAdvertisementsAgeOut() throws Exception {
        Cap a = newPeer("a", 1);
        Cap b = newPeer("b", 2, "a");
        tickAll(3);
        a.capabilities().register(a.aggregate());
        assertThat(b.capabilities().providersOf(PushSumAggregate.TYPE)).hasSize(1);

        // No refresh for longer than the 15-minute TTL: the offer disappears.
        clock.advance(Duration.ofMinutes(16));

        assertThat(b.capabilities().providersOf(PushSumAggregate.TYPE)).isEmpty();
    }

    @Test
    void pushSumConvergesToTheFleetAverage() throws Exception {
        Cap a = newPeer("a", 1, new String[0]);
        Cap b = newPeer("b", 2, "a");
        Cap c = newPeer("c", 3, "a");
        Cap d = newPeer("d", 4, "a");
        tickAll(5);

        // Local values 2, 4, 6, 8: the fleet average is 5.
        a.aggregate().start("battery", 2.0);
        b.aggregate().start("battery", 4.0);
        c.aggregate().start("battery", 6.0);
        d.aggregate().start("battery", 8.0);

        List<Cap> fleet = List.of(a, b, c, d);
        for (int round = 0; round < 40; round++) {
            fleet.forEach(cap -> cap.aggregate().tick());
        }

        for (Cap cap : fleet) {
            assertThat(cap.aggregate().estimate("battery")).isPresent();
            assertThat(cap.aggregate().estimate("battery").getAsDouble())
                    .isCloseTo(5.0, within(0.01));
        }
    }

    /** SPEC §8 aggregate: an eight-member fleet is within 1% of the mean in a small multiple of log2(N) rounds — the O(log N) convergence claim, given a number. */
    @Test
    void pushSumConvergesInLogarithmicRounds() throws Exception {
        List<Cap> fleet = new ArrayList<>();
        fleet.add(newPeer("a", 1));
        for (int i = 1; i < 8; i++) {
            fleet.add(newPeer("n" + i, 10 + i, "a"));
        }
        tickAll(8); // membership converges so every sampler sees the whole fleet

        // Values 1..8: mean 4.5.
        for (int i = 0; i < fleet.size(); i++) {
            fleet.get(i).aggregate().start("load", i + 1.0);
        }

        int rounds = 0;
        boolean converged = false;
        while (rounds < 60 && !converged) {
            fleet.forEach(cap -> cap.aggregate().tick());
            rounds++;
            converged = fleet.stream().allMatch(cap ->
                    Math.abs(cap.aggregate().estimate("load").orElseThrow() - 4.5) <= 0.045);
        }

        assertThat(converged).as("every member within 1% of the fleet mean").isTrue();
        // log2(8) = 3; allow a generous constant factor, but nowhere near O(N)
        // round budgets: 60 is the loop bound, 30 is the assertion.
        assertThat(rounds).as("rounds to 1% for N=8").isLessThanOrEqualTo(30);
    }

    /** SPEC §8 aggregate: epochs are independent — two aggregations on the same fleet converge to their own means without mixing mass. */
    @Test
    void independentEpochsDoNotMix() throws Exception {
        Cap a = newPeer("a", 1);
        Cap b = newPeer("b", 2, "a");
        Cap c = newPeer("c", 3, "a");
        Cap d = newPeer("d", 4, "a");
        tickAll(5);
        List<Cap> fleet = List.of(a, b, c, d);

        // battery: 2,4,6,8 (mean 5); backlog: 100,0,0,0 (mean 25).
        a.aggregate().start("battery", 2.0);
        b.aggregate().start("battery", 4.0);
        c.aggregate().start("battery", 6.0);
        d.aggregate().start("battery", 8.0);
        a.aggregate().start("backlog", 100.0);
        b.aggregate().start("backlog", 0.0);
        c.aggregate().start("backlog", 0.0);
        d.aggregate().start("backlog", 0.0);

        for (int round = 0; round < 40; round++) {
            fleet.forEach(cap -> cap.aggregate().tick());
        }

        for (Cap cap : fleet) {
            assertThat(cap.aggregate().estimate("battery").orElseThrow()).isCloseTo(5.0, within(0.01));
            assertThat(cap.aggregate().estimate("backlog").orElseThrow()).isCloseTo(25.0, within(0.05));
            assertThat(cap.aggregate().estimate("never-started")).isEmpty();
        }
    }

    /** SPEC §8 pattern: the discovered advertisement carries the whole contract — type URI, version, binding, parameters, cost hints, TTL, issuer, group. */
    @Test
    void advertisementCarriesTheFullContract() throws Exception {
        Cap a = newPeer("a", 1);
        Cap b = newPeer("b", 2, "a");
        tickAll(3);

        a.capabilities().register(a.aggregate());

        List<CapabilityAdvertisement> found = b.capabilities().providersOf(PushSumAggregate.TYPE);
        assertThat(found).hasSize(1);
        CapabilityAdvertisement ad = found.get(0);
        assertThat(ad.capabilityType()).isEqualTo("aspace:cap/aggregate");
        assertThat(ad.version()).isEqualTo("0.1");
        assertThat(ad.binding()).isEqualTo("pipe");
        assertThat(ad.parameters()).containsEntry("modes", "sum,avg,count,min,max,quantile");
        assertThat(ad.costHints()).isEmpty();
        assertThat(ad.ttl()).isEqualTo(Duration.ofMinutes(15));
        assertThat(ad.issued()).isEqualTo(clock.instant());
        assertThat(ad.issuer()).isEqualTo(a.node().peerId());
        assertThat(ad.group()).isEqualTo(groupId);
        assertThat(ad.id()).isEqualTo("aspace://" + groupId.value() + "/cap/aggregate/"
                + a.node().peerId().value());
        assertThat(ad.expired(clock.instant().plus(Duration.ofMinutes(14)))).isFalse();
        assertThat(ad.expired(clock.instant().plus(Duration.ofMinutes(16)))).isTrue();
    }

    /** SPEC §8 / P2: refreshing keeps the leased advertisement alive past its original TTL; stopping the refresh is the whole shutdown protocol. */
    @Test
    void refreshKeepsTheAdvertisementAlive() throws Exception {
        Cap a = newPeer("a", 1);
        Cap b = newPeer("b", 2, "a");
        tickAll(3);
        java.time.Instant registeredAt = clock.instant();
        a.capabilities().register(a.aggregate());
        assertThat(b.capabilities().providersOf(PushSumAggregate.TYPE)).hasSize(1);

        // Refresh within the TTL: the consumer sees a newer issue time, and the
        // offer outlives the moment the original would have lapsed.
        clock.advance(Duration.ofMinutes(14));
        a.capabilities().refreshTick();
        assertThat(b.capabilities().providersOf(PushSumAggregate.TYPE))
                .singleElement()
                .satisfies(ad -> assertThat(ad.issued()).isEqualTo(registeredAt.plus(Duration.ofMinutes(14))));

        clock.advance(Duration.ofMinutes(14)); // 28 min after registration: original TTL long gone
        assertThat(b.capabilities().providersOf(PushSumAggregate.TYPE))
                .as("the refreshed lease is still current").hasSize(1);

        // No further refresh: the offer lapses by itself.
        clock.advance(Duration.ofMinutes(2));
        assertThat(b.capabilities().providersOf(PushSumAggregate.TYPE)).isEmpty();
    }

    /** A provider stub recording lifecycle calls and describing whatever issuer it is told to. */
    private static final class RecordingProvider implements CapabilityProvider {
        private final String type;
        private final ai.badmonkey.agentspaces.common.id.PeerId issuer;
        private final InstantSource clock;
        int started;
        int stopped;

        RecordingProvider(String type, ai.badmonkey.agentspaces.common.id.PeerId issuer,
                          InstantSource clock) {
            this.type = type;
            this.issuer = issuer;
            this.clock = clock;
        }

        @Override
        public String capabilityType() {
            return type;
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            return new CapabilityAdvertisement("aspace://" + group.value() + "/" + type,
                    issuer, group, clock.instant(), Duration.ofMinutes(15),
                    type, "0.1", "pipe", java.util.Map.of(), java.util.Map.of());
        }

        @Override
        public void start() {
            started++;
        }

        @Override
        public void stop() {
            stopped++;
        }
    }

    /** SPEC §8 / §10.5 CapabilityProvider SPI: register starts the provider once, close stops it once, and an advertisement for a foreign issuer is refused. */
    @Test
    void closeStopsProvidersAndRejectsForeignIssuers() throws Exception {
        Cap a = newPeer("a", 1);
        Cap b = newPeer("b", 2, "a");
        tickAll(3);

        RecordingProvider honest = new RecordingProvider("aspace:cap/recording", a.node().peerId(), clock);
        a.capabilities().register(honest);
        assertThat(honest.started).isEqualTo(1);
        assertThat(honest.stopped).isZero();
        assertThat(b.capabilities().providersOf("aspace:cap/recording")).hasSize(1);

        // The runtime signs with the local identity, so it will not publish an
        // advertisement claiming to come from someone else. (A distinct type: the
        // runtime keys providers by type, and a rejected registration currently
        // still replaces whatever was registered under that type.)
        RecordingProvider impostor = new RecordingProvider("aspace:cap/impostor", b.node().peerId(), clock);
        assertThatThrownBy(() -> a.capabilities().register(impostor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different issuer");
        assertThat(b.capabilities().providersOf("aspace:cap/impostor"))
                .as("the foreign-issuer advertisement was never published").isEmpty();
        assertThat(b.capabilities().providersOf("aspace:cap/recording")).hasSize(1);

        a.capabilities().close();
        assertThat(honest.stopped).isEqualTo(1);
        assertThat(honest.started).isEqualTo(1);
        // No unpublish on close (spec P2): the offer is still cached until it lapses.
        assertThat(b.capabilities().providersOf("aspace:cap/recording")).hasSize(1);
        clock.advance(Duration.ofMinutes(16));
        assertThat(b.capabilities().providersOf("aspace:cap/recording")).isEmpty();
    }
    /** A numeric entry for template-mode aggregation. */
    public record Task(String name, double backlog) {
    }

    private List<Cap> fourNodeFleet() throws IOException {
        Cap a = newPeer("a", 1);
        Cap b = newPeer("b", 2, "a");
        Cap c = newPeer("c", 3, "a");
        Cap d = newPeer("d", 4, "a");
        tickAll(5);
        return List.of(a, b, c, d);
    }

    private static void gossip(List<Cap> fleet, int rounds) {
        for (int round = 0; round < rounds; round++) {
            fleet.forEach(cap -> cap.aggregate().tick());
        }
    }

    private static double expected(Mode mode) {
        return switch (mode) {
            case SUM -> 20.0;
            case AVG -> 5.0;
            case COUNT -> 4.0;
            case MIN -> 2.0;
            case MAX -> 8.0;
            case QUANTILE -> 5.0; // the median of 2, 4, 6, 8
        };
    }

    /** SPEC §8 aggregate: every operator (sum, avg, count, min, max, quantile) over the local values 2, 4, 6, 8 converges on all four nodes to 20, 5, 4, 2, 8, and the median 5 within one bucket. */
    @ParameterizedTest
    @EnumSource(Mode.class)
    void everyOperatorConvergesOnFourNodes(Mode mode) throws Exception {
        List<Cap> fleet = fourNodeFleet();
        Quantile median = new Quantile(0.5, 0.0, 10.0, 10); // bucket width 1
        double[] values = {2.0, 4.0, 6.0, 8.0};
        for (int i = 0; i < fleet.size(); i++) {
            if (mode == Mode.QUANTILE) {
                fleet.get(i).aggregate().start("op", median, values[i]);
            } else {
                fleet.get(i).aggregate().start("op", mode, values[i]);
            }
        }

        gossip(fleet, 40);

        double tolerance = mode == Mode.QUANTILE ? median.width() : 0.05;
        for (Cap cap : fleet) {
            assertThat(cap.aggregate().estimate("op")).as(mode + " estimate present").isPresent();
            assertThat(cap.aggregate().estimate("op").getAsDouble())
                    .as(mode.name()).isCloseTo(expected(mode), within(tolerance));
        }
    }

    /** SPEC §8 aggregate template mode: each node aggregates a numeric field of the entries it sees locally; over four local spaces holding backlogs {2,4}, {6}, {8}, {} the fleet sum is 20, the entry-weighted mean 5, the count 4, min 2, max 8, and the median 5. */
    @Test
    void aggregatesANumericFieldOfMatchingEntries() throws Exception {
        List<Cap> fleet = fourNodeFleet();
        List<LocalSpace> spaces = new ArrayList<>();
        double[][] backlogs = {{2.0, 4.0}, {6.0}, {8.0}, {}};
        for (int i = 0; i < fleet.size(); i++) {
            LocalSpace space = LocalSpace.builder("tasks",
                    new ai.badmonkey.agentspaces.common.id.AgentId(
                            fleet.get(i).node().peerId(), "worker")).clock(clock).build();
            for (double backlog : backlogs[i]) {
                space.write(new Task("t" + backlog, backlog), Lease.of(Duration.ofHours(1)));
            }
            // A decoy of another name: the template must not aggregate it.
            space.write(new Task("ignored", 1000.0), Lease.of(Duration.ofHours(1)));
            spaces.add(space);
        }
        Template<Task> pending = Template.of(Task.class)
                .where("name", ai.badmonkey.agentspaces.api.space.Matchers.ne("ignored"));
        Quantile median = new Quantile(0.5, 0.0, 10.0, 10);
        try {
            for (int i = 0; i < fleet.size(); i++) {
                PushSumAggregate agg = fleet.get(i).aggregate();
                for (Mode mode : Mode.values()) {
                    if (mode == Mode.QUANTILE) {
                        agg.start("q", median, spaces.get(i), pending, Task::backlog);
                    } else {
                        agg.start(mode.name(), mode, spaces.get(i), pending, Task::backlog);
                    }
                }
            }

            gossip(fleet, 40);

            for (Cap cap : fleet) {
                for (Mode mode : Mode.values()) {
                    String epoch = mode == Mode.QUANTILE ? "q" : mode.name();
                    double tolerance = mode == Mode.QUANTILE ? median.width() : 0.05;
                    assertThat(cap.aggregate().estimate(epoch).getAsDouble())
                            .as("template " + mode).isCloseTo(expected(mode), within(tolerance));
                }
            }
        } finally {
            spaces.forEach(LocalSpace::close);
        }
    }

    /** A deterministic ScheduledExecutorService: records fixed-rate tasks and runs them only when told. */
    private static final class ManualScheduler extends AbstractExecutorService
            implements ScheduledExecutorService {
        record Scheduled(Runnable task, long periodMillis, ManualFuture future) {
        }

        static final class ManualFuture implements ScheduledFuture<Object> {
            volatile boolean cancelled;

            @Override
            public long getDelay(TimeUnit unit) {
                return 0;
            }

            @Override
            public int compareTo(Delayed o) {
                return 0;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                cancelled = true;
                return true;
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }

            @Override
            public boolean isDone() {
                return cancelled;
            }

            @Override
            public Object get() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object get(long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }
        }

        final List<Scheduled> scheduled = new ArrayList<>();

        /** Fires every live fixed-rate task once. */
        void runPending() {
            for (Scheduled s : List.copyOf(scheduled)) {
                if (!s.future().cancelled) {
                    s.task().run();
                }
            }
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
                                                      long period, TimeUnit unit) {
            ManualFuture future = new ManualFuture();
            scheduled.add(new Scheduled(command, unit.toMillis(period), future));
            return future;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                         long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    /** SPEC §8 / P2: a runtime told to refresh itself keeps its advertisements alive past their TTL on a host-supplied scheduler, and close() cancels the schedule so they lapse. */
    @Test
    void autoRefreshKeepsAdvertisementsAlive() throws Exception {
        Cap a = newPeer("a", 1);
        Cap b = newPeer("b", 2, "a");
        tickAll(3);
        ManualScheduler scheduler = new ManualScheduler();
        a.capabilities().autoRefresh(Duration.ofMinutes(5), scheduler);
        assertThat(a.capabilities().isAutoRefreshing()).isTrue();
        assertThat(scheduler.scheduled).singleElement()
                .satisfies(s -> assertThat(s.periodMillis()).isEqualTo(Duration.ofMinutes(5).toMillis()));

        a.capabilities().register(a.aggregate());
        java.time.Instant registeredAt = clock.instant();

        // Four periods = 20 minutes: well past the 15-minute TTL of the original.
        for (int i = 1; i <= 4; i++) {
            clock.advance(Duration.ofMinutes(5));
            scheduler.runPending();
            assertThat(b.capabilities().providersOf(PushSumAggregate.TYPE))
                    .as("alive after refresh " + i).singleElement()
                    .satisfies(ad -> assertThat(ad.issued()).isEqualTo(clock.instant()));
        }
        assertThat(clock.instant()).isEqualTo(registeredAt.plus(Duration.ofMinutes(20)));

        // Closing cancels the schedule; the scheduler's later firings are no-ops
        // and the offer lapses on its own.
        a.capabilities().close();
        assertThat(a.capabilities().isAutoRefreshing()).isFalse();
        assertThat(scheduler.scheduled.get(0).future().isCancelled()).isTrue();
        clock.advance(Duration.ofMinutes(16));
        scheduler.runPending();
        assertThat(b.capabilities().providersOf(PushSumAggregate.TYPE)).isEmpty();
    }

    /** SPEC §8 aggregate: a refresh period that is zero or negative is refused. */
    @Test
    void autoRefreshRejectsNonPositivePeriods() throws Exception {
        Cap a = newPeer("a", 1);
        assertThatThrownBy(() -> a.capabilities().autoRefresh(Duration.ZERO, new ManualScheduler()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
