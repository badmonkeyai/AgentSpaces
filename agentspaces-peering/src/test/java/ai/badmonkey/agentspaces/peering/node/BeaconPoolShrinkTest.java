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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.time.LoopPool;
import ai.badmonkey.agentspaces.common.time.SystemTime;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.bootstrap.MulticastBeacon;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * C03 of EL-A-03 (srad/agentspaces-event-loop.md §10.37): the beacon's retry table (EL-O-11) while the loop pool
 * shrinks. Real time on an isolated system source, with short configured bounds: three nodes' overlapping runs take
 * the pool to three threads; then the load stops and the pool's min drops to one, so the watchdog's ticks retire two
 * threads while one node's beacon steps through its table (200 ms first step: 200, 400, 2,000 and 4,000 ms, wrapped).
 * PREVENTED: every send keeps its slot (no retry lost, doubled or late by more than 150 ms on a pool thread retiring
 * under it), and the watchdog finds nothing.
 */
@Timeout(60)
class BeaconPoolShrinkTest {

    private static final long MS = 1_000_000L;

    @Test
    void c03TheBeaconKeepsItsRetryTableWhileTheLoopPoolShrinks() throws Exception {
        SystemTime system = SystemTime.isolated("c03-timer");
        LoopPool pool = system.loopPool();
        LoopPool.Options d = pool.options();
        pool.options(new LoopPool.Options(3, 3, Duration.ofMillis(300), d.growLateness(), d.growTicks(), d.stuck(),
                d.stretchLateness(), d.stretchTicks(), d.stretchClear(), d.stretchCap()));
        List<Long> sends = new CopyOnWriteArrayList<>();
        MulticastBeacon.Datagrams air = new MulticastBeacon.Datagrams() {
            @Override
            public void send(byte[] datagram) {
                sends.add(System.nanoTime());
            }

            @Override
            public void onReceive(Consumer<byte[]> receiver) {
                // nobody else on the air
            }

            @Override
            public void close() {
                // nothing held
            }
        };
        List<PeerNode> nodes = new ArrayList<>();
        AtomicBoolean burst = new AtomicBoolean(true);
        try {
            PeerNode beaconing = PeerNode.builder(PeerIdentity.generate()).clock(system).time(system).randomSeed(3)
                    .beacon(air, Duration.ofMillis(200)).build();
            nodes.add(beaconing);
            for (int i = 0; i < 2; i++) {
                nodes.add(PeerNode.builder(PeerIdentity.generate()).clock(system).time(system).randomSeed(4 + i)
                        .build());
            }
            for (PeerNode n : nodes) {
                n.startTicking(Duration.ofMillis(250));
                n.loop().register("burst", NodeLoop.Kind.OTHER, "", now -> {
                    if (!burst.get()) {
                        return ai.badmonkey.agentspaces.api.spi.DueWork.NEVER;
                    }
                    long until = System.nanoTime() + 40 * MS; // 40 ms of work in each 50 ms: the runs overlap
                    while (System.nanoTime() < until) {
                        Thread.onSpinWait();
                    }
                    return now + 50 * MS;
                });
            }
            long deadline = System.nanoTime() + 5_000 * MS;
            while (pool.threads() < 3) {
                assertThat(System.nanoTime()).as("the overlapping runs take the pool to its min of three")
                        .isLessThan(deadline);
                Thread.sleep(20);
            }
            long shrinks0 = pool.stats().shrinks();
            long detections0 = system.watchdog().stats().detections();
            burst.set(false);
            pool.options(new LoopPool.Options(1, 3, Duration.ofMillis(300), d.growLateness(), d.growTicks(),
                    d.stuck(), d.stretchLateness(), d.stretchTicks(), d.stretchClear(), d.stretchCap()));
            long joined = System.nanoTime();
            beaconing.joinGroup(new GroupAdvertisement("aspace://zC03", PeerIdentity.generate().peerId(),
                    GroupId.of("zC03"), Instant.EPOCH, Duration.ofDays(1), "c03",
                    GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                    GroupAdvertisement.GossipParameters.defaults()), GroupMembership.Config.defaults(), List.of());
            int minThreads = 3;
            deadline = System.nanoTime() + 12_000 * MS;
            while (sends.size() < 7) { // at 0, 200, 600, 2,600, 6,600, 6,800 and 7,200 ms
                assertThat(System.nanoTime()).as("seven sends within 12 s").isLessThan(deadline);
                minThreads = Math.min(minThreads, pool.threads());
                Thread.sleep(10);
            }
            List<Long> gaps = new ArrayList<>();
            for (int i = 1; i < sends.size(); i++) {
                gaps.add((sends.get(i) - sends.get(i - 1)) / MS);
            }
            long shrank = pool.stats().shrinks() - shrinks0;
            System.out.printf("C03: beacon sends at %s ms after the join, gaps %s ms (table 200, 400, 2000, 4000, 200,"
                    + " 400), pool shrinks %d,"
                    + " fewest threads %d, watchdog detections %d%n", sends.stream().map(t -> (t - joined) / MS).toList(),
                    gaps, shrank, minThreads,
                    system.watchdog().stats().detections() - detections0);
            // each send's slot, from the join: the join announces at once and each retry is armed from the run that
            // sent the one before, so the slots are the table's running sums (a first send's encoding cost may make
            // the announcement itself late; the slots after it do not move)
            long[] slots = {0, 200, 600, 2_600, 6_600, 6_800, 7_200};
            List<Long> offsets = sends.stream().map(t -> (t - joined) / MS).toList();
            for (int i = 0; i < slots.length; i++) {
                assertThat(offsets.get(i)).as("send %d keeps its slot: %s", i, offsets)
                        .isBetween(slots[i] - 5, slots[i] + 150);
            }
            assertThat(shrank).as("the pool shrank under the beacon").isGreaterThanOrEqualTo(2);
            assertThat(minThreads).as("down to its new min").isEqualTo(1);
            assertThat(system.watchdog().stats().detections() - detections0).as("the watchdog found nothing").isZero();
        } finally {
            burst.set(false);
            nodes.forEach(PeerNode::close);
        }
    }
}
