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
package ai.badmonkey.agentspaces.connect;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * srad/agentspaces-event-loop.md §10.35 (EL-W-05's idle half): a connector that advertises no asset yet waits for one
 * on its clock's time source, under the 30 s backstop, and an asset added wakes both its loops at once. It slept a
 * real second at a time, which no virtual clock moved and no change ended.
 */
class ConnectorIdleWaitTest {

    private final VirtualTime time = VirtualTime.create();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zConnectIdle");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zConnectIdle",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "connect-idle",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    /** One permit each time a connector loop starts a wait on the time. */
    private final Semaphore parks = new Semaphore(0);
    private final Set<Thread> loops = ConcurrentHashMap.newKeySet();
    private final List<String> log = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AutoCloseable parkWatch = time.onOutsideWait(() -> {
        if (Thread.currentThread().getName().startsWith("connector-")) {
            loops.add(Thread.currentThread());
            log.add(Thread.currentThread().getName() + "@" + (time.monoNanos() / 1_000_000L));
            parks.release();
        }
    });
    private PeerNode node;
    private ConnectorRuntime connector;

    @AfterEach
    void tearDown() throws Exception {
        if (connector != null) {
            connector.close();
        }
        if (node != null) {
            node.close();
        }
        parkWatch.close();
    }

    @Test
    @Timeout(60)
    void aConnectorWithNoAssetWaitsOnItsTimeSourceAndAnAssetAddedWakesBothLoops() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(time).time(time).build();
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, time), codec, identity.peerId());
        LocalSpace data = LocalSpace.builder("data", identity.agent("connector")).clock(time).build();
        AtomicBoolean holding = new AtomicBoolean();
        TableAssetProvider orders = new TableAssetProvider("orders", "postgres://ops/public.orders",
                "Customer orders", "com.example.OrderRow#v1", Duration.ofMinutes(5),
                List.of(Map.of("id", "1", "region", "eu")));
        AssetProvider later = new AssetProvider() {
            @Override
            public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
                return holding.get() ? orders.describeAssets(group, issuer, now) : List.of();
            }

            @Override
            public QueryResult query(String asset, Map<String, String> parameters) {
                return orders.query(asset, parameters);
            }
        };
        connector = new ConnectorRuntime(data, discovery, identity, "late-connector", later, groupId, time,
                Duration.ofMinutes(1));
        connector.start();

        assertThat(parks.tryAcquire(2, 5, TimeUnit.SECONDS))
                .as("both loops, with no asset to take for, wait on the connector's time source").isTrue();
        assertThat(loops).hasSize(2).allMatch(time::waitingOutside);

        // ten idle minutes: each loop looks again only at the 30 s backstop (a second at a time read 600 each)
        for (int s = 0; s < 600; s += 30) {
            time.advance(Duration.ofSeconds(30));
            assertThat(parks.tryAcquire(2, 5, TimeUnit.SECONDS)).as("both loops wait again after the backstop").isTrue();
        }
        assertThat(parks.availablePermits()).as("no other look in ten idle minutes (%s)", log).isZero();

        long changes = connector.assetChanges();
        long monoBefore = time.monoNanos();
        holding.set(true);
        connector.refreshCards();
        assertThat(connector.assetChanges()).isEqualTo(changes + 1);
        assertThat(parks.tryAcquire(2, 5, TimeUnit.SECONDS))
                .as("the asset added woke both loops into their takes, with no virtual time passed").isTrue();
        assertThat(time.monoNanos()).isEqualTo(monoBefore);
    }
}
