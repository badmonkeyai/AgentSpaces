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
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Per-stream probe hashes for shared streams (RE-T-07, RE-P-03 and RE-P-04 of srad/replication-by-entry.md): a busy
 * member's probes carry no hashes; a group that mixes a member attaching hashes to every probe with one that omits
 * them repairs a dropped rumor within the probe cycle, well inside the 32x anti-entropy bound; and a hub member
 * of twenty streams, in step with an agent, sends a hash per stream rather than the root, so neither side opens
 * a targeted round from its probes.
 */
class ProbeHashStreamsTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zProbeStreams",
            PeerIdentity.generate().peerId(), GroupId.of("zProbeStreams"), Instant.EPOCH, Duration.ofDays(1),
            "probe-streams", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, int probeHashStreams) throws IOException {
        // A long peer-ad interval: the measured windows below hold no advertisement refresh, which would differ
        // between the two members for a tick and open a round that has nothing to do with probe hashes.
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed)
                .peerAdInterval(Duration.ofMinutes(5)).probeHashStreams(probeHashStreams).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private GroupRuntime join(PeerNode node, String... seeds) {
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", seed, 0));
        }
        return node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
    }

    private void tick(int seconds) {
        for (int i = 0; i < seconds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** A stream whose digest never changes: both sides hold the same thing. */
    private static ReconcilableState quiet(String digest) {
        return new ReconcilableState() {
            @Override public byte[] digest() { return digest.getBytes(StandardCharsets.UTF_8); }
            @Override public byte[] deltaFor(byte[] remoteDigest) { return new byte[0]; }
            @Override public void applyDelta(byte[] delta) { }
            @Override public boolean applyDeltaChanged(byte[] delta) { return false; }
        };
    }

    /** A grow-only set of items: rumors add to it, and anti-entropy sends what the other side's digest lacks. */
    private static final class Items implements ReconcilableState {
        final Set<String> held = new ConcurrentSkipListSet<>();

        @Override public byte[] digest() {
            return String.join("\n", held).getBytes(StandardCharsets.UTF_8);
        }

        @Override public byte[] deltaFor(byte[] remoteDigest) {
            Set<String> theirs = new TreeSet<>(List.of(new String(remoteDigest, StandardCharsets.UTF_8).split("\n")));
            Set<String> missing = new TreeSet<>(held);
            missing.removeAll(theirs);
            return missing.isEmpty() ? new byte[0] : String.join("\n", missing).getBytes(StandardCharsets.UTF_8);
        }

        @Override public void applyDelta(byte[] delta) {
            applyDeltaChanged(delta);
        }

        @Override public boolean applyDeltaChanged(byte[] delta) {
            boolean changed = false;
            for (String item : new String(delta, StandardCharsets.UTF_8).split("\n")) {
                if (!item.isEmpty()) {
                    changed |= held.add(item);
                }
            }
            return changed;
        }
    }

    private static Items items(GroupRuntime runtime, String stream) {
        Items items = new Items();
        runtime.gossip().reconcile(stream, items);
        runtime.gossip().onStream(stream, (from, id, payload) -> items.held.add(new String(payload, StandardCharsets.UTF_8)));
        return items;
    }

    private List<Bodies.Ping> pings(List<byte[]> frames) {
        List<Bodies.Ping> out = new ArrayList<>();
        for (byte[] frame : frames) {
            Optional<Envelope> envelope = wire.decode(frame);
            if (envelope.isPresent() && envelope.get().kind() == Envelope.Kind.PING) {
                out.add(codec.fromWire(envelope.get().body(), Bodies.Ping.class));
            }
        }
        return out;
    }

    /** Ticks until the item is held, at most {@code limit} seconds; returns the seconds it took, or -1. */
    private int secondsUntilHeld(Items items, String item, int limit) {
        for (int second = 0; second <= limit; second++) {
            if (items.held.contains(item)) {
                return second;
            }
            tick(1);
        }
        return -1;
    }

    /**
     * RE-P-03: X attaches hashes to every probe (as an older node or another implementation may), Y keeps the default
     * rule and is busy on a stream X does not hold, so Y's PINGs carry none. A rumor on the stream they share, dropped
     * on its way in either direction, is repaired within the probe cycle and inside the 32x bound.
     */
    @Test
    void aMixedGroupRepairsADroppedRumorWithinTheProbeCycle() throws Exception {
        PeerNode x = node("x", 1, GossipBus.PROBE_DIGEST_STREAMS);
        PeerNode y = node("y", 2, GossipBus.PROBE_DIGEST_STREAMS);
        x.probeHashesAlways(true);
        GroupRuntime rx = join(x);
        GroupRuntime ry = join(y, "x");
        Items atX = items(rx, "shared");
        Items atY = items(ry, "shared");
        ry.gossip().onStream("busy", (from, id, payload) -> { });
        List<byte[]> xToY = new CopyOnWriteArrayList<>();
        List<byte[]> yToX = new CopyOnWriteArrayList<>();
        network.tap("x", "y", xToY::add);
        network.tap("y", "x", yToX::add);
        int busy = 0;
        for (int i = 0; i < 40; i++) {
            ry.gossip().publish("busy", "b" + busy++, "position".getBytes(StandardCharsets.UTF_8));
            tick(1);
        }
        assertThat(rx.membership().member(y.peerId())).isPresent();
        assertThat(ry.antiEntropyStretched()).as("Y writes every tick").isFalse();
        assertThat(rx.antiEntropyStretched()).as("Y's busy stream is no activity at X").isTrue();
        assertThat(pings(yToX)).as("Y probed X").isNotEmpty();
        assertThat(pings(yToX)).as("a busy member's PING carries no hashes")
                .allMatch(p -> p.root() == null && p.hashes() == null);
        assertThat(pings(xToY)).as("X attaches its hashes to every probe")
                .isNotEmpty().allMatch(p -> p.hashes() != null && p.hashes().containsKey("shared"));
        long period = groupAd.gossip().period().toSeconds();
        int bound = (int) (32 * period);

        // Y's rumor to X is lost: X, backed off, hears of it from the hashes Y's ACKs carry back to X's probes.
        // The direction stays down for one tick, so a rumor the node sends at its tick rather than at once is lost too.
        network.oneWay("y", "x");
        atY.held.add("from-y");
        ry.gossip().publish("shared", "from-y", "from-y".getBytes(StandardCharsets.UTF_8));
        ry.gossip().publish("busy", "b" + busy++, "position".getBytes(StandardCharsets.UTF_8));
        tick(1);
        network.twoWay("y", "x");
        assertThat(network.oneWayDropped("y", "x")).isPositive();
        assertThat(atX.held).as("the rumor was lost").doesNotContain("from-y");
        long roundsAtY = ry.gossip().antiEntropyRounds();
        int toX = secondsUntilHeld(atX, "from-y", bound);
        int cycle = (int) (rx.membership().maxQuietIntervalMillis(1) / 1000) + 4;
        assertThat(toX).as("repaired at X within the probe cycle (seconds: %s)", toX).isBetween(0, cycle);
        // X's probes stretch as far as the pair's TTL bound (idle-loop item 3), so busy Y's base-period round may
        // carry the hashes first; either way a round opened by hashes repaired X, inside the 32x bound
        assertThat(rx.gossip().pacingSignals().probeRoundsHashes() + (ry.gossip().antiEntropyRounds() - roundsAtY))
                .as("a round opened by hashes repaired X").isPositive();

        // X's rumor to Y is lost: Y, busy, runs its anti-entropy at the base period and repairs it there.
        long droppedBefore = network.oneWayDropped("x", "y");
        network.oneWay("x", "y");
        atX.held.add("from-x");
        rx.gossip().publish("shared", "from-x", "from-x".getBytes(StandardCharsets.UTF_8));
        ry.gossip().publish("busy", "b" + busy++, "position".getBytes(StandardCharsets.UTF_8));
        tick(1);
        network.twoWay("x", "y");
        assertThat(network.oneWayDropped("x", "y")).isGreaterThan(droppedBefore);
        assertThat(atY.held).as("the rumor was lost").doesNotContain("from-x");
        int toY = -1;
        for (int second = 0; second <= bound && toY < 0; second++) {
            if (atY.held.contains("from-x")) {
                toY = second;
            } else {
                ry.gossip().publish("busy", "b" + busy++, "position".getBytes(StandardCharsets.UTF_8));
                tick(1);
            }
        }
        assertThat(toY).as("repaired at Y within the probe cycle (seconds: %s)", toY).isBetween(0, 8);
    }

    /**
     * RE-P-04: a hub member holds twenty space streams, an agent three of them, and the two are in step.
     * The hub's probes carry a hash per stream, which the agent compares on the three it holds, so no targeted
     * round opens from any probe. Under the old bound of sixteen the hub sent its root alone, which never matches
     * the agent's, and the agent opened a round from the hub's probes once a period.
     */
    @Test
    void aHubMemberOfTwentyStreamsInStepWithAnAgentOpensNoTargetedRoundFromItsProbes() throws Exception {
        assertThat(roundsBetweenAHubAndAnAgent("h", "g", GossipBus.PROBE_DIGEST_STREAMS))
                .as("per-stream hashes: nothing differs on a stream both hold").isZero();
        assertThat(roundsBetweenAHubAndAnAgent("h16", "g16", 16))
                .as("the old bound of sixteen: the root alone, which always differs").isPositive();
    }

    /** Runs a hub and an agent in step for a minute and returns the targeted rounds their probes opened. */
    private long roundsBetweenAHubAndAnAgent(String hubAddress, String agentAddress, int bound)
            throws Exception {
        PeerNode hub = node(hubAddress, 3, bound);
        PeerNode agent = node(agentAddress, 4, bound);
        GroupRuntime rc = join(hub);
        GroupRuntime rg = join(agent, hubAddress);
        for (int i = 0; i < 20; i++) {
            rc.gossip().reconcile("space:s" + i, quiet("state " + i));
        }
        for (int i = 0; i < 3; i++) {
            rg.gossip().reconcile("space:s" + i, quiet("state " + i));
        }
        List<byte[]> hubToAgent = new CopyOnWriteArrayList<>();
        network.tap(hubAddress, agentAddress, hubToAgent::add);
        tick(40);
        assertThat(rc.membership().member(agent.peerId())).isPresent();
        assertThat(rc.antiEntropyStretched()).isTrue();
        assertThat(rg.antiEntropyStretched()).isTrue();
        long before = rc.gossip().pacingSignals().probeRounds() + rg.gossip().pacingSignals().probeRounds();
        hubToAgent.clear();
        tick(60);
        List<Bodies.Ping> fromHub = pings(hubToAgent);
        assertThat(fromHub).as("the hub probed the agent").isNotEmpty();
        java.util.Map<String, byte[]> own = rc.gossip().probeHashes();
        if (own != null) {
            assertThat(own.keySet()).as("twenty spaces and the node's own streams").hasSizeGreaterThan(20)
                    .contains("space:s0", "space:s19");
            assertThat(fromHub).as("a hash per stream, never the root")
                    .allMatch(p -> p.root() == null && p.hashes() != null && p.hashes().keySet().equals(own.keySet()));
        } else {
            assertThat(fromHub).as("above the bound: the root alone")
                    .allMatch(p -> p.root() != null && p.hashes() == null);
        }
        return rc.gossip().pacingSignals().probeRounds() + rg.gossip().pacingSignals().probeRounds() - before;
    }

    /** The bound is a setting with a floor and a ceiling, on the builder and at run time (RE-F-03). */
    @Test
    void theBoundIsASettingWithAFloorAndACeiling() throws Exception {
        assertThatThrownBy(() -> PeerNode.builder(PeerIdentity.generate()).probeHashStreams(-1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("probe-hash-streams");
        assertThatThrownBy(() -> PeerNode.builder(PeerIdentity.generate())
                .probeHashStreams(GossipBus.PROBE_DIGEST_STREAMS_CEILING + 1))
                .isInstanceOf(IllegalArgumentException.class);
        PeerNode node = node("n", 5, 0);
        GroupRuntime runtime = join(node);
        assertThat(runtime.gossip().probeDigestStreams()).isZero();
        assertThat(runtime.gossip().probeHashes()).as("a bound of zero: the root alone").isNull();
        runtime.gossip().probeDigestStreams(GossipBus.PROBE_DIGEST_STREAMS);
        assertThat(runtime.gossip().probeHashes()).isNotNull();
        assertThat(node("m", 6, GossipBus.PROBE_DIGEST_STREAMS).joinGroup(groupAd,
                GroupMembership.Config.defaults(), List.of()).gossip().probeDigestStreams()).isEqualTo(32);
    }
}
