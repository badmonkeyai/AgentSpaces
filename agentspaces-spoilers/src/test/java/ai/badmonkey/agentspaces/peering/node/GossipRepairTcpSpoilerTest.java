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
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.test.EnvelopeHeaders;
import ai.badmonkey.agentspaces.test.FrameHeader;
import ai.badmonkey.agentspaces.test.VirtualThreadsIdle;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-SP-12 of srad/agentspaces-event-loop.md §5.1, over TCP: a spoiler in the writer's transport holds the first frame
 * that carries a RUMOR on the stream for 3 s, then drops the next such frame. Two members on the starter's defaults
 * (their own loops at 250 ms, coalescing 20 ms, SIGNED) on real TCP, so the only path between them is the spoiled link
 * and the repair is the protocol's own: the lazy push, the pull, or anti-entropy. REPAIRED when each item reaches the
 * reader exactly once within the §5.4 limit (32 anti-entropy periods); the delayed frame, landing after the repair,
 * delivers nothing twice. Each item's time from publish to delivery goes to the store as
 * {@code core.gossip.repair-after-loss}.
 *
 * <p>The frames cross real TCP; the two nodes' loops, their coalescing and the spoiler's hold run on one
 * {@link VirtualTime} (srad/agentspaces-event-loop.md EL-Q-TST-03), which the test moves in 10 ms steps, settling the
 * reader threads between steps. The quiet stretch and the hold are waited out in fleet time, not slept, and the
 * repair times are fleet milliseconds.
 */
@Timeout(120)
class GossipRepairTcpSpoilerTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Duration WINDOW = Duration.ofMillis(20);
    private static final Duration HOLD = Duration.ofSeconds(3);
    private static final String STREAM = "s";

    private final GroupId groupId = GroupId.of("zGossipRepairTcp");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zGossipRepairTcp",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "gossip-repair-tcp",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();
    /** The pair's time: their loops, their coalescing and the spoiler's hold. */
    private final VirtualTime time = VirtualTime.create();
    /** One step of the pair's time. */
    private static final Duration STEP = Duration.ofMillis(10);

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /** Moves the pair's time a step, then lets the TCP readers hand over what arrived. */
    private void step() {
        time.advance(STEP);
        VirtualThreadsIdle.await(Duration.ofSeconds(2));
    }

    /** Moves the pair's time by {@code span}, a step at a time. */
    private void run(Duration span) {
        long end = time.monoNanos() + span.toNanos();
        while (time.monoNanos() < end) {
            step();
        }
    }

    /** Moves the pair's time until the condition holds or {@code bound} of it has passed; returns whether it held. */
    private boolean runUntil(Duration bound, java.util.function.BooleanSupplier until) {
        long end = time.monoNanos() + bound.toNanos();
        while (!until.getAsBoolean()) {
            if (time.monoNanos() >= end) {
                return false;
            }
            step();
        }
        return true;
    }

    /** Whether the frame, or a frame it carries, is a RUMOR on the stream. */
    private static boolean rumorOn(FrameHeader h, String stream) {
        if (h.kind().equals("RUMOR") && h.streams().contains(stream)) {
            return true;
        }
        for (FrameHeader inner : h.inner()) {
            if (rumorOn(inner, stream)) {
                return true;
            }
        }
        return false;
    }

    /** The writer's TCP transport with the script in its send path: hold the first matching frame, drop the next. */
    private final class SpoilingTcp implements Transport {
        private final TcpTransport tcp = new TcpTransport();
        private final EnvelopeHeaders headers = new EnvelopeHeaders();
        /** 0 = not armed; 1 = hold the next; 2 = drop the next; 3 = done. */
        final AtomicInteger stage = new AtomicInteger();
        volatile long heldAtNanos;
        volatile long releasedAtNanos;
        volatile long droppedAtNanos;

        @Override
        public String scheme() {
            return tcp.scheme();
        }

        @Override
        public Set<Capability> capabilities() {
            return tcp.capabilities();
        }

        @Override
        public boolean signalled() {
            return tcp.signalled();
        }

        @Override
        public TransportConnection dial(String address) throws IOException {
            return wrap(tcp.dial(address));
        }

        @Override
        public TransportConnection dial(String address, PeerId target) throws IOException {
            return wrap(tcp.dial(address, target));
        }

        @Override
        public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
            return tcp.listen(bindAddress, c -> onAccept.accept(wrap(c)));
        }

        private TransportConnection wrap(TransportConnection c) {
            return new TransportConnection() {
                @Override
                public String remoteAddress() {
                    return c.remoteAddress();
                }

                @Override
                public Optional<PeerId> attestedPeer() {
                    return c.attestedPeer();
                }

                @Override
                public X509Certificate[] remoteChain() {
                    return c.remoteChain();
                }

                @Override
                public void send(byte[] frame) throws IOException {
                    int s = stage.get();
                    if (s == 1 || s == 2) {
                        boolean match = headers.header(frame).map(h -> rumorOn(h, STREAM)).orElse(false);
                        if (match && stage.compareAndSet(s, s + 1)) {
                            if (s == 1) {
                                heldAtNanos = time.monoNanos();
                                time.schedule(HOLD, () -> {
                                    try {
                                        releasedAtNanos = time.monoNanos();
                                        c.send(frame);
                                    } catch (IOException e) {
                                        // the link went away while the frame was held: it is lost, as on the wire
                                    }
                                });
                            } else {
                                droppedAtNanos = time.monoNanos();
                            }
                            return;
                        }
                    }
                    c.send(frame);
                }

                @Override
                public void onReceive(Consumer<byte[]> receiver) {
                    c.onReceive(receiver);
                }

                @Override
                public void onClose(Runnable listener) {
                    c.onClose(listener);
                }

                @Override
                public void close() {
                    c.close();
                }
            };
        }
    }

    private static int freePort() throws IOException {
        // below the ephemeral range, so no other process takes it before the test binds it (parallel forks)
        return ai.badmonkey.agentspaces.test.Ports.free();
    }

    @Test
    void elSp12aHeldRumorAndADroppedRumorAreEachRepairedOnceOverTcp() throws Exception {
        SpoilingTcp spoiler = new SpoilingTcp();
        int pa = freePort();
        int pb = freePort();
        PeerNode a = PeerNode.builder(PeerIdentity.generate()).clock(time).coalesce(WINDOW).build();
        PeerNode b = PeerNode.builder(PeerIdentity.generate()).clock(time).coalesce(WINDOW).build();
        nodes.add(a);
        nodes.add(b);
        a.listen(spoiler, "127.0.0.1:" + pa);
        b.listen(new TcpTransport(), "127.0.0.1:" + pb);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + pa, 0)));
        Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();
        Map<String, Long> arrivedAt = new ConcurrentHashMap<>();
        rb.gossip().onStream(STREAM, (from, id, payload) -> {
            arrivedAt.putIfAbsent(id, time.monoNanos());
            deliveries.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
        });
        a.startTicking(TICK);
        b.startTicking(TICK);
        assertThat(runUntil(Duration.ofSeconds(30), () -> ra.membership().allMembers().size() == 1
                && rb.membership().allMembers().size() == 1)).as("the pair forms within 30 s").isTrue();
        // a warm-up item crosses the clean link, then the pair goes quiet so its rounds stretch
        ra.gossip().publish(STREAM, "warm", "warm".getBytes(StandardCharsets.UTF_8));
        assertThat(runUntil(Duration.ofSeconds(10), () -> arrivedAt.containsKey("warm"))).as("the warm-up item crosses")
                .isTrue();
        run(Duration.ofSeconds(10)); // ten quiet seconds of the pair's time

        spoiler.stage.set(1);
        long publishedHeld = time.monoNanos();
        ra.gossip().publish(STREAM, "held", "held".getBytes(StandardCharsets.UTF_8));
        run(Duration.ofMillis(500));
        long publishedDropped = time.monoNanos();
        ra.gossip().publish(STREAM, "dropped", "dropped".getBytes(StandardCharsets.UTF_8));

        long limitMillis = 32 * groupAd.gossip().period().toMillis(); // §5.4: 32 anti-entropy periods
        runUntil(Duration.ofMillis(limitMillis + 2_000), () -> arrivedAt.containsKey("held") && arrivedAt.containsKey("dropped"));
        // the held frame lands 3 s after it was sent: run past it, so a second delivery would be seen
        long releaseBy = Math.max(spoiler.heldAtNanos, publishedHeld) + HOLD.toNanos() + Duration.ofSeconds(2).toNanos();
        run(Duration.ofNanos(Math.max(0, releaseBy - time.monoNanos())));
        assertThat(spoiler.stage.get()).as("the script held one frame and dropped the next").isEqualTo(3);
        assertThat(arrivedAt).as("REPAIRED: both items reached the reader").containsKeys("held", "dropped");
        long heldMillis = (arrivedAt.get("held") - publishedHeld) / 1_000_000L;
        long droppedMillis = (arrivedAt.get("dropped") - publishedDropped) / 1_000_000L;
        boolean heldByRepair = spoiler.releasedAtNanos == 0 || arrivedAt.get("held") < spoiler.releasedAtNanos;
        Measure.record("core.gossip.repair-after-loss", Unit.MS, Direction.LOWER, heldMillis,
                Rec.with().params(Params.of("harness", "tcp", "loss", "held-3s")).kind(Kind.PERF));
        Measure.record("core.gossip.repair-after-loss", Unit.MS, Direction.LOWER, droppedMillis,
                Rec.with().params(Params.of("harness", "tcp", "loss", "dropped")).kind(Kind.PERF));
        System.out.printf("EL-SP-12 over TCP: the held item reached the reader after %d ms (%s), the dropped item"
                        + " after %d ms; deliveries %s%n", heldMillis,
                heldByRepair ? "repaired before the held frame landed" : "by the held frame itself", droppedMillis,
                deliveries);
        assertThat(heldMillis).as("§5.4 limit: 32 anti-entropy periods").isLessThanOrEqualTo(limitMillis);
        assertThat(droppedMillis).as("§5.4 limit: 32 anti-entropy periods").isLessThanOrEqualTo(limitMillis);
        assertThat(deliveries.get("held").get()).as("the held item delivered once, the late frame deduplicated")
                .isEqualTo(1);
        assertThat(deliveries.get("dropped").get()).as("the dropped item delivered once").isEqualTo(1);
    }
}
