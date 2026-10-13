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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;

import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The wire, as the console peer sees it (PERF2 §4, ROBUSTNESS.md §6b): the node's traffic
 * counters as rates since the previous read, frames and bytes by {@link Envelope.Kind}, the
 * signed, session and bare split, and the robustness counters that say what the network did to
 * this node rather than what a signer did: frames dropped as unverifiable, as stale, as replays,
 * and the members held quarantined. Per group: the view and its suspects, anti-entropy rounds,
 * the gossip tree's lazy peers and its HAVE, GRAFT and PRUNE frames, and rumors by stream.
 *
 * <p>Registered like any {@link ConsolePanel}; the served UI polls its data every few seconds,
 * which is the sampling period of the rates. The first read reports totals only (no rates), and
 * a read sooner than a second after the previous one reports the previous rates again, so a
 * second consumer cannot zero the first's window.
 */
public final class TrafficPanel implements ConsolePanel {

    /** The frame kinds counted as the data plane: everything but membership probes and introductions. */
    private static final long MIN_WINDOW_MILLIS = 1_000L;

    private final PeerNode node;
    private final InstantSource clock;
    private Sample previous;
    private Map<String, Object> lastRates = Map.of();

    private record Sample(long atMillis, PeerNode.Traffic traffic, long signed, long session, long bare,
                          long dropped, long stale, long replayed) {
    }

    private TrafficPanel(PeerNode node, InstantSource clock) {
        this.node = Objects.requireNonNull(node, "node");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** A panel over a node's counters, timed by the system clock. */
    public static TrafficPanel of(PeerNode node) {
        return new TrafficPanel(node, InstantSource.system());
    }

    /** A panel timed by the given clock (tests). */
    public static TrafficPanel of(PeerNode node, InstantSource clock) {
        return new TrafficPanel(node, clock);
    }

    @Override
    public String id() {
        return "traffic";
    }

    @Override
    public String title() {
        return "Wire traffic";
    }

    @Override
    public synchronized Object data() {
        long now = clock.millis();
        PeerNode.Traffic traffic = node.traffic();
        Sample current = new Sample(now, traffic, node.signedFramesSent(), node.sessionFramesSent(),
                node.bareFramesSent(), node.framesDropped(), node.framesStale(), node.framesReplayed());
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("peer", node.peerId().value());
        doc.put("totals", totals(current));
        if (previous != null && now - previous.atMillis() >= MIN_WINDOW_MILLIS) {
            lastRates = rates(previous, current);
            previous = current;
        } else if (previous == null) {
            previous = current;
        }
        doc.put("rates", lastRates);
        Map<String, Long> modes = new TreeMap<>();
        node.channelModes().values().forEach(mode -> modes.merge(mode, 1L, Long::sum));
        doc.put("channels", modes);
        List<Map<String, Object>> groups = new java.util.ArrayList<>();
        for (GroupRuntime runtime : node.groups()) {
            groups.add(group(runtime));
        }
        doc.put("groups", groups);
        return doc;
    }

    private static Map<String, Object> totals(Sample s) {
        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("framesSent", s.traffic().framesSent());
        totals.put("framesReceived", s.traffic().framesReceived());
        totals.put("bytesSent", s.traffic().bytesSent());
        totals.put("bytesReceived", s.traffic().bytesReceived());
        totals.put("signedSent", s.signed());
        totals.put("sessionSent", s.session());
        totals.put("bareSent", s.bare());
        totals.put("droppedUnverifiable", s.dropped());
        totals.put("droppedStale", s.stale());
        totals.put("droppedReplayed", s.replayed());
        totals.put("sentByKind", byKind(s.traffic().sentByKind()));
        totals.put("receivedByKind", byKind(s.traffic().receivedByKind()));
        totals.put("bytesSentByKind", byKind(s.traffic().bytesSentByKind()));
        return totals;
    }

    private static Map<String, Long> byKind(Map<Envelope.Kind, Long> counts) {
        Map<String, Long> out = new TreeMap<>();
        counts.forEach((kind, count) -> {
            if (count != 0) {
                out.put(kind.name(), count);
            }
        });
        return out;
    }

    /** Per-second rates over the window between two samples, rounded to a hundredth. */
    private static Map<String, Object> rates(Sample from, Sample to) {
        double seconds = (to.atMillis() - from.atMillis()) / 1000.0;
        Map<String, Object> rates = new LinkedHashMap<>();
        rates.put("windowSeconds", round(seconds));
        rates.put("framesSentPerSecond", round((to.traffic().framesSent() - from.traffic().framesSent()) / seconds));
        rates.put("framesReceivedPerSecond", round((to.traffic().framesReceived() - from.traffic().framesReceived()) / seconds));
        rates.put("bytesSentPerSecond", round((to.traffic().bytesSent() - from.traffic().bytesSent()) / seconds));
        rates.put("bytesReceivedPerSecond", round((to.traffic().bytesReceived() - from.traffic().bytesReceived()) / seconds));
        rates.put("signedSentPerSecond", round((to.signed() - from.signed()) / seconds));
        rates.put("sessionSentPerSecond", round((to.session() - from.session()) / seconds));
        rates.put("droppedPerSecond", round((to.dropped() + to.stale() + to.replayed()
                - from.dropped() - from.stale() - from.replayed()) / seconds));
        Map<String, Double> sent = new TreeMap<>();
        to.traffic().sentByKind().forEach((kind, count) -> {
            long delta = count - from.traffic().sentByKind().getOrDefault(kind, 0L);
            if (delta != 0) {
                sent.put(kind.name(), round(delta / seconds));
            }
        });
        rates.put("sentByKindPerSecond", sent);
        Map<String, Double> received = new TreeMap<>();
        to.traffic().receivedByKind().forEach((kind, count) -> {
            long delta = count - from.traffic().receivedByKind().getOrDefault(kind, 0L);
            if (delta != 0) {
                received.put(kind.name(), round(delta / seconds));
            }
        });
        rates.put("receivedByKindPerSecond", received);
        return rates;
    }

    private static Map<String, Object> group(GroupRuntime runtime) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("group", runtime.id().value());
        List<GroupMembership.Member> members = runtime.membership().allMembers();
        doc.put("members", members.size());
        doc.put("suspects", members.stream().filter(GroupMembership.Member::suspect).count());
        GossipBus gossip = runtime.gossip();
        doc.put("antiEntropyRounds", gossip.antiEntropyRounds());
        doc.put("lazyPeers", gossip.lazyPeers().size());
        Map<String, Long> tree = new TreeMap<>();
        for (String kind : List.of("HAVE", "GRAFT", "PRUNE")) {
            tree.put(kind, gossip.lazyFramesSent(kind));
        }
        doc.put("treeFramesSent", tree);
        doc.put("rumorsSentByStream", new TreeMap<>(gossip.rumorsSentByStream()));
        return doc;
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
