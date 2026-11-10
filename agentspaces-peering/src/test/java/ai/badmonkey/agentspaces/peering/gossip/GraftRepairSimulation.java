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
package ai.badmonkey.agentspaces.peering.gossip;

import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Swarm retries (ROBUSTNESS.md §10.4): real {@link GossipBus} instances on a discrete-event fabric in virtual time,
 * seeded, so the cost of an eager copy that is merely late shows without a TCP fleet and reruns give the same figures.
 *
 * <p>The fabric models what made the TCP runs late. Each node sends one frame at a time and handles one frame at a
 * time, each costing a fixed part plus its bytes over a rate, so a relay that forwards a large item to several eager
 * peers delays the last of them; every link adds a base delay, a short random tail, and now and then a hiccup of tens
 * of milliseconds. Every node ticks every 250 ms at its own phase and probes one member a second, the probe and its
 * acknowledgement carrying the recent items as the node's do. The workload is the {@code PdtStress push} run's: three
 * rounds in which every member publishes a small item, then one large item from member 0. The fabric has no
 * anti-entropy, so an item that lazy repair misses stays missed.
 */
final class GraftRepairSimulation {

    /** The node tick of a fleet ({@code MiniFleet.TICK}). */
    static final long TICK_MILLIS = 250;
    /** The SWIM probe interval: each node probes one member a second. */
    static final long PROBE_MILLIS = 1_000;
    /** The large item of the run, about one push bundle of P2-Q-PRF-04. */
    static final int LARGE_BYTES = 220_000;
    static final String LARGE_STREAM = "pdt:refs:plans";

    /**
     * What a run adds to the honest workload (swarm retries, flood shares, ROBUSTNESS.md §10.7).
     *
     * @param floodEveryMillis how often the last member announces {@link GossipBus#HAVE_ITEMS_MAX} new items it never
     *                         serves to every member, or 0 for no flood. The flooder otherwise follows the protocol and
     *                         publishes no small items, as in the gate's adversarial review (§10.6)
     * @param burst            how many small items member 1 publishes at once in the second round, so its announcements
     *                         name that many items: 1 in the honest workload, hundreds for a busy group
     */
    record Scenario(long floodEveryMillis, int burst) {
        static final Scenario HONEST = new Scenario(0, 1);
    }

    /**
     * One run's figures.
     *
     * @param writerCopies   RUMOR frames of the large item the writer sent: eager pushes and GRAFT answers
     * @param itemCopies     RUMOR frames of the large item every member sent
     * @param lastCopyMillis from the publish until the last honest member held the large item, or -1 if one never did
     * @param grafts         GRAFT frames every member sent in the run
     * @param premature      grafts every member counted as premature: a copy from another member came anyway
     * @param largeMissed    honest members that never got the large item by gossip
     * @param smallMissed    small items an honest member published that another honest member never got: what lazy
     *                       repair missed
     * @param eagerEdges     the members the writer pushes to at the publish: its view less its lazy members
     * @param eagerCopies    the writer's eager pushes of the large item
     * @param graftAnswers   the copies of the large item the writer sent in answer to a GRAFT
     * @param answersAfterOther of those, the answers to a member that had heard another member announce the item before
     *                       it asked (P2-Q-PRF-04 (a), as the owner restated it on 2026-11-09)
     * @param answersWriterFirst of those, the answers to a member that heard the writer announce the item first and
     *                       another member since, which the rule "never the first heard while another is left" forbids
     */
    record Result(long writerCopies, long itemCopies, long lastCopyMillis, long grafts, long premature,
                  int largeMissed, int smallMissed, int eagerEdges, long eagerCopies, long graftAnswers, long answersAfterOther,
                  long answersWriterFirst) {
    }

    private record Event(long at, long seq, Runnable run) {
    }

    private final Random random;
    private final int peers;
    private final Scenario scenario;
    /** The flooding member, or -1 when the run has no flood. */
    private final int flooder;
    /** The member that published each small item, by the item's index. */
    private final List<Integer> smallPublishers = new ArrayList<>();
    /** The small items each member got, by their ids. */
    private final List<java.util.Set<String>> smallHeld = new ArrayList<>();
    private final List<PeerId> ids = new ArrayList<>();
    private final Map<PeerId, Integer> index = new HashMap<>();
    private final List<GossipBus> buses = new ArrayList<>();
    private final long[] uplinkFree;
    private final long[] cpuFree;
    private final long[][] baseLatency;
    private final PriorityQueue<Event> events = new PriorityQueue<>((a, b) -> a.at != b.at
            ? Long.compare(a.at, b.at) : Long.compare(a.seq, b.seq));
    private long seq;
    private long now;
    private long writerCopies;
    private long itemCopies;
    private final long[] firstCopyAt;
    /** Whether each member had heard a member other than the writer announce the large item. */
    private final boolean[] heardOther;
    /** Whether each member heard the large item announced, and whether the writer's announcement came first. */
    private final boolean[] heardAny;
    private final boolean[] heardWriterFirst;
    private long answersWriterFirst;
    private int eagerEdges;
    private long graftAnswers;
    private long answersAfterOther;
    /** Set while the writer handles a GRAFT for the large item, so the copies it sends count as answers. */
    private boolean answering;
    private boolean answeringAfterOther;
    private boolean answeringWriterFirst;

    private GraftRepairSimulation(long seed, int peers, Scenario scenario) {
        this.random = new Random(seed);
        this.peers = peers;
        this.scenario = scenario;
        this.flooder = scenario.floodEveryMillis() > 0 ? peers - 1 : -1;
        this.uplinkFree = new long[peers];
        this.cpuFree = new long[peers];
        this.heardOther = new boolean[peers];
        this.heardAny = new boolean[peers];
        this.heardWriterFirst = new boolean[peers];
        this.baseLatency = new long[peers][peers];
        this.firstCopyAt = new long[peers];
        java.util.Arrays.fill(firstCopyAt, -1);
        for (int i = 0; i < peers; i++) {
            byte[] key = new byte[32];
            key[0] = (byte) i;
            key[1] = (byte) (i >> 8);
            key[31] = 1;
            PeerId id = PeerId.fromPublicKey(key);
            ids.add(id);
            index.put(id, i);
        }
        for (int i = 0; i < peers; i++) {
            for (int j = 0; j < peers; j++) {
                baseLatency[i][j] = 1 + random.nextInt(3);
            }
        }
        for (int i = 0; i < peers; i++) {
            int self = i;
            List<PeerId> others = new ArrayList<>(ids);
            others.remove(ids.get(i));
            PeerSampler sampler = n -> n >= others.size() ? List.copyOf(others) : others.subList(0, n);
            GossipBus bus = new GossipBus(sampler, sender(self), CborCodec.defaultCodec(), 3, GossipBus.DEFAULT_HOPS,
                    () -> now, new Random(random.nextLong()));
            bus.lazyPush(peer -> true);
            buses.add(bus);
        }
        for (int i = 0; i < peers; i++) {
            int self = i;
            java.util.Set<String> held = new java.util.HashSet<>();
            smallHeld.add(held);
            buses.get(i).onStream("space", (from, id, payload) -> held.add(id));
            buses.get(i).onStream(LARGE_STREAM, (from, id, payload) -> {
                if (firstCopyAt[self] < 0) {
                    firstCopyAt[self] = now;
                }
            });
        }
    }

    /**
     * Runs the {@code PdtStress push} workload once.
     *
     * @param seed  the seed of the fabric and every bus
     * @param peers the members, the writer among them
     * @return the figures
     */
    static Result run(long seed, int peers) {
        return run(seed, peers, Scenario.HONEST);
    }

    /**
     * Runs the {@code PdtStress push} workload once with what the scenario adds.
     *
     * @param seed     the seed of the fabric and every bus
     * @param peers    the members, the writer among them
     * @param scenario a flood, a burst, or {@link Scenario#HONEST}
     * @return the figures
     */
    static Result run(long seed, int peers, Scenario scenario) {
        return new GraftRepairSimulation(seed, peers, scenario).run();
    }

    private Result run() {
        for (int i = 0; i < peers; i++) {
            int node = i;
            long phase = random.nextInt((int) TICK_MILLIS);
            schedule(phase, () -> tickLoop(node));
            List<Integer> order = new ArrayList<>();
            for (int j = 0; j < peers; j++) {
                if (j != node) {
                    order.add(j);
                }
            }
            Collections.shuffle(order, random);
            schedule(random.nextInt((int) PROBE_MILLIS), () -> probeLoop(node, order, 0));
        }
        if (flooder >= 0) {
            schedule(random.nextInt((int) scenario.floodEveryMillis()), this::floodLoop);
        }
        long round = 2_000;
        for (int r = 0; r < 3; r++) {
            for (int i = 0; i < peers; i++) {
                if (i == flooder) {
                    continue;
                }
                int node = i;
                List<byte[]> payloads = new ArrayList<>();
                List<String> itemIds = new ArrayList<>();
                for (int n = node == 1 && r == 1 ? scenario.burst() : 1; n > 0; n--) {
                    byte[] payload = new byte[1_000];
                    random.nextBytes(payload);
                    payloads.add(payload);
                    itemIds.add("small-" + smallPublishers.size());
                    smallPublishers.add(node);
                }
                schedule(round + random.nextInt(100), () -> {
                    for (int n = 0; n < payloads.size(); n++) {
                        buses.get(node).publish("space", itemIds.get(n), payloads.get(n));
                    }
                });
            }
            round += 1_500;
        }
        long publishAt = round;
        byte[] large = new byte[LARGE_BYTES];
        random.nextBytes(large);
        schedule(publishAt, () -> {
            firstCopyAt[0] = now;
            eagerEdges = peers - 1 - buses.get(0).lazyPeers().size();
            buses.get(0).publish(LARGE_STREAM, "large", large);
        });
        long end = publishAt + 20_000;
        while (!events.isEmpty() && events.peek().at <= end) {
            Event event = events.poll();
            now = event.at;
            event.run.run();
        }
        long last = 0;
        int largeMissed = 0;
        int smallMissed = 0;
        for (int i = 0; i < peers; i++) {
            if (i == flooder) {
                continue;
            }
            if (firstCopyAt[i] < 0) {
                largeMissed++;
            } else {
                last = Math.max(last, firstCopyAt[i] - publishAt);
            }
            for (int item = 0; item < smallPublishers.size(); item++) {
                if (smallPublishers.get(item) != i && !smallHeld.get(i).contains("small-" + item)) {
                    smallMissed++;
                }
            }
        }
        long grafts = 0;
        long premature = 0;
        for (GossipBus bus : buses) {
            grafts += bus.lazyFramesSent("GRAFT");
            premature += bus.repairCounters().getOrDefault("GRAFT_PREMATURE", 0L);
        }
        return new Result(writerCopies, itemCopies, largeMissed > 0 ? -1 : last, grafts, premature, largeMissed,
                smallMissed, eagerEdges, writerCopies - graftAnswers, graftAnswers, answersAfterOther, answersWriterFirst);
    }

    private void schedule(long at, Runnable run) {
        events.add(new Event(at, seq++, run));
    }

    /** The flooder announces new items it never serves to every member, then again after the scenario's period. */
    private void floodLoop() {
        List<Bodies.HaveItem> items = new ArrayList<>();
        for (int i = 0; i < GossipBus.HAVE_ITEMS_MAX; i++) {
            byte[] hash = new byte[32];
            random.nextBytes(hash);
            items.add(new Bodies.HaveItem("space", hash));
        }
        Bodies.Have have = new Bodies.Have(items);
        for (int target = 0; target < peers; target++) {
            if (target != flooder) {
                int to = target;
                transmit(flooder, to, 120 + 40 * items.size(), () -> buses.get(to).onHave(ids.get(flooder), have));
            }
        }
        schedule(now + scenario.floodEveryMillis(), this::floodLoop);
    }

    private void tickLoop(int node) {
        buses.get(node).tick(now);
        schedule(now + TICK_MILLIS, () -> tickLoop(node));
    }

    private void probeLoop(int node, List<Integer> order, int next) {
        int target = order.get(next % order.size());
        List<Bodies.HaveItem> carried = buses.get(node).piggyback(ids.get(target));
        transmit(node, target, 120 + 40 * size(carried), () -> {
            if (carried != null) {
                noteHave(target, node, carried);
                buses.get(target).onHave(ids.get(node), new Bodies.Have(carried));
            }
            List<Bodies.HaveItem> back = buses.get(target).piggyback(ids.get(node));
            transmit(target, node, 120 + 40 * size(back), () -> {
                if (back != null) {
                    noteHave(node, target, back);
                    buses.get(node).onHave(ids.get(target), new Bodies.Have(back));
                }
            });
        });
        schedule(now + PROBE_MILLIS, () -> probeLoop(node, order, next + 1));
    }

    private static int size(List<?> list) {
        return list == null ? 0 : list.size();
    }

    /** Sends one frame: the sender's uplink, the link's delay, then the receiver's handling, each in turn. */
    private void transmit(int from, int to, int bytes, Runnable deliver) {
        long departs = Math.max(now, uplinkFree[from]) + cost(bytes);
        uplinkFree[from] = departs;
        long tail = (long) (-Math.log(1 - random.nextDouble()) * 4);
        long hiccup = random.nextInt(100) < 5 ? 20 + random.nextInt(130) : 0;
        long arrives = departs + baseLatency[from][to] + tail + hiccup;
        schedule(arrives, () -> {
            long handled = Math.max(now, cpuFree[to]) + cost(bytes);
            cpuFree[to] = handled;
            schedule(handled, deliver);
        });
    }

    /** What sending or handling a frame costs a node: a fixed part and its bytes at 10 MB a second. */
    private static long cost(int bytes) {
        return 1 + bytes / 10_000;
    }

    /** Notes who announced the large item to a member: the writer first or not, and another member than the writer. */
    private void noteHave(int receiver, int announcer, List<Bodies.HaveItem> items) {
        if (items.stream().noneMatch(item -> LARGE_STREAM.equals(item.streamId()))) {
            return;
        }
        if (!heardAny[receiver]) {
            heardAny[receiver] = true;
            heardWriterFirst[receiver] = announcer == 0;
        }
        if (announcer != 0) {
            heardOther[receiver] = true;
        }
    }

    private GossipBus.FrameSender sender(int self) {
        return new GossipBus.FrameSender() {
            @Override
            public void sendRumor(PeerId to, Bodies.Rumor rumor) {
                if (rumor.streamId().equals(LARGE_STREAM)) {
                    itemCopies++;
                    if (self == 0) {
                        writerCopies++;
                        if (answering) {
                            graftAnswers++;
                            answersAfterOther += answeringAfterOther ? 1 : 0;
                            answersWriterFirst += answeringAfterOther && answeringWriterFirst ? 1 : 0;
                        }
                    }
                }
                int target = index.get(to);
                transmit(self, target, rumor.payload().length + 120, () -> buses.get(target).onRumor(ids.get(self), rumor));
            }

            @Override
            public void sendDigest(PeerId to, Bodies.Digest digests) {
            }

            @Override
            public void sendPullResp(PeerId to, Bodies.PullResp deltas) {
            }

            @Override
            public void sendHave(PeerId to, Bodies.Have have) {
                int target = index.get(to);
                transmit(self, target, 120 + 40 * have.items().size(), () -> {
                    noteHave(target, self, have.items());
                    buses.get(target).onHave(ids.get(self), have);
                });
            }

            @Override
            public void sendGraft(PeerId to, Bodies.Graft graft) {
                int target = index.get(to);
                // what the asker had heard when it asked decides whether an answer from the writer breaks P2-Q-PRF-04 (a)
                boolean other = heardOther[self];
                boolean writerFirst = heardWriterFirst[self];
                boolean large = LARGE_STREAM.equals(graft.streamId());
                transmit(self, target, 120, () -> {
                    answering = target == 0 && large;
                    answeringAfterOther = other;
                    answeringWriterFirst = writerFirst;
                    buses.get(target).onGraft(ids.get(self), graft);
                    answering = false;
                });
            }

            @Override
            public void sendPrune(PeerId to, Bodies.Prune prune) {
                int target = index.get(to);
                transmit(self, target, 120, () -> buses.get(target).onPrune(ids.get(self), prune));
            }
        };
    }
}
