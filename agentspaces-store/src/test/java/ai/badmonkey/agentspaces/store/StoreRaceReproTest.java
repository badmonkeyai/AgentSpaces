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

import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.space.replicated.SpaceWire;
import ai.badmonkey.agentspaces.space.replicated.TakeClaim;
import ai.badmonkey.agentspaces.test.LinkFilter;
import ai.badmonkey.agentspaces.test.SimNetwork;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;

/**
 * SCRATCH (store-race experiment, 2026-11-22; not part of any build): repeats StorePeerTest's
 * loser-keeps-holding race with the interleaving recorded, under optional CPU stress, injected send jitter and
 * extra fleet ticks. Runs only with {@code -Dstore.race.repro=true}.
 */
@EnabledIfSystemProperty(named = "store.race.repro", matches = "true")
class StoreRaceReproTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final int RUNS = Integer.getInteger("store.race.runs", 20);
    private static final int STRESS = Integer.getInteger("store.race.stress", 0);
    private static final int JITTER = Integer.getInteger("store.race.jitter", 0);
    private static final int PRETICKS = Integer.getInteger("store.race.preticks", 0);
    private static final int MIDTICKS = Integer.getInteger("store.race.midticks", 0);
    /** "tick" ticks the fleet while the stores claim, as the test does; "still" waits in real time with no tick. */
    private static final String CLAIM_WAIT = System.getProperty("store.race.claimwait", "tick");
    /** Fleet ticks between holding the links and writing the pin. */
    private static final int PREHOLD = Integer.getInteger("store.race.prehold", 0);
    /**
     * Forces the order of the two anti-entropy exchanges between the stores once the winner's link to the loser opens:
     * "fail" drops every frame to the winner that carries the loser's completed state under the loser's claim (the
     * loser's and the writer's anti-entropy answers that would otherwise race the winner's claim); "pass" gates the winner's frames (to the loser and the writer) that carry its claim until the winner noted
     * the loser's completion (the winner's pull learns the completion under the loser's claim first). Unset: no forcing.
     */
    private static final String FORCE = System.getProperty("store.race.force", "");
    private static final boolean VERBOSE_ALL = Boolean.getBoolean("store.race.verbose");

    private final ConcurrentLinkedQueue<String> log = new ConcurrentLinkedQueue<>();
    private volatile long t0;
    private volatile int tick;
    private volatile int claimedTick;
    private final Set<String> heldLinks = new CopyOnWriteArraySet<>();
    private volatile String entry;
    private volatile Map<String, String> names = Map.of();
    private volatile Map<String, String> nameToId = Map.of();
    /** The tick at which each store's claim rumor was first sent toward the other store, by the claimant's name. */
    /** force=fail: the winner's name, to which no frame carrying the loser's completion under the loser's claim passes. */
    private volatile String dropToWinner;
    private volatile String loserLetter;
    private final Set<String> gatingClaims = new CopyOnWriteArraySet<>();
    private final Map<String, Integer> claimRumorToRival = new java.util.concurrent.ConcurrentHashMap<>();

    private void note(String s) {
        log.add(String.format("%7.1fms t%-4d [%s] %s", (System.nanoTime() - t0) / 1e6, tick,
                Thread.currentThread().getName().isEmpty() ? "v" + Thread.currentThread().threadId() : Thread.currentThread().getName(), s));
    }

    @Test
    void repeat() throws Exception {
        List<Thread> stress = new ArrayList<>();
        for (int i = 0; i < STRESS; i++) {
            Thread t = Thread.ofPlatform().daemon().name("stress-" + i).start(() -> {
                long x = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    x += Long.numberOfTrailingZeros(x * 31 + 7);
                }
            });
            stress.add(t);
        }
        int failed = 0;
        Map<String, Integer> outcomes = new LinkedHashMap<>();
        try {
            for (int run = 0; run < RUNS; run++) {
                log.clear();
                String outcome;
                try {
                    outcome = once(run);
                } catch (Throwable e) {
                    outcome = "ERROR " + e;
                }
                outcomes.merge(outcome.startsWith("PASS") ? "PASS" : outcome.split(":")[0], 1, Integer::sum);
                boolean fail = !outcome.startsWith("PASS");
                if (fail) {
                    failed++;
                }
                System.out.println("STORE-RACE run " + run + " " + outcome);
                if (fail || VERBOSE_ALL) {
                    log.forEach(l -> System.out.println("  | " + l));
                }
            }
        } finally {
            stress.forEach(Thread::interrupt);
        }
        System.out.println("STORE-RACE SUMMARY runs=" + RUNS + " failed=" + failed + " stress=" + STRESS + " jitter="
                + JITTER + " preticks=" + PRETICKS + " midticks=" + MIDTICKS + " claimwait=" + CLAIM_WAIT + " prehold=" + PREHOLD + " force=" + FORCE + " " + outcomes);
    }

    private String once(int run) throws Exception {
        try (StoreFleet fleet = new StoreFleet()) {
            t0 = System.nanoTime();
            tick = 0;
            heldLinks.clear();
            dropToWinner = null;
            gatingClaims.clear();
            claimRumorToRival.clear();
            entry = null;
            StoreFleet.Spec racer = StoreFleet.Spec.store(StorePolicy.of(1L << 30)).storeBuilder(sb -> sb.takeLease(Duration.ofDays(1)));
            StoreFleet.Peer a = fleet.newPeer("race-a", 16, racer);
            StoreFleet.Peer b = fleet.newPeer("race-b", 17, racer, "race-a");
            StoreFleet.Peer writer = fleet.newPeer("writer", 18, StoreFleet.Spec.member(), "race-a");
            names = Map.of(a.id().toString(), "A", b.id().toString(), "B", writer.id().toString(), "Wr");
            nameToId = Map.of("race-a", a.id().toString(), "race-b", b.id().toString(), "writer", writer.id().toString());
            fleet.network.filter(null, null, frame -> {
                String dir = frame.from() + ">" + frame.to();
                String held = heldLinks.contains(dir) ? " HELD" : "";
                String items = entry == null ? "" : describe(frame.bytes());
                String w = dropToWinner;
                if (w != null && frame.to().equals(w) && items.contains("COMPLETED") && items.contains("claim[" + loserLetter + "#")) {
                    note("drop " + dir + " " + items);
                    return LinkFilter.Verdict.drop();
                }
                if (gatingClaims.contains(dir) && items.contains("claim[" + names.get(nameToId.get(frame.from())))) {
                    note("gate claims:" + dir + " " + items);
                    return LinkFilter.Verdict.hold("claims:" + dir);
                }
                if (items.contains("RUMOR(c,") && frame.from().startsWith("race-") && frame.to().startsWith("race-")) {
                    claimRumorToRival.putIfAbsent(frame.from(), tick);
                }
                if (!items.isEmpty()) {
                    note("send " + frame.from() + ">" + frame.to() + held + " " + items);
                }
                if (JITTER > 0 && entry != null) {
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(JITTER + 1));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return LinkFilter.Verdict.pass();
            });
            fleet.settle(6);
            until(fleet, 30, () -> storeCards(writer).size() == 2, null);
            String root = StoreFleet.facts(writer, "late", 300, 11).root();

            List<String[]> links = List.of(new String[]{"race-a", "race-b"}, new String[]{"race-b", "race-a"},
                    new String[]{"race-a", "writer"}, new String[]{"race-b", "writer"});
            links.forEach(l -> {
                fleet.network.hold(l[0], l[1]);
                heldLinks.add(l[0] + ">" + l[1]);
            });
            if ("pass".equals(FORCE)) {
                gatingClaims.addAll(List.of("race-a>race-b", "race-b>race-a", "race-a>writer", "race-b>writer"));
            }
            for (int i = 0; i < PREHOLD; i++) {
                step(fleet, null);
            }
            var handle = fleet.writePin(writer, new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 3600, 1, writer.id(), List.of(), null),
                    Duration.ofMinutes(10));
            entry = handle.entryId().toString();
            note("pin written, entry " + entry);
            Probe probe = new Probe(a, b, writer, handle.entryId(), root);
            if ("still".equals(CLAIM_WAIT)) {
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (System.nanoTime() < deadline && !(a.contentSpace().currentClaim(handle.entryId()).isPresent()
                        && b.contentSpace().currentClaim(handle.entryId()).isPresent())) {
                    Thread.sleep(2);
                }
                probe.check();
            }
            if (!until(fleet, 30, () -> a.contentSpace().currentClaim(handle.entryId()).isPresent()
                    && b.contentSpace().currentClaim(handle.entryId()).isPresent(), probe)) {
                return "NOCLAIM";
            }
            TakeClaim claimA = a.contentSpace().currentClaim(handle.entryId()).orElseThrow();
            TakeClaim claimB = b.contentSpace().currentClaim(handle.entryId()).orElseThrow();
            boolean aWins = TakeClaim.merge(claimA, claimB).equals(claimA);
            StoreFleet.Peer winner = aWins ? a : b;
            StoreFleet.Peer loser = aWins ? b : a;
            probe.roles(winner, loser);
            claimedTick = tick;
            loserLetter = names.get(loser.id().toString());
            if ("fail".equals(FORCE)) {
                dropToWinner = winner.name();
            }
            note("both claimed; winner " + winner.name() + " loser " + loser.name());
            if (PRETICKS > 0) {
                for (int i = 0; i < PRETICKS; i++) {
                    step(fleet, probe);
                }
                note("pre-ticks done: " + PRETICKS);
            }

            release(fleet, loser.name(), winner.name());
            openGates(fleet, loser.name() + ">" + winner.name());
            release(fleet, loser.name(), "writer");
            openGates(fleet, loser.name() + ">writer");
            writer.content().holdTree(root);
            if (!until(fleet, 60, () -> loser.contentStore().status().taken() == 1, probe)) {
                return "NOLOSERCOMPLETE: " + loser.contentStore().status();
            }
            note("loser completed");
            if (MIDTICKS > 0) {
                for (int i = 0; i < MIDTICKS; i++) {
                    step(fleet, probe);
                }
                note("mid-ticks done: " + MIDTICKS);
            }
            release(fleet, winner.name(), loser.name());
            if ("pass".equals(FORCE)) {
                boolean noted = until(fleet, 10, () -> probe.outbidLine(winner), probe);
                note("forced: the winner noted the loser's completion before its claim reaches the loser: " + noted);
            }
            openGates(fleet, winner.name() + ">" + loser.name());
            if (!until(fleet, 30, () -> StoreFleet.receiptHolders(winner, root).contains(loser.id()), probe)) {
                return "NORECEIPTATWINNER";
            }
            note("premise: loser's receipt at winner; winner taken " + winner.contentStore().status().taken()
                    + ", outbid line " + probe.outbidLine(winner));
            if (winner.contentStore().status().taken() != 0) {
                return "PREMISE: winner completed first";
            }
            release(fleet, winner.name(), "writer");
            openGates(fleet, winner.name() + ">writer");
            until(fleet, 30, () -> loser.contentSpace().currentClaim(handle.entryId())
                    .map(c -> c.holder().peer().equals(winner.id())).orElse(false), probe);
            until(fleet, 60, () -> StoreFleet.receiptHolders(writer, root).size() == 1
                    && a.contentStore().status().taken() + a.contentStore().status().abandoned()
                    + b.contentStore().status().taken() + b.contentStore().status().abandoned() >= 2, probe);
            for (int i = 0; i < 2 * (int) (StoreFleet.HOLDING.toMillis() / 250); i++) {
                step(fleet, probe);
            }
            boolean ok = loser.contentStore().ledger().holds(root) && winner.contentStore().status().taken() == 0
                    && winner.contentStore().status().abandoned() == 1;
            String summary = "winner " + winner.name() + " taken " + winner.contentStore().status().taken() + " abandoned "
                    + winner.contentStore().status().abandoned() + " outbidAck "
                    + outbidAck(winner) + " dup " + winner.contentSpace().duplicateCompletions()
                    + "; loser taken " + loser.contentStore().status().taken() + " abandoned "
                    + loser.contentStore().status().abandoned() + " holds " + loser.contentStore().ledger().holds(root)
                    + "; ticks " + tick + "; winner's claim rumor to the loser at tick "
                    + claimRumorToRival.getOrDefault(winner.name(), -1) + ", loser's to the winner at tick "
                    + claimRumorToRival.getOrDefault(loser.name(), -1) + "; claimed by tick " + claimedTick;
            return (ok ? "PASS " : "FAIL: ") + summary;
        }
    }

    private static long outbidAck(StoreFleet.Peer p) {
        try {
            var m = ReplicatedSpace.class.getDeclaredMethod("outbidAcknowledged");
            m.setAccessible(true);
            return (long) m.invoke(p.contentSpace());
        } catch (ReflectiveOperationException e) {
            return -1;
        }
    }

    private void openGates(StoreFleet fleet, String dir) {
        if (gatingClaims.remove(dir)) {
            note("open claims:" + dir + ": " + fleet.network.releaseGate("claims:" + dir, SimNetwork.Order.FIFO, 1).size() + " frame(s)");
        }
    }

    private void release(StoreFleet fleet, String from, String to) {
        heldLinks.remove(from + ">" + to);
        int n = fleet.network.release(from, to, SimNetwork.Order.FIFO, 1);
        note("release " + from + ">" + to + ": " + n + " frame(s)");
    }

    private void step(StoreFleet fleet, Probe probe) throws InterruptedException {
        fleet.tickAll();
        tick++;
        Thread.sleep(3);
        if (probe != null) {
            probe.check();
        }
    }

    private boolean until(StoreFleet fleet, int seconds, BooleanSupplier done, Probe probe) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
        if (probe != null) {
            probe.check();
        }
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            step(fleet, probe);
        }
        return true;
    }

    /** Polls each peer's view of the race after every tick and notes each change. */
    private final class Probe {
        final StoreFleet.Peer a;
        final StoreFleet.Peer b;
        final StoreFleet.Peer writer;
        final EntryId id;
        final String root;
        StoreFleet.Peer winner;
        StoreFleet.Peer loser;
        final Map<String, String> last = new LinkedHashMap<>();

        Probe(StoreFleet.Peer a, StoreFleet.Peer b, StoreFleet.Peer writer, EntryId id, String root) {
            this.a = a;
            this.b = b;
            this.writer = writer;
            this.id = id;
            this.root = root;
        }

        void roles(StoreFleet.Peer w, StoreFleet.Peer l) {
            winner = w;
            loser = l;
        }

        boolean outbidLine(StoreFleet.Peer p) {
            try {
                var f = ReplicatedSpace.class.getDeclaredField("outbidCompletions");
                f.setAccessible(true);
                var lock = ReplicatedSpace.class.getDeclaredField("lock");
                lock.setAccessible(true);
                synchronized (lock.get(p.contentSpace())) {
                    return ((Map<?, ?>) f.get(p.contentSpace())).containsKey(id);
                }
            } catch (ReflectiveOperationException e) {
                return false;
            }
        }

        String claimOf(StoreFleet.Peer p) {
            return p.contentSpace().currentClaim(id).map(c -> names.getOrDefault(c.holder().peer().toString(), "?") + "#" + c.epoch()).orElse("-");
        }

        String view(StoreFleet.Peer p) {
            StringBuilder s = new StringBuilder();
            for (PeerId m : p.runtime().sampler().members()) {
                s.append(names.getOrDefault(m.toString(), "?"));
            }
            char[] c = s.toString().toCharArray();
            java.util.Arrays.sort(c);
            return new String(c);
        }

        void check() {
            Map<String, String> now = new LinkedHashMap<>();
            for (StoreFleet.Peer p : List.of(a, b, writer)) {
                String n = names.get(p.id().toString());
                now.put(n + ".claim", claimOf(p));
                now.put(n + ".view", view(p));
                now.put(n + ".receipts", StoreFleet.receiptHolders(p, root).stream().map(x -> names.getOrDefault(x.toString(), "?")).sorted().toList().toString());
                if (p.contentStore() != null) {
                    var st = p.contentStore().status();
                    now.put(n + ".store", "taken " + st.taken() + " abandoned " + st.abandoned() + " holds " + p.contentStore().ledger().holds(root));
                    now.put(n + ".outbidLine", Boolean.toString(outbidLine(p)));
                    now.put(n + ".state", p.contentSpace().recordOf(id).isPresent() ? "present"
                            : p.contentSpace().recordOf(id, true).isPresent() ? "completed" : "none");
                }
            }
            for (Map.Entry<String, String> e : now.entrySet()) {
                if (!e.getValue().equals(last.get(e.getKey()))) {
                    note("state " + e.getKey() + " = " + e.getValue());
                }
            }
            last.clear();
            last.putAll(now);
        }
    }

    private static List<AssetCard> storeCards(StoreFleet.Peer peer) {
        return peer.discovery().find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind"))
                && c.expiresAt().isAfter(java.time.Instant.EPOCH.plusMillis(0)));
    }

    // ---- a minimal decoder of the frames that carry the race's entry ----

    private String describe(byte[] frame) {
        List<String> out = new ArrayList<>();
        WireCodec.Signed signed = decode(frame, WireCodec.Signed.class);
        if (signed != null && signed.envelope() != null) {
            envelope(signed.envelope(), out, 0);
        }
        return String.join(" ", out);
    }

    private void envelope(Envelope envelope, List<String> out, int depth) {
        byte[] body = envelope.body();
        switch (envelope.kind()) {
            case RUMOR -> {
                Bodies.Rumor rumor = decode(body, Bodies.Rumor.class);
                if (rumor != null && rumor.streamId() != null && rumor.streamId().startsWith("space:content")) {
                    SpaceWire.Delta delta = decode(rumor.payload(), SpaceWire.Delta.class);
                    if (delta != null) {
                        String s = state(delta.state());
                        String c = claim(delta.claim());
                        if (!s.isEmpty() || !c.isEmpty()) {
                            out.add("RUMOR(" + rumor.itemId().split(":")[0] + ",hops " + rumor.hopsRemaining() + ")" + s + c);
                        } else if (delta.state() != null && delta.state().record() != null
                                && delta.state().record().type() != null && delta.state().record().type().contains("Receipt")) {
                            out.add("RUMOR(receipt write)");
                        }
                    }
                }
            }
            case PULL_RESP -> {
                Bodies.PullResp pull = decode(body, Bodies.PullResp.class);
                if (pull != null && pull.deltas() != null) {
                    for (Map.Entry<String, byte[]> e : pull.deltas().entrySet()) {
                        if (!e.getKey().startsWith("space:content")) {
                            continue;
                        }
                        SpaceWire.SyncDelta sync = decode(e.getValue(), SpaceWire.SyncDelta.class);
                        if (sync == null) {
                            continue;
                        }
                        StringBuilder s = new StringBuilder();
                        if (sync.states() != null) {
                            sync.states().forEach(x -> s.append(state(x)));
                            long receipts = sync.states().stream().filter(x -> x != null && x.record() != null
                                    && x.record().type() != null && x.record().type().contains("Receipt")).count();
                            if (receipts > 0) {
                                s.append(" receipts=").append(receipts);
                            }
                        }
                        if (sync.claims() != null && sync.claims().containsKey(entry)) {
                            s.append(claim(sync.claims().get(entry)));
                        }
                        if (!s.isEmpty()) {
                            out.add("PULL_RESP" + s);
                        }
                    }
                }
            }
            case BATCH -> {
                Bodies.Batch batch = decode(body, Bodies.Batch.class);
                if (batch != null && batch.frames() != null && depth < 4) {
                    for (byte[] inner : batch.frames()) {
                        Envelope e = decode(inner, Envelope.class);
                        if (e != null) {
                            envelope(e, out, depth + 1);
                        }
                    }
                }
            }
            default -> { }
        }
    }

    private String state(SpaceWire.EntryStateDto s) {
        if (s == null || s.entryIdOf() == null || !s.entryIdOf().toString().equals(entry)) {
            return "";
        }
        return " state[" + (s.completed() ? "COMPLETED" : "present") + (s.record() == null ? ",recordless" : "") + "]";
    }

    private String claim(SpaceWire.SignedClaim c) {
        if (c == null || c.claim() == null || !c.claim().entryId().toString().equals(entry)) {
            return "";
        }
        return " claim[" + names.getOrDefault(c.claim().holder().peer().toString(), "?") + "#" + c.claim().epoch() + "]";
    }

    private static <T> T decode(byte[] bytes, Class<T> type) {
        if (bytes == null) {
            return null;
        }
        try {
            return CODEC.fromBytes(bytes, type);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
