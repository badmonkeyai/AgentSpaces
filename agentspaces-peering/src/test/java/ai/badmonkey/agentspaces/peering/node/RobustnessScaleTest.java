package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §3.10 past the twenty-member seed: fifty members. Three things change shape at
 * that size and are measured here rather than assumed: how long fifty admissions take to settle,
 * what an idle member costs per tick (the direct self-advertisement refresh goes to every member,
 * so the fleet-wide cost is quadratic by design and must stay small per member), and how many
 * ticks every view needs to drop a crashed member, which the probe alone (one member per second,
 * round robin, stretched fourfold when quiet) cannot bound at fifty and the advertisement TTL
 * must. A churn round at that size then runs the invariants.
 */
class RobustnessScaleTest {

    private static final int MEMBERS = 50;
    private static final GroupMembership.Config CONFIG =
            new GroupMembership.Config(Duration.ofSeconds(20), Duration.ofSeconds(2), 2);

    private RobustnessFleet fleet;

    @AfterEach
    void tearDown() {
        if (fleet != null) {
            fleet.close();
        }
    }

    private long framesSent() {
        long total = 0;
        for (PeerNode node : fleet.nodes.values()) {
            total += node.traffic().framesSent();
        }
        return total;
    }

    private Map<Envelope.Kind, Long> sentByKind() {
        Map<Envelope.Kind, Long> totals = new java.util.EnumMap<>(Envelope.Kind.class);
        for (PeerNode node : fleet.nodes.values()) {
            node.traffic().sentByKind().forEach((k, v) -> totals.merge(k, v, Long::sum));
        }
        return totals;
    }

    /** Ticks until the budget runs out or no view holds the member; returns the ticks it took, or -1. */
    private int ticksUntilNobodyHolds(String address, int budget) {
        var id = fleet.identities.get(address).peerId();
        for (int t = 1; t <= budget; t++) {
            fleet.tickAll(1);
            boolean held = false;
            for (GroupRuntime runtime : fleet.runtimes.values()) {
                if (runtime.membership().member(id).isPresent()) {
                    held = true;
                    break;
                }
            }
            if (!held) {
                return t;
            }
        }
        return -1;
    }

    /** Ticks on which some view was short of the live set: a member expired or dropped and was re-admitted. */
    private int unstableTicks;

    private double idleFramesPerMemberPerTick(int ticks) {
        long before = framesSent();
        unstableTicks = 0;
        for (int t = 0; t < ticks; t++) {
            fleet.tickAll(1);
            for (GroupRuntime runtime : fleet.runtimes.values()) {
                if (runtime.membership().allMembers().size() < fleet.nodes.size() - 1) {
                    unstableTicks++;
                    break;
                }
            }
        }
        return (framesSent() - before) / (double) (fleet.nodes.size() * ticks);
    }

    private Map<String, Long> rumorsByStream() {
        Map<String, Long> totals = new java.util.TreeMap<>();
        for (GroupRuntime runtime : fleet.runtimes.values()) {
            runtime.gossip().rumorsSentByStream().forEach((k, v) -> totals.merge(k, v, Long::sum));
        }
        return totals;
    }

    /**
     * The small fleet keeps its cadence: six members' probe cycle fits inside the default TTL, so
     * the refresh stays at its 30 s interval and an idle member costs under a frame a tick (PERF2
     * round three measured 0.7 a second on six peers). Pinned here so the scale rules cannot
     * drift the small case.
     */
    @Test
    @Timeout(120)
    void sixMembersKeepTheirIdleCadence() throws Exception {
        fleet = new RobustnessFleet("Small");
        fleet.add("n0", null);
        for (int i = 1; i < 6; i++) {
            fleet.add("n" + i, "n0");
        }
        assertThat(fleet.settle(40)).isTrue();
        fleet.tickAll(40);
        double perMemberPerTick = idleFramesPerMemberPerTick(120);
        System.out.printf("scale 6 idle: %.2f frames per member per tick%n", perMemberPerTick);
        assertThat(unstableTicks).isZero();
        assertThat(fleet.nodes.get("n0").refreshIntervalMillis(fleet.runtimes.get("n0"), 5))
                .as("six members refresh at the configured interval").isEqualTo(30_000L);
        assertThat(perMemberPerTick).as("an idle member of six costs under a frame a tick").isLessThan(1.0);
    }

    @Test
    @Timeout(600)
    void fiftyMembersSettleRunIdleCheaplyDetectACrashWithinTheTtlAndSurviveChurn() throws Exception {
        fleet = new RobustnessFleet("Scale");
        long wall = System.nanoTime();
        fleet.add("n0", null, PeerNode.ChannelAuth.SESSION, CONFIG);
        for (int i = 1; i < MEMBERS; i++) {
            fleet.add("n" + i, "n0", PeerNode.ChannelAuth.SESSION, CONFIG);
        }
        int settleTicks = -1;
        for (int t = 1; t <= 200; t++) {
            if (fleet.settle(1)) {
                settleTicks = t;
                break;
            }
        }
        System.out.printf("scale %d: settled in %d ticks (%.1f s wall), frames so far %d%n", MEMBERS, settleTicks,
                (System.nanoTime() - wall) / 1e9, framesSent());
        assertThat(settleTicks).as("fifty admissions settle").isPositive();

        // Idle cost, after the admission burst and the HAVE cadence have died down.
        fleet.tickAll(40);
        Map<Envelope.Kind, Long> before = sentByKind();
        Map<String, Long> streamsBefore = rumorsByStream();
        double perMemberPerTick = idleFramesPerMemberPerTick(60);
        Map<Envelope.Kind, Long> after = sentByKind();
        Map<String, Long> streamsAfter = rumorsByStream();
        streamsAfter.replaceAll((k, v) -> v - streamsBefore.getOrDefault(k, 0L));
        System.out.printf("scale %d idle: views short of the live set on %d of 60 ticks; rumors by stream %s%n",
                MEMBERS, unstableTicks, streamsAfter);
        StringBuilder kinds = new StringBuilder();
        after.forEach((k, v) -> {
            long delta = v - before.getOrDefault(k, 0L);
            if (delta > 0) {
                kinds.append(k).append('=').append(String.format("%.2f", delta / (60.0 * MEMBERS))).append(' ');
            }
        });
        System.out.printf("scale %d idle: %.2f frames per member per tick (%s)%n", MEMBERS, perMemberPerTick, kinds);
        // The direct refresh is the keepalive once the probe cycle outgrows the TTL: (N-1) frames
        // per refresh interval, plus the probes. Anything well above that model is a storm.
        double refreshSeconds = Math.min(30, CONFIG.memberTtl().toSeconds() / 2.0);
        double model = (MEMBERS - 1) / refreshSeconds + 1.0;
        assertThat(unstableTicks).as("no view lost a live member while idle").isZero();
        assertThat(perMemberPerTick).as("an idle member at fifty costs about the refresh model, %.1f", model)
                .isLessThan(model + 1.5);

        // A crash is noticed everywhere within the advertisement TTL plus one probe cycle.
        fleet.crash("n7");
        int detect = ticksUntilNobodyHolds("n7", 120);
        System.out.printf("scale %d: crash of n7 dropped from every view after %d ticks%n", MEMBERS, detect);
        assertThat(detect).as("detected within the TTL and a probe round").isBetween(1,
                (int) CONFIG.memberTtl().toSeconds() + (int) CONFIG.pingTimeout().toSeconds() * 2 + 4);
        assertThat(fleet.settle(60)).as("the fleet settles after the crash").isTrue();

        // One churn round of the §3.10 kind at this size.
        Random random = new Random(50);
        int next = MEMBERS;
        for (int round = 0; round < 4; round++) {
            List<String> live = new ArrayList<>(fleet.nodes.keySet());
            String x = live.get(random.nextInt(live.size()));
            String y = live.get(random.nextInt(live.size()));
            switch (round) {
                case 0 -> fleet.add("n" + next++, x, PeerNode.ChannelAuth.SESSION, CONFIG);
                case 1 -> { if (!x.equals("n0")) { fleet.crash(x); } }
                case 2 -> { if (!x.equals(y)) { fleet.network.oneWay(x, y); } }
                default -> { if (!x.equals(y)) { fleet.network.hold(x, y); } }
            }
            for (int i = 0; i < 5; i++) {
                String from = new ArrayList<>(fleet.nodes.keySet()).get(random.nextInt(fleet.nodes.size()));
                fleet.publish(from, "r" + round + "-" + i);
            }
            fleet.tickAll(3);
            fleet.network.heal();
            for (String p : List.copyOf(fleet.nodes.keySet())) {
                for (String q : List.copyOf(fleet.nodes.keySet())) {
                    if (!p.equals(q)) {
                        fleet.network.twoWay(p, q);
                        fleet.network.release(p, q, ai.badmonkey.agentspaces.test.SimNetwork.Order.values()[random.nextInt(4)], 50);
                    }
                }
            }
            long t0 = System.nanoTime();
            assertThat(fleet.settle(90)).as("round %d settles", round).isTrue();
            System.out.printf("scale %d: churn round %d settled (%.1f s wall)%n", MEMBERS, round, (System.nanoTime() - t0) / 1e9);
        }
        for (String address : fleet.nodes.keySet()) {
            assertThat(fleet.nodes.get(address).framesDropped()).as("%s dropped nothing as malformed", address).isZero();
        }
        System.out.printf("scale %d: total %.1f s wall%n", MEMBERS, (System.nanoTime() - wall) / 1e9);
    }
}
