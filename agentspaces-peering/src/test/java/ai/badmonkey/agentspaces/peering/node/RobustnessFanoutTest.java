package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PERF2 §3 item 4, the half that lazy push did not cover: the random fan-out toward members
 * without the tree hint (older nodes, the dial-only clients). Six tree members and K such
 * members; for each item published by a tree member, how many RUMOR frames the fleet sends, how
 * many copies each hint-less member receives, and whether every one of them got the item by
 * push at all (before anti-entropy would have carried it).
 */
class RobustnessFanoutTest {

    private static final int TREE_MEMBERS = 6;
    private static final int ITEMS = 20;

    private RobustnessFleet fleet;

    @AfterEach
    void tearDown() {
        if (fleet != null) {
            fleet.close();
        }
    }

    private long rumorsReceived(String address) {
        return fleet.nodes.get(address).traffic().receivedByKind().getOrDefault(Envelope.Kind.RUMOR, 0L);
    }

    private long rumorsSentFleetWide() {
        long total = 0;
        for (PeerNode node : fleet.nodes.values()) {
            total += node.traffic().sentByKind().getOrDefault(Envelope.Kind.RUMOR, 0L);
        }
        return total;
    }

    /** Builds the fleet, settles it, publishes ITEMS items from a tree member, and reports what the hint-less members saw. */
    private Result run(int plain) throws Exception {
        fleet = new RobustnessFleet("Fanout" + plain);
        fleet.add("t0", null);
        for (int i = 1; i < TREE_MEMBERS; i++) {
            fleet.add("t" + i, "t0");
        }
        List<String> plains = new ArrayList<>();
        for (int i = 0; i < plain; i++) {
            String address = "p" + i;
            plains.add(address);
            fleet.add(address, "t0", PeerNode.ChannelAuth.SESSION, GroupMembership.Config.defaults(), false);
        }
        if (!fleet.settle(40)) {
            try {
                fleet.assertInvariants();
            } catch (AssertionError e) {
                String m = e.getMessage();
                System.out.println("DIAG settle(40) failed: " + m.substring(0, Math.min(300, m.length())).replace('\n', ' '));
            }
            for (String p : plains) {
                int view = fleet.runtimes.get(p).membership().allMembers().size();
                if (view != fleet.nodes.size() - 1) {
                    System.out.println("DIAG " + p + " view " + view + " of " + (fleet.nodes.size() - 1)
                            + " connections " + fleet.nodes.get(p).channelModes().size());
                }
            }
            int more = 0;
            while (more < 200 && !fleet.settle(1)) {
                more++;
            }
            System.out.println("DIAG settled after " + (more < 200 ? (40 + more) + " ticks" : "never (240)"));
        }
        fleet.tickAll(40); // past the HAVE cadence after the admissions
        long sentBefore = rumorsSentFleetWide();
        List<Long> receivedBefore = new ArrayList<>();
        for (String p : plains) {
            receivedBefore.add(rumorsReceived(p));
        }
        for (int i = 0; i < ITEMS; i++) {
            fleet.publish("t" + (i % TREE_MEMBERS), "item-" + i);
        }
        // Delivery by push alone: the simulated network is synchronous, nothing has ticked yet.
        int pushed = 0;
        int missing = 0;
        for (String p : plains) {
            for (int i = 0; i < ITEMS; i++) {
                if (fleet.news.get(p).contains("item-" + i)) {
                    pushed++;
                } else {
                    missing++;
                }
            }
        }
        double copiesPerPlainPerItem = 0;
        for (int k = 0; k < plains.size(); k++) {
            copiesPerPlainPerItem += (rumorsReceived(plains.get(k)) - receivedBefore.get(k)) / (double) ITEMS;
        }
        copiesPerPlainPerItem /= Math.max(1, plains.size());
        double sentPerItem = (rumorsSentFleetWide() - sentBefore) / (double) ITEMS;
        int servedByPush = 0;
        for (String p : plains) {
            for (int i = 0; i < ITEMS; i++) {
                if (fleet.news.get(p).contains("item-" + i)) {
                    servedByPush++;
                    break;
                }
            }
        }
        assertThat(fleet.settle(10)).as("anti-entropy completes what push missed").isTrue();
        Result result = new Result(plain, sentPerItem, copiesPerPlainPerItem, pushed, missing, servedByPush);
        System.out.println(result);
        return result;
    }

    record Result(int plain, double rumorsSentPerItem, double copiesPerPlainPerItem, int pushed, int missing, int servedByPush) {
        @Override
        public String toString() {
            return String.format("fanout: %d tree + %d plain members: %.1f RUMOR frames per item fleet-wide, "
                    + "%.2f copies per plain member per item, %d of %d plain deliveries by push (%d by anti-entropy), %d of %d plain members pushed to at all",
                    TREE_MEMBERS, plain, rumorsSentPerItem, copiesPerPlainPerItem, pushed, pushed + missing, missing, servedByPush, plain);
        }
    }

    @Test
    @Timeout(120)
    void hintLessMembersGetEachItemAboutOnceByPushWhateverTheirNumber() throws Exception {
        List<Result> results = new ArrayList<>();
        for (int plain : new int[] {1, 3, 8, 16}) {
            results.add(run(plain));
            fleet.close();
            fleet = null;
        }
        for (Result r : results) {
            // One copy from a tree member (the origin's pick, or the seed's link for a dial-only
            // client); what rides above it is the hint-less members' own random forwarding to each
            // other, which an older node does by design. Before the rule: 2.25, 3.97 and 2.61 copies.
            assertThat(r.copiesPerPlainPerItem()).as("%d plain members: about one copy each from the tree", r.plain())
                    .isLessThanOrEqualTo(2.0);
            assertThat(r.servedByPush()).as("%d plain members: the weak slot leaves nobody out", r.plain())
                    .isEqualTo(r.plain());
            if (r.plain() <= TREE_MEMBERS / 2) {
                assertThat(r.missing()).as("%d plain members: all by push", r.plain()).isZero();
            }
        }
    }
}
