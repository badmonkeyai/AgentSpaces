package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.peering.wire.Envelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §5, the retry bound on a one-way partition: a loss that outlasts the lost-member
 * retry window ({@link PeerNode#LOST_RETRY_WINDOW_MILLIS}). While it lasts, the retries must
 * cost a bounded number of frames and connections; once the direction returns, the pair must
 * find each other again, through a third member when there is one and on their own when not.
 */
class RobustnessRetryBoundTest {

    /** Ticks past the window, with margin: the window plus two minutes. */
    private static final int PAST_THE_WINDOW = (int) (PeerNode.LOST_RETRY_WINDOW_MILLIS / 1000) + 120;

    private final RobustnessFleet fleet = new RobustnessFleet("RetryBound");

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    /** How many retry rounds the backoff allows in a span: 5, 10, 20, 30, 30... seconds apart. */
    private static long retriesIn(long spanMillis) {
        long interval = PeerNode.LOST_RETRY_INTERVAL_MILLIS;
        long at = 0;
        long count = 0;
        while (at + interval <= spanMillis) {
            at += interval;
            count++;
            interval = Math.min(interval * 2, PeerNode.LOST_RETRY_MAX_INTERVAL_MILLIS);
        }
        return count;
    }

    private static long rumors(PeerNode node) {
        Map<Envelope.Kind, Long> sent = node.traffic().sentByKind();
        return sent.getOrDefault(Envelope.Kind.RUMOR, 0L);
    }

    @Test
    @Timeout(120)
    void aOneWayLossPastTheWindowHealsThroughAThirdMemberAtBoundedCost() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        fleet.add("c", "a");
        assertThat(fleet.settle(10)).isTrue();

        long aRumorsBefore = rumors(fleet.nodes.get("a"));
        long bRumorsBefore = rumors(fleet.nodes.get("b"));
        fleet.network.oneWay("a", "b");
        int maxConnectionsA = 0;
        int maxConnectionsB = 0;
        for (int i = 0; i < PAST_THE_WINDOW; i++) {
            fleet.tickAll(1);
            maxConnectionsA = Math.max(maxConnectionsA, fleet.nodes.get("a").channelModes().size());
            maxConnectionsB = Math.max(maxConnectionsB, fleet.nodes.get("b").channelModes().size());
            if (i % 60 == 0) {
                fleet.publish("c", "during-" + i);
            }
        }
        long aRumorsDuring = rumors(fleet.nodes.get("a")) - aRumorsBefore;
        long bRumorsDuring = rumors(fleet.nodes.get("b")) - bRumorsBefore;
        System.out.printf("three members, one-way for %d s: A sent %d rumor frames, B %d; max connections A %d, B %d%n",
                PAST_THE_WINDOW, aRumorsDuring, bRumorsDuring, maxConnectionsA, maxConnectionsB);
        // The retries are bounded: one introduction per interval for the window, plus the refreshes.
        long retries = retriesIn(PeerNode.LOST_RETRY_WINDOW_MILLIS);
        long refreshes = PAST_THE_WINDOW / 30 + 1;
        assertThat(aRumorsDuring).as("A's introductions during the loss are bounded").isLessThanOrEqualTo(3 * (retries + refreshes) + 20);
        assertThat(bRumorsDuring).as("B's introductions during the loss are bounded").isLessThanOrEqualTo(3 * (retries + refreshes) + 20);
        assertThat(maxConnectionsA).as("A's connections stay bounded").isLessThanOrEqualTo(3);
        assertThat(maxConnectionsB).as("B's connections stay bounded").isLessThanOrEqualTo(3);
        assertThat(fleet.nodes.get("b").quarantined(fleet.id("a"))).isFalse();
        assertThat(fleet.nodes.get("a").quarantined(fleet.id("b"))).isFalse();
        assertThat(fleet.news.get("b")).as("C's news still reached B").contains("during-0", "during-600");

        fleet.network.twoWay("a", "b");
        assertThat(fleet.settle(90)).as("the pair heals through C once the direction returns").isTrue();
        fleet.publish("a", "after");
        fleet.tickAll(4);
        assertThat(fleet.news.get("b")).contains("after");
        fleet.assertInvariants();
    }

    @Test
    @Timeout(120)
    void aOneWayLossPastTheWindowHealsBetweenTwoMembersOnTheirOwn() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(10)).isTrue();

        long aRumorsBefore = rumors(fleet.nodes.get("a"));
        long bRumorsBefore = rumors(fleet.nodes.get("b"));
        fleet.network.oneWay("a", "b");
        int maxConnectionsA = 0;
        int maxConnectionsB = 0;
        for (int i = 0; i < PAST_THE_WINDOW; i++) {
            fleet.tickAll(1);
            maxConnectionsA = Math.max(maxConnectionsA, fleet.nodes.get("a").channelModes().size());
            maxConnectionsB = Math.max(maxConnectionsB, fleet.nodes.get("b").channelModes().size());
        }
        long aRumorsDuring = rumors(fleet.nodes.get("a")) - aRumorsBefore;
        long bRumorsDuring = rumors(fleet.nodes.get("b")) - bRumorsBefore;
        System.out.printf("two members, one-way for %d s: A sent %d rumor frames, B %d; max connections A %d, B %d; views A=%d B=%d%n",
                PAST_THE_WINDOW, aRumorsDuring, bRumorsDuring, maxConnectionsA, maxConnectionsB,
                fleet.runtimes.get("a").membership().allMembers().size(),
                fleet.runtimes.get("b").membership().allMembers().size());
        // B's view is empty, so B re-dials its seed at the backed-off cadence for the whole loss, and
        // A answers each dial with at most three frames (the re-admission flood, the introduction
        // back, a direct refresh) before dropping B for silence again.
        long retries = retriesIn(PAST_THE_WINDOW * 1000L);
        long refreshes = PAST_THE_WINDOW / 30 + 1;
        assertThat(bRumorsDuring).as("B's seed re-dials back off").isLessThanOrEqualTo(retries + refreshes + 10);
        assertThat(aRumorsDuring).as("A's answers follow B's cadence").isLessThanOrEqualTo(4 * retries + refreshes + 10);
        assertThat(maxConnectionsA).isLessThanOrEqualTo(2);
        assertThat(maxConnectionsB).isLessThanOrEqualTo(2);
        assertThat(fleet.nodes.get("b").quarantined(fleet.id("a"))).isFalse();
        assertThat(fleet.nodes.get("a").quarantined(fleet.id("b"))).isFalse();

        fleet.network.twoWay("a", "b");
        int cap = (int) (PeerNode.LOST_RETRY_MAX_INTERVAL_MILLIS / 1000);
        assertThat(fleet.settle(cap + 30)).as("the pair heals on its own within the retry cap once the direction returns").isTrue();
        fleet.publish("a", "after");
        fleet.tickAll(4);
        assertThat(fleet.news.get("b")).contains("after");
        fleet.assertInvariants();
    }
}
