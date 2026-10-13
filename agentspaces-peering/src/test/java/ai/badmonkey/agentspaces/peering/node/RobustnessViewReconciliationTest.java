package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §6b: the membership view converges by anti-entropy, not by flood luck. A member
 * whose introduction never reached another (the random fan-out toward hint-less members, or a
 * one-way loss at the moment of joining) used to stay a stranger to it for good once nothing
 * re-flooded the advertisement; the view is now anti-entropy state like a space.
 */
class RobustnessViewReconciliationTest {

    private final RobustnessFleet fleet = new RobustnessFleet("ViewReconcile");

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @Test
    @Timeout(60)
    void aMemberWhoseIntroductionWasLostIsLearnedByAntiEntropy() throws Exception {
        fleet.add("a", null);
        fleet.add("c", "a", PeerNode.ChannelAuth.SESSION, GroupMembership.Config.defaults(), false);
        assertThat(fleet.settle(10)).isTrue();
        // B joins while nothing B sends reaches C and nothing A forwards reaches C: C never hears
        // B's introduction, and B, which hears C's, holds C while C does not hold B.
        fleet.network.oneWay("b", "c");
        fleet.network.oneWay("a", "c");
        fleet.add("b", "a");
        fleet.tickAll(3);
        assertThat(fleet.runtimes.get("a").membership().member(fleet.id("b"))).isPresent();
        assertThat(fleet.runtimes.get("c").membership().member(fleet.id("b"))).as("C missed the introduction").isEmpty();
        fleet.network.twoWay("b", "c");
        fleet.network.twoWay("a", "c");
        // No flood repeats B's introduction; C learns B from the next anti-entropy round's view
        // delta (its partner's digest lists a member C lacks) or from B's direct refresh.
        assertThat(fleet.settle(20)).as("the views converge once traffic flows again").isTrue();
        assertThat(fleet.runtimes.get("c").membership().member(fleet.id("b"))).isPresent();
        fleet.publish("b", "from-b");
        fleet.tickAll(3);
        assertThat(fleet.news.get("c")).contains("from-b");
        fleet.assertInvariants();
    }
}
