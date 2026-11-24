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
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Owner decision 5 of phase 6 (srad/agentspaces-event-loop.md §10.33): the 16-to-8 shrink of a mixed-version group.
 * Sixteen members on virtual time, four of them configured as the older peers (0.6.0's 30 s member TTL with no cycle
 * margin above it, and the 30 s keepalive). The group idles until each new member's cycle-bound TTL has risen
 * (90 s at sixteen under the defaults); then eight members fall silent at once, two of them older. As the views shrink
 * the cycle-bound TTL falls (§3.11).
 *
 * <p>PREVENTED: no survivor drops another survivor at any point (no member heard within the old TTL is dropped by the
 * fall). REPAIRED: every departed member leaves every survivor's view within the TTL its judge held when it fell silent
 * plus 5 s, and after the shrink settles a member that then falls silent leaves within the new, eight-member TTL plus
 * 5 s.
 *
 * <p>The older peers are today's code configured as 0.6.0 was: what this harness cannot show is 0.6.0's own probe
 * order and its lack of a targeted relay (MixedTtlIT covers those over TCP with the released jar).
 */
@Timeout(900)
class MixedShrinkSpoilerTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final int SIZE = 16;
    private static final Set<Integer> OLDER = Set.of(3, 7, 11, 15);
    /** The members that fall silent at once: half the group, two of them older (11 and 15). */
    private static final Set<Integer> DEPART = Set.of(8, 9, 10, 11, 12, 13, 14, 15);
    /** The member that falls silent after the shrink settles. */
    private static final int LATE = 6;

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<GroupRuntime> runtimes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zMixedShrink");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zMixedShrink",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "mixed-shrink", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /** 0.6.0's membership as today's code can be configured: a 30 s TTL, no cycle margin above it, 30 s keepalive. */
    private static GroupMembership.Config older() {
        return new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2,
                GroupMembership.Config.DEFAULT_PROBE_INTERVAL, Duration.ofSeconds(30), 1.0);
    }

    private void member(int i) throws IOException {
        String name = "m" + i;
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).randomSeed(7_000 + i).build();
        node.listen(network.register(name), name);
        GroupRuntime rt = node.joinGroup(groupAd, OLDER.contains(i) ? older() : GroupMembership.Config.defaults(),
                i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "m0", 0)));
        node.startTicking(TICK);
        nodes.add(node);
        runtimes.add(rt);
    }

    private boolean holds(int judge, int member) {
        return runtimes.get(judge).membership().member(nodes.get(member).peerId()).isPresent();
    }

    private void silence(int member) {
        for (int i = 0; i < SIZE; i++) {
            if (i != member) {
                network.partition("m" + member, "m" + i);
            }
        }
    }

    @Test
    void sixteenToEightDropsNoSurvivorAndEveryDepartedMemberWithinItsBound() throws Exception {
        for (int i = 0; i < SIZE; i++) {
            member(i);
        }
        long waited = 0;
        while (!runtimes.stream().allMatch(rt -> rt.membership().allMembers().size() == SIZE - 1)) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("sixteen members form within 300 s of virtual time").isLessThan(300_000);
        }
        time.advance(Duration.ofMinutes(10)); // quiet: the probe interval stretches and the cycle-bound TTL rises
        assertThat(runtimes.stream().allMatch(rt -> rt.membership().allMembers().size() == SIZE - 1))
                .as("no member left a quiet group of sixteen").isTrue();
        long[] ttlBefore = new long[SIZE];
        for (int i = 0; i < SIZE; i++) {
            ttlBefore[i] = runtimes.get(i).membership().memberTtlMillis();
        }
        long newTtl16 = ttlBefore[0];
        System.out.printf("shrink: member TTL at sixteen, new %d ms, older %d ms%n", newTtl16, ttlBefore[3]);

        // eight fall silent at once
        long t0 = time.millis();
        DEPART.forEach(this::silence);
        List<Integer> survivors = new ArrayList<>();
        for (int i = 0; i < SIZE; i++) {
            if (!DEPART.contains(i)) {
                survivors.add(i);
            }
        }
        Map<Integer, Long> goneAt = new LinkedHashMap<>();
        int survivorDrops = 0;
        long steps = 0;
        while (goneAt.size() < DEPART.size()) {
            time.advance(TICK);
            steps++;
            for (int d : DEPART) {
                if (!goneAt.containsKey(d) && survivors.stream().noneMatch(s -> holds(s, d))) {
                    goneAt.put(d, time.millis() - t0);
                }
            }
            for (int s : survivors) {
                for (int o : survivors) {
                    if (s != o && !holds(s, o)) {
                        survivorDrops++;
                    }
                }
            }
            assertThat(steps).as("the shrink completes within 400 s of virtual time").isLessThan(1_600);
        }
        for (int k = 0; k < 4 * 60 * 2; k++) { // two minutes more: the TTL has fallen; no survivor may go
            time.advance(TICK);
            for (int s : survivors) {
                for (int o : survivors) {
                    if (s != o && !holds(s, o)) {
                        survivorDrops++;
                    }
                }
            }
        }
        long newTtl8 = runtimes.get(0).membership().memberTtlMillis();
        long worst = goneAt.values().stream().mapToLong(Long::longValue).max().orElseThrow();
        System.out.printf("shrink 16 to 8: departed gone after %s ms (worst %d); member TTL now %d ms (was %d); "
                + "survivor drops %d%n", goneAt, worst, newTtl8, newTtl16, survivorDrops);
        Measure.record("core.shrink.departed-gone", Unit.MS, Direction.LOWER, worst,
                Rec.with().params(Params.of("harness", "virtual", "from", SIZE, "to", SIZE - DEPART.size(),
                        "older", OLDER.size())).kind(Kind.PERF));
        assertThat(survivorDrops).as("PREVENTED: no survivor dropped another while the TTL fell").isZero();
        long bound = java.util.Arrays.stream(ttlBefore).max().orElseThrow() + 5_000;
        assertThat(worst).as("REPAIRED: every departed member gone within the TTL its judges held, plus 5 s")
                .isLessThanOrEqualTo(bound);
        assertThat(newTtl8).as("the cycle-bound TTL fell with the view").isLessThan(newTtl16);

        // after the shrink settles: one more falls silent and is judged by the new, smaller TTL
        long t1 = time.millis();
        silence(LATE);
        long lateGone = -1;
        for (long k = 0; k < 1_000 && lateGone < 0; k++) {
            time.advance(TICK);
            int late = LATE;
            if (survivors.stream().filter(s -> s != late).noneMatch(s -> holds(s, late))) {
                lateGone = time.millis() - t1;
            }
        }
        long ttlAt8 = survivors.stream().filter(s -> s != LATE)
                .mapToLong(s -> runtimes.get(s).membership().memberTtlMillis()).max().orElseThrow();
        System.out.printf("after the shrink: a member falling silent gone after %d ms (member TTL %d ms)%n",
                lateGone, ttlAt8);
        assertThat(lateGone).as("a late departure is found").isPositive();
        assertThat(lateGone).as("REPAIRED: within the new TTL plus 5 s").isLessThanOrEqualTo(ttlAt8 + 5_000);
    }
}
