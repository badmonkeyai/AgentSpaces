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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A dial-only member's answers to a {@code DIGEST} (SPEC v0.1.37 §9.4): its own seed's is answered as before; one
 * that reaches it relayed or from another member is answered only for the spaces where it holds a displaced claim or
 * completion, with only those entries, so a winner's targeted round reaches it while no member draws its whole state
 * through the seed.
 *
 * <p>Three nodes on virtual time over the in-memory fabric: a seed {@code s}, a listening member {@code w} and a
 * dial-only member {@code d} of the seed. The space {@code secret} is held by {@code d}, and by {@code w} from the
 * moment it asks, never by the seed, so only a {@code DIGEST} from {@code w} to {@code d} can carry it.
 */
@Timeout(120)
class DialOnlyDigestTest {

    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final SimNetwork network = new SimNetwork();
    private final GroupId groupId = GroupId.of("zDialOnlyDigest");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zDialOnlyDigest",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "dial-only-digest",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();
    private final Map<String, PeerNode> byName = new HashMap<>();
    private final Map<String, PeerIdentity> identities = new HashMap<>();
    private final Map<String, GroupRuntime> runtimes = new HashMap<>();

    @BeforeEach
    void fleet() throws Exception {
        node("s", true);
        node("w", true, "s");
        node("d", false, "s");
        await(() -> sees("s", "w") && sees("s", "d") && sees("w", "d") && sees("d", "w") && sees("w", "s")
                && sees("d", "s"), Duration.ofSeconds(60), "the views form");
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private void node(String name, boolean listening, String... seeds) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(name.hashCode()).build();
        if (listening) {
            node.listen(network.register(name), name);
        } else {
            node.transport(network.register(name));
        }
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", seed, 0));
        }
        runtimes.put(name, node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints));
        node.startTicking(Duration.ofMillis(250));
        nodes.add(node);
        byName.put(name, node);
        identities.put(name, identity);
    }

    private ReplicatedSpace space(String node, String name) {
        return ReplicatedSpace.builder(runtimes.get(node), name, identities.get(node), "worker").clock(time)
                .settleWindow(Duration.ZERO).build();
    }

    private boolean sees(String host, String other) {
        return runtimes.get(host).membership().member(byName.get(other).peerId()).isPresent();
    }

    private void await(BooleanSupplier condition, Duration bound, String what) {
        long until = time.monoNanos() + bound.toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(time.monoNanos()).as("%s within %s", what, bound).isLessThan(until);
            time.advance(Duration.ofMillis(50));
        }
    }

    /** Asks d from w by a targeted anti-entropy round, as a winner does toward a displaced holder, and settles. */
    private void wAsksD() {
        runtimes.get("w").gossip().antiEntropyWith(byName.get("d").peerId());
        time.advance(Duration.ofSeconds(3));
    }

    private static List<String> topics(ReplicatedSpace space) {
        return space.readAll(Template.of(TaskEntry.class), 10).stream().map(TaskEntry::topic).sorted().toList();
    }

    @Test
    void aDigestFromAnotherMemberDrawsNothingWhenNothingIsDisplacedButTheSeedsIsAnsweredAsBefore() {
        ReplicatedSpace dSecret = space("d", "secret");
        ReplicatedSpace dTasks = space("d", "tasks");
        dSecret.write(new TaskEntry("private-1", 1), Lease.of(Duration.ofHours(1)));
        dTasks.write(new TaskEntry("shared-1", 1), Lease.of(Duration.ofHours(1)));
        time.advance(Duration.ofSeconds(15)); // the write's push and announcements are over: nobody else held these
        long ignored0 = runtimes.get("d").gossip().digestsIgnored();

        ReplicatedSpace wSecret = space("w", "secret");
        wAsksD();
        System.out.printf("[final] nothing displaced: w holds %s, d ignored %d DIGESTs%n", topics(wSecret),
                runtimes.get("d").gossip().digestsIgnored() - ignored0);
        assertThat(topics(wSecret)).as("a DIGEST that reached d relayed draws none of d's state").isEmpty();
        assertThat(runtimes.get("d").gossip().digestsIgnored() - ignored0).as("ignored and counted").isPositive();

        ReplicatedSpace sTasks = space("s", "tasks");
        runtimes.get("s").gossip().antiEntropyWith(byName.get("d").peerId());
        time.advance(Duration.ofSeconds(3));
        assertThat(topics(sTasks)).as("d's own seed's DIGEST is answered as before").containsExactly("shared-1");
    }

    @Test
    void aDisplacedHolderAnswersAnotherMembersDigestWithItsDisplacedEntriesAlone() {
        ReplicatedSpace dSecret = space("d", "secret");
        dSecret.write(new TaskEntry("contested", 1), Lease.of(Duration.ofHours(1)));
        Optional<TakenEntry<TaskEntry>> taken = dSecret.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(10)),
                Duration.ZERO);
        assertThat(taken).isPresent();
        EntryId contested = taken.get().entryId();
        dSecret.write(new TaskEntry("private-2", 1), Lease.of(Duration.ofHours(1)));
        time.advance(Duration.ofSeconds(15));
        dSecret.noteDisplacedOwnForTest(contested); // a better claim displaced d's: the winner wants this entry
        time.advance(Duration.ofMillis(100));

        ReplicatedSpace wSecret = space("w", "secret");
        long answers0 = dSecret.displacedAnswers();
        wAsksD();
        System.out.printf("[final] displaced: w holds %s and the contested entry's claim %s; d's displaced answers %d,"
                        + " bus answers %d%n", topics(wSecret), wSecret.currentClaim(contested).isPresent(),
                dSecret.displacedAnswers() - answers0, runtimes.get("d").gossip().displacedDigestAnswers());
        assertThat(wSecret.currentClaim(contested)).as("the displaced entry and its claim reach the asker").isPresent();
        assertThat(wSecret.currentClaim(contested).get().holder().peer()).isEqualTo(byName.get("d").peerId());
        assertThat(topics(wSecret)).as("and nothing else d holds").isEmpty();
        assertThat(dSecret.displacedAnswers() - answers0).isPositive();
    }
}
