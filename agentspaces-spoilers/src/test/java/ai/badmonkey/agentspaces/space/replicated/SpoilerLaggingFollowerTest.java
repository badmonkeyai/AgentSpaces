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

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Measurement;
import ai.badmonkey.agentspaces.measure.SimWait;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Ceiling;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.FleetInvariants;
import ai.badmonkey.agentspaces.test.VirtualTime;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SC-B6, the core form (srad/spoiler-peers.md §3.8 B): a follower whose log index lags. The ordered log's leader takes
 * an entry; every PIPE_DATA frame from the leader to one follower (its AppendEntries among them) is held, while the
 * space's gossip delivers the committed claim to that follower; the hold is released when the follower raises TAKEN for
 * the entry. REPAIR: the follower's own log then applies the claim, its commit index moves past zero, and its committed
 * listener hears the same claim the leader committed. The bound after the release is one append retry plus an
 * anti-entropy round on the fleet clock, asserted on ticks; the leader's take runs as a task of the fleet's virtual time,
 * so its commit is a hard bound too (srad/agentspaces-event-loop.md decision 12, §10.17). Before, the order came from the wall-clock wait of the app's
 * desk log (Superfriends {@code DeskLog}, 10 s); the app form follows after 0.7.0.
 */
class SpoilerLaggingFollowerTest {

    /** One append retry plus a round, in ticks of {@link SpaceFleet#PERIOD}, with room: checked against the runs. */
    private static final int APPEND_REPAIR_TICKS = 20;
    /** The ordered log's attempt window (3 s) in ticks of {@link SpaceFleet#PERIOD}: the take's hard bound. */
    private static final int ATTEMPT_TICKS = 10;

    @Test
    @Timeout(120)
    void aFollowerThatHearsTheClaimBeforeItsLogRecordsTheIndexWhenItsLogApplies() throws Exception {
        runLaggingFollower(true);
    }

    @Test
    @Timeout(120)
    void theNegativeControlRecordsTheIndexInOrder() throws Exception {
        runLaggingFollower(false);
    }

    private void runLaggingFollower(boolean spoil) throws Exception {
        try (SpaceFleet fleet = new SpaceFleet(spoil ? "LaggingFollower" : "LaggingFollowerControl",
                ConflictStrategyType.LEASE_RACE, "desk")) {
            fleet.agentName = "desk";
            fleet.member("a", 1, b -> b.settleWindow(Duration.ZERO));
            fleet.member("b", 2, b -> b.settleWindow(Duration.ZERO), "a");
            fleet.member("c", 3, b -> b.settleWindow(Duration.ZERO), "a");
            fleet.tickAll(4);
            Map<String, OrderedTakes> logs = new LinkedHashMap<>();
            Map<String, Map<EntryId, TakeClaim>> committed = new LinkedHashMap<>();
            int seed = 200;
            for (SpaceFleet.Member m : fleet.members.values()) {
                OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(m.runtime(), fleet.codec), fleet.codec,
                        m.identity(), "desk", fleet.ids(), fleet.clock, seed++, m.space());
                m.group().ordered("desk", ordered);
                logs.put(m.name(), ordered);
                Map<EntryId, TakeClaim> heard = new ConcurrentHashMap<>();
                committed.put(m.name(), heard);
                fleet.own(ordered.onCommitted(heard::put));
            }
            SimWait.ticks("spoiler.lagging-follower.leader", 60, fleet::tickAll,
                    () -> logs.values().stream().filter(o -> o.raft().isLeader()).count() == 1, Wait.with());
            String leader = logs.entrySet().stream().filter(e -> e.getValue().raft().isLeader()).findFirst().orElseThrow()
                    .getKey();
            String follower = fleet.members.keySet().stream().filter(n -> !n.equals(leader)).findFirst().orElseThrow();
            SpaceFleet.Member atLeader = fleet.get(leader);
            SpaceFleet.Member atFollower = fleet.get(follower);
            atLeader.space().write(new TaskEntry("order", 1), Lease.of(Duration.ofMinutes(30)));
            fleet.tickAll(2);
            long indexBefore = logs.get(follower).raft().commitIndex();

            FleetInvariants oracle = FleetInvariants.baseline(fleet.probe(Set.of(), Template.of(FindingEntry.class)));
            Script script = !spoil ? Script.honest(leader) : Script.of(leader).seed(4730)
                    .rule("hold-appends")
                    .when(Trigger.frame(Envelope.Kind.PIPE_DATA).from(leader).to(follower))
                    .act(Action.hold()).always()
                    .until(Trigger.raised(follower, SpaceEvent.Kind.TAKEN))
                    .ceiling(Ceiling.periods(40))
                    .build();
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, SpaceFleet.PERIOD,
                    PeerNode.ChannelAuth.SIGNED, script).watch(follower, atFollower.space(), Template.of(TaskEntry.class));
            fleet.afterTick.add(runner::tick);

            // the leader takes through the log, as a task of the fleet's time: its attempt window waits on that time
            // (decision 12), so the commit is a hard bound in ticks: inside one attempt window, the other follower's
            // appends making the majority while this one's are held
            VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> take = fleet.callTicking("spoiler.lagging-follower.take",
                    ATTEMPT_TICKS, () -> logs.get(leader).take(Template.of(TaskEntry.class),
                            Lease.of(Duration.ofMinutes(5)), Duration.ofSeconds(10)));
            Optional<TakenEntry<TaskEntry>> taken = take.value();
            assertThat(taken).as("the leader's take committed").isPresent();
            assertThat(take.took()).as("within one attempt window").isLessThanOrEqualTo(Duration.ofSeconds(3));
            EntryId entry = taken.get().entryId();

            // after the release (or at once, honestly) the follower's log applies the claim and its listener hears it
            Measurement repair = SimWait.ticks("spoiler.lagging-follower.index", APPEND_REPAIR_TICKS, fleet::tickSlowly,
                    () -> committed.get(follower).containsKey(entry)
                            && logs.get(follower).raft().commitIndex() > indexBefore,
                    Wait.with().diagnostics(() -> "follower " + follower + " commit index "
                            + logs.get(follower).raft().commitIndex() + ", heard " + committed.get(follower).keySet()
                            + ", firings " + runner.report().signatures()));
            assertThat(committed.get(follower).get(entry)).as("the follower's log committed the leader's claim")
                    .isEqualTo(committed.get(leader).get(entry));
            assertThat(atFollower.space().currentClaim(entry)).as("the follower's space holds the same claim")
                    .contains(committed.get(leader).get(entry));
            ScriptRunner.Report report = runner.finish();
            System.out.printf("SC-B6 %s: follower %s recorded index %d after %.0f ticks; firing sequence %s%n",
                    spoil ? "lagging" : "control", follower, logs.get(follower).raft().commitIndex(), repair.value(),
                    report.signatures());
            if (spoil) {
                assertThat(report.signatures()).as("appends held, then released by the follower's TAKEN")
                        .anyMatch(s -> s.startsWith("hold-appends hold"))
                        .anyMatch(s -> s.startsWith("hold-appends release") && s.contains("TAKEN"));
            }
            oracle.assertHolds(report.expectedDrops(), true);
        }
    }
}
