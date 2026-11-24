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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-SP-02 of srad/agentspaces-event-loop.md §5.1 on the loop's own pace: a node sleeps between deadlines while its
 * block fetch retry waits on a silent owner, and a second holder appears 3 s after the write. REPAIRED: the retry asks
 * the second holder at its next due and the block lands; no backstop run finds work. Every node runs on one virtual
 * time with its loop started and nobody calls {@code tick()}; the block retries still run through the deprecated
 * {@code onTick} adapter at {@code tick-millis} until phase 2 makes them due work (EL-T-09), so this is the phase-1
 * shape of the scenario, and its seam-cut half (the retry's own {@code wake()} cut fails the test, EL-A-02) waits for
 * that wake to exist.
 *
 * <p>A fetch attempt waits for its answer on its node's time source ({@code BlockExchange.fetch}, phase 6), so the
 * attempts' own timeouts (800 ms, then 2 s) pass in virtual time with every other deadline, and the test only advances
 * it: no real-time wait remains.
 */
@Timeout(300)
class BlockRetrySpoilerTest {

    /** An entry whose payload is over the 64 KiB inline limit, so it travels as a block. */
    public record Scan(String name, byte[] data) {
    }

    private static final Duration TICK = Duration.ofMillis(250);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-21T00:00:00Z"));
    private final GroupId groupId = GroupId.of("zBlockRetrySpoiler");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zBlockRetrySpoiler",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "block-retry",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<String, Member> members = new LinkedHashMap<>();

    private record Member(PeerNode node, GroupRuntime runtime, ReplicatedSpace space, BlockExchange blocks) {
    }

    @AfterEach
    void tearDown() {
        members.values().forEach(m -> m.node().close());
    }

    private Member member(String name, String seed) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        BlockExchange blocks = new BlockExchange(runtime, CborCodec.defaultCodec(), time,
                BlockExchange.DEFAULT_QUOTA_BYTES, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "scans", identity, "writer").clock(time)
                .settleWindow(Duration.ZERO).blocks(blocks).build();
        node.startTicking(TICK);
        Member m = new Member(node, runtime, space, blocks);
        members.put(name, m);
        return m;
    }

    /** Advances virtual time in 50 ms steps until the condition holds, within a bound of virtual time. */
    private void advanceUntil(String what, Duration bound, BooleanSupplier done) {
        long end = time.monoNanos() + bound.toNanos();
        while (!done.getAsBoolean()) {
            assertThat(time.monoNanos()).as("%s within %s of virtual time", what, bound).isLessThan(end);
            time.advance(Duration.ofMillis(50));
        }
    }

    @Test
    void elSp02aRetryWaitingOnASilentOwnerAsksASecondHolderAtItsNextDue() throws Exception {
        Member a = member("a", null);
        Member b = member("b", "a");
        Member c = member("c", "a");
        long formed = 0;
        while (!members.values().stream().allMatch(m -> m.runtime().membership().allMembers().size() == 2)) {
            time.advance(TICK);
            formed += TICK.toMillis();
            assertThat(formed).as("the group forms within 120 s of virtual time").isLessThan(120_000);
        }
        time.advance(Duration.ofMinutes(1)); // quiet: every loop sleeps between its deadlines

        // the owner falls silent for blocks: it keeps its records flowing but never answers a BLOCK_WANT
        Script script = Script.of("a").seed(2102)
                .rule("owner-silent").when(Trigger.frame(Envelope.Kind.BLOCK).from("a")).act(Action.drop()).always()
                .build();
        ScriptRunner runner = ScriptRunner.attach(network, time, Duration.ofSeconds(1), PeerNode.ChannelAuth.SIGNED,
                script);
        // a watcher on B: the replica starts the block fetch itself and owes it the deferred WRITTEN event
        List<String> seen = new CopyOnWriteArrayList<>();
        b.space().notify(Template.of(Scan.class), event -> {
            if (event.kind() == SpaceEvent.Kind.WRITTEN) {
                seen.add(event.entry().name());
            }
        }, Lease.of(Duration.ofMinutes(30)));
        long found0 = b.node().loopStats().backstopFoundWork();
        byte[] data = new byte[150_000];
        new Random(2102).nextBytes(data);
        long written = time.millis();
        a.space().write(new Scan("scan-1", data), Lease.of(Duration.ofMinutes(30)));
        assertThat(a.blocks().store().cids()).as("the owner holds the block").hasSize(1);
        String cid = a.blocks().store().cids().iterator().next();
        byte[] block = a.blocks().local(cid).orElseThrow();
        advanceUntil("B's first attempt fails on the silent owner", Duration.ofSeconds(2),
                () -> b.space().pendingBlockRetries() == 1);
        assertThat(b.blocks().local(cid)).as("nothing reached B from the silent owner").isEmpty();

        long appeared = -1;
        long landed = -1;
        while (landed < 0) {
            time.advance(Duration.ofMillis(50));
            if (appeared < 0 && time.millis() - written >= 3_000) {
                c.blocks().put(block); // the second holder appears, 3 s after the write
                appeared = time.millis();
            }
            if (b.blocks().local(cid).isPresent()) {
                landed = time.millis();
            }
            assertThat(time.millis() - written).as("the block lands within 60 s of virtual time").isLessThan(60_000);
        }
        runner.tick();
        long latency = landed - appeared;
        long found = b.node().loopStats().backstopFoundWork() - found0;
        runner.finish();
        Measure.record("core.retry.second-holder", Unit.MS, Direction.LOWER, latency,
                Rec.with().params(Params.of("harness", "loop", "members", 3)).kind(Kind.PERF));
        System.out.printf("EL-SP-02: the second holder appeared %d ms after the write; the block landed on B %d ms "
                + "after that; backstop runs that found work %d%n", appeared - written, latency, found);
        // the retry schedule (ReplicatedSpace BLOCK_RETRY_DELAYS_MILLIS 2 s, 5 s, ...; attempts time out after 800 ms,
        // then 2 s): attempts 1 and 2 start before the holder appears, so attempt 3, 5 s after attempt 2 failed, is the
        // next due that can ask it
        long nextStep = ReplicatedSpace.BLOCK_RETRY_DELAYS_MILLIS[1];
        assertThat(latency).as("§5.4 limit: twice the retry's next backoff step").isLessThanOrEqualTo(2 * nextStep);
        assertThat(b.blocks().local(cid)).as("REPAIRED: the block landed on B from the second holder").isPresent();
        assertThat(found).as("no backstop run found work (EL-Q-REL-03)").isZero();
        advanceUntil("the deferred event", Duration.ofSeconds(5), () -> !seen.isEmpty());
        assertThat(seen).as("the watcher's deferred WRITTEN event fired once").containsExactly("scan-1");
    }
}
