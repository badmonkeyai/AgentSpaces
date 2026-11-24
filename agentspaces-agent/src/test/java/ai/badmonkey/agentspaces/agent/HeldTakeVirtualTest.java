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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.OrderedTake;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Held {@code @OrderedTake} takes on {@link VirtualTime} (srad/agentspaces-event-loop.md §10.37), on a three-member
 * ordered log whose members run on their own loops:
 *
 * <ul>
 *   <li>seam wd07: the binder registers a held handler with its watchdog, which reports it past the report bound and
 *   again each time the hold doubles;</li>
 *   <li>C05 (owner decision 5 of 2026-11-23): a laptop sleep during an open take. A wake (a wall step of the sleep's
 *   length) re-checks the take at once against its wall-time lease. Past the lease it has lapsed: it is never renewed
 *   again, another member takes the entry, and the sleeper's late result is dropped (nothing completed, nothing
 *   written). Inside the lease it is renewed at once. The hold limit stays on the monotonic clock, so the sleep does
 *   not use it up.</li>
 * </ul>
 */
@Timeout(300)
class HeldTakeVirtualTest {

    public record Order(String id, long cents) {
    }

    public record Receipt(String id, String clerk) {
    }

    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zHeldTake");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zHeldTake",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "ordered",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    private record Member(String name, PeerIdentity identity, TimeSource time, PeerNode node, ReplicatedSpace orders,
                          CapabilityRuntime capabilities, AgentSpaces.GroupContext group) {
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        nodes.forEach(PeerNode::close);
    }

    /** A clerk whose handler holds its entry until released, waiting on its member's time. */
    @AgentSpec(name = "clerk", description = "Confirms orders, sometimes slowly", goals = {"confirm"})
    public static class Clerk {
        final String name;
        final TimeSource time;
        final AtomicBoolean hold = new AtomicBoolean(true);
        final AtomicBoolean released = new AtomicBoolean();
        final List<String> entered = new CopyOnWriteArrayList<>();
        final List<String> returned = new CopyOnWriteArrayList<>();

        Clerk(String name, TimeSource time) {
            this.name = name;
            this.time = time;
        }

        @OrderedTake(space = "orders", group = "ordered", lease = "30s", pollTimeout = "200ms")
        public Receipt confirm(Order order) throws InterruptedException {
            entered.add(order.id());
            while (hold.get() && !released.get()) {
                time.sleep(Duration.ofMillis(500));
            }
            returned.add(order.id());
            return new Receipt(order.id(), name);
        }
    }

    private List<Member> fleet(boolean firstSleeps) throws Exception {
        return fleet(i -> i == 0 && firstSleeps ? time.unwatchedNode() : time);
    }

    /** Three members, each on the time source the function gives it. */
    private List<Member> fleet(java.util.function.IntFunction<TimeSource> sources) throws Exception {
        List<PeerIdentity> identities = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> ids = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String name = "m" + i;
            PeerIdentity identity = identities.get(i);
            // a member that sleeps runs on a view of its own, which the shared watchdog does not watch
            TimeSource src = sources.apply(i);
            PeerNode node = PeerNode.builder(identity).clock(src).time(src).randomSeed(500 + i).build();
            node.listen(network.register(name), name);
            nodes.add(node);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "m0", 0)));
            ReplicatedSpace orders = ReplicatedSpace.builder(runtime, "orders", identity, "clerk").clock(src)
                    .settleWindow(Duration.ZERO).build();
            DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, src), codec,
                    identity.peerId());
            CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
            AgentSpaces spaces = new AgentSpaces(identity, src);
            AgentSpaces.GroupContext group = spaces.register("ordered", groupId, runtime, discovery)
                    .capabilities(capabilities);
            group.space("orders", orders);
            closeables.add(spaces);
            node.startTicking(Duration.ofMillis(250));
            members.add(new Member(name, identity, src, node, orders, capabilities, group));
        }
        await(() -> viewsFormed(members), Duration.ofSeconds(60), "the views form");
        for (int i = 0; i < 3; i++) {
            Member m = members.get(i);
            m.group().ordered("orders", OrderedTakes.over(new CapabilityPipes(groupRuntime(m), codec), codec,
                    m.identity(), "clerk", ids, m.time(), 900 + i, m.orders()));
        }
        await(() -> members.stream().filter(this::leads).count() == 1, Duration.ofSeconds(30), "a leader");
        return members;
    }

    private GroupRuntime groupRuntime(Member m) {
        return m.node().group(groupId).orElseThrow();
    }

    private boolean viewsFormed(List<Member> members) {
        return members.stream().allMatch(m -> m.node().group(groupId).map(r -> r.membership().size() == 2)
                .orElse(false));
    }

    private boolean leads(Member m) {
        return m.capabilities().providersOf(ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog.TYPE).stream()
                .anyMatch(ad -> "leader".equals(ad.parameters().get("role"))
                        && ad.issuer().equals(m.identity().peerId()));
    }

    /** Advances the fleet's time in 50 ms steps until the condition holds, within a bound of virtual time. */
    private void await(BooleanSupplier condition, Duration bound, String what) {
        long until = time.monoNanos() + bound.toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(time.monoNanos()).as("%s within %s", what, bound).isLessThan(until);
            time.advance(Duration.ofMillis(50));
        }
    }

    /** A clerk whose take lease is two minutes, renewed each minute: a wake inside it finds no renewal due. */
    @AgentSpec(name = "slow-clerk", description = "Confirms orders on a long lease", goals = {"confirm"})
    public static class SlowClerk {
        final AtomicBoolean released = new AtomicBoolean();
        final List<String> entered = new CopyOnWriteArrayList<>();
        final TimeSource time;

        SlowClerk(TimeSource time) {
            this.time = time;
        }

        @OrderedTake(space = "orders", group = "ordered", lease = "2m", pollTimeout = "200ms")
        public Receipt confirm(Order order) throws InterruptedException {
            entered.add(order.id());
            while (!released.get()) {
                time.sleep(Duration.ofMillis(500));
            }
            return new Receipt(order.id(), "slow");
        }
    }

    private Clerk bind(Member m) {
        Clerk clerk = new Clerk(m.name(), m.time());
        closeables.add(m.group().bind(clerk));
        return clerk;
    }

    private static List<Receipt> receipts(Member m) {
        return m.orders().readAll(Template.of(Receipt.class), 10);
    }

    /** Seam wd07: a held handler of a bound agent is reported by the watchdog the binder registered it with. */
    @Test
    void theWatchdogReportsAHeldHandlerOfABoundAgentAndAgainAsItsHoldDoubles() throws Exception {
        List<Member> members = fleet(false);
        Member m = members.get(1);
        Clerk clerk = bind(m);
        long reports0 = time.watchdog().heldReports();
        m.orders().write(new Order("held-1", 100), Lease.of(Duration.ofMinutes(30)));
        await(() -> !clerk.entered.isEmpty(), Duration.ofSeconds(30), "the handler took the entry");
        long tookAt = time.monoNanos();
        time.advance(Duration.ofSeconds(50));
        assertThat(time.watchdog().heldReports() - reports0).as("not before the 1 min report bound").isZero();
        time.advance(Duration.ofSeconds(14));
        assertThat(time.watchdog().heldReports() - reports0).as("reported past 1 min").isEqualTo(1);
        time.advance(Duration.ofSeconds(60));
        assertThat(time.watchdog().heldReports() - reports0).as("again once the hold doubled to 2 min")
                .isEqualTo(2);
        assertThat(m.group().binder().takeRenewals()).as("renewed meanwhile, as a held take is").isPositive();
        clerk.released.set(true);
        await(() -> !clerk.returned.isEmpty(), Duration.ofSeconds(5), "the handler returned");
        long reports = time.watchdog().heldReports();
        time.advance(Duration.ofMinutes(3));
        assertThat(time.watchdog().heldReports()).as("its registration closed with the handler").isEqualTo(reports);
        System.out.printf("wd07: held %d s, %d reports%n", (time.monoNanos() - tookAt) / 1_000_000_000L,
                reports - reports0);
    }

    /** C05: a sleep that outlasts the take's lease. The take lapses, another member takes it, the late result drops. */
    @Test
    void c05ASleepPastTheLeaseLapsesTheTakeAndItsLateResultIsDropped() throws Exception {
        List<Member> members = fleet(true);
        Member sleeper = members.get(0);
        VirtualTime.NodeTime sleeperTime = (VirtualTime.NodeTime) sleeper.time();
        sleeper.group().binder().holdLimit(Duration.ofMinutes(10)); // on, and far: the lapse is the lease's
        Clerk slow = bind(sleeper);
        sleeper.orders().write(new Order("o-1", 100), Lease.of(Duration.ofMinutes(30)));
        await(() -> !slow.entered.isEmpty(), Duration.ofSeconds(30), "the sleeper's handler took the entry");
        Clerk other = bind(members.get(1));
        other.hold.set(false); // the other clerk answers at once
        time.advance(Duration.ofSeconds(5));
        sleeperTime.suspend(); // the lid closes for two minutes; the 30 s lease runs out meanwhile
        time.advance(Duration.ofMinutes(2));
        await(() -> !other.returned.isEmpty(), Duration.ofSeconds(60), "another member took the lapsed entry");
        Duration slept = sleeperTime.resume();
        time.advance(Duration.ofMillis(100));
        AgentBinder binder = sleeper.group().binder();
        assertThat(binder.lateTakes()).as("the wake found the take lapsed at once").isEqualTo(1);
        long renewals = binder.takeRenewals();
        slow.released.set(true);
        await(() -> !slow.returned.isEmpty(), Duration.ofSeconds(5), "the sleeper's handler returned");
        time.advance(Duration.ofSeconds(40));
        assertThat(binder.takeRenewals()).as("a lapsed take is never renewed again").isEqualTo(renewals);
        assertThat(binder.holdLimitStops()).as("the hold limit (monotonic, 10 min) never acted").isZero();
        for (Member m : members.subList(1, 3)) {
            assertThat(receipts(m)).as("%s holds one receipt, the other member's: the late result was dropped",
                    m.name()).extracting(Receipt::clerk).containsExactly(members.get(1).name());
        }
        System.out.printf("C05 (past the lease): slept %d s, lateTakes %d, renewals %d%n", slept.toSeconds(),
                binder.lateTakes(), renewals);
    }

    /**
     * C05: a sleep inside the take's lease, with the hold limit on. The wake renews the take at once, and the hold
     * limit is judged on the monotonic clock, so the 20 s asleep does not count toward it.
     */
    @Test
    void c05ASleepInsideTheLeaseRenewsTheTakeAtOnceAndTheHoldLimitStaysMonotonic() throws Exception {
        List<Member> members = fleet(true);
        Member sleeper = members.get(0);
        VirtualTime.NodeTime sleeperTime = (VirtualTime.NodeTime) sleeper.time();
        AgentBinder binder = sleeper.group().binder();
        binder.holdLimit(Duration.ofSeconds(45));
        Clerk slow = bind(sleeper);
        sleeper.orders().write(new Order("o-2", 100), Lease.of(Duration.ofMinutes(30)));
        await(() -> !slow.entered.isEmpty(), Duration.ofSeconds(30), "the sleeper's handler took the entry");
        long tookWall = time.millis();
        time.advance(Duration.ofSeconds(20)); // renewed at 15 s: the lease now ends 45 s after the take
        sleeperTime.suspend();
        time.advance(Duration.ofSeconds(20)); // asleep 20 s: wall 40 s after the take, monotonic 20 s
        long wakeRenewals = binder.wakeRenewals();
        sleeperTime.resume();
        time.advance(Duration.ofMillis(10));
        assertThat(binder.wakeRenewals() - wakeRenewals).as("the wake renewed the take at once").isEqualTo(1);
        time.advance(Duration.ofSeconds(20)); // wall 60 s after the take (past 45 s), monotonic 40 s
        assertThat(time.millis() - tookWall).isGreaterThanOrEqualTo(60_000L);
        assertThat(binder.holdLimitStops()).as("the 45 s hold limit counts no sleep: not reached at 40 s monotonic")
                .isZero();
        slow.released.set(true);
        await(() -> !slow.returned.isEmpty(), Duration.ofSeconds(5), "the handler returned");
        await(() -> !receipts(members.get(1)).isEmpty(), Duration.ofSeconds(10), "the receipt replicated");
        assertThat(binder.lateTakes()).as("never lapsed").isZero();
        assertThat(receipts(members.get(1))).as("the sleeper's result stands, once").extracting(Receipt::clerk)
                .containsExactly(sleeper.name());
        System.out.printf("C05 (inside the lease): wake renewals %d, hold-limit stops %d%n",
                binder.wakeRenewals() - wakeRenewals, binder.holdLimitStops());
    }

    /**
     * C05 (SPEC v0.1.37 §7.4): a take has lapsed once its claim is no longer this taker's, whatever its own lease end
     * says. A member whose wall clock runs 20 s ahead sees the sleeper's 30 s claim lapse 20 s early and takes the entry
     * through the ordered log; the holder finds its claim gone at its next renewal, well inside its own lease, counts
     * the take late, and its result completes nothing.
     */
    @Test
    void c05AClaimNoLongerOursLapsesTheTakeInsideItsOwnLease() throws Exception {
        List<Member> members = fleet(i -> i == 1 ? time.node().skew(Duration.ofSeconds(20)) : time);
        Member holder = members.get(0);
        AgentBinder binder = holder.group().binder();
        Clerk slow = bind(holder);
        holder.orders().write(new Order("o-3", 100), Lease.of(Duration.ofMinutes(30)));
        await(() -> !slow.entered.isEmpty(), Duration.ofSeconds(30), "the holder's handler took the entry");
        long tookWall = time.millis();
        Clerk ahead = bind(members.get(1));
        ahead.hold.set(false);
        await(() -> !ahead.returned.isEmpty(), Duration.ofSeconds(28), "the member ahead took the entry");
        await(() -> binder.lateTakes() == 1, Duration.ofSeconds(28 - (time.millis() - tookWall) / 1_000),
                "the holder found its claim gone at its next renewal");
        long lapsedAfter = time.millis() - tookWall;
        assertThat(lapsedAfter).as("inside the holder's own 30 s lease").isLessThan(30_000L);
        long renewals = binder.takeRenewals();
        slow.released.set(true);
        await(() -> !slow.returned.isEmpty(), Duration.ofSeconds(5), "the holder's handler returned");
        time.advance(Duration.ofSeconds(20));
        assertThat(binder.takeRenewals()).as("a lapsed take is never renewed again").isEqualTo(renewals);
        assertThat(binder.lateTakes()).as("counted once").isEqualTo(1);
        assertThat(receipts(members.get(2))).as("one receipt, the member ahead's: the late result completed nothing")
                .extracting(Receipt::clerk).containsExactly(members.get(1).name());
        System.out.printf("C05 (claim no longer ours): lapsed %d ms after the take, lateTakes %d%n", lapsedAfter,
                binder.lateTakes());
    }

    /**
     * SPEC v0.1.37 §9.4: a timer wake more than 30 s late with no wall step (a frozen process) is a wake, and re-checks
     * a held take at once. The take's two-minute lease is renewed each minute; a 35 s freeze 5 s after the take ends
     * with no renewal due, and the late wake renews it at once.
     */
    @Test
    void aFrozenProcessIsAWakeAndRenewsAHeldTakeAtOnce() throws Exception {
        List<Member> members = fleet(true);
        Member frozen = members.get(0);
        VirtualTime.NodeTime frozenTime = (VirtualTime.NodeTime) frozen.time();
        AgentBinder binder = frozen.group().binder();
        SlowClerk slow = new SlowClerk(frozen.time());
        closeables.add(frozen.group().bind(slow));
        frozen.orders().write(new Order("o-4", 100), Lease.of(Duration.ofMinutes(30)));
        await(() -> !slow.entered.isEmpty(), Duration.ofSeconds(30), "the handler took the entry");
        time.advance(Duration.ofSeconds(5));
        long wakeRenewals = binder.wakeRenewals();
        frozenTime.freeze(); // SIGSTOP: both clocks count on, no wall step
        time.advance(Duration.ofSeconds(35));
        frozenTime.resume();
        time.advance(Duration.ofMillis(10));
        System.out.printf("frozen 35 s: wake renewals %d, renewals %d, lateTakes %d%n",
                binder.wakeRenewals() - wakeRenewals, binder.takeRenewals(), binder.lateTakes());
        assertThat(binder.wakeRenewals() - wakeRenewals).as("the late wake renewed the take at once").isEqualTo(1);
        assertThat(binder.lateTakes()).isZero();
        slow.released.set(true);
        await(() -> !receipts(members.get(1)).isEmpty(), Duration.ofSeconds(10), "the receipt replicated");
        assertThat(receipts(members.get(1))).extracting(Receipt::clerk).containsExactly("slow");
    }
}
