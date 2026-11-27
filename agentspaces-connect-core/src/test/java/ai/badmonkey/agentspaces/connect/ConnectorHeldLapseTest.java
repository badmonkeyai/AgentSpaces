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
package ai.badmonkey.agentspaces.connect;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * C05 for a connector's held invocation (SPEC v0.1.37 §7.4 and §9.4, as {@code AgentBinder}'s held take; carried
 * from srad/agentspaces-event-loop.md §10.37 and §10.45): while an operation runs, the invocation's claim is renewed
 * at half its lease, and it lapses when the claim is no longer this connector's ({@link TakenEntry#held()}), when a
 * renewal is refused, or when the wall clock passes its own lease end, which a wake or a wall step re-checks at once.
 * A lapsed invocation is never renewed again and its result completes nothing. Before, {@code runHeld} renewed until
 * a renewal threw and completed whatever came back.
 *
 * <p>Once the lapsed claim's own lease ends in the space, the invocation reappears, and this connector's loop takes it
 * again: it answers from the result it remembers, so the target never runs twice (CC-L-08). That retake is the one
 * completion and the one result, and it comes only after the lapsed run returned (the invoke loop is one thread). The
 * tests wait for it and count it ({@link ConnectorRuntime#repeated()}); before, they read the completions at a moment
 * that raced the loop (slice 6.7 of srad/cards.md §8.7: the sleep test failed with one completion, the claim test
 * passed only because it read first).
 *
 * <p>The invocation space is a {@link LocalSpace} behind a fake that can say a take's claim is gone (another
 * replica's claim displaced it), on virtual time: no sockets, no sleeps.
 */
@Timeout(60)
class ConnectorHeldLapseTest {

    private final VirtualTime time = VirtualTime.create();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zConnectLapse");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zConnectLapse",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "connect-lapse",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    /** Whether the claim of the take the fake handed out is still this connector's. */
    private final AtomicBoolean claimOurs = new AtomicBoolean(true);
    private final AtomicInteger completions = new AtomicInteger();
    private final AtomicBoolean entered = new AtomicBoolean();
    private final AtomicBoolean released = new AtomicBoolean();
    /** The connector's renewer thread, once it first waits on the virtual clock. */
    private final java.util.concurrent.atomic.AtomicReference<Thread> renewer = new java.util.concurrent.atomic.AtomicReference<>();
    private AutoCloseable waits;
    /** How many times the target ran. */
    private final AtomicInteger invokes = new AtomicInteger();
    private PeerNode node;
    private LocalSpace data;
    private ConnectorRuntime connector;

    /** An inventory whose one operation holds until the test releases it, waiting on the virtual clock. */
    private final class Held implements AssetProvider {
        @Override
        public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
            return List.of(new AssetCard("aspace://" + group.value() + "/asset/inventory", issuer, group, now,
                    Duration.ofMinutes(15), "inventory", "workers://shop/do/Inventory", "Stock by SKU",
                    "com.example.Stock#v1", "PT5M", Map.of("objects", "1"),
                    Map.of("mode", "read-only,invoke", "query", "aspace:cap/data-query",
                            "invoke", AssetInvocations.CAPABILITY, "invoke-ops", "reserve",
                            "consistency", "single-writer")));
        }

        @Override
        public QueryResult query(String asset, Map<String, String> parameters) {
            return QueryResult.of(List.of());
        }

        @Override
        public InvokeResult invoke(String asset, String operation, Map<String, String> params, String invocationId) {
            entered.set(true);
            invokes.incrementAndGet();
            while (!released.get()) {
                try {
                    time.sleep(Duration.ofMillis(500));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return InvokeResult.failed("target-error", "test", "interrupted");
                }
            }
            return InvokeResult.of(Map.of("reserved", params.getOrDefault("qty", "0")));
        }
    }

    /** A take whose {@link #held()} the test can turn false, as a displacing claim from another replica would. */
    private final class Claimed<T> implements TakenEntry<T> {
        final TakenEntry<T> taken;

        Claimed(TakenEntry<T> taken) {
            this.taken = taken;
        }

        @Override
        public T entry() {
            return taken.entry();
        }

        @Override
        public EntryId entryId() {
            return taken.entryId();
        }

        @Override
        public void renew(Duration extension) {
            taken.renew(extension);
        }

        @Override
        public boolean held() {
            return claimOurs.get() && taken.held();
        }
    }

    /** The data space behind the fake: takes come back {@link Claimed}; completions are counted and unwrapped. */
    private Space claimable(Space space) {
        return (Space) Proxy.newProxyInstance(Space.class.getClassLoader(), new Class<?>[]{Space.class},
                (proxy, method, args) -> {
                    Object[] real = args == null ? null : args.clone();
                    if (real != null) {
                        for (int i = 0; i < real.length; i++) {
                            if (real[i] instanceof Claimed<?> c) {
                                real[i] = c.taken;
                            }
                        }
                    }
                    if (method.getName().equals("complete")) {
                        completions.incrementAndGet();
                    }
                    Object out;
                    try {
                        out = method.invoke(space, real);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (method.getName().equals("take") && out instanceof Optional<?> o && o.isPresent()) {
                        return Optional.of(new Claimed<>((TakenEntry<?>) o.get()));
                    }
                    return out;
                });
    }

    @BeforeEach
    void start() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(time).time(time).build();
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, time), codec, identity.peerId());
        data = LocalSpace.builder("data", identity.agent("connector")).clock(time).build();
        // the renewer is the connector's own thread, which virtual time does not start or settle: it is seen when it
        // first waits on the clock, so the tests step time only once its first renewal is due at a known instant
        waits = time.onOutsideWait(() -> {
            if (Thread.currentThread().getName().startsWith("connector-renew-")) {
                renewer.compareAndSet(null, Thread.currentThread());
            }
        });
        connector = new ConnectorRuntime(claimable(data), discovery, identity, "inventory-connector", new Held(),
                groupId, time, Duration.ofMinutes(1));
        connector.start();
        data.write(AssetInvocation.of("inventory", "reserve", Map.of("qty", "2")), Lease.of(Duration.ofMinutes(30)));
        // The connector's invoke loop is its own virtual thread, which this source does not run or settle on, and its
        // take of a present entry needs no time to pass. Stepping virtual time while that thread had not yet started
        // spent the 10 s bound in a few milliseconds of real time under a parallel build (core reactor, 2026-11-23),
        // so the start is awaited on real time and virtual time stays where it is.
        ai.badmonkey.agentspaces.measure.Measure.await("connect.held-lapse.entered", Duration.ofSeconds(30), entered::get);
        // and the renewer parked on its first sleep, due 15 s on: before, a parallel build's load could leave it unstarted
        // while the test stepped all 20 s of its bound (the phase 6 gate, 2026-11-26: "the claim found gone ... within
        // PT20S" failed with the renewer never run)
        ai.badmonkey.agentspaces.measure.Measure.await("connect.held-lapse.renewer-parked", Duration.ofSeconds(30),
                () -> renewer.get() != null && time.waitingOutside(renewer.get()));
    }

    @AfterEach
    void tearDown() {
        released.set(true);
        if (waits != null) {
            try {
                waits.close();
            } catch (Exception ignored) {
                // a listener registration
            }
        }
        if (connector != null) {
            connector.close();
        }
        if (node != null) {
            node.close();
        }
    }

    /**
     * Waits until the condition holds, stepping virtual time 50 ms per poll and never past the bound. The connector's
     * threads run on real time once a step releases them, so time moves only as fast as the polls and stops at the
     * bound: the condition holds within the bound of virtual time, however slowly the threads run (before, the test
     * stepped the whole bound in a few milliseconds of real time and raced them). The real-time ceiling is 30 s.
     */
    private void await(BooleanSupplier condition, Duration bound, String metric) {
        long until = time.monoNanos() + bound.toNanos();
        ai.badmonkey.agentspaces.measure.Measure.await(metric, Duration.ofSeconds(30), () -> {
            if (condition.getAsBoolean()) {
                return true;
            }
            if (time.monoNanos() + STEP.toNanos() <= until) {
                time.advance(STEP);
            }
            return false;
        }, ai.badmonkey.agentspaces.measure.Wait.with().poll(POLL).diagnostics(() -> "virtual "
                + Duration.ofNanos(time.monoNanos()) + " of a bound ending at " + Duration.ofNanos(until) + "; lateTakes "
                + connector.lateTakes() + ", renewals " + connector.renewals() + ", retakes " + connector.repeated()
                + ", completions " + completions.get() + ", renewer waiting " + (renewer.get() != null
                && time.waitingOutside(renewer.get()))));
    }

    /** A step of virtual time per poll of real time: 50 ms each 2 ms, so a 20 s bound is reached in under a second. */
    private static final Duration POLL = Duration.ofMillis(2);

    private static final Duration STEP = Duration.ofMillis(50);

    /**
     * Waits for the connector's loop to take the reappeared invocation again and answer it from memory, within 5 s
     * more of virtual time. The loop is a thread of its own that virtual time does not settle.
     */
    private void awaitTheRetake(String metric) {
        await(() -> connector.repeated() >= 1 && completions.get() >= 1, Duration.ofSeconds(5), metric);
    }

    /**
     * The late result completed nothing: the one completion and the one result are the retake's, answered from
     * memory, and the target ran once. Had the late result completed, the invocation would never have reappeared:
     * one completion, no retake.
     */
    private void assertTheLateResultCompletedNothing() {
        assertThat(connector.repeated()).as("the reappeared invocation was taken again and answered from memory").isEqualTo(1);
        assertThat(completions.get()).as("the late result completed nothing: the one completion is the retake's").isEqualTo(1);
        assertThat(invokes.get()).as("the target never ran twice").isEqualTo(1);
        assertThat(results()).as("one result, the remembered one").hasSize(1)
                .allSatisfy(r -> assertThat(r.ok()).isTrue());
    }

    private List<AssetInvocationResult> results() {
        return data.readAll(Template.of(AssetInvocationResult.class), 10);
    }

    @Test
    void c05AnInvocationWhoseClaimIsNoLongerOursLapsesAndItsResultCompletesNothing() {
        long took = time.monoNanos();
        claimOurs.set(false); // another replica's claim displaced this one
        await(() -> connector.lateTakes() == 1, Duration.ofSeconds(20), "connect.held-lapse-claim-gone.lapse"); // at the next renewal
        long lapsedAfter = Duration.ofNanos(time.monoNanos() - took).toMillis();
        int renewals = connector.renewals();
        released.set(true);
        time.advance(Duration.ofSeconds(40)); // past the take's own 30 s lease: the invocation reappears
        awaitTheRetake("connect.held-lapse-claim-gone.retake");
        System.out.printf("[close-out] connector, claim gone: lapsed %d ms after, lateTakes %d, renewals %d,"
                + " completions %d, retakes %d, invokes %d, results %s%n", lapsedAfter, connector.lateTakes(),
                connector.renewals(), completions.get(), connector.repeated(), invokes.get(), results());
        assertThat(lapsedAfter).as("inside the invocation's own 30 s lease").isLessThan(30_000L);
        assertThat(connector.renewals()).as("a lapsed claim is never renewed again").isEqualTo(renewals);
        assertTheLateResultCompletedNothing();
        assertThat(connector.lateTakes()).as("counted once").isEqualTo(1);
    }

    @Test
    void c05ASleepPastTheLeaseLapsesTheInvocationAtTheWake() {
        time.advance(Duration.ofSeconds(5));
        time.stepWall(Duration.ofSeconds(40)); // a laptop sleep: the wall moved 40 s, the monotonic clock did not
        time.advance(Duration.ofMillis(10));
        long lateAtWake = connector.lateTakes();
        released.set(true);
        time.advance(Duration.ofSeconds(5));
        awaitTheRetake("connect.held-lapse-sleep.retake");
        System.out.printf("[close-out] connector, 40 s sleep: lateTakes at the wake %d, completions %d, retakes %d,"
                + " invokes %d, results %s%n", lateAtWake, completions.get(), connector.repeated(), invokes.get(),
                results());
        assertThat(lateAtWake).as("the wake re-checked the claim at once, past its wall-time lease").isEqualTo(1);
        assertTheLateResultCompletedNothing();
    }

    @Test
    void c05ASleepInsideTheLeaseRenewsTheInvocationAtTheWakeAndItCompletes() {
        time.advance(Duration.ofSeconds(5));
        time.stepWall(Duration.ofSeconds(20)); // inside the 30 s lease: the claim still holds
        time.advance(Duration.ofMillis(10));
        long wakeRenewals = connector.wakeRenewals();
        released.set(true);
        await(() -> !results().isEmpty(), Duration.ofSeconds(5), "connect.held-lapse-inside.result");
        System.out.printf("[close-out] connector, 20 s sleep: wake renewals %d, lateTakes %d, results %s%n",
                wakeRenewals, connector.lateTakes(), results());
        assertThat(wakeRenewals).as("the wake renewed the claim at once").isEqualTo(1);
        assertThat(connector.lateTakes()).isZero();
        assertThat(completions.get()).isEqualTo(1);
    }
}
