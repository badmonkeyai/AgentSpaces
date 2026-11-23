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
package ai.badmonkey.agentspaces.capabilities.runtime;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.DueCapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The Layer 4 pattern made uniform (spec §8): a provider registers, the runtime
 * signs and publishes its {@link CapabilityAdvertisement} into the group's
 * discovery flow, {@link #refreshTick()} keeps the leased advertisement fresh
 * while the provider runs, and consumers discover providers through the same
 * ad-cache as everything else. Stopping the refresh is the whole shutdown
 * protocol: the advertisement lapses by itself (spec P2, §6.2).
 *
 * <p><b>Two clocks.</b> They run at different rates and must not be confused
 * (QA3 A3-3).
 *
 * <ul>
 * <li><b>The protocol clock</b>, {@link #protocolTick()}, advances every
 * registered provider's own algorithm by one round. The runtime registers it
 * with the group's peer tick in the constructor, so it is <em>already driven</em>
 * the moment the runtime exists: an application schedules nothing, exactly as it
 * schedules nothing to make a replicated space converge. This is the Layer 3
 * pattern applied to Layer 4 (QA3 A3-1). A host that wants determinism, a test
 * above all, calls {@link #detachFromPeerTick()} and drives
 * {@link #protocolTick()} itself.</li>
 * <li><b>The advertisement clock</b>, {@link #refreshTick()}, re-publishes leased
 * advertisements and does no protocol work whatever. It runs on the order of
 * minutes, driven by the host's card refresh or by
 * {@link #autoRefresh(Duration, ScheduledExecutorService)}.</li>
 * </ul>
 *
 * <p><b>Republish on change.</b> Some advertisements describe protocol state — the
 * ordered log's {@code role}, {@code term}, and leader lease — and that state
 * moves on the protocol clock, not the advertisement clock. Waiting minutes to
 * tell the fleet who leads would make "find the leader through discovery" (spec
 * §8) true only on a delay, so after each provider's {@link CapabilityProvider#tick()}
 * the runtime fingerprints what the provider now {@link CapabilityProvider#describe
 * describes} (everything but the issue instant) and republishes at once when it
 * differs from what was last published (QA4 A4-10). The advertisement clock
 * remains the thing that keeps an <em>unchanged</em> advertisement alive.
 *
 * <p>Stopping the refresh is the whole shutdown protocol: the advertisement
 * lapses by itself (spec P2, §6.2).
 */
public final class CapabilityRuntime implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(CapabilityRuntime.class.getName());

    private final GroupRuntime runtime;
    private final DiscoveryService discovery;
    private final PeerIdentity identity;
    private final AdvertisementSigner signer = new AdvertisementSigner();
    /**
     * Registered providers by advertisement id, not capability type: one peer may
     * offer a capability more than once (a vote per vote space), and each offer is
     * its own advertisement.
     */
    private final Map<String, CapabilityProvider> providers = new ConcurrentHashMap<>();
    /** Each provider's place on the participant, by advertisement id (srad/agentspaces-event-loop.md EL-T-05). */
    private final Map<String, Slot> slots = new ConcurrentHashMap<>();
    /** Cancels the self-refresh: a one-shot re-armed from completion (EL-T-13); null when none. */
    private volatile Runnable refresh;
    private volatile java.util.function.BooleanSupplier refreshLive = () -> false;
    /** The capability participant on the node's loop; null once detached. */
    private volatile DueWork.Registration tickRegistration;
    /** Content fingerprint of each provider's last published advertisement, by advertisement id. */
    private final Map<String, String> published = new ConcurrentHashMap<>();
    /**
     * The cadence providers have been told about, or null when nobody knows it:
     * a hand-driven node has no wall-clock rate to report, and a guess would be
     * worse than the constructor value a provider was given (QA3 A3-4).
     */
    private volatile Duration cadence;

    /**
     * Creates the capability runtime for one group and joins the group's peer
     * tick, so every provider registered here is driven from the moment it is
     * registered (QA3 A3-1). {@link #close()} leaves the tick again.
     *
     * @param runtime   the group runtime
     * @param discovery the group's discovery service
     * @param identity  the local peer identity (signs advertisements)
     */
    public CapabilityRuntime(GroupRuntime runtime, DiscoveryService discovery,
                             PeerIdentity identity) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.cadence = runtime.tickPeriod().orElse(null);
        this.tickRegistration = runtime.onDue("capabilities", this::runDue);
    }

    /** The legacy cadence of a provider that overrides {@code tick()} or {@code requiresTick()} (owner answer 3). */
    static final Duration LEGACY_PERIOD = Duration.ofMillis(250);

    /** How the participant drives a provider (srad/agentspaces-event-loop.md EL-T-05, owner answer 3, 2026-11-22). */
    enum Drive {
        /** A {@link DueCapabilityProvider}: its own deadlines, its own change signal. */
        DUE,
        /** A provider that overrides {@code tick()} or {@code requiresTick()}: every 250 ms with the fingerprint. */
        LEGACY,
        /** Neither: never run on a started loop; its card is republished by the refresh. */
        STATIC
    }

    /** Whether a provider overrides the legacy {@code tick()} or {@code requiresTick()}, judged once by reflection. */
    static Drive driveOf(CapabilityProvider provider) {
        if (provider instanceof DueCapabilityProvider) {
            return Drive.DUE;
        }
        try {
            Class<?> tick = provider.getClass().getMethod("tick").getDeclaringClass();
            Class<?> requires = provider.getClass().getMethod("requiresTick").getDeclaringClass();
            return tick != CapabilityProvider.class || requires != CapabilityProvider.class ? Drive.LEGACY : Drive.STATIC;
        } catch (NoSuchMethodException e) {
            return Drive.LEGACY;
        }
    }

    /**
     * The cadence a provider on the node's loop is told (owner decision 2, 2026-11-23): the period it is really
     * stepped at. A legacy provider is stepped every {@link #LEGACY_PERIOD} on a started loop whatever
     * {@code tick-millis} says; any other is told the node's tick period, which its own {@code tick()} keeps for the
     * direct runs that call it.
     */
    static Duration cadenceFor(Drive drive, Duration tickPeriod) {
        return drive == Drive.LEGACY ? LEGACY_PERIOD : tickPeriod;
    }

    /** One provider's place on the capability participant, and the driver it was given (DW-2, DW-5). */
    private final class Slot implements DueWork.Driver {
        final CapabilityProvider provider;
        final Drive drive;
        /** Asked to run on the next run (a wake); cleared by the run that reads it. */
        volatile boolean asked = true;
        /** Moves on every {@link #describeChanged()}; the run keeps its own copy (DW-5). */
        final java.util.concurrent.atomic.AtomicLong changes = new java.util.concurrent.atomic.AtomicLong();
        long seenChanges;
        /** The provider's next due on the loop's base, as last reported; the legacy step's last run. */
        long next = DueWork.NEVER;
        long last = Long.MIN_VALUE;

        Slot(CapabilityProvider provider) {
            this.provider = provider;
            this.drive = driveOf(provider);
        }

        @Override
        public void wake() {
            asked = true;
            DueWork.Registration registration = tickRegistration;
            if (registration != null) {
                registration.wake();
            }
        }

        @Override
        public void describeChanged() {
            changes.incrementAndGet();
            wake();
        }

        @Override
        public long monoNanos() {
            return runtime.monoNanos();
        }

        @Override
        public long dueAt(long wallMillis) {
            return runtime.dueAt(runtime.time(), wallMillis);
        }

        @Override
        public ai.badmonkey.agentspaces.common.time.TimeSource time() {
            return runtime.time();
        }

        @Override
        public boolean timed() {
            return tickRegistration != null && runtime.loopRunning() && !runtime.directRun();
        }
    }

    /**
     * The capability participant (srad/agentspaces-event-loop.md EL-T-05): on a started loop each provider runs at its
     * own deadline. A {@link DueCapabilityProvider} runs when it is due or asked, and its card is republished only
     * when it said {@code describeChanged()}; a provider that overrides the legacy {@code tick()} keeps its 250 ms
     * step with the fingerprint; any other provider never runs and its card lives on the refresh. A direct run
     * ({@code tick()}) and a hand-driven node step every provider once, as the tick did (QA3 A3-1, A3-4).
     */
    private long runDue(long now) {
        Duration period = runtime.tickPeriod().orElse(null);
        boolean cadenceChanged = period != null && !period.equals(cadence);
        if (cadenceChanged) {
            cadence = period;
        }
        boolean timed = runtime.loopRunning() && !runtime.directRun();
        long next = DueWork.NEVER;
        for (Slot slot : slots.values()) {
            CapabilityProvider provider = slot.provider;
            try {
                if (cadenceChanged) {
                    provider.driverCadence(cadenceFor(slot.drive, period));
                }
                if (!timed) {
                    provider.tick();
                    republishIfChanged(provider);
                    if (slot.drive == Drive.LEGACY) {
                        slot.last = now;
                        slot.next = runtime.loopRunning() ? now + LEGACY_PERIOD.toNanos() : DueWork.NEVER;
                    }
                } else if (slot.drive == Drive.DUE) {
                    if (slot.asked || now >= slot.next) {
                        slot.asked = false;
                        slot.next = ((DueCapabilityProvider) provider).run(now);
                        long changed = slot.changes.get();
                        if (changed != slot.seenChanges) {
                            slot.seenChanges = changed;
                            republishIfChanged(provider);
                        }
                    }
                } else if (slot.drive == Drive.LEGACY) {
                    if (slot.last == Long.MIN_VALUE || now - slot.last >= LEGACY_PERIOD.toNanos()) {
                        slot.last = now;
                        provider.tick();
                        republishIfChanged(provider);
                    }
                    slot.next = slot.last + LEGACY_PERIOD.toNanos();
                } else {
                    slot.next = DueWork.NEVER;
                }
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "capability run failed: " + provider.capabilityType(), e);
                if (slot.drive == Drive.LEGACY) {
                    slot.next = now + LEGACY_PERIOD.toNanos();
                }
            }
            next = Math.min(next, slot.next);
        }
        return next;
    }

    /**
     * Advances every registered provider's protocol by one round, and tells any
     * provider whose semantics depend on the cadence what that cadence now is.
     * Registered with the peer tick by the constructor, so a host normally never
     * calls this; a host that has {@link #detachFromPeerTick() detached} drives
     * it itself.
     *
     * <p>A provider that throws is logged and skipped: one bad capability can
     * neither stop the others nor kill the peer's tick.
     */
    public void protocolTick() {
        // startTicking may have been called after this runtime was built, so the
        // cadence is re-read each round; it stays null while hand-driven and no
        // provider is told anything it should not believe.
        Duration now = runtime.tickPeriod().orElse(null);
        boolean cadenceChanged = now != null && !now.equals(cadence);
        if (cadenceChanged) {
            cadence = now;
        }
        for (CapabilityProvider provider : providers.values()) {
            try {
                if (cadenceChanged) {
                    provider.driverCadence(now);
                }
                provider.tick(); // every provider's legacy step once, for a detached host
                republishIfChanged(provider);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING,
                        "capability tick failed: " + provider.capabilityType(), e);
            }
        }
    }

    /**
     * Publishes the provider's advertisement again if what it describes has
     * changed since the last publication (QA4 A4-10): protocol state such as a
     * Raft role or leader lease reaches discovery within one tick of changing,
     * rather than on the next advertisement refresh.
     */
    private void republishIfChanged(CapabilityProvider provider) {
        CapabilityAdvertisement ad = provider.describe(runtime.id());
        String fingerprint = fingerprint(ad);
        if (!fingerprint.equals(published.get(ad.id()))) {
            publish(provider, ad);
        }
    }

    /** Everything an advertisement says except when it said it. */
    private static String fingerprint(CapabilityAdvertisement ad) {
        return ad.capabilityType() + '|' + ad.version() + '|' + ad.binding() + '|'
                + new java.util.TreeMap<>(ad.parameters()) + '|'
                + new java.util.TreeMap<>(ad.costHints()) + '|' + ad.ttl();
    }

    /**
     * Leaves the peer tick, so {@link #protocolTick()} runs only when the host
     * calls it. This is what a deterministic test wants: drive the node's tick
     * for membership and anti-entropy, and drive capability rounds separately so
     * each exchange is an exact, countable step.
     *
     * @return this runtime
     */
    public CapabilityRuntime detachFromPeerTick() {
        DueWork.Registration current = tickRegistration;
        if (current != null) {
            tickRegistration = null;
            current.close();
        }
        return this;
    }

    /** Whether the protocol tick is currently driven by the peer's clock. */
    public boolean isDrivenByPeerTick() {
        return tickRegistration != null;
    }

    /**
     * The cadence providers have been told {@link #protocolTick()} arrives at,
     * or empty when the node is hand-driven and only the host knows.
     *
     * @return the current driver cadence, if any
     */
    public java.util.Optional<Duration> cadence() {
        return java.util.Optional.ofNullable(cadence);
    }

    /**
     * Declares the cadence {@link #protocolTick()} is really driven at, for a
     * host that drives it itself after {@link #detachFromPeerTick()}. Every
     * registered provider is told at once, and later registrations inherit it.
     *
     * @param period the true period between protocol ticks
     * @return this runtime
     */
    public CapabilityRuntime cadence(Duration period) {
        Objects.requireNonNull(period, "period");
        if (period.isZero() || period.isNegative()) {
            throw new IllegalArgumentException("cadence must be positive: " + period);
        }
        this.cadence = period;
        providers.values().forEach(provider -> provider.driverCadence(period));
        return this;
    }

    /**
     * Registers, starts, and advertises a provider. Providers are keyed by the id
     * of the advertisement they describe, so two providers of one capability type
     * coexist when their advertisements differ, and registering a provider whose
     * advertisement id is already taken replaces the earlier one.
     *
     * @param provider the provider
     */
    public void register(CapabilityProvider provider) {
        Objects.requireNonNull(provider, "provider");
        // Validate and publish before mutating: a provider whose advertisement
        // is refused must not displace a registered provider of the same type,
        // be left started, or poison every later refreshTick().
        // Tell it the cadence first: a provider that expresses its semantics in
        // ticks must advertise wall-clock numbers that are true in its very
        // first advertisement, which publish() is about to build (QA3 A3-4).
        // Configuring a provider that then fails validation is harmless -- it is
        // neither registered nor started.
        if (cadence != null) {
            provider.driverCadence(tickRegistration != null ? cadenceFor(driveOf(provider), cadence) : cadence);
        }
        CapabilityAdvertisement ad = provider.describe(runtime.id());
        publish(provider, ad);
        providers.put(ad.id(), provider);
        Slot slot = new Slot(provider);
        if (provider instanceof DueCapabilityProvider due) {
            due.attach(slot);
        }
        slots.put(ad.id(), slot);
        if (provider instanceof PushSumAggregate aggregate && !aggregate.hasParticipantRule()) {
            // Shares go only to members that advertise the aggregate: a bystander
            // without one would swallow them (2026-10-09, the council and Spring runs).
            aggregate.participants(PushSumAggregate.advertisedIn(discovery));
        }
        provider.start();
        slot.wake(); // its first run files its deadlines
    }

    /** Re-publishes every registered provider's advertisement with a fresh issue time. */
    public void refreshTick() {
        providers.values().forEach(this::publish);
    }

    /**
     * Schedules {@link #refreshTick()} at a fixed period on the given executor so
     * advertisements stay alive without a host-driven tick; the period must be
     * shorter than the shortest advertised TTL (15 minutes for the shipped
     * capabilities). {@link #close()} cancels the schedule; calling this again
     * replaces an earlier schedule. A refresh that throws is logged by the
     * executor's failure handling and does not cancel later runs.
     *
     * @param period   how often to refresh; positive
     * @param executor the scheduler to run on; not shut down by this runtime
     * @return this runtime
     */
    public CapabilityRuntime autoRefresh(Duration period, ScheduledExecutorService executor) {
        Objects.requireNonNull(period, "period");
        Objects.requireNonNull(executor, "executor");
        checkPeriod(period);
        // One-shot, re-armed from completion (srad/agentspaces-event-loop.md EL-T-13): a refresh that ran late never
        // runs twice back to back to catch up, as a fixed rate would.
        cancelRefresh();
        java.util.concurrent.atomic.AtomicReference<ScheduledFuture<?>> pending =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        Runnable[] task = new Runnable[1];
        task[0] = () -> {
            if (cancelled.get()) {
                return;
            }
            try {
                refreshTick();
            } catch (RuntimeException e) {
                // keep the schedule alive; the next run retries
            }
            if (!cancelled.get()) {
                try {
                    pending.set(executor.schedule(task[0], period.toMillis(), TimeUnit.MILLISECONDS));
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    cancelled.set(true); // the host shut its executor down
                }
            }
        };
        pending.set(executor.schedule(task[0], period.toMillis(), TimeUnit.MILLISECONDS));
        refresh = () -> {
            cancelled.set(true);
            ScheduledFuture<?> f = pending.get();
            if (f != null) {
                f.cancel(false);
            }
        };
        refreshLive = () -> !cancelled.get() && !executor.isShutdown();
        return this;
    }

    /**
     * Refreshes every advertisement on the node's own time source, one-shot and re-armed from completion
     * (srad/agentspaces-event-loop.md EL-T-12, EL-T-13): no thread of the host's. {@link #close()} cancels it.
     *
     * @param period how often to refresh; positive
     * @return this runtime
     */
    public CapabilityRuntime autoRefresh(Duration period) {
        Objects.requireNonNull(period, "period");
        checkPeriod(period);
        cancelRefresh();
        ai.badmonkey.agentspaces.common.time.TimeSource time = runtime.time();
        java.util.concurrent.atomic.AtomicReference<ai.badmonkey.agentspaces.common.time.TimeSource.Scheduled> pending =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        Runnable[] task = new Runnable[1];
        task[0] = () -> {
            if (cancelled.get()) {
                return;
            }
            try {
                refreshTick();
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "capability card refresh failed; the next one retries", e);
            }
            if (!cancelled.get()) {
                pending.set(time.schedule(period, task[0]));
            }
        };
        pending.set(time.schedule(period, task[0]));
        refresh = () -> {
            cancelled.set(true);
            ai.badmonkey.agentspaces.common.time.TimeSource.Scheduled f = pending.get();
            if (f != null) {
                f.cancel();
            }
        };
        refreshLive = () -> !cancelled.get();
        return this;
    }

    private static void checkPeriod(Duration period) {
        if (period.isZero() || period.isNegative()) {
            throw new IllegalArgumentException("refresh period must be positive: " + period);
        }
    }

    private void cancelRefresh() {
        Runnable current = refresh;
        refresh = null;
        refreshLive = () -> false;
        if (current != null) {
            current.run();
        }
    }

    /** Returns whether a self-refresh schedule is currently active. */
    public boolean isAutoRefreshing() {
        return refresh != null && refreshLive.getAsBoolean();
    }

    /**
     * Finds current providers of a capability in the group.
     *
     * @param capabilityType the capability type URI
     * @return unexpired advertisements for that capability
     */
    public List<CapabilityAdvertisement> providersOf(String capabilityType) {
        Objects.requireNonNull(capabilityType, "capabilityType");
        return discovery.find(CapabilityAdvertisement.class,
                ad -> capabilityType.equals(ad.capabilityType()));
    }

    @Override
    public void close() {
        detachFromPeerTick();
        cancelRefresh();
        providers.values().forEach(CapabilityProvider::stop);
        providers.clear();
        slots.clear();
        published.clear();
        // No unpublish: the advertisements lapse on their own (spec P2).
    }

    private void publish(CapabilityProvider provider) {
        publish(provider, provider.describe(runtime.id()));
    }

    private void publish(CapabilityProvider provider, CapabilityAdvertisement described) {
        if (!described.issuer().equals(identity.peerId())) {
            throw new IllegalArgumentException("provider " + provider.capabilityType()
                    + " described an advertisement for a different issuer: " + described.issuer());
        }
        CapabilityAdvertisement ad = afterLastIssue(described);
        discovery.publish(signer.sign(ad, identity));
        published.put(ad.id(), fingerprint(ad));
    }

    /** The issue millisecond of each card id last published, so the next one is strictly later (§10.14). */
    private final Map<String, Long> lastIssuedMillis = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The card with an issue instant at least a millisecond past the last one published under its id
     * (srad/agentspaces-event-loop.md §10.14, the phase 2 wire adversary). A change-driven republish can follow the
     * last publication within the same millisecond (two role changes, a change and the refresh). The cache keeps the
     * version it holds on an equal issue instant, and the gossip item id is the card's id and issue millisecond, which
     * every peer had already seen, so such a card reached nobody while its fingerprint was recorded as published: the
     * change was lost until the next refresh. The 250 ms tick hid it for Raft, whose lease moved every tick.
     */
    private CapabilityAdvertisement afterLastIssue(CapabilityAdvertisement ad) {
        long described = ad.issued().toEpochMilli();
        long issued = lastIssuedMillis.merge(ad.id(), described, (last, now) -> Math.max(now, last + 1));
        if (issued == described) {
            return ad;
        }
        return new CapabilityAdvertisement(ad.id(), ad.issuer(), ad.group(), java.time.Instant.ofEpochMilli(issued),
                ad.ttl(), ad.capabilityType(), ad.version(), ad.binding(), ad.parameters(), ad.costHints());
    }
}
