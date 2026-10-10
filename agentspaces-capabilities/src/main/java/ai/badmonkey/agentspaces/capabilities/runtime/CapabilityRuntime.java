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
    private volatile ScheduledFuture<?> refresh;
    /** Deregisters the protocol tick from the peer clock; null once detached. */
    private volatile AutoCloseable tickRegistration;
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
        this.tickRegistration = runtime.onTick(this::protocolTick);
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
                provider.tick();
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
        AutoCloseable current = tickRegistration;
        if (current != null) {
            tickRegistration = null;
            try {
                current.close();
            } catch (Exception e) {
                throw new IllegalStateException("failed detaching from the peer tick", e);
            }
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
            provider.driverCadence(cadence);
        }
        CapabilityAdvertisement ad = provider.describe(runtime.id());
        publish(provider, ad);
        providers.put(ad.id(), provider);
        if (provider instanceof PushSumAggregate aggregate && !aggregate.hasParticipantRule()) {
            // Shares go only to members that advertise the aggregate: a bystander
            // without one would swallow them (2026-10-09, the council and Spring runs).
            aggregate.participants(PushSumAggregate.advertisedIn(discovery));
        }
        provider.start();
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
        if (period.isZero() || period.isNegative()) {
            throw new IllegalArgumentException("refresh period must be positive: " + period);
        }
        ScheduledFuture<?> previous = refresh;
        if (previous != null) {
            previous.cancel(false);
        }
        refresh = executor.scheduleAtFixedRate(() -> {
            try {
                refreshTick();
            } catch (RuntimeException e) {
                // keep the schedule alive; the next run retries
            }
        }, period.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
        return this;
    }

    /** Returns whether a self-refresh schedule is currently active. */
    public boolean isAutoRefreshing() {
        ScheduledFuture<?> current = refresh;
        return current != null && !current.isCancelled() && !current.isDone();
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
        ScheduledFuture<?> current = refresh;
        if (current != null) {
            current.cancel(false);
            refresh = null;
        }
        providers.values().forEach(CapabilityProvider::stop);
        providers.clear();
        published.clear();
        // No unpublish: the advertisements lapse on their own (spec P2).
    }

    private void publish(CapabilityProvider provider) {
        publish(provider, provider.describe(runtime.id()));
    }

    private void publish(CapabilityProvider provider, CapabilityAdvertisement ad) {
        if (!ad.issuer().equals(identity.peerId())) {
            throw new IllegalArgumentException("provider " + provider.capabilityType()
                    + " described an advertisement for a different issuer: " + ad.issuer());
        }
        discovery.publish(signer.sign(ad, identity));
        published.put(ad.id(), fingerprint(ad));
    }
}
