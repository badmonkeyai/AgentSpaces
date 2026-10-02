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
package ai.badmonkey.agentspaces.api.spi;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.common.id.GroupId;

import java.time.Duration;

/**
 * A provider of one capability service (spec §8, P6). Providers describe
 * themselves as {@link CapabilityAdvertisement}s; the runtime publishes and
 * refreshes the advertisement while the provider is running, and the capability's
 * own mini-spec defines the interaction protocol.
 *
 * <p><b>Two clocks, both driven for you.</b> A capability may need periodic work
 * to make progress: push-sum needs to exchange, gossip learning needs to offer
 * and merge, a Raft member needs to time out and campaign. That work is
 * {@link #tick()}, and a provider registered on a {@code CapabilityRuntime} has
 * it driven automatically by the peer's own tick, the same clock that already
 * drives membership, gossip, and space anti-entropy. This mirrors Layer 3: a
 * replicated space converges because constructing it registers it with that
 * clock, and a capability advances for exactly the same reason (QA3 A3-1).
 * Advertisement freshness is the <em>other</em> clock and runs far slower; see
 * {@code CapabilityRuntime.refreshTick()}. Do not conflate them.
 *
 * <p>A provider that needs no periodic work implements nothing extra: the
 * default {@link #tick()} is a no-op and {@link #requiresTick()} is {@code false},
 * which is the right answer for request/response capabilities like vote, key
 * wrap, and semantic discovery.
 */
public interface CapabilityProvider {

    /** Returns the capability type URI, e.g. {@code aspace:cap/aggregate}. */
    String capabilityType();

    /**
     * Describes this provider's offering to a group.
     *
     * @param group the group to advertise into
     * @return the advertisement to publish (unsigned; the runtime signs it)
     */
    CapabilityAdvertisement describe(GroupId group);

    /** Starts serving. Called once before the first advertisement is published. */
    default void start() {
    }

    /**
     * Advances this capability's own protocol by one round: one push-sum
     * exchange, one gossip-learning offer, one Raft election-timer step. The
     * capability runtime calls this on every peer tick for every registered
     * provider, so a capability is driven by being registered and an
     * application never schedules anything of its own.
     *
     * <p>It shares the peer's tick thread with membership probing and gossip,
     * so it must return promptly and must not block on the network. It must
     * also tolerate being called before, during, and after any consumer call;
     * implementations are responsible for their own synchronization. A tick
     * that throws is logged by the runtime and never cancels later ticks.
     */
    default void tick() {
    }

    /**
     * Whether {@link #tick()} must be driven for this provider to make
     * progress. Purely declarative: the runtime drives every provider either
     * way, since the default tick costs nothing. It exists so that hosts and
     * diagnostics can tell a continuous capability from a request/response one
     * without special-casing type URIs.
     *
     * @return {@code true} when this provider needs a clock
     */
    default boolean requiresTick() {
        return false;
    }

    /**
     * Tells the provider how often {@link #tick()} will actually be called,
     * before the first tick and again whenever the cadence changes. A provider
     * whose semantics are expressed in ticks rather than in wall time uses this
     * to convert between the two: the Raft log advertises a leader lease of
     * {@code electionTimeout × cadence}, which is a lie unless the cadence it
     * was told matches the one it is driven at (QA3 A3-4).
     *
     * @param period the wall-clock period between {@link #tick()} calls
     */
    default void driverCadence(Duration period) {
    }

    /** Stops serving. The advertisement lapses by itself once refresh stops. */
    default void stop() {
    }
}
