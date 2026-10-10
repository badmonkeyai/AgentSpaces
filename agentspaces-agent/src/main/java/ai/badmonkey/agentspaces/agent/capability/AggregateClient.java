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
package ai.badmonkey.agentspaces.agent.capability;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.common.codec.CborCodec;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * The typed client for {@code aspace:cap/aggregate} (spec §8, §10.5): a thin
 * wrapper over this group's {@link PushSumAggregate}, so application code joins
 * an aggregation epoch and reads the converging fleet average through
 * {@code spaces.group("g").capability(AggregateClient.class)}.
 *
 * <p>Resolution, by the {@link Factory}: a {@code PushSumAggregate} registered
 * locally through {@code GroupContext.provide(...)} backs the client; otherwise
 * one is created over the group's {@code CapabilityPipes} and peer sampler and
 * registered as this peer's provider, since every push-sum participant both
 * pushes and receives shares. Push-sum is symmetric, so there is no
 * consumer-only mode to resolve.
 */
public final class AggregateClient {

    private final PushSumAggregate aggregate;
    private final AgentSpaces.GroupContext group;

    AggregateClient(PushSumAggregate aggregate, AgentSpaces.GroupContext group) {
        this.aggregate = Objects.requireNonNull(aggregate, "aggregate");
        this.group = Objects.requireNonNull(group, "group");
    }

    /**
     * Joins an aggregation epoch with this peer's local value.
     *
     * @param epochId    the epoch identifier agreed among participants
     * @param localValue this peer's contribution
     * @return this client
     */
    public AggregateClient start(String epochId, double localValue) {
        aggregate.start(epochId, localValue);
        return this;
    }

    /** Runs one push-sum round for every active epoch. */
    /**
     * Waits for the fleet's estimate of an epoch to become available: the
     * first exchange of the peer tick's push-sum rounds. Empty if none arrives
     * within the timeout (no other participant, or the epoch never started).
     *
     * @param epochId the epoch
     * @param timeout how long to wait
     * @return the estimate once the exchange has run
     */
    public OptionalDouble awaitEstimate(String epochId, java.time.Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            OptionalDouble estimate = aggregate.estimate(epochId);
            if (estimate.isPresent() || System.nanoTime() >= deadline) {
                return estimate;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return OptionalDouble.empty();
            }
        }
    }

    /**
     * Returns the current estimate of the fleet average for an epoch.
     *
     * @param epochId the epoch
     * @return the estimate, or empty when this peer never joined the epoch
     */
    public OptionalDouble estimate(String epochId) {
        return aggregate.estimate(epochId);
    }

    /** {@link PushSumAggregate#onEstimate}: a listener fired once per matching epoch when it settles. */
    public AutoCloseable onEstimate(java.util.function.Predicate<String> epochs,
                                    PushSumAggregate.Settle settle,
                                    java.util.function.Consumer<PushSumAggregate.Estimate> listener) {
        return aggregate.onEstimate(epochs, settle, listener);
    }

    /**
     * Blocks until the epoch's estimate has settled under the rule, or the
     * timeout elapses; the procedural form of {@link #onEstimate} (ISSUE-OnEstimate).
     */
    public OptionalDouble awaitSettled(String epochId, PushSumAggregate.Settle settle,
                                       java.time.Duration timeout) {
        java.util.concurrent.CompletableFuture<Double> settled = new java.util.concurrent.CompletableFuture<>();
        try (AutoCloseable watch = aggregate.onEstimate(epochId::equals, settle,
                estimate -> settled.complete(estimate.value()))) {
            return OptionalDouble.of(settled.get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS));
        } catch (java.util.concurrent.TimeoutException e) {
            return OptionalDouble.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return OptionalDouble.empty();
        } catch (Exception e) {
            throw new IllegalStateException("awaiting the settled estimate of " + epochId, e);
        }
    }

    /**
     * Returns the current, unexpired advertisements of the aggregate
     * capability in this group.
     *
     * @return the advertisements
     */
    public List<CapabilityAdvertisement> providers() {
        return group.discovery().find(CapabilityAdvertisement.class,
                ad -> PushSumAggregate.TYPE.equals(ad.capabilityType()));
    }

    /** Returns the underlying aggregator. */
    public PushSumAggregate capability() {
        return aggregate;
    }

    /** Resolves {@link AggregateClient}s; registered through {@code ServiceLoader}. */
    public static final class Factory implements CapabilityClientFactory<AggregateClient> {

        @Override
        public Class<AggregateClient> clientType() {
            return AggregateClient.class;
        }

        @Override
        public AggregateClient create(AgentSpaces.GroupContext group) {
            Objects.requireNonNull(group, "group");
            Optional<PushSumAggregate> local = group.provider(PushSumAggregate.TYPE)
                    .filter(PushSumAggregate.class::isInstance)
                    .map(PushSumAggregate.class::cast);
            if (local.isPresent()) {
                return new AggregateClient(local.get(), group);
            }
            if (group.runtime() == null) {
                throw new IllegalStateException("group '" + group.name()
                        + "' is not networked; provide(new PushSumAggregate(...)) locally");
            }
            PushSumAggregate aggregate = new PushSumAggregate(group.pipes(),
                    group.runtime().sampler(), group.identity().peerId(),
                    CborCodec.defaultCodec(), group.clock());
            group.provide(aggregate);
            return new AggregateClient(aggregate, group);
        }
    }
}
