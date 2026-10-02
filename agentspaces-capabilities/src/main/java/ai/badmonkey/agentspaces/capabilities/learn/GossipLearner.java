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
package ai.badmonkey.agentspaces.capabilities.learn;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.capabilities.runtime.PipeChannel;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;

/**
 * The typed {@code aspace:cap/gossip-learn} capability (spec §8): decentralized
 * model improvement over any {@link MergeableModel}. Every participant holds a
 * local model per model id; each {@link #tick()} offers it to one sampled
 * member; on encounter both sides replace their model with
 * {@code merge(mine, theirs)} followed by the optional application-supplied
 * local update (SGD on local data, clipping), so models converge to the fleet
 * merge while raw data never moves.
 *
 * <p><b>Exchange protocol (mass-conserving under asynchronous delivery).</b>
 * Frames are {@code Exchange{modelId, token, kind, round, inline?, cid?}} with
 * {@code kind ∈ OFFER|ACCEPT|BUSY}. A node with no outstanding offer for a model
 * sends {@code OFFER(token, snapshot)} to one sampled member and locks the model
 * (no further offers, and incoming offers are answered {@code BUSY}) until the
 * reply or a timeout of {@value #PENDING_TIMEOUT_TICKS} ticks. A receiver that
 * is not itself locked replies {@code ACCEPT(token, its pre-merge snapshot)} and
 * merges in the same step; a locked receiver replies {@code BUSY(token)}. The
 * offerer merges the accepted snapshot only if the token matches its
 * outstanding offer, which it must, because the lock kept its model at the
 * offered snapshot. Both sides therefore merge exactly the pair
 * {@code (offered, accepted)}, so reordering and interleaving of any number of
 * concurrent exchanges conserve the fleet sum of a commutative merge. Only a
 * lost {@code ACCEPT} can move mass one-sidedly; the offerer then times out.
 *
 * <p><b>Content addressing.</b> A model travels as its {@link MergeableModel#encode
 * encoding}; an encoding above the space's inline limit
 * ({@value EntryRecord#INLINE_PAYLOAD_LIMIT} bytes) is stored in the group's
 * {@link BlockExchange} and the frame carries its CID instead, which the
 * receiver fetches from the sender. {@link #contentId(Object)} is the sha-256
 * CID of the encoding, the same identifier the block exchange uses.
 *
 * <p><b>Epochs.</b> {@link #endEpoch(int, ToDoubleFunction)} evaluates every
 * local model and writes an {@link Evaluation} entry per model into the
 * configured metrics space under the configured lease, so a fleet's per-epoch
 * losses are auditable from the space like any other entry.
 *
 * @param <M> the model type
 */
public final class GossipLearner<M> implements CapabilityProvider {

    /** The capability type URI. */
    public static final String TYPE = "aspace:cap/gossip-learn";

    /** Ticks an outstanding offer waits for its reply before it is abandoned. */
    public static final int PENDING_TIMEOUT_TICKS = 3;

    /** How long a receiver waits for a content-addressed model's block. */
    public static final Duration DEFAULT_BLOCK_FETCH_TIMEOUT = Duration.ofSeconds(5);

    /**
     * One node's evaluation of one model at the end of an epoch; a metrics-space
     * entry whose authenticated issuer is the evaluating node.
     *
     * @param modelId   the model
     * @param epoch     the epoch that just ended
     * @param loss      the evaluation result (lower is better by convention)
     * @param contentId the CID of the evaluated model's encoding
     */
    public record Evaluation(String modelId, int epoch, double loss, String contentId) {
    }

    private record Exchange(String modelId, long token, String kind, long round,
                            byte[] inline, String cid) {
    }

    private static final String OFFER = "OFFER";
    private static final String ACCEPT = "ACCEPT";
    private static final String BUSY = "BUSY";

    private static final class Model<M> {
        M value;
        long round;
        long pendingToken;
        int pendingAge;

        Model(M value) {
            this.value = value;
        }
    }

    /** What to send once the model lock is released. */
    private record Outgoing(PeerId to, Exchange frame) {
    }

    private final PipeChannel pipes;
    private final PeerSampler sampler;
    private final PeerId self;
    private final CborCodec codec;
    private final InstantSource clock;
    private final MergeableModel<M> mergeable;
    private final UnaryOperator<M> localUpdate;
    private final BlockExchange blocks;
    private final Duration blockFetchTimeout;
    private final Space metrics;
    private final Lease metricsLease;
    private final Map<String, Model<M>> models = new ConcurrentHashMap<>();
    private final AtomicLong tokens = new AtomicLong();

    private GossipLearner(Builder<M> b) {
        this.pipes = b.pipes;
        this.sampler = b.sampler;
        this.self = b.self;
        this.codec = b.codec;
        this.clock = b.clock;
        this.mergeable = b.mergeable;
        this.localUpdate = b.localUpdate;
        this.blocks = b.blocks;
        this.blockFetchTimeout = b.blockFetchTimeout;
        this.metrics = b.metrics;
        this.metricsLease = b.metricsLease;
        pipes.onCapability(TYPE, this::onExchange);
    }

    /**
     * Starts building a learner.
     *
     * @param pipes     the group's capability pipes
     * @param sampler   the group's peer sampler
     * @param self      the local peer id (used in the advertisement)
     * @param codec     the CBOR codec
     * @param clock     the time source for advertisement freshness
     * @param mergeable the model SPI
     * @param <M>       the model type
     * @return the builder
     */
    public static <M> Builder<M> builder(PipeChannel pipes, PeerSampler sampler, PeerId self,
                                         CborCodec codec, InstantSource clock,
                                         MergeableModel<M> mergeable) {
        return new Builder<>(pipes, sampler, self, codec, clock, mergeable);
    }

    /**
     * Builder for {@link GossipLearner}.
     *
     * @param <M> the model type
     */
    public static final class Builder<M> {
        private final PipeChannel pipes;
        private final PeerSampler sampler;
        private final PeerId self;
        private final CborCodec codec;
        private final InstantSource clock;
        private final MergeableModel<M> mergeable;
        private UnaryOperator<M> localUpdate;
        private BlockExchange blocks;
        private Duration blockFetchTimeout = DEFAULT_BLOCK_FETCH_TIMEOUT;
        private Space metrics;
        private Lease metricsLease;

        private Builder(PipeChannel pipes, PeerSampler sampler, PeerId self, CborCodec codec,
                        InstantSource clock, MergeableModel<M> mergeable) {
            this.pipes = Objects.requireNonNull(pipes, "pipes");
            this.sampler = Objects.requireNonNull(sampler, "sampler");
            this.self = Objects.requireNonNull(self, "self");
            this.codec = Objects.requireNonNull(codec, "codec");
            this.clock = Objects.requireNonNull(clock, "clock");
            this.mergeable = Objects.requireNonNull(mergeable, "mergeable");
        }

        /**
         * Applies a local update (SGD, clipping) to every merged model.
         *
         * @param localUpdate the update; null for pure merging
         * @return this builder
         */
        public Builder<M> localUpdate(UnaryOperator<M> localUpdate) {
            this.localUpdate = localUpdate;
            return this;
        }

        /**
         * Routes encodings above the inline limit through the block exchange.
         *
         * @param blocks the group's block exchange
         * @return this builder
         */
        public Builder<M> blocks(BlockExchange blocks) {
            this.blocks = Objects.requireNonNull(blocks, "blocks");
            return this;
        }

        /**
         * Bounds the wait for a content-addressed model's block.
         *
         * @param timeout the timeout; positive
         * @return this builder
         */
        public Builder<M> blockFetchTimeout(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("timeout must be positive: " + timeout);
            }
            this.blockFetchTimeout = timeout;
            return this;
        }

        /**
         * Configures where {@link #endEpoch} writes its evaluations.
         *
         * @param metrics the metrics space
         * @param lease   the lease each evaluation entry is written under
         * @return this builder
         */
        public Builder<M> metrics(Space metrics, Lease lease) {
            this.metrics = Objects.requireNonNull(metrics, "metrics");
            this.metricsLease = Objects.requireNonNull(lease, "lease");
            return this;
        }

        /** Builds the learner and registers it on the pipes. */
        public GossipLearner<M> build() {
            return new GossipLearner<>(this);
        }
    }

    @Override
    public String capabilityType() {
        return TYPE;
    }

    @Override
    public CapabilityAdvertisement describe(GroupId group) {
        return new CapabilityAdvertisement(
                "aspace://" + group.value() + "/cap/gossip-learn/" + self.value(),
                self, group, clock.instant(), Duration.ofMinutes(15),
                TYPE, "0.1", "pipe",
                Map.of("merge", mergeable.name(),
                        "exchange", "offer-accept",
                        "content", blocks == null ? "inline" : "inline,cid"),
                Map.of());
    }

    /** Returns the model SPI this learner merges with. */
    public MergeableModel<M> mergeable() {
        return mergeable;
    }

    /**
     * Registers this node's local model for a model id, replacing any earlier one.
     *
     * @param modelId the model identifier agreed among participants
     * @param model   this node's initial model
     */
    public void start(String modelId, M model) {
        Objects.requireNonNull(modelId, "modelId");
        Objects.requireNonNull(model, "model");
        models.put(modelId, new Model<>(model));
    }

    /**
     * Runs one gossip round for every registered model: offer the model to one
     * sampled member unless an earlier offer is still outstanding (abandoned
     * after {@value #PENDING_TIMEOUT_TICKS} ticks).
     *
     * @throws IllegalStateException when a model's encoding exceeds the inline
     *                               limit and no block exchange is configured
     */
    @Override
    public boolean requiresTick() {
        return true;
    }

    @Override
    public void tick() {
        for (Map.Entry<String, Model<M>> e : models.entrySet()) {
            Model<M> model = e.getValue();
            Outgoing offer;
            synchronized (model) {
                if (model.pendingToken != 0) {
                    if (++model.pendingAge <= PENDING_TIMEOUT_TICKS) {
                        continue; // still waiting for the reply
                    }
                    model.pendingToken = 0; // abandoned: nothing was merged
                }
                List<PeerId> target = sampler.randomMembers(1);
                if (target.isEmpty()) {
                    continue;
                }
                long token = tokens.incrementAndGet();
                model.pendingToken = token;
                model.pendingAge = 0;
                offer = new Outgoing(target.get(0),
                        frame(e.getKey(), token, OFFER, model.round, model.value));
            }
            send(offer);
        }
    }

    /**
     * Returns the current model.
     *
     * @param modelId the model
     * @return the model, or empty when this node never joined
     */
    public Optional<M> model(String modelId) {
        Model<M> model = models.get(modelId);
        if (model == null) {
            return Optional.empty();
        }
        synchronized (model) {
            return Optional.of(model.value);
        }
    }

    /**
     * Returns how many merges this node has folded into a model.
     *
     * @param modelId the model
     * @return the merge round counter; 0 when never merged or unknown
     */
    public long round(String modelId) {
        Model<M> model = models.get(modelId);
        if (model == null) {
            return 0;
        }
        synchronized (model) {
            return model.round;
        }
    }

    /**
     * Returns the content address of a model: the sha-256 CID of its encoding.
     *
     * @param model the model
     * @return the CID
     */
    public String contentId(M model) {
        return BlockExchange.cidOf(mergeable.encode(Objects.requireNonNull(model, "model")));
    }

    /**
     * Returns the content address of this node's current model for an id.
     *
     * @param modelId the model
     * @return the CID, or empty when this node never joined
     */
    public Optional<String> contentIdOf(String modelId) {
        return model(modelId).map(this::contentId);
    }

    /**
     * Ends an epoch: evaluates every local model and writes one
     * {@link Evaluation} per model into the metrics space.
     *
     * @param epoch    the epoch that just ended
     * @param evaluate the loss function over a model
     * @return the evaluations written, in no particular order
     * @throws IllegalStateException when no metrics space is configured
     */
    public List<Evaluation> endEpoch(int epoch, ToDoubleFunction<M> evaluate) {
        Objects.requireNonNull(evaluate, "evaluate");
        if (metrics == null) {
            throw new IllegalStateException("no metrics space: configure builder.metrics(space, lease)");
        }
        List<Evaluation> written = new ArrayList<>();
        for (Map.Entry<String, Model<M>> e : models.entrySet()) {
            M snapshot;
            synchronized (e.getValue()) {
                snapshot = e.getValue().value;
            }
            Evaluation evaluation = new Evaluation(e.getKey(), epoch,
                    evaluate.applyAsDouble(snapshot), contentId(snapshot));
            metrics.write(evaluation, metricsLease);
            written.add(evaluation);
        }
        return written;
    }

    // --------------------------------------------------------------- receive

    private void onExchange(PeerId from, byte[] payload) {
        Exchange exchange;
        try {
            exchange = codec.fromBytes(payload, Exchange.class);
        } catch (RuntimeException e) {
            return;
        }
        if (exchange == null || exchange.modelId() == null || exchange.kind() == null) {
            return;
        }
        Model<M> model = models.get(exchange.modelId());
        switch (exchange.kind()) {
            case OFFER -> send(onOffer(from, exchange, model));
            case ACCEPT -> onAccept(from, exchange, model);
            case BUSY -> onBusy(exchange, model);
            default -> {
                // unknown kind: a newer protocol; ignore
            }
        }
    }

    private Outgoing onOffer(PeerId from, Exchange offer, Model<M> model) {
        Exchange busy = new Exchange(offer.modelId(), offer.token(), BUSY, 0, null, null);
        if (model == null) {
            return new Outgoing(from, busy); // not participating: unlock the offerer
        }
        synchronized (model) {
            if (model.pendingToken != 0) {
                return new Outgoing(from, busy); // our own offer is outstanding
            }
            Optional<M> theirs = materialize(from, offer);
            if (theirs.isEmpty()) {
                return new Outgoing(from, busy);
            }
            M merged;
            try {
                merged = mergeable.merge(model.value, theirs.get());
            } catch (IllegalArgumentException e) {
                return new Outgoing(from, busy); // incompatible: no change either side
            }
            Exchange accept = frame(offer.modelId(), offer.token(), ACCEPT, model.round, model.value);
            model.value = localUpdate == null ? merged : localUpdate.apply(merged);
            model.round++;
            return new Outgoing(from, accept);
        }
    }

    private void onAccept(PeerId from, Exchange accept, Model<M> model) {
        if (model == null) {
            return;
        }
        synchronized (model) {
            if (model.pendingToken == 0 || model.pendingToken != accept.token()) {
                return; // stale or unsolicited: the offer it answers is gone
            }
            model.pendingToken = 0;
            Optional<M> theirs = materialize(from, accept);
            if (theirs.isEmpty()) {
                return;
            }
            M merged;
            try {
                merged = mergeable.merge(model.value, theirs.get());
            } catch (IllegalArgumentException e) {
                return;
            }
            model.value = localUpdate == null ? merged : localUpdate.apply(merged);
            model.round++;
        }
    }

    private void onBusy(Exchange busy, Model<M> model) {
        if (model == null) {
            return;
        }
        synchronized (model) {
            if (model.pendingToken == busy.token()) {
                model.pendingToken = 0;
            }
        }
    }

    // -------------------------------------------------------------- transport

    private Exchange frame(String modelId, long token, String kind, long round, M model) {
        byte[] bytes = mergeable.encode(model);
        if (bytes.length <= EntryRecord.INLINE_PAYLOAD_LIMIT) {
            return new Exchange(modelId, token, kind, round, bytes, null);
        }
        if (blocks == null) {
            throw new IllegalStateException("model " + modelId + " encodes to " + bytes.length
                    + " bytes, over the inline limit; configure builder.blocks(...)");
        }
        return new Exchange(modelId, token, kind, round, null, blocks.put(bytes));
    }

    private Optional<M> materialize(PeerId from, Exchange exchange) {
        byte[] bytes = exchange.inline();
        if (bytes == null && exchange.cid() != null) {
            if (blocks == null) {
                return Optional.empty(); // cannot fetch content-addressed models here
            }
            bytes = blocks.fetch(exchange.cid(), List.of(from), blockFetchTimeout.toMillis())
                    .orElse(null);
        }
        if (bytes == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(mergeable.decode(bytes));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private void send(Outgoing outgoing) {
        if (outgoing != null) {
            pipes.send(outgoing.to(), TYPE, codec.toBytes(outgoing.frame()));
        }
    }
}
