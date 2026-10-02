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
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.InstantSource;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;

/**
 * The {@code aspace:cap/gossip-learn} capability (spec §8) for parameter-vector
 * models: {@link GossipLearner} specialised to {@code double[]} under
 * {@link WeightAveraging}, so every exchange is a pairwise mean and the fleet's
 * models converge to the fleet mean without any parameter server. An optional
 * local update step runs after every merge, which is where SGD on local data
 * plugs in: train locally, average globally, and no raw data leaves any peer.
 *
 * <p>Convergence: randomized pairwise averaging contracts the spread
 * geometrically, so a fleet is within tolerance of the mean in O(log N) rounds
 * per unit of spread. The primitive suits federated fine-tuning signals, shared
 * bandit statistics, and drift detectors run across a fleet. For any other
 * model type, or to configure the block exchange and a metrics space, build a
 * {@link GossipLearner} directly (or wrap one with
 * {@link #GossipLearning(GossipLearner)}); the exchange protocol and its
 * mass-conservation argument are documented there.
 */
public final class GossipLearning implements CapabilityProvider {

    /** The capability type URI. */
    public static final String TYPE = GossipLearner.TYPE;

    private final GossipLearner<double[]> learner;

    /**
     * Creates the learner with no local update step (pure averaging).
     *
     * @param pipes   the group's capability pipes
     * @param sampler the group's peer sampler
     * @param self    the local peer id (used in the advertisement)
     * @param codec   the CBOR codec
     * @param clock   the time source for advertisement freshness
     */
    public GossipLearning(CapabilityPipes pipes, PeerSampler sampler, PeerId self,
                          CborCodec codec, InstantSource clock) {
        this(pipes, sampler, self, codec, clock, null);
    }

    /**
     * Creates the learner with a local update step applied after every merge.
     *
     * @param pipes       the group's capability pipes
     * @param sampler     the group's peer sampler
     * @param self        the local peer id (used in the advertisement)
     * @param codec       the CBOR codec
     * @param clock       the time source for advertisement freshness
     * @param localUpdate applied to the merged parameters (local SGD, clipping);
     *                    null for pure averaging
     */
    public GossipLearning(CapabilityPipes pipes, PeerSampler sampler, PeerId self,
                          CborCodec codec, InstantSource clock,
                          UnaryOperator<double[]> localUpdate) {
        this(GossipLearner.builder(pipes, sampler, self, codec, clock, WeightAveraging.INSTANCE)
                .localUpdate(localUpdate)
                .build());
    }

    /**
     * Wraps a fully configured {@code double[]} learner (block exchange, metrics
     * space, custom timeouts).
     *
     * @param learner the learner
     */
    public GossipLearning(GossipLearner<double[]> learner) {
        this.learner = Objects.requireNonNull(learner, "learner");
    }

    /** Returns the underlying typed learner. */
    public GossipLearner<double[]> learner() {
        return learner;
    }

    @Override
    public String capabilityType() {
        return TYPE;
    }

    @Override
    public CapabilityAdvertisement describe(GroupId group) {
        return learner.describe(group);
    }

    /**
     * Registers this node's local model for a model id.
     *
     * @param modelId    the model identifier agreed among participants
     * @param parameters this node's initial parameter vector
     */
    public void start(String modelId, double[] parameters) {
        Objects.requireNonNull(parameters, "parameters");
        learner.start(modelId, parameters.clone());
    }

    /**
     * Runs one gossip round for every registered model: offer the current
     * parameters to one sampled member and merge its accepted reply.
     */
    @Override
    public boolean requiresTick() {
        return true;
    }

    @Override
    public void tick() {
        learner.tick();
    }

    /**
     * Returns the current parameters of a model.
     *
     * @param modelId the model
     * @return a copy of the parameters, or empty when this node never joined
     */
    public Optional<double[]> model(String modelId) {
        return learner.model(modelId).map(double[]::clone);
    }

    /**
     * Returns how many merges this node has folded into a model.
     *
     * @param modelId the model
     * @return the merge round counter; 0 when never merged or unknown
     */
    public long round(String modelId) {
        return learner.round(modelId);
    }

    /**
     * Returns the content address of this node's current parameters for a model.
     *
     * @param modelId the model
     * @return the sha-256 CID of the encoding, or empty when this node never joined
     */
    public Optional<String> contentId(String modelId) {
        return learner.contentIdOf(modelId);
    }

    /**
     * Ends an epoch: evaluates every local model and writes one
     * {@link GossipLearner.Evaluation} per model into the metrics space the
     * underlying learner was built with.
     *
     * @param epoch    the epoch that just ended
     * @param evaluate the loss function over a parameter vector
     * @return the evaluations written
     * @throws IllegalStateException when the learner has no metrics space
     */
    public List<GossipLearner.Evaluation> endEpoch(int epoch, ToDoubleFunction<double[]> evaluate) {
        return learner.endEpoch(epoch, evaluate);
    }
}
