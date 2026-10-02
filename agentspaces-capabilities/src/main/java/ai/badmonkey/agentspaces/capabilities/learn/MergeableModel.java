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

/**
 * The mergeable-model SPI of {@code aspace:cap/gossip-learn} (spec §8): how two
 * peers' models combine on encounter and how a model travels the wire. The
 * capability is agnostic to what a model is; {@link WeightAveraging} is the
 * default for parameter vectors, and an application supplies its own for
 * anything else (bandit statistics, sketches, bid policies).
 *
 * <p>Contract: {@link #merge} must be commutative, so that both sides of a
 * pairwise exchange arrive at the same model and the exchange conserves the
 * fleet's mass; it may throw {@link IllegalArgumentException} for incompatible
 * models (a different shape), in which case the exchange is abandoned with no
 * change on either side. {@link #encode}/{@link #decode} must round-trip: the
 * bytes are the model's content address ({@code sha-256} CID) and, above the
 * inline limit, what the block exchange stores.
 *
 * @param <M> the model type
 */
public interface MergeableModel<M> {

    /**
     * Combines two models into one.
     *
     * @param a one model
     * @param b the other
     * @return the merged model
     * @throws IllegalArgumentException when the models cannot be merged
     */
    M merge(M a, M b);

    /**
     * Serializes a model.
     *
     * @param model the model
     * @return the canonical bytes
     */
    byte[] encode(M model);

    /**
     * Deserializes a model.
     *
     * @param bytes bytes from {@link #encode}
     * @return the model
     * @throws IllegalArgumentException when the bytes are not a model
     */
    M decode(byte[] bytes);

    /**
     * Returns a short name for the advertisement's {@code merge} parameter.
     *
     * @return the merge rule's name; the simple class name by default
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
