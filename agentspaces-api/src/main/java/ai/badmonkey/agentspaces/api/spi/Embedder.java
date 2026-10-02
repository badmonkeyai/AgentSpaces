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

/**
 * Maps text to a fixed-dimension vector for similarity ranking (spec §8, the
 * semantic-discovery capability). The base protocol stays model-free: the
 * reference implementation is a deterministic feature-hashing embedder that
 * needs no model, and deployments plug in richer embedders (a local model, a
 * provider API) without protocol changes. Implementations MUST be
 * deterministic for equal inputs within one process lifetime and SHOULD
 * L2-normalize, so cosine similarity reduces to a dot product.
 */
public interface Embedder {

    /**
     * Embeds one text.
     *
     * @param text the text
     * @return the embedding, of {@link #dimensions()} length
     */
    double[] embed(String text);

    /** Returns the embedding dimension this embedder produces. */
    int dimensions();

    /**
     * A stable name for this embedder and its model, advertised on the
     * semantic-discovery capability (SPEC §8) so peers can tell whether they
     * rank with the same embedding; for example {@code hashing} or
     * {@code spring-ai:text-embedding-3-small}. Peers that disagree rank
     * differently, and a query is sent only to peers that agree.
     *
     * @return the embedder's identity
     */
    default String identity() {
        return getClass().getSimpleName();
    }

    /** Whether this embedder L2-normalizes its vectors. */
    default boolean normalized() {
        return true;
    }
}
