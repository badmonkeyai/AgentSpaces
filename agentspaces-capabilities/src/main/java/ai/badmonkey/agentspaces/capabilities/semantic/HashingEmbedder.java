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
package ai.badmonkey.agentspaces.capabilities.semantic;

import ai.badmonkey.agentspaces.api.spi.Embedder;

import java.util.Locale;

/**
 * The model-free reference {@link Embedder} (spec §8): the feature-hashing
 * trick over lowercased word tokens. Each token hashes to one of the vector's
 * buckets with a hash-derived sign, token counts accumulate, and the result is
 * L2-normalized, so texts sharing vocabulary land near each other under cosine
 * similarity. Deterministic everywhere ({@code String.hashCode} is specified),
 * needs no model, and is honest about what it is: lexical overlap dressed as a
 * vector, good enough to route "who holds customer order history?" to an
 * orders table, and cleanly replaceable by a learned embedder through the SPI.
 */
public final class HashingEmbedder implements Embedder {

    /** The default embedding dimension. */
    public static final int DEFAULT_DIMENSIONS = 256;

    private final int dimensions;

    /** Creates the embedder with {@link #DEFAULT_DIMENSIONS}. */
    public HashingEmbedder() {
        this(DEFAULT_DIMENSIONS);
    }

    /**
     * Creates the embedder.
     *
     * @param dimensions the vector dimension; larger reduces hash collisions
     */
    public HashingEmbedder(int dimensions) {
        if (dimensions <= 0) {
            throw new IllegalArgumentException("dimensions must be positive: " + dimensions);
        }
        this.dimensions = dimensions;
    }

    @Override
    public int dimensions() {
        return dimensions;
    }

    @Override
    public double[] embed(String text) {
        double[] vector = new double[dimensions];
        if (text == null) {
            return vector;
        }
        for (String token : text.toLowerCase(Locale.ROOT).split("[^\\p{L}\\p{N}#/.-]+")) {
            if (token.isEmpty()) {
                continue;
            }
            int hash = token.hashCode();
            int bucket = Math.floorMod(hash, dimensions);
            double sign = ((hash >>> 31) & 1) == 0 ? 1.0 : -1.0;
            vector[bucket] += sign;
        }
        double norm = 0;
        for (double v : vector) {
            norm += v * v;
        }
        if (norm > 0) {
            norm = Math.sqrt(norm);
            for (int i = 0; i < vector.length; i++) {
                vector[i] /= norm;
            }
        }
        return vector;
    }

    /**
     * Cosine similarity of two same-dimension vectors; a plain dot product when
     * both are normalized, as this embedder's outputs are.
     *
     * @param a one vector
     * @param b the other
     * @return the similarity in [-1, 1]; 0 when dimensions differ
     */
    public static double cosine(double[] a, double[] b) {
        if (a.length != b.length) {
            return 0;
        }
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) {
            return 0;
        }
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    /** The model-free hashing embedder, identified with its dimensions. */
    @Override
    public String identity() {
        return "hashing";
    }
}
