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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

class HashingEmbedderTest {

    private final HashingEmbedder embedder = new HashingEmbedder();

    @Test
    void isDeterministicAndNormalized() {
        double[] a = embedder.embed("customer order history");
        double[] b = embedder.embed("customer order history");
        assertThat(a).isEqualTo(b);

        double norm = 0;
        for (double v : a) {
            norm += v * v;
        }
        assertThat(Math.sqrt(norm)).isCloseTo(1.0, offset(1e-9));
    }

    @Test
    void sharedVocabularyScoresAboveDisjointVocabulary() {
        double[] query = embedder.embed("who holds customer order history?");
        double related = HashingEmbedder.cosine(query,
                embedder.embed("Customer order history with line items"));
        double unrelated = HashingEmbedder.cosine(query,
                embedder.embed("Summarizes legal contracts and PDF documents"));
        assertThat(related).isGreaterThan(unrelated);
        assertThat(related).isGreaterThan(0.3);
    }

    @Test
    void caseAndPunctuationDoNotMatter() {
        assertThat(embedder.embed("Order History!"))
                .isEqualTo(embedder.embed("order history"));
    }

    @Test
    void emptyAndNullTextsEmbedToZero() {
        assertThat(embedder.embed("")).containsOnly(0.0);
        assertThat(embedder.embed(null)).containsOnly(0.0);
        assertThat(HashingEmbedder.cosine(embedder.embed(""), embedder.embed("x")))
                .isZero();
    }

    @Test
    void schemaNamesSurviveTokenization() {
        // '#', '.', '/', '-' stay inside tokens so schema names and URIs match
        // as whole units rather than dissolving into common words.
        double sim = HashingEmbedder.cosine(
                embedder.embed("com.example.OrderRow#v1"),
                embedder.embed("consumes com.example.OrderRow#v1 entries"));
        assertThat(sim).isGreaterThan(0.3);
    }

    @Test
    void mismatchedDimensionsScoreZeroAndBadDimensionsAreRejected() {
        assertThat(HashingEmbedder.cosine(new double[]{1}, new double[]{1, 0})).isZero();
        assertThatThrownBy(() -> new HashingEmbedder(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
