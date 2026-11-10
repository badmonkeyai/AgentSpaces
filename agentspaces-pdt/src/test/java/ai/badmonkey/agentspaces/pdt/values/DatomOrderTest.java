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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 2, P2-D-11 (srad/agentspaces-pdt2.md §3.1.1): the three index orders follow
 * DataScript's comparators over the decoded datom values, so a Java walk of an index that
 * DataScript wrote seeks where DataScript does.
 */
class DatomOrderTest {

    private static final long TX = 536870913L;

    private static Datom d(long e, String a, Object v) {
        return new Datom(e, a, v, TX);
    }

    @Test
    void attributesCompareAsKeywordsNamespaceFirst() {
        // as strings ":a-b/x" sorts before ":a/x"; as keywords namespace "a" sorts before "a-b"
        assertThat(DatomOrder.compareAttributes(":a/x", ":a-b/x")).isNegative();
        assertThat(DatomOrder.compareAttributes(":x", ":a/x")).as("no namespace sorts first").isNegative();
        assertThat(DatomOrder.compareAttributes(":p/age", ":p/name")).isNegative();
        assertThat(DatomOrder.compareAttributes(":p/name", ":p/name")).isZero();
        assertThat(DatomOrder.compareAttributes(":a/b/c", ":a/b")).as("the first slash splits").isPositive();
    }

    @Test
    void valuesCompareNumericallyAcrossNumberTypesAndByClassNameOtherwise() {
        assertThat(DatomOrder.compareValues(1L, 1.5d)).isNegative();
        assertThat(DatomOrder.compareValues(2.0d, 2L)).isZero();
        assertThat(DatomOrder.compareValues(Datom.Tagged.bigint(BigInteger.TWO.pow(70)), Long.MAX_VALUE)).isPositive();
        assertThat(DatomOrder.compareValues(":status/a", "text")).as("a keyword sorts before a string").isNegative();
        assertThat(DatomOrder.compareValues("\\:text", ":kw")).as("an escaped colon is a string").isPositive();
        assertThat(DatomOrder.compareValues(true, 1L)).as("java.lang.Boolean before java.lang.Long").isNegative();
        assertThat(DatomOrder.compareValues(false, true)).isNegative();
        assertThat(DatomOrder.compareValues("b", "a")).isPositive();
        assertThat(DatomOrder.compareValues(Datom.Tagged.instant(5), Datom.Tagged.instant(7))).isNegative();
        UUID low = new UUID(-1L, 0L);
        UUID high = new UUID(1L, 0L);
        assertThat(DatomOrder.compareValues(Datom.Tagged.uuid(low), Datom.Tagged.uuid(high)))
                .as("UUIDs compare as signed halves, as java.util.UUID does").isNegative();
        assertThat(DatomOrder.compareValues(new byte[]{1}, new byte[]{2})).isNegative();
    }

    @Test
    void theThreeIndexesOrderTheirComponentsAndTxLast() {
        List<Datom> datoms = new ArrayList<>(List.of(d(2, ":p/age", 45L), d(1, ":p/name", "Ada"), d(1, ":p/age", 36L),
                d(2, ":p/name", "Grace"), new Datom(1, ":p/age", 36L, TX + 1)));
        datoms.sort(DatomOrder.EAVT);
        assertThat(datoms).containsExactly(d(1, ":p/age", 36L), new Datom(1, ":p/age", 36L, TX + 1),
                d(1, ":p/name", "Ada"), d(2, ":p/age", 45L), d(2, ":p/name", "Grace"));
        datoms.sort(DatomOrder.AEVT);
        assertThat(datoms.get(0)).isEqualTo(d(1, ":p/age", 36L));
        assertThat(datoms.get(2)).isEqualTo(d(2, ":p/age", 45L));
        datoms.sort(DatomOrder.AVET);
        assertThat(datoms.subList(3, 5)).containsExactly(d(1, ":p/name", "Ada"), d(2, ":p/name", "Grace"));
        assertThat(DatomOrder.of(DnNode.AVET)).isSameAs(DatomOrder.AVET);
        assertThatThrownBy(() -> DatomOrder.of(DnNode.TAIL)).isInstanceOf(IllegalArgumentException.class);
    }
}
