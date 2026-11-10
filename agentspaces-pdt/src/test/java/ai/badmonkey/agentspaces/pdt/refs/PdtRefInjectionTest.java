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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.PdtRef;
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.pdt.blob.FileEntry;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.values.PDb;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PSet;
import ai.badmonkey.agentspaces.pdt.values.PVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code Pdt.refFor} (PDT 2 phase 7, P2-X-03, PD-R-11): the declared generic type of a {@code @PdtRef} site picks
 * the ref's kind and codecs, and a binder over {@code pdt::refFor} injects working handles.
 */
class PdtRefInjectionTest {

    public record Fact(String subject, int weight) {
    }

    /** Sites whose generic types the tests read. */
    @SuppressWarnings("unused")
    static final class Sites {
        Ref<PMap<String, Fact>> facts;
        Ref<PSet<String>> tags;
        Ref<PVector<Fact>> events;
        Ref<PBlob> report;
        Ref<PMap<String, FileEntry>> site;
        Ref<PDb> graph;
        Ref<PMap> untyped;
        Ref<PMap<String, ?>> wildcard;
        Ref raw;
        Ref<?> unknown;
        Ref<String> notAValue;
        List<String> notARef;
    }

    /** An agent with a field and a verb parameter, bound through a binder over the facade. */
    public static final class Keeper {
        @PdtRef("facts")
        Ref<PMap<String, Fact>> facts;
    }

    private RefFleet fleet;
    private RefFleet.Peer peer;

    @BeforeEach
    void setUp() throws Exception {
        fleet = new RefFleet();
        peer = fleet.newPeer("a", 1);
        fleet.settle(4);
    }

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static Type typeOf(String field) throws NoSuchFieldException {
        return Sites.class.getDeclaredField(field).getGenericType();
    }

    @Test
    void eachValueTypeGivesItsKind() throws Exception {
        assertThat(peer.pdt().refFor("facts", typeOf("facts")).kind()).isEqualTo(PRef.KIND_MAP);
        assertThat(peer.pdt().refFor("tags", typeOf("tags")).kind()).isEqualTo(PRef.KIND_SET);
        assertThat(peer.pdt().refFor("events", typeOf("events")).kind()).isEqualTo(PRef.KIND_VECTOR);
        assertThat(peer.pdt().refFor("report", typeOf("report")).kind()).isEqualTo(PRef.KIND_BLOB);
        assertThat(peer.pdt().refFor("site", typeOf("site")).kind()).as("the tree ref of PD-B-05").isEqualTo(PRef.KIND_TREE);
        assertThat(peer.pdt().refFor("graph", typeOf("graph")).kind()).isEqualTo(PRef.KIND_DB);
        assertThat(peer.pdt().refFor("untyped", typeOf("untyped")).kind()).isEqualTo(PRef.KIND_MAP);
        assertThat(peer.pdt().refFor("wildcard", typeOf("wildcard")).kind()).isEqualTo(PRef.KIND_MAP);
    }

    @Test
    @SuppressWarnings("unchecked")
    void aTypedMapRefDecodesItsValuesAsTheDeclaredRecord() throws Exception {
        Ref<PMap<String, Fact>> facts = (Ref<PMap<String, Fact>>) peer.pdt().refFor("facts", typeOf("facts"));

        facts.swap(m -> m.assoc("acme", new Fact("acme", 3)));

        Ref<PMap<String, Fact>> again = (Ref<PMap<String, Fact>>) peer.pdt().refFor("facts", typeOf("facts"));
        assertThat(again.deref().get("acme")).contains(new Fact("acme", 3));
    }

    @Test
    void aTypeThatNamesNoRefValueIsRefusedSayingWhy() throws Exception {
        assertThatThrownBy(() -> peer.pdt().refFor("raw", typeOf("raw")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Ref<PMap<String, Fact>>");
        assertThatThrownBy(() -> peer.pdt().refFor("unknown", typeOf("unknown")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Ref<PMap<String, Fact>>");
        assertThatThrownBy(() -> peer.pdt().refFor("notAValue", typeOf("notAValue")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("PMap, PSet, PVector, PBlob, or PDb");
        assertThatThrownBy(() -> peer.pdt().refFor("notARef", typeOf("notARef")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("is not a Ref");
    }

    @Test
    void aBinderOverTheFacadeInjectsAWorkingHandle() {
        AgentBinder binder = new AgentBinder(peer.identity(), fleet.groupId, null, InstantSource.system());
        try {
            binder.refs(peer.pdt()::refFor);
            Keeper keeper = new Keeper();

            binder.bind(keeper);
            keeper.facts.swap(m -> m.assoc("acme", new Fact("acme", 5)));

            assertThat(peer.pdt().mapRef("facts", ai.badmonkey.agentspaces.pdt.values.Codec.of(String.class),
                    ai.badmonkey.agentspaces.pdt.values.Codec.of(Fact.class)).deref().toMap())
                    .isEqualTo(Map.of("acme", new Fact("acme", 5)));
        } finally {
            binder.close();
        }
    }
}
