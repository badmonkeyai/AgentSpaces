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
package ai.badmonkey.agentspaces.space.local;

import ai.badmonkey.agentspaces.space.replicated.SpaceCredential;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The delegating namespace registry (ISSUE-WorkflowShape §9.1): classes in a
 * mapped package are named {@code <namespace><SimpleName>}, everything else
 * keeps the default {@code <fqcn>#v1}, and {@code classFor} resolves both.
 */
class NamespaceSchemaRegistryTest {

    private static final String NS = "https://example.org/test#";

    private final NamespaceSchemaRegistry registry =
            NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.test", NS));

    @Test
    void aMappedPackageNamesByNamespaceAndSimpleName() {
        assertThat(registry.register(TaskEntry.class)).isEqualTo(NS + "TaskEntry");
        assertThat(registry.schemaNameOf(TaskEntry.class)).isEqualTo(NS + "TaskEntry");
        assertThat(registry.classFor(NS + "TaskEntry")).contains(TaskEntry.class);
    }

    @Test
    void anUnmappedPackageKeepsTheDefaultRule() {
        String expected = SpaceCredential.class.getName() + "#v1";
        assertThat(registry.register(SpaceCredential.class)).isEqualTo(expected);
        assertThat(registry.schemaNameOf(SpaceCredential.class)).isEqualTo(expected);
        assertThat(registry.classFor(expected)).contains(SpaceCredential.class);
        assertThat(new SimpleSchemaRegistry().register(SpaceCredential.class))
                .as("the reserved types keep the names every other peer uses")
                .isEqualTo(expected);
    }

    @Test
    void aSubpackageOfAMappedPackageIsMappedToo() {
        NamespaceSchemaRegistry wide =
                NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces", NS));
        assertThat(wide.register(TaskEntry.class)).isEqualTo(NS + "TaskEntry");
        // The longest mapped package wins when several match.
        NamespaceSchemaRegistry nested = NamespaceSchemaRegistry.of(Map.of(
                "ai.badmonkey.agentspaces", "https://example.org/all#",
                "ai.badmonkey.agentspaces.test", NS));
        assertThat(nested.register(FindingEntry.class)).isEqualTo(NS + "FindingEntry");
    }

    @Test
    void aPackageThatMerelySharesAPrefixIsNotMapped() {
        NamespaceSchemaRegistry registry =
                NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.tes", NS));
        assertThat(registry.register(TaskEntry.class))
                .isEqualTo(TaskEntry.class.getName() + "#v1");
    }

    @Test
    void theVersionFragmentIsAppendedWhenAsked() {
        NamespaceSchemaRegistry versioned = registry.withVersionFragment("#v1");
        assertThat(versioned.register(TaskEntry.class)).isEqualTo(NS + "TaskEntry#v1");
        assertThat(versioned.classFor(NS + "TaskEntry#v1")).contains(TaskEntry.class);
        assertThat(versioned.classFor(NS + "TaskEntry")).isEmpty();
    }

    @Test
    void unregisteredNamesAreUnknownAndUnregisteredTypesAreRefused() {
        assertThat(registry.classFor(NS + "Nothing")).isEmpty();
        assertThat(registry.classFor("com.nope.Missing#v1")).isEmpty();
        assertThatThrownBy(() -> registry.schemaNameOf(TaskEntry.class))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.schemaNameOf(SpaceCredential.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anUnregisteredIriStillResolvesWhenTheMappedPackageHoldsTheClass() {
        // A caller peer resolving a foreign card's IRI needs the class, not a
        // prior registration here; the mapped package is where to look.
        NamespaceSchemaRegistry fresh =
                NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.space.local", NS));
        assertThat(fresh.classFor(NS + "SimpleSchemaRegistry")).contains(SimpleSchemaRegistry.class);
        assertThat(fresh.classFor(NS + "Missing")).isEmpty();
        assertThat(fresh.classFor(NS + "../Escape")).isEmpty();
    }

    @Test
    void twoNestedClassesWithOneSimpleNameCollideAndAreRefused() {
        NamespaceSchemaRegistry local =
                NamespaceSchemaRegistry.of(Map.of("ai.badmonkey.agentspaces.space.local", NS));
        assertThat(local.register(Outer.Thing.class)).isEqualTo(NS + "Thing");
        assertThat(local.register(Outer.Thing.class)).as("idempotent").isEqualTo(NS + "Thing");
        assertThatThrownBy(() -> local.register(Other.Thing.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(NS + "Thing");
    }

    @Test
    void mappingsAreValidated() {
        assertThatThrownBy(() -> NamespaceSchemaRegistry.of(Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NamespaceSchemaRegistry.of(Map.of("", NS)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NamespaceSchemaRegistry.of(Map.of("org.example", " ")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.withVersionFragment(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Nested records of one simple name in this test's package. */
    static final class Outer {
        record Thing(String a) {
        }
    }

    static final class Other {
        record Thing(String b) {
        }
    }
}
