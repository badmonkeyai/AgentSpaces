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
package ai.badmonkey.agentspaces.space.crdt;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.EvaluationResult;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * RE-Q-MNT-01 (srad/replication-by-entry.md): the trie is one structure, and every change to it goes through
 * {@link SpaceStateCrdt}. Nothing outside this package names {@link HashTrie} or {@link Slot}, so no other class can
 * hold a second structure over the same keys or change a slot behind the map's counts; the replicated space reads and
 * changes its slots through the map's own methods.
 */
class TrieConfinementTest {

    private static final String CRDT = "ai.badmonkey.agentspaces.space.crdt..";

    private static final ArchRule CONFINED = noClasses().that().resideOutsideOfPackage(CRDT)
            .should().dependOnClassesThat().belongToAnyOf(HashTrie.class, Slot.class)
            .because("the trie is the map of SpaceStateCrdt alone (RE-Q-MNT-01)");

    @Test
    void nothingOutsideTheCrdtPackageTouchesTheTrie() {
        JavaClasses production = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("ai.badmonkey.agentspaces");
        assertThat(production.stream().anyMatch(c -> c.getName().endsWith(".replicated.ReplicatedSpace")))
                .as("the replicated space is among the classes checked").isTrue();
        CONFINED.check(production);
    }

    /** The rule bites: a class outside the package that names the trie is a violation. */
    @Test
    void theRuleCatchesAClassOutsideThePackage() {
        JavaClasses probe = new ClassFileImporter().importClasses(
                ai.badmonkey.agentspaces.space.replicated.TrieConfinementProbe.class);
        EvaluationResult result = CONFINED.evaluate(probe);
        assertThat(result.hasViolation()).isTrue();
    }
}
