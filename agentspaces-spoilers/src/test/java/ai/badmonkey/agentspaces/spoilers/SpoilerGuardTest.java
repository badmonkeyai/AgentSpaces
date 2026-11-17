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
package ai.badmonkey.agentspaces.spoilers;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The guards of srad/spoiler-peers.md §4.7 that keep spoiler code out of every shipped artifact: no
 * main-scope class of any module uses a spoiler package (SP-G-02), no main-scope source carries the
 * {@code TRIAGE-DIAG} diagnostic (SP-G-04), and this module stays undeployed and out of the BOM (SP-G-01).
 */
class SpoilerGuardTest {

    private static final String SPOILERS = "ai.badmonkey.agentspaces.spoilers..";
    /** The reactor root, from this module's directory, where surefire runs. */
    private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

    /** Main-scope classes only: test classes and the test-jars of peering, space and pdt are left out. */
    private static final ImportOption MAIN_ONLY = new ImportOption() {
        @Override
        public boolean includes(Location location) {
            return !location.contains("/test-classes/") && !location.contains("-tests.jar");
        }
    };

    @Test
    void noMainScopeClassOfAnyModuleUsesASpoilerPackage() {
        JavaClasses main = new ClassFileImporter().withImportOption(MAIN_ONLY).importPackages("ai.badmonkey.agentspaces");
        assertThat(main.contain("ai.badmonkey.agentspaces.peering.node.PeerNode")).as("the core's main classes were imported").isTrue();
        // the spoilers' own main classes may use each other; the test-support fabric names only its own interfaces
        noClasses().that().resideOutsideOfPackage(SPOILERS)
                .should().dependOnClassesThat().resideInAPackage(SPOILERS)
                .because("spoiler code never ships (srad/spoiler-peers.md SP-G-02)")
                .check(main);
    }

    /**
     * The classes above are the ones on this module's classpath; a module downstream of none of them (the
     * console, the store, Spring, the examples) could still name this module in its POM without a reactor
     * cycle, so every POM is read too: none may depend on it outside test scope.
     */
    @Test
    void noModuleDependsOnTheSpoilersOutsideTestScope() throws IOException {
        List<String> found = new ArrayList<>();
        try (Stream<Path> poms = Files.walk(ROOT, 4)) {
            for (Path pom : poms.filter(p -> p.getFileName().toString().equals("pom.xml"))
                    .filter(p -> !p.toString().contains("/target/") && !p.toString().contains("/node_modules/")).toList()) {
                if (pom.getParent().getFileName().toString().equals("agentspaces-spoilers")) {
                    continue;
                }
                String text = Files.readString(pom);
                int at = text.indexOf("<artifactId>agentspaces-spoilers</artifactId>");
                while (at >= 0) {
                    int end = text.indexOf("</dependency>", at);
                    String dependency = end < 0 ? text.substring(at) : text.substring(at, end);
                    if (text.lastIndexOf("<module>", at) < text.lastIndexOf("<dependency>", at)
                            && !dependency.contains("<scope>test</scope>")) {
                        found.add(ROOT.relativize(pom).toString());
                    }
                    at = text.indexOf("<artifactId>agentspaces-spoilers</artifactId>", at + 1);
                }
            }
        }
        assertThat(found).as("POMs that depend on agentspaces-spoilers outside test scope (SP-G-01)").isEmpty();
    }

    @Test
    void noMainScopeSourceCarriesTheTriageDiagnostic() throws IOException {
        List<String> found = new ArrayList<>();
        try (Stream<Path> modules = Files.list(ROOT)) {
            for (Path module : modules.filter(Files::isDirectory).toList()) {
                Path main = module.resolve("src/main");
                if (!Files.isDirectory(main)) {
                    continue;
                }
                try (Stream<Path> files = Files.walk(main)) {
                    for (Path f : files.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".java")).toList()) {
                        if (Files.readString(f).contains("TRIAGE-DIAG")) {
                            found.add(ROOT.relativize(f).toString());
                        }
                    }
                }
            }
        }
        assertThat(found).as("main-scope sources with the TRIAGE-DIAG diagnostic (SP-G-04)").isEmpty();
    }

    @Test
    void thisModuleIsNeverDeployedAndNotInTheBom() throws IOException {
        String pom = Files.readString(ROOT.resolve("agentspaces-spoilers/pom.xml"));
        assertThat(pom).contains("<maven.deploy.skip>true</maven.deploy.skip>").contains("<skipPublishing>true</skipPublishing>");
        assertThat(Files.readString(ROOT.resolve("agentspaces-dependencies/pom.xml"))).doesNotContain("agentspaces-spoilers");
        // the release profile turns the three test-jars off, and every module that attaches one reads the switch
        assertThat(Files.readString(ROOT.resolve("pom.xml"))).contains("<agentspaces.testjars.skip>true</agentspaces.testjars.skip>");
        for (String module : List.of("agentspaces-peering", "agentspaces-space", "agentspaces-pdt")) {
            assertThat(Files.readString(ROOT.resolve(module + "/pom.xml"))).as(module)
                    .contains("<skip>${agentspaces.testjars.skip}</skip>");
        }
    }
}
