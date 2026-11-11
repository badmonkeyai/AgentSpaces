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
package ai.badmonkey.agentspaces.api.neutrality;

import ai.badmonkey.agentspaces.api.neutrality.NeutralityGuard.Finding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/**
 * The neutrality guard of the platform-names review (ISSUE-AppCardOriginFormats §17 step 6): the golden vectors, the
 * api, a2ui, and commerce pins, the specification, and the main sources of the core, a2ui, and commerce name no part
 * of the platform that builds on AgentSpaces. The last test sweeps the real workspace, so the core gate fails on the
 * first platform name.
 */
class NeutralityGuardTest {

    private static final HexFormat HEX = HexFormat.of();

    @Test
    void findsAPlatformNameInTextAndNamesItsLine() {
        List<Finding> found = NeutralityGuard.scan("SPEC.md", "intro\nthe catalog `bma-widgets` holds six components\n");

        assertThat(found).extracting(Finding::file, Finding::line, Finding::name)
                .containsExactly(tuple("SPEC.md", 2, "bma-widgets"));
    }

    @Test
    void findsANameInsideAHexVector() {
        // {"catalog": "bma-viz"} in CBOR: the text header 0x67 is the letter g, right before the name.
        String hex = cborMap("catalog", "bma-viz");

        List<Finding> found = NeutralityGuard.scan("golden.json", "{\n  \"agent_card_ui_cbor\" : \"" + hex + "\"\n}\n");

        assertThat(found).extracting(Finding::line, Finding::name).containsExactly(tuple(2, "bma-viz"));
        assertThat(found.get(0).context()).contains("hex").contains("bma-viz");
    }

    @Test
    void findsThePlatformNamespacesProjectPackagesAndProduct() {
        String text = String.join("\n",
                "type https://badmonkey.ai/ns/apps/v1/ChatMessage",
                "type https://badmonkey.ai/console#DataUse",
                "see enterprise-platform/docs",
                "import ai.badmonkey.bma.console.records.AppCard;",
                "the BMA console",
                "the Bad Monkey Agentic AI Platform");

        assertThat(NeutralityGuard.scan("X.java", text)).extracting(Finding::line, Finding::name).containsExactly(
                tuple(1, "badmonkey.ai/ns"),
                tuple(2, "badmonkey.ai/console"),
                tuple(3, "enterprise-platform"),
                tuple(4, "bma"),
                tuple(5, "BMA"),
                tuple(6, "Bad Monkey Agentic AI Platform"));
    }

    @Test
    void leavesTheVendorNamesAndTheOpenValuesAlone() {
        String text = String.join("\n",
                "package ai.badmonkey.agentspaces.api.ad;",
                "\"https://badmonkey.ai/agentspaces/a2ui/v1/ChatMessage\"",
                "origin.system realm or console, signOn idp, and a reader refuses tenant",
                "the catalogs example-widgets and agentspaces-viz",
                "a submarine and an ambassador",
                "Copyright 2026 Bad Monkey, Inc.",
                "\"" + cborMap("catalog", "example-widgets") + "\"");

        assertThat(NeutralityGuard.scan("SPEC.md", text)).isEmpty();
    }

    @Test
    void ignoresLettersInBinaryThatNoStringHolds() {
        // The bytes b, m, a sit between NUL bytes, and no CBOR string header spans them.
        assertThat(NeutralityGuard.scan("x_cbor.hex", "00626d6100ff00ff")).isEmpty();
    }

    @Test
    void sweepReadsEveryInputAndNeverTheExamples(@TempDir Path workspace) throws IOException {
        Path core = workspace.resolve("agentspaces");
        write(core.resolve("tools/golden/golden.json"), "{\n  \"ping_body_cbor\" : \"a0\"\n}\n");
        write(core.resolve("agentspaces-api/src/main/java/A.java"), "class A {\n\n    String c = \"bma-viz\";\n}\n");
        write(core.resolve("agentspaces-api/src/test/java/ATest.java"), "// bma-viz in a test source\n");
        write(core.resolve("agentspaces-api/target/classes/a.txt"), "bma-viz\n");
        write(core.resolve("agentspaces-api/src/test/resources/vectors/x_cbor.hex"), cborMap("c", "bma-widgets") + "\n");
        Path spec = workspace.resolve("agentspaces-spec");
        for (String doc : List.of("SPEC.md", "TECH-SPEC.md", "REALM.md", "REGISTRY.md", "README.md")) {
            write(spec.resolve(doc), "# " + doc + "\n");
        }
        write(spec.resolve("golden.json"), "{}\n");
        write(spec.resolve("golden-examples.json"), "{ \"x\" : \"bma-widgets\" }\n");
        Path a2ui = workspace.resolve("agentspaces-a2ui");
        write(a2ui.resolve("src/main/resources/catalogs/c.json"), "{\n  \"$id\" : \"https://badmonkey.ai/ns/apps/v1/X\"\n}\n");
        write(a2ui.resolve("src/test/resources/golden/surfaces.json"), "{ \"catalog\" : \"bma-widgets\" }\n");
        write(a2ui.resolve("src/test/java/T.java"), "// bma-widgets in a test source\n");
        Path commerce = workspace.resolve("agentspaces-commerce");
        write(commerce.resolve("src/main/java/M.java"), "/** A mandate. */\nclass M {\n    String c = \"bma-commerce-merchant\";\n}\n");
        write(commerce.resolve("src/test/resources/vectors/mandate_cbor.hex"), cborMap("role", "bma-shopper") + "\n");
        write(commerce.resolve("src/test/java/MTest.java"), "// bma-commerce-payment in a test source\n");

        NeutralityGuard.Report report = NeutralityGuard.sweep(core);

        assertThat(report.findings()).extracting(Finding::file, Finding::line, Finding::name).containsExactly(
                tuple("agentspaces-a2ui/src/main/resources/catalogs/c.json", 2, "badmonkey.ai/ns"),
                tuple("agentspaces-a2ui/src/test/resources/golden/surfaces.json", 1, "bma-widgets"),
                tuple("agentspaces-commerce/src/main/java/M.java", 3, "bma-commerce-merchant"),
                tuple("agentspaces-commerce/src/test/resources/vectors/mandate_cbor.hex", 1, "bma-shopper"),
                tuple("agentspaces/agentspaces-api/src/main/java/A.java", 3, "bma-viz"),
                tuple("agentspaces/agentspaces-api/src/test/resources/vectors/x_cbor.hex", 1, "bma-widgets"));
        assertThat(report.read())
                .contains("agentspaces/tools/golden/golden.json", "agentspaces-spec/golden.json", "agentspaces-spec/SPEC.md",
                        "agentspaces-spec/README.md")
                .noneMatch(file -> file.contains("golden-examples") || file.contains("/target/") || file.contains("/src/test/java/"));
    }

    @Test
    void sweepReadsTheSpecificationsDocumentsFolder(@TempDir Path workspace) throws IOException {
        Path core = workspace.resolve("agentspaces");
        write(core.resolve("tools/golden/golden.json"), "{}\n");
        Path spec = workspace.resolve("agentspaces-spec");
        for (String doc : List.of("SPEC.md", "TECH-SPEC.md", "REALM.md", "REGISTRY.md", "README.md")) {
            write(spec.resolve(doc), "# " + doc + "\n");
        }
        write(spec.resolve("golden.json"), "{}\n");
        write(spec.resolve("docs/README.md"), "# Documents\n");
        write(spec.resolve("docs/records/vectors.md"), "# Vectors\n\nthe catalog bma-viz\n");

        NeutralityGuard.Report report = NeutralityGuard.sweep(core);

        assertThat(report.findings()).extracting(Finding::file, Finding::line, Finding::name)
                .containsExactly(tuple("agentspaces-spec/docs/records/vectors.md", 3, "bma-viz"));
        assertThat(report.read()).contains("agentspaces-spec/docs/README.md", "agentspaces-spec/docs/records/vectors.md");
    }

    @Test
    void sweepFindsAPlantedInternalReferenceInEverySpecificationDocument(@TempDir Path workspace) throws IOException {
        Path core = workspace.resolve("agentspaces");
        write(core.resolve("tools/golden/golden.json"), "{}\n");
        write(core.resolve("agentspaces-api/src/main/java/A.java"), "/** Built for ISSUE-AppCard, see srad/x.md. */\nclass A {\n}\n");
        Path spec = workspace.resolve("agentspaces-spec");
        write(spec.resolve("SPEC.md"), "# SPEC\n\n**6.1a.2 The AppCard (v0.1.25 addendum; ISSUE-AppCard).**\n");
        write(spec.resolve("TECH-SPEC.md"), "# TECH-SPEC\n\nThe DTLS binding (v0.1.19, SRAD `srad/dtls-support.md`) runs.\n");
        write(spec.resolve("REALM.md"), "# REALM\n\nSee `reviews/realm-spec-review-2026-11-10.md`.\n");
        write(spec.resolve("REGISTRY.md"), "# REGISTRY\n\nWritten under owner decision 1 of 2026-11-09.\n");
        write(spec.resolve("README.md"), "# README\n\nA reader ignores a key it does not know (P2-F-03).\n");
        write(spec.resolve("golden.json"), "{}\n");
        write(spec.resolve("docs/vectors.md"), "# Vectors\n\nThe subordinate-key vectors (QA4 A4-7 phase 1).\n");

        NeutralityGuard.Report report = NeutralityGuard.sweep(core);

        assertThat(report.findings()).extracting(Finding::file, Finding::line, Finding::name).containsExactly(
                tuple("agentspaces-spec/README.md", 3, "P2-F-03"),
                tuple("agentspaces-spec/REALM.md", 3, "reviews/"),
                tuple("agentspaces-spec/REGISTRY.md", 3, "owner decision"),
                tuple("agentspaces-spec/SPEC.md", 3, "ISSUE-AppCard"),
                tuple("agentspaces-spec/TECH-SPEC.md", 3, "SRAD"),
                tuple("agentspaces-spec/TECH-SPEC.md", 3, "srad/"),
                tuple("agentspaces-spec/docs/vectors.md", 3, "QA4"),
                tuple("agentspaces-spec/docs/vectors.md", 3, "A4-7"));
    }

    @Test
    void findsEachKindOfInternalReferenceInASpecificationDocument() {
        String text = String.join("\n",
                "the AppCard revision of ISSUE-AppCardOriginFormats",
                "PDT 2 (srad/agentspaces-pdt2.md §10)",
                "see reviews/realm-spec-review-2026-11-10.md",
                "VCDIFF frames (P2-Z-05) and the relay (LB-E-04, JS-W-11, FR-22, ASF-047, JC-4)",
                "the QA2 workstreams and the PERF2 round",
                "the DTLS revision (SRAD dtls-support)",
                "(HLC round six, 2026-11-08, owner ruling F4)",
                "the owner's rulings of that day, and the owner answers 3");

        assertThat(NeutralityGuard.scanSpecification("SPEC.md", text)).extracting(Finding::line, Finding::name)
                .containsExactly(
                        tuple(1, "ISSUE-AppCardOriginFormats"),
                        tuple(2, "srad/"),
                        tuple(3, "reviews/"),
                        tuple(4, "P2-Z-05"),
                        tuple(4, "LB-E-04"),
                        tuple(4, "JS-W-11"),
                        tuple(4, "FR-22"),
                        tuple(4, "ASF-047"),
                        tuple(4, "JC-4"),
                        tuple(5, "QA2"),
                        tuple(5, "PERF2"),
                        tuple(6, "SRAD"),
                        tuple(7, "HLC round"),
                        tuple(7, "owner ruling"),
                        tuple(8, "owner's rulings"),
                        tuple(8, "owner answers"));
    }

    @Test
    void leavesTheSpecificationsOwnWordsAlone() {
        String text = String.join("\n",
                "`DIRECTIVE_ISSUER` and `MAX_CLAIM_PREDATES_ISSUE_MILLIS`",
                "UTF-8, UTF-16, SHA-256, AES-256, P-256, ISO-8601, SipHash-2-4, DAG-CBOR",
                "rule F4 of §9.2, the item rules S1 to S12, and P2: everything is leased",
                "nothing lives forever without an owner renewing it; its owner sets a price",
                "an adversarial review of the same day, and the fifth review round",
                "https://github.com/example/agentspaces/issues/16 and the `issues` space");

        assertThat(NeutralityGuard.scanSpecification("SPEC.md", text)).isEmpty();
    }

    @Test
    void internalReferencesCountOnlyInTheSpecification() {
        assertThat(NeutralityGuard.scan("A.java", "// built for ISSUE-AppCard, see srad/x.md (P2-F-03)\n")).isEmpty();
    }

    @Test
    void sweepFailsWhenASpecificationDocumentIsMissing(@TempDir Path workspace) throws IOException {
        Path core = workspace.resolve("agentspaces");
        write(core.resolve("tools/golden/golden.json"), "{}\n");
        write(workspace.resolve("agentspaces-spec/golden.json"), "{}\n");

        assertThatThrownBy(() -> NeutralityGuard.sweep(core))
                .isInstanceOf(NoSuchFileException.class)
                .hasMessageContaining("SPEC.md");
    }

    @Test
    void theWorkspaceNamesNoPlatform() throws IOException {
        NeutralityGuard.Report report = NeutralityGuard.sweep(Path.of(".."));

        assertThat(report.read()).contains("agentspaces/tools/golden/golden.json");
        assertThat(report.findings())
                .as("platform names in the neutral inputs, or internal references in the specification"
                        + " (run agentspaces/tools/neutrality-check.sh)")
                .isEmpty();
    }

    /** A one-entry CBOR map of two short text strings, as hex. */
    private static String cborMap(String key, String value) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        byte[] v = value.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[3 + k.length + v.length];
        out[0] = (byte) 0xa1;
        out[1] = (byte) (0x60 + k.length);
        System.arraycopy(k, 0, out, 2, k.length);
        out[2 + k.length] = (byte) (0x60 + v.length);
        System.arraycopy(v, 0, out, 3 + k.length, v.length);
        return HEX.formatHex(out);
    }

    private static void write(Path file, String text) throws IOException {
        Files.createDirectories(file.getParent());
        Files.writeString(file, text);
    }
}
