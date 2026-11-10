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
 * api and a2ui pins, the specification, and the core's main sources name no part of the platform that builds on
 * AgentSpaces. The last test sweeps the real workspace, so the core gate fails on the first platform name.
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

        NeutralityGuard.Report report = NeutralityGuard.sweep(core);

        assertThat(report.findings()).extracting(Finding::file, Finding::line, Finding::name).containsExactly(
                tuple("agentspaces-a2ui/src/main/resources/catalogs/c.json", 2, "badmonkey.ai/ns"),
                tuple("agentspaces-a2ui/src/test/resources/golden/surfaces.json", 1, "bma-widgets"),
                tuple("agentspaces/agentspaces-api/src/main/java/A.java", 3, "bma-viz"),
                tuple("agentspaces/agentspaces-api/src/test/resources/vectors/x_cbor.hex", 1, "bma-widgets"));
        assertThat(report.read())
                .contains("agentspaces/tools/golden/golden.json", "agentspaces-spec/golden.json", "agentspaces-spec/SPEC.md",
                        "agentspaces-spec/README.md")
                .noneMatch(file -> file.contains("golden-examples") || file.contains("/target/") || file.contains("/src/test/java/"));
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
        assertThat(report.findings()).as("platform names in the neutral inputs (run agentspaces/tools/neutrality-check.sh)")
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
