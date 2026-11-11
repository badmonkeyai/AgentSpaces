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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The neutrality guard of the platform-names review (ISSUE-AppCardOriginFormats §17 step 6). The owner ruled on
 * 2026-11-09 that the protocol, its golden vectors, the core libraries, and the specification name no part of the
 * platform that builds on AgentSpaces; a platform name travels only as data in a generic field at run time.
 *
 * <p>A sweep reads the inputs the ruling covers, relative to the core directory {@code agentspaces/} and its workspace:
 * <ul>
 *   <li>{@code agentspaces-spec/golden.json} and its vendored copy {@code agentspaces/tools/golden/golden.json};</li>
 *   <li>the api pins {@code agentspaces/agentspaces-api/src/test/resources/vectors/}, the a2ui pins
 *       {@code agentspaces-a2ui/src/test/resources/vectors/} and {@code golden/}, and the commerce pins
 *       {@code agentspaces-commerce/src/test/resources/vectors/};</li>
 *   <li>the specification: {@code SPEC.md}, {@code TECH-SPEC.md}, {@code REALM.md}, {@code REGISTRY.md}, every
 *       other {@code agentspaces-spec/*.md}, and every Markdown file under {@code agentspaces-spec/docs/};</li>
 *   <li>every file under a {@code src/main} of the core, under {@code agentspaces-a2ui/src/main}, and under
 *       {@code agentspaces-commerce/src/main}.</li>
 * </ul>
 * It never reads {@code golden-examples.json}, the non-normative file of one console's realistic vectors (owner
 * answer 6). The specification, a2ui, and commerce inputs are read when their directories sit beside the core, as
 * they do in the workspace; a standalone core checkout has none of them.
 *
 * <p>Every line is matched as text, and every run of hex in it is decoded and matched too: each CBOR text or byte
 * string inside the bytes that holds printable UTF-8, and the whole run when it is such text. That is how the review
 * found {@code bma-widgets} inside {@code agent_card_ui_cbor}. The four patterns of {@link #DENIED} never match what
 * the issue and {@code REALM.md} allow: the vendor's own names ({@code ai.badmonkey.agentspaces},
 * {@code https://badmonkey.ai/agentspaces/}, Bad Monkey), the registered origin systems {@code realm} and
 * {@code console} of {@code REGISTRY.md}, the open catalogs {@code agentspaces-viz} and {@code agentspaces-widgets},
 * and the words realm, console, and tenant in prose and refusal vectors.
 *
 * <p>The specification is public, so its documents ({@code agentspaces-spec/*.md} and the Markdown files under
 * {@code agentspaces-spec/docs/}) also name nothing that points into the vendor's own planning: the patterns of
 * {@link #INTERNAL} find an internal ticket name ({@code ISSUE-} and a name), a path into the planning or review
 * folders ({@code srad/}, {@code reviews/}), an internal plan or finding id (such as {@code P2-F-03}, {@code A4-7},
 * {@code ASF-047}, {@code QA4}, or {@code PERF2}), the word SRAD, a review-round label ({@code HLC round}), and
 * process wording about the owner's rulings, decisions, or answers. A document says instead what a rule is for and
 * which version of the specification brought it. These patterns never match the specification's own words:
 * {@code DIRECTIVE_ISSUER}, UTF-8, SHA-256, P-256, the rule labels S1 to S12, P1 to P4, F1 to F4, and R1 to R12, the
 * principle P2, an owner who renews a lease, or an adversarial review. They read the specification alone; the core,
 * a2ui, and commerce sources keep their own references.
 *
 * <p>The core gate runs it as {@code NeutralityGuardTest}. The plain command line, from the workspace root, is
 * {@code agentspaces/tools/neutrality-check.sh}, or with the JDK alone
 * {@code java agentspaces/agentspaces-api/src/test/java/ai/badmonkey/agentspaces/api/neutrality/NeutralityGuard.java agentspaces};
 * it prints each finding as {@code file:line:column} and exits 1 when it finds any platform name or internal
 * reference.
 */
public final class NeutralityGuard {

    /** One platform name, or one internal reference in a specification document, in one line of one file. */
    public record Finding(String file, int line, int column, String name, String names, String context) {
        @Override
        public String toString() {
            return file + ":" + line + ":" + column + ": " + name + " (" + names + ") in " + context;
        }
    }

    /**
     * What one sweep read and found.
     *
     * @param read     the files read, relative to the workspace, in order
     * @param findings the platform names, by file, line, and column
     */
    public record Report(List<String> read, List<Finding> findings) {
    }

    private record Deny(Pattern pattern, String names) {
    }

    private static final List<Deny> DENIED = List.of(
            new Deny(Pattern.compile("(?<![A-Za-z0-9])(?i:bma)(?:[-_][A-Za-z0-9]+)*(?![A-Za-z0-9])"),
                    "the platform's prefix: its modules, catalogs, packages, and abbreviation"),
            new Deny(Pattern.compile("badmonkey\\.ai/(?:ns|console|platform|package|ucp)(?![A-Za-z0-9])"),
                    "a platform IRI namespace"),
            new Deny(Pattern.compile("enterprise-platform"), "the platform's project"),
            new Deny(Pattern.compile("(?i)bad monkey agentic ai platform"), "the platform's product name"));

    /** What a public specification document never names: references into the vendor's own planning and reviews. */
    private static final List<Deny> INTERNAL = List.of(
            new Deny(Pattern.compile("(?<![A-Za-z0-9_])ISSUE-[A-Za-z0-9][A-Za-z0-9-]*"), "an internal ticket name"),
            new Deny(Pattern.compile("(?<![A-Za-z0-9_/.-])(?:srad|reviews)/"), "a path into the internal planning or review folders"),
            new Deny(Pattern.compile("(?<![A-Za-z0-9_-])(?:P2|PD|S6|AD|LB|CQ|FR|A4|ASF|JC|JD|JS|G2|PRF|TODO)-(?:[A-Z]-)?[0-9]{1,3}(?![0-9A-Za-z])"),
                    "an internal plan or finding id"),
            new Deny(Pattern.compile("(?<![A-Za-z0-9_])(?:QA|PERF)[0-9](?![0-9A-Za-z])"), "an internal review or performance pass"),
            new Deny(Pattern.compile("(?<![A-Za-z0-9_])SRAD(?![A-Za-z0-9_])"), "an internal design document"),
            new Deny(Pattern.compile("(?<![A-Za-z0-9_])HLC round(?![A-Za-z0-9_])"), "an internal review-round label"),
            new Deny(Pattern.compile("(?i)(?<![A-Za-z0-9_])owner(?:'s)? (?:rulings?|decisions?|answers?)(?![A-Za-z0-9_])"),
                    "process wording about internal decisions"));

    /** The specification documents a sweep requires beside {@code golden.json}. */
    private static final List<String> SPEC_DOCUMENTS = List.of("SPEC.md", "TECH-SPEC.md", "REALM.md", "REGISTRY.md");
    private static final Set<String> BUILD_DIRECTORIES = Set.of("target", "node_modules", ".git");
    private static final Pattern HEX_RUN = Pattern.compile("(?<![0-9A-Za-z])(?:[0-9a-fA-F]{2}){4,}(?![0-9A-Za-z])");
    private static final int CONTEXT = 60;

    private NeutralityGuard() {
    }

    /**
     * Sweeps the core directory and the specification, a2ui, and commerce directories beside it.
     *
     * @param core the core directory, {@code agentspaces/}
     * @return the files read and the platform names found
     * @throws IOException when a required file is missing or a file cannot be read
     */
    public static Report sweep(Path core) throws IOException {
        Path root = core.toAbsolutePath().normalize();
        Path workspace = root.getParent();
        List<Path> files = new ArrayList<>();
        Set<Path> documents = new HashSet<>();
        files.add(root.resolve("tools/golden/golden.json"));
        files.addAll(tree(root.resolve("agentspaces-api/src/test/resources/vectors"), file -> true));
        files.addAll(tree(root, file -> isMain(root.relativize(file))));
        Path spec = workspace.resolve("agentspaces-spec");
        if (Files.isDirectory(spec)) {
            // golden.json and the *.md documents only: golden-examples.json is the platform's file and stays unread.
            files.add(spec.resolve("golden.json"));
            SPEC_DOCUMENTS.forEach(document -> files.add(spec.resolve(document)));
            try (Stream<Path> others = Files.list(spec)) {
                others.filter(file -> file.getFileName().toString().endsWith(".md"))
                        .filter(file -> !SPEC_DOCUMENTS.contains(file.getFileName().toString()))
                        .sorted()
                        .forEach(files::add);
            }
            files.addAll(tree(spec.resolve("docs"), file -> file.getFileName().toString().endsWith(".md")));
            files.stream().filter(file -> file.startsWith(spec) && file.getFileName().toString().endsWith(".md"))
                    .forEach(documents::add);
        }
        Path a2ui = workspace.resolve("agentspaces-a2ui");
        if (Files.isDirectory(a2ui)) {
            files.addAll(tree(a2ui.resolve("src/main"), file -> true));
            files.addAll(tree(a2ui.resolve("src/test/resources/vectors"), file -> true));
            files.addAll(tree(a2ui.resolve("src/test/resources/golden"), file -> true));
        }
        Path commerce = workspace.resolve("agentspaces-commerce");
        if (Files.isDirectory(commerce)) {
            files.addAll(tree(commerce.resolve("src/main"), file -> true));
            files.addAll(tree(commerce.resolve("src/test/resources/vectors"), file -> true));
        }
        List<String> read = new ArrayList<>();
        List<Finding> findings = new ArrayList<>();
        for (Path file : files) {
            byte[] bytes = Files.readAllBytes(file);
            if (isBinary(bytes)) {
                continue;
            }
            String name = workspace.relativize(file).toString().replace(file.getFileSystem().getSeparator(), "/");
            read.add(name);
            String text = new String(bytes, StandardCharsets.UTF_8);
            findings.addAll(documents.contains(file) ? scanSpecification(name, text) : scan(name, text));
        }
        findings.sort(Comparator.comparing(Finding::file).thenComparingInt(Finding::line).thenComparingInt(Finding::column));
        return new Report(List.copyOf(read), List.copyOf(findings));
    }

    /**
     * Finds every platform name in one file's text, one finding per name and line.
     *
     * @param file the file's name, as the findings report it
     * @param text the file's text
     * @return the findings, by line and column
     */
    public static List<Finding> scan(String file, String text) {
        return scan(file, text, false);
    }

    /**
     * Finds every platform name and every internal reference in one specification document's text, one finding per
     * name and line.
     *
     * @param file the document's name, as the findings report it
     * @param text the document's text
     * @return the findings, by line and column
     */
    public static List<Finding> scanSpecification(String file, String text) {
        return scan(file, text, true);
    }

    private static List<Finding> scan(String file, String text, boolean specification) {
        List<Finding> found = new ArrayList<>();
        String[] lines = text.split("\n", -1);
        for (int index = 0; index < lines.length; index++) {
            String line = lines[index];
            Map<String, Finding> named = new LinkedHashMap<>();
            match(file, index + 1, 0, line, null, named, DENIED);
            if (specification) {
                match(file, index + 1, 0, line, null, named, INTERNAL);
            }
            Matcher hex = HEX_RUN.matcher(line);
            while (hex.find()) {
                for (String decoded : strings(HexFormat.of().parseHex(hex.group()))) {
                    match(file, index + 1, hex.start(), decoded, "hex-decoded", named, DENIED);
                }
            }
            named.values().stream().sorted(Comparator.comparingInt(Finding::column)).forEach(found::add);
        }
        return found;
    }

    /**
     * Sweeps from the command line and exits 1 when it finds a platform name or an internal reference.
     *
     * @param args the core directory, {@code .} by default
     * @throws IOException when a required file is missing or a file cannot be read
     */
    public static void main(String[] args) throws IOException {
        Report report = sweep(Path.of(args.length > 0 ? args[0] : "."));
        report.findings().forEach(System.out::println);
        System.out.printf("neutrality check: %d files read, %d platform names or internal references found%n",
                report.read().size(), report.findings().size());
        if (!report.findings().isEmpty()) {
            System.exit(1);
        }
    }

    private static void match(String file, int line, int column, String text, String decoded, Map<String, Finding> named,
            List<Deny> denied) {
        for (Deny deny : denied) {
            Matcher matcher = deny.pattern().matcher(text);
            while (matcher.find()) {
                String context = excerpt(text, matcher.start(), matcher.end());
                Finding finding = new Finding(file, line, decoded == null ? column + matcher.start() + 1 : column + 1,
                        matcher.group(), deny.names(), decoded == null ? context : decoded + " \"" + context + "\"");
                named.putIfAbsent(finding.name(), finding);
            }
        }
    }

    /** Each CBOR text or byte string in the bytes that holds printable UTF-8, and the whole bytes when they are such text. */
    private static List<String> strings(byte[] bytes) {
        List<String> found = new ArrayList<>();
        text(bytes, 0, bytes.length).ifPresent(found::add);
        for (int at = 0; at < bytes.length; at++) {
            int head = bytes[at] & 0xff;
            int major = head >>> 5;
            int info = head & 0x1f;
            int width = info < 24 ? 0 : info == 24 ? 1 : info == 25 ? 2 : info == 26 ? 4 : -1;
            if (major != 2 && major != 3 || width < 0 || at + 1 + width > bytes.length) {
                continue;
            }
            long length = info < 24 ? info : 0;
            for (int k = 1; k <= width; k++) {
                length = length << 8 | bytes[at + k] & 0xff;
            }
            int start = at + 1 + width;
            if (length >= 3 && start + length <= bytes.length) {
                text(bytes, start, (int) length).ifPresent(found::add);
            }
        }
        return found;
    }

    private static Optional<String> text(byte[] bytes, int from, int length) {
        for (int at = from; at < from + length; at++) {
            int b = bytes[at] & 0xff;
            if (b < 0x20 && b != '\t' && b != '\n' && b != '\r' || b == 0x7f) {
                return Optional.empty();
            }
        }
        try {
            return Optional.of(StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes, from, length)).toString());
        } catch (CharacterCodingException e) {
            return Optional.empty();
        }
    }

    private static String excerpt(String text, int start, int end) {
        int from = Math.max(0, start - CONTEXT);
        int to = Math.min(text.length(), end + CONTEXT);
        return (from > 0 ? "..." : "") + text.substring(from, to).strip() + (to < text.length() ? "..." : "");
    }

    private static boolean isMain(Path relative) {
        for (int at = 0; at + 1 < relative.getNameCount(); at++) {
            if (relative.getName(at).toString().equals("src") && relative.getName(at + 1).toString().equals("main")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isBinary(byte[] bytes) {
        for (byte b : bytes) {
            if (b == 0) {
                return true;
            }
        }
        return false;
    }

    /** The regular files under a directory, sorted, outside build output; none when the directory is absent. */
    private static List<Path> tree(Path directory, Predicate<Path> keep) throws IOException {
        List<Path> files = new ArrayList<>();
        if (!Files.isDirectory(directory)) {
            return files;
        }
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) {
                return BUILD_DIRECTORIES.contains(dir.getFileName().toString()) ? FileVisitResult.SKIP_SUBTREE
                        : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                if (attributes.isRegularFile() && keep.test(file)) {
                    files.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        files.sort(Comparator.naturalOrder());
        return files;
    }
}
