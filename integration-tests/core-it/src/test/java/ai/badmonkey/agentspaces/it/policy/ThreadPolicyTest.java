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
package ai.badmonkey.agentspaces.it.policy;

import com.tngtech.archunit.core.domain.AccessTarget;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnitAccess;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The thread policy (srad/agentspaces-event-loop.md §3.8, decision 13, EL-X-05 to EL-X-08): one reactor-wide ArchUnit
 * test over every core main class, with one allowlist file, {@value #ALLOWLIST}.
 *
 * <p>It fails, outside the allowlist, on these calls (method references and constructor references included, since a
 * {@code Thread::new} thread factory makes platform threads as surely as a call does):
 * <ul>
 *   <li><b>thread</b> (EL-X-06): {@code new Thread} (or any {@code Thread} subclass), {@code Thread.ofPlatform()}, the
 *   {@code Executors} factories that return a pooled or scheduled executor, {@code new ThreadPoolExecutor},
 *   {@code new ScheduledThreadPoolExecutor}, {@code new ForkJoinPool}, {@code new java.util.Timer} and
 *   {@code Runtime.addShutdownHook}. ArchUnit sees the call, not its argument, so a virtual factory is no exemption:
 *   virtual work goes through {@code TimeSource}, {@code SerialExecutor} or
 *   {@code Executors.newVirtualThreadPerTaskExecutor()}.</li>
 *   <li><b>fixed-rate</b> (EL-X-07, EL-T-13): {@code scheduleAtFixedRate} and {@code scheduleWithFixedDelay} on any
 *   owner.</li>
 *   <li><b>raw-clock</b> (EL-X-08): {@code System.currentTimeMillis()} and {@code Instant.now()} in core main code, and
 *   {@code System.nanoTime()} in the protocol modules (peering, space, capabilities, agent, content, PDT, store,
 *   discovery).</li>
 *   <li><b>sleep</b> (group W, EL-W-05, §10.35): {@code Thread.sleep} in core main code. A wait goes through the time
 *   source ({@code TimeSource.sleep}, {@code waitOn}, a {@code ChangeSignal}), so a virtual clock moves it and a change
 *   ends it; only the system time source itself sleeps.</li>
 * </ul>
 *
 * <p>"Core main classes" are the main classes of the modules of §1.2, read from this module's test class path, which
 * depends on each of them. The other reactor modules are read from their {@code target/classes} when built, and
 * reported, not failed, in this revision. Every allowlist line must still match the code: a line whose site no longer
 * makes the calls it names fails as stale, so the list can only shrink as the code does.
 */
class ThreadPolicyTest {

    /** The allowlist, a test resource of this module. */
    static final String ALLOWLIST = "thread-policy.txt";

    /** The modules of §1.2 whose main classes the policy fails on (artifact ids). */
    static final Set<String> CORE = Set.of("agentspaces-common", "agentspaces-peering", "agentspaces-space",
            "agentspaces-capabilities", "agentspaces-agent", "agentspaces-content", "agentspaces-pdt",
            "agentspaces-store", "agentspaces-discovery", "agentspaces-transport-dtls",
            "agentspaces-transport-webrtc", "agentspaces-spring-boot-autoconfigure", "agentspaces-test-support",
            "agentspaces-connect-core");

    /** The protocol modules, where {@code System.nanoTime()} also falls under the raw-clock rule (EL-X-08). */
    static final Set<String> PROTOCOL = Set.of("agentspaces-peering", "agentspaces-space", "agentspaces-capabilities",
            "agentspaces-agent", "agentspaces-content", "agentspaces-pdt", "agentspaces-store",
            "agentspaces-discovery");

    /** Reactor modules that hold no main code of their own to check, or only test code. */
    private static final Set<String> NOT_REPORTED = Set.of("agentspaces-dependencies", "agentspaces-spoilers",
            "agentspaces-perf");

    private static final Set<String> POOLED_FACTORIES = Set.of("newSingleThreadExecutor",
            "newSingleThreadScheduledExecutor", "newFixedThreadPool", "newScheduledThreadPool", "newCachedThreadPool",
            "newWorkStealingPool", "defaultThreadFactory", "privilegedThreadFactory");

    /**
     * {@code CompletableFuture} methods that start the JDK's own platform threads (review defect R4,
     * srad/agentspaces-event-loop.md §10.23): {@code orTimeout}, {@code completeOnTimeout} and
     * {@code delayedExecutor} schedule on the JDK's {@code CompletableFutureDelayScheduler} thread, a real-time timer
     * beside the time source that no virtual clock moves.
     */
    private static final Set<String> FUTURE_TIMERS = Set.of("orTimeout", "completeOnTimeout", "delayedExecutor");

    private static final Pattern JAR = Pattern.compile("([A-Za-z0-9.-]+?)-\\d[^/!]*\\.jar!");
    private static final Pattern CLASSES_DIR = Pattern.compile("/([A-Za-z0-9.-]+)/target/classes/");
    private static final Pattern LAMBDA = Pattern.compile("lambda\\$(.+)\\$\\d+");

    /** One call the policy forbids, at one site. */
    record Violation(String module, String rule, String owner, String method, String call, String location) {

        String line() {
            return rule + " | " + owner + "#" + method + " | " + call + " | " + location;
        }
    }

    /** One allowlist line: a rule, a class (or {@code class#method}), the calls it may make, and why. */
    record Entry(int lineNo, String rule, String owner, String method, Set<String> calls, String reason) {

        boolean covers(Violation v) {
            return rule.equals(v.rule()) && calls.contains(v.call())
                    && (v.owner().equals(owner) || v.owner().startsWith(owner + "$"))
                    && (method == null || method.equals(v.method()));
        }

        String site() {
            return method == null ? owner : owner + "#" + method;
        }
    }

    private static List<Violation> core;
    private static Map<String, List<Violation>> reported;
    private static Set<String> coreModulesSeen;

    @BeforeAll
    static void scan() throws IOException {
        JavaClasses onClassPath = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("ai.badmonkey.agentspaces");
        core = new ArrayList<>();
        coreModulesSeen = new TreeSet<>();
        for (JavaClass c : onClassPath) {
            String module = moduleOf(c);
            if (module != null && CORE.contains(module)) {
                coreModulesSeen.add(module);
                core.addAll(violations(module, c));
            }
        }
        reported = new TreeMap<>();
        Path root = Path.of(System.getProperty("user.dir")).resolve("../..").normalize();
        try (Stream<Path> modules = Files.list(root)) {
            for (Path dir : modules.sorted().toList()) {
                String name = dir.getFileName().toString();
                Path classes = dir.resolve("target/classes");
                if (!(name.startsWith("agentspaces-") || name.startsWith("embabel-")) || CORE.contains(name)
                        || NOT_REPORTED.contains(name) || !Files.isDirectory(classes)) {
                    continue;
                }
                List<Violation> found = new ArrayList<>();
                for (JavaClass c : new ClassFileImporter().importPath(classes)) {
                    found.addAll(violations(name, c));
                }
                reported.put(name, found);
            }
        }
    }

    @Test
    void everyCoreModuleIsOnTheClassPath() {
        assertThat(coreModulesSeen).as("the core modules this test reads (a missing one would pass unchecked)")
                .containsExactlyInAnyOrderElementsOf(CORE);
    }

    @Test
    void coreMainCodeMakesNoPlatformThreadFixedRateScheduleOrRawClockReadOutsideTheAllowlist() throws IOException {
        List<Entry> entries = allowlist();
        List<String> outside = new ArrayList<>();
        for (Violation v : core) {
            if (entries.stream().noneMatch(e -> e.covers(v))) {
                outside.add(v.line());
            }
        }
        outside.sort(Comparator.naturalOrder());
        assertThat(outside).as(() -> "calls the thread policy forbids in core main code (srad/agentspaces-event-loop.md"
                        + " §3.8, EL-X-06 to EL-X-08): move each to TimeSource, SerialExecutor or"
                        + " Executors.newVirtualThreadPerTaskExecutor(), or add a line with its reason to "
                        + "integration-tests/core-it/src/test/resources/" + ALLOWLIST + "\n  "
                        + String.join("\n  ", outside))
                .isEmpty();
    }

    @Test
    void everyAllowlistLineStillMatchesTheCode() throws IOException {
        List<String> stale = new ArrayList<>();
        for (Entry e : allowlist()) {
            for (String call : e.calls()) {
                boolean found = core.stream().anyMatch(v -> e.covers(v) && v.call().equals(call));
                if (!found) {
                    stale.add(ALLOWLIST + ":" + e.lineNo() + " " + e.rule() + " | " + e.site() + " | " + call);
                }
            }
        }
        assertThat(stale).as(() -> "allowlist lines whose site no longer makes the call (remove them: the list may only"
                + " shrink)\n  " + String.join("\n  ", stale)).isEmpty();
    }

    @Test
    void theAllowlistHoldsFivePlatformThreadSitesEachWithItsReason() throws IOException {
        List<Entry> entries = allowlist();
        assertThat(entries).allSatisfy(e -> assertThat(e.reason()).as(ALLOWLIST + ":" + e.lineNo() + " reason")
                .hasSizeGreaterThan(20));
        assertThat(entries.stream().filter(e -> e.rule().equals("thread")).map(Entry::site).toList())
                .as("the platform-thread sites of §3.8 (the timer, the watchdog, the loop pool, space-verify,"
                        + " agentspaces-blake3)")
                .containsExactlyInAnyOrder(
                        "ai.badmonkey.agentspaces.common.time.SystemTime#startTimerLocked",
                        "ai.badmonkey.agentspaces.common.time.Watchdog#startThreadLocked",
                        "ai.badmonkey.agentspaces.common.time.LoopPool#startWorkerLocked",
                        "ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace$VerifyPool#<clinit>",
                        "ai.badmonkey.agentspaces.pdt.blob.GroupHasher$Pool#create");
        assertThat(entries.stream().filter(e -> e.rule().equals("thread") && e.method() == null).map(Entry::site))
                .as("a platform-thread line names its method: a line for a whole class would let a second platform"
                        + " thread anywhere in it, or in its nested classes, pass unseen (review defect R4)")
                .isEmpty();
        assertThat(entries).as("no fixed-rate schedule is ever allowed in core (EL-T-13)")
                .noneMatch(e -> e.rule().equals("fixed-rate"));
    }

    /**
     * The rules themselves, on a class that makes every forbidden call once (never run): each is found, named as the
     * allowlist names it, and placed at its method and line, a lambda's at the method that declares it.
     */
    @Test
    void eachForbiddenCallIsFoundAtItsSite() {
        JavaClass offender = new ClassFileImporter().importClasses(Offender.class).get(Offender.class);
        List<Violation> found = violations("agentspaces-peering", offender);
        Set<String> calls = found.stream().map(v -> v.rule() + " " + v.call())
                .collect(Collectors.toCollection(TreeSet::new));
        assertThat(calls).containsExactlyInAnyOrder(
                        "thread new Thread", "thread Thread.ofPlatform", "thread new ThreadPoolExecutor",
                        "thread new ScheduledThreadPoolExecutor", "thread new ForkJoinPool", "thread new Timer",
                        "thread Runtime.addShutdownHook", "thread Executors.newSingleThreadExecutor",
                        "thread Executors.newSingleThreadScheduledExecutor", "thread Executors.newFixedThreadPool",
                        "thread Executors.newScheduledThreadPool", "thread Executors.newCachedThreadPool",
                        "thread Executors.newWorkStealingPool", "thread Executors.defaultThreadFactory",
                        "thread Executors.privilegedThreadFactory", "thread CompletableFuture.orTimeout",
                        "thread CompletableFuture.completeOnTimeout", "thread CompletableFuture.delayedExecutor",
                        "thread CompletableFuture.async-on-common-pool", "thread ForkJoinPool.commonPool",
                        "thread parallel-stream", "fixed-rate scheduleAtFixedRate",
                        "fixed-rate scheduleWithFixedDelay", "raw-clock System.currentTimeMillis",
                        "raw-clock Instant.now", "raw-clock System.nanoTime", "sleep Thread.sleep");
        assertThat(found).as("a Thread::new factory is a platform thread too")
                .anyMatch(v -> v.call().equals("new Thread") && v.method().equals("factory"));
        assertThat(found).as("a read inside a lambda is placed at the method that declares it")
                .anyMatch(v -> v.call().equals("System.nanoTime") && v.method().equals("inLambda"));
        assertThat(found).allMatch(v -> v.owner().equals(Offender.class.getName())
                && v.location().startsWith("ThreadPolicyTest.java:"));
        assertThat(violations("agentspaces-common", offender)).as("nanoTime outside the protocol modules is allowed")
                .noneMatch(v -> v.call().equals("System.nanoTime"));
        assertThat(forbiddenIn(Allowed.class)).as("virtual threads, Instant.now(clock) and one-shot schedules").isEmpty();
    }

    private static List<Violation> forbiddenIn(Class<?> type) {
        return violations("agentspaces-peering", new ClassFileImporter().importClasses(type).get(type));
    }

    /** Every call the policy forbids, once each. Never run. */
    @SuppressWarnings({"unused", "resource"})
    static final class Offender {
        void threads() throws Exception {
            new Thread(() -> { }).start();
            Thread.ofPlatform().start(() -> { });
            new java.util.concurrent.ThreadPoolExecutor(1, 1, 1, java.util.concurrent.TimeUnit.SECONDS,
                    new java.util.concurrent.LinkedBlockingQueue<>());
            new java.util.concurrent.ScheduledThreadPoolExecutor(1);
            new java.util.concurrent.ForkJoinPool();
            new java.util.Timer();
            Runtime.getRuntime().addShutdownHook(null);
            java.util.concurrent.Executors.newSingleThreadExecutor();
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
                    .scheduleAtFixedRate(() -> { }, 1, 1, java.util.concurrent.TimeUnit.SECONDS);
            java.util.concurrent.Executors.newFixedThreadPool(2);
            java.util.concurrent.Executors.newScheduledThreadPool(1)
                    .scheduleWithFixedDelay(() -> { }, 1, 1, java.util.concurrent.TimeUnit.SECONDS);
            java.util.concurrent.Executors.newCachedThreadPool();
            java.util.concurrent.Executors.newWorkStealingPool();
            java.util.concurrent.Executors.defaultThreadFactory();
            java.util.concurrent.Executors.privilegedThreadFactory();
        }

        void jdkThreads(java.util.List<String> list) {
            java.util.concurrent.CompletableFuture<String> f = new java.util.concurrent.CompletableFuture<>();
            f.orTimeout(1, java.util.concurrent.TimeUnit.SECONDS);
            f.completeOnTimeout("late", 1, java.util.concurrent.TimeUnit.SECONDS);
            java.util.concurrent.CompletableFuture.delayedExecutor(1, java.util.concurrent.TimeUnit.SECONDS);
            java.util.concurrent.CompletableFuture.supplyAsync(() -> "common pool");
            f.thenApplyAsync(String::length);
            java.util.concurrent.ForkJoinPool.commonPool();
            list.parallelStream().count();
            list.stream().parallel().count();
        }

        java.util.concurrent.ThreadFactory factory() {
            return Thread::new;
        }

        long clocks() {
            return System.currentTimeMillis() + java.time.Instant.now().toEpochMilli();
        }

        Runnable inLambda() {
            return () -> System.out.println(System.nanoTime());
        }

        void nap() throws InterruptedException {
            Thread.sleep(1);
        }
    }

    /** What the policy allows. Never run. */
    @SuppressWarnings({"unused", "resource"})
    static final class Allowed {
        void fine(java.time.Clock clock, java.util.concurrent.ScheduledExecutorService s) {
            Thread.ofVirtual().start(() -> { });
            Thread.startVirtualThread(() -> { });
            java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
            java.time.Instant.now(clock);
            s.schedule(() -> { }, 1, java.util.concurrent.TimeUnit.SECONDS);
            java.util.concurrent.CompletableFuture.supplyAsync(() -> "virtual", Thread::startVirtualThread)
                    .thenApplyAsync(String::length, Thread::startVirtualThread);
            java.util.List.of("a").stream().count();
        }
    }

    /** The other reactor modules: reported, not failed, in this revision (EL-X-06). */
    @Test
    void theOtherReactorModulesAreReported() throws IOException {
        StringBuilder report = new StringBuilder("thread policy, reported (not failed) outside the core:\n");
        int total = 0;
        for (Map.Entry<String, List<Violation>> m : reported.entrySet()) {
            Map<String, Long> byRule = m.getValue().stream()
                    .collect(Collectors.groupingBy(Violation::rule, TreeMap::new, Collectors.counting()));
            report.append(String.format(Locale.ROOT, "  %-40s %s%n", m.getKey(), byRule.isEmpty() ? "none" : byRule));
            for (Violation v : m.getValue().stream().sorted(Comparator.comparing(Violation::line)).toList()) {
                report.append("      ").append(v.line()).append('\n');
            }
            total += m.getValue().size();
        }
        report.append("  ").append(total).append(" sites in ").append(reported.size()).append(" modules\n");
        System.out.print(report);
        Path out = Path.of(System.getProperty("user.dir"), "target", "thread-policy-report.txt");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report, StandardCharsets.UTF_8);
        assertThat(core).as("the core's own sites are read too").isNotEmpty();
    }

    // ---------------------------------------------------------------------------------------------------------------

    static List<Violation> violations(String module, JavaClass c) {
        List<Violation> out = new ArrayList<>();
        for (JavaCodeUnitAccess<?> access : c.getCodeUnitAccessesFromSelf()) {
            String call = forbidden(module, access.getTarget());
            if (call == null) {
                continue;
            }
            String location = c.getSource().flatMap(s -> s.getFileName()).orElse(c.getSimpleName()) + ":"
                    + access.getLineNumber();
            out.add(new Violation(module, ruleOf(call), access.getOriginOwner().getName(),
                    methodName(access.getOrigin().getName()), call, location));
        }
        return out;
    }

    /** The call's name in the allowlist's terms, or null when the policy allows it. */
    static String forbidden(String module, AccessTarget target) {
        JavaClass owner = target.getOwner();
        String type = owner.getName();
        String name = target.getName();
        boolean constructor = name.equals("<init>");
        if (constructor) {
            if (owner.isAssignableTo(Thread.class)) {
                return "new Thread";
            }
            if (owner.isAssignableTo(java.util.concurrent.ScheduledThreadPoolExecutor.class)) {
                return "new ScheduledThreadPoolExecutor";
            }
            if (owner.isAssignableTo(java.util.concurrent.ThreadPoolExecutor.class)) {
                return "new ThreadPoolExecutor";
            }
            if (owner.isAssignableTo(java.util.concurrent.ForkJoinPool.class)) {
                return "new ForkJoinPool";
            }
            if (owner.isAssignableTo(java.util.Timer.class)) {
                return "new Timer";
            }
            return null;
        }
        if (type.equals("java.lang.Thread") && name.equals("ofPlatform")) {
            return "Thread.ofPlatform";
        }
        if (type.equals("java.util.concurrent.Executors") && POOLED_FACTORIES.contains(name)) {
            return "Executors." + name;
        }
        if (type.equals("java.util.concurrent.CompletableFuture") && FUTURE_TIMERS.contains(name)) {
            return "CompletableFuture." + name;
        }
        if (owner.isAssignableTo(java.util.concurrent.CompletionStage.class) && name.endsWith("Async")
                && !lastParameterIsExecutor(target)) {
            return "CompletableFuture.async-on-common-pool"; // an async step with no executor runs on the common pool
        }
        if (type.equals("java.util.concurrent.ForkJoinPool") && name.equals("commonPool")) {
            return "ForkJoinPool.commonPool";
        }
        if ((name.equals("parallelStream") && owner.isAssignableTo(java.util.Collection.class))
                || (name.equals("parallel") && owner.isAssignableTo(java.util.stream.BaseStream.class))) {
            return "parallel-stream"; // the common pool's platform threads
        }
        if (type.equals("java.lang.Runtime") && name.equals("addShutdownHook")) {
            return "Runtime.addShutdownHook";
        }
        if (name.equals("scheduleAtFixedRate") || name.equals("scheduleWithFixedDelay")) {
            return name;
        }
        if (type.equals("java.lang.System") && name.equals("currentTimeMillis")) {
            return "System.currentTimeMillis";
        }
        if (type.equals("java.time.Instant") && name.equals("now") && parameters(target) == 0) {
            return "Instant.now";
        }
        if (type.equals("java.lang.System") && name.equals("nanoTime") && PROTOCOL.contains(module)) {
            return "System.nanoTime";
        }
        if (type.equals("java.lang.Thread") && name.equals("sleep")) {
            return "Thread.sleep";
        }
        return null;
    }

    static String ruleOf(String call) {
        if (call.startsWith("schedule")) {
            return "fixed-rate";
        }
        if (call.startsWith("System.") || call.equals("Instant.now")) {
            return "raw-clock";
        }
        if (call.equals("Thread.sleep")) {
            return "sleep";
        }
        return "thread";
    }

    private static boolean lastParameterIsExecutor(AccessTarget target) {
        if (!(target instanceof AccessTarget.CodeUnitAccessTarget unit) || unit.getRawParameterTypes().isEmpty()) {
            return false;
        }
        List<JavaClass> types = unit.getRawParameterTypes();
        return types.get(types.size() - 1).getName().equals("java.util.concurrent.Executor");
    }

    private static int parameters(AccessTarget target) {
        return target instanceof AccessTarget.CodeUnitAccessTarget unit ? unit.getRawParameterTypes().size() : -1;
    }

    /** A lambda's synthetic method is reported as the method that declares it ({@code lambda$run$3} as {@code run}). */
    static String methodName(String name) {
        Matcher m = LAMBDA.matcher(name);
        if (!m.matches()) {
            return name;
        }
        String declaring = m.group(1);
        return switch (declaring) {
            case "static" -> "<clinit>";
            case "new" -> "<init>";
            default -> declaring;
        };
    }

    /** The artifact a class was read from: a reactor module's classes directory or its jar. */
    static String moduleOf(JavaClass c) {
        Optional<URI> uri = c.getSource().map(s -> s.getUri());
        if (uri.isEmpty()) {
            return null;
        }
        String u = uri.get().toString();
        Matcher jar = JAR.matcher(u);
        if (jar.find()) {
            String file = jar.group(1);
            return file.substring(file.lastIndexOf('/') + 1);
        }
        Matcher dir = CLASSES_DIR.matcher(u);
        return dir.find() ? dir.group(1) : null;
    }

    static List<Entry> allowlist() throws IOException {
        try (InputStream in = ThreadPolicyTest.class.getClassLoader().getResourceAsStream(ALLOWLIST)) {
            assertThat(in).as("the allowlist " + ALLOWLIST + " on the test class path").isNotNull();
            List<String> lines = new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList();
            List<Entry> entries = new ArrayList<>();
            Map<String, Integer> seen = new LinkedHashMap<>();
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i).strip();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] f = line.split("\\s*\\|\\s*", 4);
                assertThat(f).as(ALLOWLIST + ":" + (i + 1) + " needs rule | site | calls | reason").hasSize(4);
                assertThat(f[0]).as(ALLOWLIST + ":" + (i + 1) + " rule").isIn("thread", "fixed-rate", "raw-clock", "sleep");
                String owner = f[1];
                String method = null;
                int hash = owner.indexOf('#');
                if (hash >= 0) {
                    method = owner.substring(hash + 1);
                    owner = owner.substring(0, hash);
                }
                Set<String> calls = Stream.of(f[2].split("\\s*,\\s*")).collect(Collectors.toCollection(TreeSet::new));
                for (String call : calls) {
                    assertThat(ruleOf(call)).as(ALLOWLIST + ":" + (i + 1) + " " + call + " falls under").isEqualTo(f[0]);
                }
                Integer before = seen.put(f[1] + "|" + f[0], i + 1);
                assertThat(before).as(ALLOWLIST + ":" + (i + 1) + " repeats the site of line " + before).isNull();
                entries.add(new Entry(i + 1, f[0], owner, method, calls, f[3]));
            }
            return entries;
        }
    }
}
