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

import ai.badmonkey.agentspaces.common.time.Durations;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The TTL register and its test (srad/agentspaces-event-loop.md §3.11, EL-E-01, EL-E-02, EL-E-09): every line of
 * {@code ttl-register.txt} is read by reflection, and the test fails on a value that differs from the code, on a
 * duration under 60 s with no reason, on a line that matches no code, and on a lease or TTL member of the binder's
 * annotations that the register does not list. It records {@code core.ttl.under-floor}: the registered durations under
 * 60 s with no reason, whose goal is 0.
 */
class TtlRegisterTest {

    /** Principle 8: a TTL, lease or expiry under this is presumed too fast unless the register names why. */
    static final Duration FLOOR = Duration.ofSeconds(60);
    private static final String ANNOTATIONS = "ai.badmonkey.agentspaces.agent.annotation";
    private static final Pattern LEASE_OR_TTL = Pattern.compile("(?i).*(lease|ttl).*");

    /** One register line. */
    record Entry(int line, String className, String member, String value, String reason) {
        String key() {
            return className + "#" + member;
        }
    }

    /** A value read from the code: a duration, or a count. */
    record Read(Duration duration, Long count) {
    }

    static List<Entry> register() throws IOException {
        List<Entry> out = new ArrayList<>();
        try (InputStream in = TtlRegisterTest.class.getResourceAsStream("/ttl-register.txt")) {
            assertThat(in).as("ttl-register.txt on the test classpath").isNotNull();
            String[] lines = new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n");
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i].strip();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\\s+", 3);
                assertThat(parts.length).as("line %d has a key and a value: %s", i + 1, line).isGreaterThanOrEqualTo(2);
                int hash = parts[0].indexOf('#');
                assertThat(hash).as("line %d names <class>#<member>: %s", i + 1, line).isPositive();
                out.add(new Entry(i + 1, parts[0].substring(0, hash), parts[0].substring(hash + 1), parts[1],
                        parts.length == 3 ? parts[2].strip() : ""));
            }
        }
        return out;
    }

    /** Reads an entry's value from the code. */
    static Read read(Entry e) throws ReflectiveOperationException {
        Class<?> type = Class.forName(e.className());
        String member = e.member();
        Object value;
        String name;
        if (member.startsWith("get")) { // a getter chain on a new instance
            Object at = type.getDeclaredConstructor().newInstance();
            name = member;
            for (String getter : member.split("\\.")) {
                String method = getter.substring(0, getter.indexOf('('));
                at = at.getClass().getMethod(method).invoke(at);
                name = method;
            }
            value = at;
        } else if (member.endsWith("()")) { // an annotation member's default
            name = member.substring(0, member.length() - 2);
            Method method = type.getDeclaredMethod(name);
            value = method.getDefaultValue();
        } else { // a static field
            Field field = type.getDeclaredField(member);
            assertThat(Modifier.isStatic(field.getModifiers())).as("%s is static", e.key()).isTrue();
            field.setAccessible(true);
            name = member;
            value = field.get(null);
        }
        return toRead(name, value, e);
    }

    private static Read toRead(String name, Object value, Entry e) {
        if (value instanceof Duration d) {
            return new Read(d, null);
        }
        if (value instanceof String s) {
            return new Read(Durations.parse(s, false), null);
        }
        if (value instanceof Long || value instanceof Integer) {
            long n = ((Number) value).longValue();
            return name.toUpperCase(Locale.ROOT).endsWith("MILLIS") ? new Read(Duration.ofMillis(n), null)
                    : new Read(null, n);
        }
        throw new AssertionError(e.key() + " is a " + (value == null ? "null" : value.getClass().getName())
                + ", not a duration, a duration string or a count");
    }

    static Read parse(Entry e) {
        if (e.value().matches("-?\\d+")) {
            return new Read(null, Long.parseLong(e.value()));
        }
        return new Read(Durations.parse(e.value(), false), null);
    }

    @Test
    void everyRegisteredDefaultMatchesTheCodeAndEveryShortOneSaysWhy() throws Exception {
        List<Entry> entries = register();
        List<String> faults = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        long underFloorUnexplained = 0;
        int under = 0;
        for (Entry e : entries) {
            if (!keys.add(e.key())) {
                faults.add("line " + e.line() + ": " + e.key() + " is registered twice");
            }
            Read registered;
            try {
                registered = parse(e);
            } catch (RuntimeException bad) {
                faults.add("line " + e.line() + ": " + e.value() + " is not a duration or a count");
                continue;
            }
            Read code;
            try {
                code = read(e);
            } catch (ReflectiveOperationException | RuntimeException missing) {
                faults.add("line " + e.line() + ": " + e.key() + " matches no code (" + missing + ")");
                continue;
            }
            if (!registered.equals(code)) {
                faults.add("line " + e.line() + ": " + e.key() + " is " + (code.duration() != null ? code.duration()
                        : code.count()) + " in the code, " + e.value() + " in the register");
            }
            if (code.duration() != null && code.duration().compareTo(FLOOR) < 0) {
                under++;
                if (e.reason().isEmpty()) {
                    underFloorUnexplained++;
                    faults.add("line " + e.line() + ": " + e.key() + " is " + code.duration()
                            + ", under 60 s, and the register gives no reason (principle 8)");
                }
            }
        }
        Measure.record("core.ttl.under-floor", Unit.COUNT, Direction.LOWER, underFloorUnexplained,
                Rec.with().kind(Kind.COUNT));
        System.out.printf("[ttl-register] %d entries, %d under 60 s, %d of them without a reason%n", entries.size(),
                under, underFloorUnexplained);
        assertThat(faults).as("the register against the code").isEmpty();
        assertThat(entries).as("the register is not empty").hasSizeGreaterThan(40);
    }

    @Test
    void everyLeaseOrTtlMemberOfTheBindersAnnotationsIsRegistered() throws Exception {
        Set<String> registered = new HashSet<>();
        for (Entry e : register()) {
            registered.add(e.key());
        }
        List<String> missing = new ArrayList<>();
        int seen = 0;
        for (Class<?> type : annotationTypes()) {
            for (Method m : type.getDeclaredMethods()) {
                Object def = m.getDefaultValue();
                if (!LEASE_OR_TTL.matcher(m.getName()).matches() || !(def instanceof String s) || s.isBlank()) {
                    continue;
                }
                seen++;
                if (!registered.contains(type.getName() + "#" + m.getName() + "()")) {
                    missing.add(type.getName() + "#" + m.getName() + "() default \"" + s + "\"");
                }
            }
        }
        assertThat(seen).as("lease and TTL members found in the binder's annotations").isGreaterThan(10);
        assertThat(missing).as("binder annotation leases and TTLs missing from ttl-register.txt").isEmpty();
    }

    /** The annotation types of the binder's annotation package, found from the jar or directory one of them is in. */
    private static List<Class<?>> annotationTypes() throws IOException, ClassNotFoundException, URISyntaxException {
        Class<?> anchor = Class.forName(ANNOTATIONS + ".OrderedTake");
        URL location = anchor.getProtectionDomain().getCodeSource().getLocation();
        Path root = Path.of(location.toURI());
        List<String> names = new ArrayList<>();
        String dir = ANNOTATIONS.replace('.', '/');
        if (Files.isDirectory(root)) {
            try (Stream<Path> files = Files.list(root.resolve(dir))) {
                files.map(p -> p.getFileName().toString()).forEach(names::add);
            }
        } else {
            try (java.util.jar.JarFile jar = new java.util.jar.JarFile(root.toFile())) {
                jar.stream().map(java.util.jar.JarEntry::getName)
                        .filter(n -> n.startsWith(dir + "/") && n.indexOf('/', dir.length() + 1) < 0)
                        .map(n -> n.substring(dir.length() + 1)).forEach(names::add);
            }
        }
        List<Class<?>> out = new ArrayList<>();
        for (String file : names) {
            if (!file.endsWith(".class") || file.contains("$")) {
                continue;
            }
            Class<?> type = Class.forName(ANNOTATIONS + "." + file.substring(0, file.length() - ".class".length()));
            if (type.isAnnotation() && type != Annotation.class) {
                out.add(type);
            }
        }
        return out;
    }
}
