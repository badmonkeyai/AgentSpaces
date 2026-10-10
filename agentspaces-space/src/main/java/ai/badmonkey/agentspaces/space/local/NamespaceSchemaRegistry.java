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

import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A delegating schema registry that names the classes of chosen packages by an
 * IRI namespace (ISSUE-WorkflowShape §9.1): a class whose package equals or lies
 * under a mapped package is named {@code <namespace><SimpleName>}, optionally
 * followed by a {@linkplain #withVersionFragment(String) version fragment};
 * every other class goes through a {@link SimpleSchemaRegistry}, so the
 * reserved types ({@code SpaceCredential}, the vote and ordered-log records)
 * keep the {@code <fqcn>#v1} names every other peer and both clients use.
 *
 * <pre>{@code
 * SchemaRegistry schemas = NamespaceSchemaRegistry.of(
 *         Map.of("org.example.claims", "https://example.org/claims#"));
 * schemas.register(org.example.claims.Claim.class); // "https://example.org/claims#Claim"
 * }</pre>
 *
 * <p>When several mapped packages contain a class, the longest wins. A nested
 * class is named by its own simple name ({@code Outer.Claim} is {@code
 * <namespace>Claim}), so two nested classes of one simple name in one mapped
 * package collide; the second registration is refused rather than silently
 * renaming either. Anonymous and local classes have no simple name and are
 * refused.
 *
 * <p>{@link #classFor} resolves names of both forms. An IRI under a mapped
 * namespace that no registration here produced is also tried as a class of the
 * mapped package itself (never a subpackage), loaded without initialization, so
 * a peer resolving a foreign card's IRI needs the class on its classpath, not a
 * prior registration. Share one instance between the spaces and the binders of
 * a peer: the space names what it writes and the binder names what its cards
 * advertise, and only one registry keeps the two from disagreeing.
 */
public final class NamespaceSchemaRegistry implements SchemaRegistry {

    /** Mapped packages, longest first, so the most specific package wins. */
    private final List<Map.Entry<String, String>> namespaces;
    private final String versionFragment;
    private final SimpleSchemaRegistry delegate = new SimpleSchemaRegistry();
    private final Map<String, Class<?>> byName = new ConcurrentHashMap<>();

    private NamespaceSchemaRegistry(Map<String, String> packageToNamespace,
                                    String versionFragment) {
        List<Map.Entry<String, String>> ordered = new ArrayList<>();
        for (Map.Entry<String, String> mapping : packageToNamespace.entrySet()) {
            String pkg = mapping.getKey();
            String namespace = mapping.getValue();
            if (pkg == null || pkg.isBlank() || pkg.startsWith(".") || pkg.endsWith(".")) {
                throw new IllegalArgumentException("not a package name: '" + pkg + "'");
            }
            if (namespace == null || namespace.isBlank()) {
                throw new IllegalArgumentException(
                        "package '" + pkg + "' maps to a blank namespace");
            }
            ordered.add(Map.entry(pkg, namespace));
        }
        ordered.sort(Comparator.comparingInt((Map.Entry<String, String> e) -> e.getKey().length())
                .reversed().thenComparing(Map.Entry::getKey));
        this.namespaces = List.copyOf(ordered);
        this.versionFragment = versionFragment;
    }

    /**
     * Creates a registry naming the classes of the given packages (and their
     * subpackages) by the given namespaces, with no version fragment.
     *
     * @param packageToNamespace package name to namespace prefix, such as
     *                           {@code "org.example.claims"} to
     *                           {@code "https://example.org/claims#"}
     * @return the registry
     * @throws IllegalArgumentException when the map is empty, a package name is
     *                                  blank, or a namespace is blank
     */
    public static NamespaceSchemaRegistry of(Map<String, String> packageToNamespace) {
        Objects.requireNonNull(packageToNamespace, "packageToNamespace");
        if (packageToNamespace.isEmpty()) {
            throw new IllegalArgumentException("at least one package must be mapped");
        }
        return new NamespaceSchemaRegistry(new LinkedHashMap<>(packageToNamespace), "");
    }

    /**
     * Returns a registry with the same mappings that appends the given fragment
     * verbatim to every namespaced name ({@code <namespace><SimpleName>#v1} for
     * {@code "#v1"}). Registrations made on this registry are not carried over;
     * call it before the first use.
     *
     * @param fragment the fragment, such as {@code "#v1"}
     * @return the versioned registry
     * @throws IllegalArgumentException when the fragment is blank
     */
    public NamespaceSchemaRegistry withVersionFragment(String fragment) {
        Objects.requireNonNull(fragment, "fragment");
        if (fragment.isBlank()) {
            throw new IllegalArgumentException("the version fragment must not be blank");
        }
        Map<String, String> mappings = new LinkedHashMap<>();
        namespaces.forEach(e -> mappings.put(e.getKey(), e.getValue()));
        return new NamespaceSchemaRegistry(mappings, fragment);
    }

    @Override
    public String register(Class<?> entryType) {
        Objects.requireNonNull(entryType, "entryType");
        Optional<String> namespaced = namespacedName(entryType);
        if (namespaced.isEmpty()) {
            return delegate.register(entryType);
        }
        String name = namespaced.get();
        Class<?> prior = byName.putIfAbsent(name, entryType);
        if (prior != null && prior != entryType) {
            throw new IllegalArgumentException("schema name " + name + " already names "
                    + prior.getName() + "; cannot also name " + entryType.getName());
        }
        return name;
    }

    @Override
    public String schemaNameOf(Class<?> entryType) {
        Objects.requireNonNull(entryType, "entryType");
        Optional<String> namespaced = namespacedName(entryType);
        if (namespaced.isEmpty()) {
            return delegate.schemaNameOf(entryType);
        }
        String name = namespaced.get();
        if (byName.get(name) != entryType) {
            throw new IllegalArgumentException("unregistered entry type: " + entryType.getName());
        }
        return name;
    }

    @Override
    public Optional<Class<?>> classFor(String schemaName) {
        Objects.requireNonNull(schemaName, "schemaName");
        Class<?> registered = byName.get(schemaName);
        if (registered != null) {
            return Optional.of(registered);
        }
        Optional<Class<?>> delegated = delegate.classFor(schemaName);
        if (delegated.isPresent()) {
            return delegated;
        }
        return loadFromMappedPackage(schemaName);
    }

    /** The namespaced name of a class in a mapped package; empty when unmapped. */
    private Optional<String> namespacedName(Class<?> type) {
        String pkg = type.getPackageName();
        if (pkg.isEmpty()) {
            return Optional.empty();
        }
        for (Map.Entry<String, String> mapping : namespaces) {
            String mapped = mapping.getKey();
            if (pkg.equals(mapped) || pkg.startsWith(mapped + ".")) {
                String simple = type.getSimpleName();
                if (simple.isEmpty()) {
                    throw new IllegalArgumentException(
                            "anonymous and local classes have no schema name: " + type.getName());
                }
                return Optional.of(mapping.getValue() + simple + versionFragment);
            }
        }
        return Optional.empty();
    }

    /**
     * Tries an unregistered namespaced name as a class of the mapped package
     * itself. Only a plain Java identifier is tried, the class is never
     * initialized, and only the operator's mapped packages are searched.
     */
    private Optional<Class<?>> loadFromMappedPackage(String schemaName) {
        if (!schemaName.endsWith(versionFragment)) {
            return Optional.empty();
        }
        String unversioned =
                schemaName.substring(0, schemaName.length() - versionFragment.length());
        for (Map.Entry<String, String> mapping : namespaces) {
            String namespace = mapping.getValue();
            if (!unversioned.startsWith(namespace)) {
                continue;
            }
            String simple = unversioned.substring(namespace.length());
            if (!isIdentifier(simple)) {
                continue;
            }
            try {
                Class<?> type = Class.forName(mapping.getKey() + "." + simple, false,
                        NamespaceSchemaRegistry.class.getClassLoader());
                return Optional.of(type);
            } catch (ClassNotFoundException | LinkageError e) {
                // Not here; another mapping may still match.
            }
        }
        return Optional.empty();
    }

    private static boolean isIdentifier(String s) {
        if (s.isEmpty() || !Character.isJavaIdentifierStart(s.charAt(0))) {
            return false;
        }
        for (int i = 1; i < s.length(); i++) {
            if (!Character.isJavaIdentifierPart(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
