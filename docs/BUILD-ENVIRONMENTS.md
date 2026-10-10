# Build Environments

The reference for the Java build itself. `BUILD.md` at the workspace root is the
companion runbook: the commands for all four suites, the environment gaps, and
the test-writing notes.

AgentSpaces is a standard Maven project: every dependency and plugin resolves
from Maven Central, versions are managed once in the parent POM (and mirrored
in the `agentspaces-dependencies` BOM for applications), and

```
mvn verify
```

builds and tests everything on JDK 21+. CI (GitHub Actions) activates the
`coverage` profile automatically via the `CI` environment variable, running
JaCoCo with the per-module line-coverage gates, and the `spring-it` profile
that boots the real-Spring integration test.

## Version policy

Third-party versions live as properties in the parent POM and are kept
current; Dependabot proposes bumps weekly and the OSV-Scanner workflow flags
known-vulnerable versions on every push (see `.github/`). The Jackson
artifacts are aligned on a single `jackson.version` — the CBOR dataformat
parses every wire byte, so it is a security-sensitive dependency
(security-findings ASF-018) and must never trail the rest of the Jackson tree.
A Jackson bump can change the canonical CBOR bytes the codec emits; the golden
vector suites (Java, Python, TypeScript) are the compatibility gate, so any
bump must regenerate `golden.json` via `tools/golden` and leave all three
suites green before it merges.

### Regenerating the golden vectors

`tools/golden/GoldenVectors.java` is a single-file program outside the Maven
reactor; it runs against the module classes plus the capabilities module's
runtime dependencies (Jackson, BouncyCastle, and the sibling snapshots), which
Maven assembles. From the repository root, after a `mvn -q install -DskipTests` so the sibling snapshots are in `~/.m2`
and `target/classes` exists:

```
mvn -q -pl agentspaces-capabilities dependency:build-classpath \
    -Dmdep.outputFile=/tmp/golden-cp.txt
CP="agentspaces-capabilities/target/classes:$(cat /tmp/golden-cp.txt)"
javac -cp "$CP" -d /tmp/golden tools/golden/GoldenVectors.java
java -cp "/tmp/golden:$CP" GoldenVectors ../agentspaces-spec/golden.json
../agentspaces-spec/sync-golden.sh          # refresh the vendored copies in every consumer
```

The argument is the output path; the clients tree sits beside this repository
at the workspace root (`agentspaces-spec/`, the specification and conformance kit), and the vendored copies (`tools/golden/golden.json` here, and in the Python and TypeScript clients) must
stay byte-identical to it (both Java conformance tests look in both places).
When the output file already exists the generator reloads the golden private
key (and the adversarial forger's key and the subordinate agent key) from it, so signatures stay stable and
only vectors whose inputs changed differ; it generates a fresh key only when no
file exists. The generator also checks its mirrors of the private wire records
(`ReplicatedSpace.SignView`, `PushSumAggregate.Frame`, `GossipLearner.Exchange`,
...) against the real records by reflection and refuses to run on drift. One
mirror, `JoinTicket` (the issue #16 reserved type, real class in
`agentspaces-agent`), stands in for a module outside this classpath: the
generator checks it only when the class loads and prints a note otherwise, so
appending `agentspaces-agent/target/classes` to `CP` turns that check on. Then
run `mvn -q -pl agentspaces-space test -Dgolden.required=true` and the Python
and TypeScript suites.

## The Spring and Embabel modules

The Spring Boot autoconfigure module compiles against
`agentspaces-spring-stubs`, a provided-scope jar carrying the exact Spring
Framework and Spring Boot fully qualified names the module touches
(annotations, `SmartLifecycle`, `BeanPostProcessor`). Compiled bytecode
references these types by name, so when an application runs with real Spring
Boot on the classpath, the real classes resolve and the stubs are never
present: the stub jar is provided scope, ships no further, and the built
autoconfigure jar contains no Spring classes at all. The Embabel extension
goes one step further and reads Embabel annotations reflectively by name, so
it needs no Embabel artifact even at compile time and activates only when
`com.embabel.agent.api.annotation.Agent` is loadable.

Two rules keep this safe. First, the stub surface stays minimal and mirrors
the published Spring API signatures exactly; any drift surfaces as a
`NoSuchMethodError` in the integration test, never silently. Second, a
Spring-context integration test (a real `SpringApplication` booting the
starter against real Spring Boot artifacts) MUST run before any release; CI
runs it via the `spring-it` profile. The in-repo unit tests cover every line
of our own wiring by invoking the bean methods, lifecycle, and post processors
directly; what they cannot cover is Spring itself honoring the annotations,
and that is precisely the integration test's job.

## The perf module (JMH)

`agentspaces-perf` (benchmarks and stress harnesses) is a standalone project in
its own repository, [badmonkeyai/agentspaces-perf](https://github.com/badmonkeyai/agentspaces-perf),
so the default reactor stays fast. It has no parent: it consumes the published
libraries through the `agentspaces-dependencies` BOM, so after a `mvn install`
here, `mvn -f ../agentspaces-perf/pom.xml clean package` builds and smoke-tests
it and writes `target/benchmarks.jar`. Its README documents the run commands.

## The QUIC module

`agentspaces-transport-quic` tests need Netty's QUIC native library. Netty
publishes it for Linux (x86_64, aarch64) and macOS (ARM, Intel); OS-activated
profiles in the module's pom select the macOS classifiers, and
`-Dquic.native.classifier=...` overrides for anything else. On a platform with
no published library the tests skip with the native library's own reason
(`Quic.unavailabilityCause()`), so the module never needs excluding.

## The non-JVM clients

The Python and TypeScript clients live in the `clients/` tree alongside this
repository and are not Maven modules. Their suites — including the shared
adversarial conformance vectors in `golden.json` — run with
`python -m pytest tests/` (needs `cryptography` and `pytest`) and `npm test`
respectively, and must stay green whenever the wire format or the golden
vectors change.
