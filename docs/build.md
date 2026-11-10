# Build

Written for: contributors who build and test the core.

JDK 21+ and Maven are the only prerequisites. `BUILD.md` at the workspace root is the full runbook.

Build and test everything (the release gate, roughly 3 minutes):

```
mvn -T 1C -Pspring-it clean verify -Dgolden.required=true
```

- `-Pspring-it` adds the real Spring Boot integration test, the starter's release gate. CI activates it.
- `-Dgolden.required=true` makes the cross-language golden-vector tests fail instead of silently skipping when golden.json is missing.
- Always `clean`. An IDE compiling alongside Maven can leave error-stub classes in `target/`.
- The QUIC module's pom picks the right native classifier per OS (Linux and macOS); elsewhere its tests skip.
- In CI (and with `-Pcoverage`) JaCoCo enforces per-module line-coverage gates.
- Use `install` instead of `verify` if other projects need the snapshots in `~/.m2`.

Faster loops:

```
# one module (install its dependencies once first)
mvn -q -pl agentspaces-space -am install -DskipTests
mvn -pl agentspaces-space test
```

```
# one test class or method
mvn -pl agentspaces-peering test -Dtest='RevocationTest#aFounderRevocationEjectsFleetWideAndSurvivesForLateJoiners'
```

If you select tests across several modules, add `-Dsurefire.failIfNoSpecifiedTests=false`. Otherwise any module with no matching test fails the build.

Security regression suites (46 tests), worth running after touching an enforcement path:

```
mvn -pl agentspaces-space,agentspaces-peering,agentspaces-capabilities,agentspaces-console test \
  -Dtest='ForgedStateRejectionTest,NonMemberIsolationTest,AdversarialVectorsTest,RevocationTest,ProbeForgeryTest,GossipBusTest,SpaceAdmissionTest,DirectiveGateTest' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Run the first example:

```
mvn -q -pl examples/example-01-hello-space exec:java
```

The full workspace gate (about 15 minutes) is `verify-all.sh`, run from the workspace root where all repos have been checked out. It runs this reactor's gate and then every standalone project against the freshly installed libraries, which is how they are kept from drifting (QA4 A4-2) without being part of the core build:

- A check that the golden-vector copies match.
- The Python, TypeScript, Clojure, and Groovy clients.
- The example apps and the Party Bus.
- agentspaces-springai.
- A compile of the perf harnesses.

`docs/BUILD-ENVIRONMENTS.md` covers the version policy, the Spring stub architecture, and regenerating the shared golden vectors.

Build success should look like:

```
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for AgentSpaces 0.6.0:
[INFO]
[INFO] AgentSpaces :: Dependencies (BOM) .................. SUCCESS [  0.044 s]
[INFO] AgentSpaces ........................................ SUCCESS [  0.044 s]
[INFO] AgentSpaces :: Common .............................. SUCCESS [  1.393 s]
[INFO] AgentSpaces :: API ................................. SUCCESS [  0.539 s]
[INFO] AgentSpaces :: Test Support ........................ SUCCESS [  0.485 s]
[INFO] AgentSpaces :: Identity (Layer 0) .................. SUCCESS [  0.784 s]
[INFO] AgentSpaces :: OIDC Authorization .................. SUCCESS [  0.884 s]
[INFO] AgentSpaces :: Peering (Layer 1) ................... SUCCESS [  6.419 s]
[INFO] AgentSpaces :: Space (Layer 3) ..................... SUCCESS [ 12.392 s]
[INFO] AgentSpaces :: Discovery (Layer 2) ................. SUCCESS [  6.189 s]
[INFO] AgentSpaces :: Capabilities (Layer 4) .............. SUCCESS [01:15 min]
[INFO] AgentSpaces :: Agent Binding ....................... SUCCESS [ 30.222 s]
[INFO] AgentSpaces :: A2A Gateway ......................... SUCCESS [  2.752 s]
[INFO] AgentSpaces :: Connector SDK ....................... SUCCESS [  4.493 s]
[INFO] AgentSpaces :: Fleet Console ....................... SUCCESS [  2.658 s]
[INFO] AgentSpaces :: Spring API Stubs (build-time only) .. SUCCESS [  0.551 s]
[INFO] AgentSpaces :: Spring Boot Autoconfigure ........... SUCCESS [  8.695 s]
[INFO] AgentSpaces :: Spring Boot Starter ................. SUCCESS [  0.010 s]
[INFO] AgentSpaces :: Embabel Extension ................... SUCCESS [  6.826 s]
[INFO] AgentSpaces :: Examples ............................ SUCCESS [  0.002 s]
[INFO] AgentSpaces :: Example 01 :: Hello Space ........... SUCCESS [  0.677 s]
[INFO] AgentSpaces :: Example 02 :: Research Fleet ........ SUCCESS [  5.715 s]
[INFO] AgentSpaces :: Example 03 :: CardsFleet ............ SUCCESS [  2.539 s]
[INFO] AgentSpaces :: Example 04 :: AuctionFleet .......... SUCCESS [  4.957 s]
[INFO] AgentSpaces :: Example 05 :: QuorumFleet ........... SUCCESS [ 11.454 s]
[INFO] AgentSpaces :: Example 06 :: WanFleet .............. SUCCESS [  2.140 s]
[INFO] AgentSpaces :: Example 07 :: A2aFleet .............. SUCCESS [  2.055 s]
[INFO] AgentSpaces :: Example 08 :: IntakeFleet ........... SUCCESS [  4.196 s]
[INFO] AgentSpaces :: Example 09 :: FleetConsole .......... SUCCESS [  9.807 s]
[INFO] AgentSpaces :: Example 10 :: DataFleet ............. SUCCESS [  3.481 s]
[INFO] AgentSpaces :: Example 11 :: Quickstart ............ SUCCESS [  1.871 s]
[INFO] AgentSpaces :: Example 12 :: ExactlyOnceDesk ....... SUCCESS [  4.520 s]
[INFO] AgentSpaces :: Example 13 :: SignedAgents .......... SUCCESS [  7.717 s]
[INFO] AgentSpaces :: Example 14 :: GossipLearning ........ SUCCESS [  4.541 s]
[INFO] AgentSpaces :: Spring Boot Integration Test ........ SUCCESS [  2.724 s]
[INFO] AgentSpaces :: Embabel Integration Test ............ SUCCESS [  2.589 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
```
