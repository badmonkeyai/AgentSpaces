#!/usr/bin/env bash
# The whole gate for the AgentSpaces workspace: the core reactor (with the
# real-Spring integration tests and the golden vectors required), then both
# non-JVM client suites, then every standalone project that consumes the
# published libraries — the four example apps, the Party Bus (with its Embabel
# profile), the Clojure bindings, agentspaces-springai, and a compile of the perf
# harnesses, agentspaces-langchain4j, agentspaces-micronaut, the bootstrap,
# beacon and social projects, celld, superfriends, the portal, the enterprise
# platform, Jungle Gym, the README quick starts, and the polyglot test (six
# languages in one group) — against the libraries the reactor just installed. Leaving
# the consumers out of the gate is how they rotted once (QA4 A4-2); keeping
# them out of the core build is how the core repository stays publishable.
#
# Usage: ./verify-all.sh            (from the workspace root; ~15 minutes)
#        PYTHON=/path/to/python ./verify-all.sh   (a Python with cryptography + pytest)
set -uo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-/tmp/aspace-venv/bin/python}"
# Ruby 3.3+ (macOS system Ruby 2.6 cannot run agentspaces-rb); Homebrew's by default.
RUBY="${RUBY:-/opt/homebrew/opt/ruby/bin/ruby}"
MVN="${MVN:-mvn}"
# Groovy 6 (the quick start and the polyglot test run scripts with @Grab); SDKMAN's
# 6.0.0 by default, because `groovy` on PATH may be another major version.
GROOVY="${GROOVY:-$HOME/.sdkman/candidates/groovy/6.0.0/bin/groovy}"
GROOVY_HOME="$(cd "$(dirname "$GROOVY")/.." && pwd)"
export GROOVY GROOVY_HOME
status=0
step() { echo; echo "== $*"; }

step "core reactor: clean install, spring-it, golden vectors required"
( cd agentspaces && $MVN -q clean install -Pspring-it -Dgolden.required=true ) || { echo "FAILED: core reactor"; status=1; }

step "golden vector copies match the source (agentspaces-spec/golden.json)"
for c in agentspaces/tools/golden/golden.json agentspaces-python/tests/golden.json agentspaces-typescript/test/golden.json agentspaces-rb/gems/agentspaces/test/golden.json; do
  cmp -s agentspaces-spec/golden.json "$c" || { echo "FAILED: $c differs from agentspaces-spec/golden.json (run agentspaces-spec/sync-golden.sh)"; status=1; }
done

step "python client"
( cd agentspaces-python && "$PYTHON" -m pytest tests/ -q ) || { echo "FAILED: agentspaces-python"; status=1; }

step "typescript client"
( cd agentspaces-typescript && npm test --silent ) || { echo "FAILED: agentspaces-typescript"; status=1; }

step "typescript packages (automerge, browser, connect, node, ordered, webrtc, workers, yjs)"
( cd agentspaces-typescript && npm test --silent --workspaces --if-present ) || { echo "FAILED: agentspaces-typescript packages"; status=1; }

step "cross-language council: Java coordinator, Python, TypeScript, and Ruby council workers (L3L4-COVERAGE.md §6.6)"
( cd agentspaces && PYTHON="$PYTHON" RUBY="$RUBY" $MVN -q -pl examples/example-02-research-fleet test -Dtest=CouncilCrossLanguageTest -Dcouncil.required=true -Dsurefire.failIfNoSpecifiedTests=false ) || { echo "FAILED: cross-language council"; status=1; }

step "clojure bindings"
( cd agentspaces-clj && clojure -M:test ) || { echo "FAILED: agentspaces-clj"; status=1; }

for d in agentspaces-example-apps/agentspaces-*-fleet/; do
  step "standalone: $d"
  ( cd "$d" && $MVN -q clean test ) || { echo "FAILED: $d"; status=1; }
done

# The embabel profile compiles the Embabel 1.5 application sources on
# Spring Boot 4.1, so the gate covers the real framework as well.
step "standalone: agentspaces-partybus (with -Pembabel)"
( cd agentspaces-partybus && $MVN -q -Pembabel clean test ) || { echo "FAILED: agentspaces-partybus"; status=1; }

# The wire records both model bridges share; installed before either consumer builds.
step "standalone: agentspaces-model-wire (golden vectors)"
( cd agentspaces-model-wire && $MVN -q clean install ) || { echo "FAILED: agentspaces-model-wire"; status=1; }

# The plain-Java runtime and the LangChain4j library the Micronaut bridge is wiring over;
# installed before the Micronaut step consumes them.
step "standalone: agentspaces-langchain4j (runtime, library, example 19)"
( cd agentspaces-langchain4j && $MVN -q clean install -Pexamples ) || { echo "FAILED: agentspaces-langchain4j"; status=1; }

step "standalone: agentspaces-groovy (library, concurrent bridge, example 21, host passthroughs)"
( cd agentspaces-groovy && $MVN -q verify -Pexamples,hosts ) || { echo "FAILED: agentspaces-groovy"; status=1; }

step "standalone: agentspaces-rb (wire peer, agent DSL, Rails, RubyLLM, and MCP gems)"
( cd agentspaces-rb && "$(dirname "$RUBY")/bundle" exec rake all ) || { echo "FAILED: agentspaces-rb"; status=1; }

step "standalone: agentspaces-springai (library, example 15, patterns)"
( cd agentspaces-springai && $MVN -q clean install -Pexamples ) || { echo "FAILED: agentspaces-springai"; status=1; }

step "standalone: agentspaces-micronaut (library, processor, LangChain4j, MCP, examples 16 and 17)"
( cd agentspaces-micronaut && $MVN -q clean install -Pexamples ) || { echo "FAILED: agentspaces-micronaut"; status=1; }

step "standalone: agentspaces-perf (compile)"
( cd agentspaces-perf && $MVN -q clean compile ) || { echo "FAILED: agentspaces-perf"; status=1; }

# Discovery: the bootstrap chain and its hosts (their tests use agentspaces-micronaut,
# installed above), the DNS resolver, the beacon, then the two social projects, which
# depend on each other by module (release-pipeline.md has the order).
step "standalone: agentspaces-bootstrap (library, chain, Spring Boot and Micronaut hosts)"
( cd agentspaces-bootstrap && $MVN -q clean install ) || { echo "FAILED: agentspaces-bootstrap"; status=1; }
step "standalone: agentspaces-bootstrap-dns"
( cd agentspaces-bootstrap-dns && $MVN -q clean install ) || { echo "FAILED: agentspaces-bootstrap-dns"; status=1; }
step "standalone: agentspaces-beacon"
( cd agentspaces-beacon && $MVN -q clean install ) || { echo "FAILED: agentspaces-beacon"; status=1; }
step "standalone: agentspaces-bootstrap-social and agentspaces-beacon-social"
( cd agentspaces-bootstrap-social && $MVN -q -pl social clean install -DskipTests \
  && cd ../agentspaces-beacon-social && $MVN -q clean install \
  && cd ../agentspaces-bootstrap-social && $MVN -q clean install ) || { echo "FAILED: agentspaces social projects"; status=1; }
step "standalone: agentspaces-connect-celld"
( cd agentspaces-connect-celld && $MVN -q clean install ) || { echo "FAILED: agentspaces-connect-celld"; status=1; }

step "standalone: superfriends (six runtimes, the fleet test)"
( cd agentspaces-example-apps/superfriends && $MVN -q clean install ) || { echo "FAILED: superfriends"; status=1; }

step "standalone: agentspaces-beacon-portal and agentspaces-portal (Clojure)"
( cd agentspaces-beacon-portal && clojure -M:test && clojure -T:build install \
  && cd ../agentspaces-portal && clojure -M:test ) || { echo "FAILED: agentspaces portal projects"; status=1; }

step "standalone: enterprise-platform (spiral 1)"
( cd enterprise-platform && $MVN -q clean install ) || { echo "FAILED: enterprise-platform"; status=1; }

step "agentspaces-junglegym: every program run for real, the page rebuilt and self-tested"
( cd agentspaces-junglegym && ./verify.sh ) || { echo "FAILED: agentspaces-junglegym"; status=1; }

step "README quick starts: two instances per host, as each README tells a reader"
( cd quickstarts && ./verify.sh ) || { echo "FAILED: quickstarts"; status=1; }

# Last, because it races seven runtimes on loopback and wants a quiet machine.
step "polyglot test: Java, Python, TypeScript, Ruby, Clojure, Groovy, LangChain4j in one group, then the council"
PYTHON="$PYTHON" RUBY="$RUBY" ./run-polyglot-test.sh || { echo "FAILED: polyglot test"; status=1; }

echo
if [ "$status" -eq 0 ]; then echo "ALL GREEN"; else echo "FAILURES ABOVE"; fi
exit $status
