#!/usr/bin/env bash
# The whole gate for the AgentSpaces workspace: the core reactor (with the
# real-Spring integration tests and the golden vectors required), then both
# non-JVM client suites, then every standalone project that consumes the
# published libraries — the four example apps, the Party Bus (with its Embabel
# profile), the Clojure bindings, agentspaces-springai, and a compile of the perf
# harnesses — against the libraries the reactor just installed. Leaving
# the consumers out of the gate is how they rotted once (QA4 A4-2); keeping
# them out of the core build is how the core repository stays publishable.
#
# Usage: ./verify-all.sh            (from the workspace root; ~15 minutes)
#        PYTHON=/path/to/python ./verify-all.sh   (a Python with cryptography + pytest)
set -uo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-/tmp/aspace-venv/bin/python}"
MVN="${MVN:-mvn}"
status=0
step() { echo; echo "== $*"; }

step "core reactor: clean install, spring-it, golden vectors required"
( cd agentspaces && $MVN -q clean install -Pspring-it -Dgolden.required=true ) || { echo "FAILED: core reactor"; status=1; }

step "golden vector copies match the source (agentspaces-spec/golden.json)"
for c in agentspaces/tools/golden/golden.json agentspaces-python/tests/golden.json agentspaces-typescript/test/golden.json; do
  cmp -s agentspaces-spec/golden.json "$c" || { echo "FAILED: $c differs from agentspaces-spec/golden.json (run agentspaces-spec/sync-golden.sh)"; status=1; }
done

step "python client"
( cd agentspaces-python && "$PYTHON" -m pytest tests/ -q ) || { echo "FAILED: agentspaces-python"; status=1; }

step "typescript client"
( cd agentspaces-typescript && npm test --silent ) || { echo "FAILED: agentspaces-typescript"; status=1; }

step "clojure bindings"
( cd agentspaces-clj && clojure -M:test ) || { echo "FAILED: agentspaces-clj"; status=1; }

for d in agentspaces-example-apps/*/; do
  step "standalone: $d"
  ( cd "$d" && $MVN -q clean test ) || { echo "FAILED: $d"; status=1; }
done

# The embabel profile compiles the Embabel 1.5 application sources on
# Spring Boot 4.1, so the gate covers the real framework as well.
step "standalone: agentspaces-partybus (with -Pembabel)"
( cd agentspaces-partybus && $MVN -q -Pembabel clean test ) || { echo "FAILED: agentspaces-partybus"; status=1; }

step "standalone: agentspaces-springai (library, example 15, patterns)"
( cd agentspaces-springai && $MVN -q clean install -Pexamples ) || { echo "FAILED: agentspaces-springai"; status=1; }

step "standalone: agentspaces-perf (compile)"
( cd agentspaces-perf && $MVN -q clean compile ) || { echo "FAILED: agentspaces-perf"; status=1; }

echo
if [ "$status" -eq 0 ]; then echo "ALL GREEN"; else echo "FAILURES ABOVE"; fi
exit $status
