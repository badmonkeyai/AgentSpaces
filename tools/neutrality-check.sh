#!/usr/bin/env bash
# The neutrality guard of the platform-names review (ISSUE-AppCardOriginFormats §17 step 6): golden.json, the api,
# a2ui, and commerce pins, the specification, and the main sources of the core, agentspaces-a2ui, and
# agentspaces-commerce name no part of the platform that builds on AgentSpaces. It needs only a JDK, prints each
# platform name as file:line:column, and exits 1 when it finds one.
# The core gate runs the same sweep as NeutralityGuardTest in agentspaces-api.
#
# Usage: agentspaces/tools/neutrality-check.sh   (from anywhere; it reads the core and the directories beside it)
set -euo pipefail
core="$(cd "$(dirname "$0")/.." && pwd)"
exec java "$core/agentspaces-api/src/test/java/ai/badmonkey/agentspaces/api/neutrality/NeutralityGuard.java" "$core"
