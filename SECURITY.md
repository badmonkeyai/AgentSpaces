# Security Policy

AgentSpaces is security-bearing infrastructure: peers authenticate each other
with Ed25519 signatures over self-certifying identities, and the threat model
is specified in `agentspaces-spec/SPEC.md` §11 and the signature inventory in
`agentspaces-spec/TECH-SPEC.md` §4. We take reports against that model seriously.

## Reporting a vulnerability

Please report vulnerabilities **privately** via GitHub's security advisories:

> https://github.com/badmonkeyai/agentspaces/security/advisories/new

Do not open public issues for suspected vulnerabilities, and do not include
exploit details in public discussions until a fix is released.

We will acknowledge your report within five business days, keep you informed of
progress, credit you in the advisory unless you prefer otherwise, and
coordinate the disclosure timeline with you. There is currently no bug bounty
program.

Questions can be sent to `oss [at] badmonkey.ai`

## Scope

In scope: the Java modules, the Python and TypeScript clients, and anything
that lets an attacker violate a guarantee the specs claim — signature or
admission bypass, forgery of entries, claims, state transitions or votes,
confidentiality failures of group payload encryption, and resource-exhaustion
attacks a single peer can mount against the fleet.

Worth knowing before reporting: the specs deliberately document accepted risks
and open gaps (`agentspaces-spec/TECH-SPEC.md` §11, `agentspaces-spec/SPEC.md` §11) — for example,
lying AUCTION bidders and unauthenticated aggregation values. Reports that
show a *documented* accepted risk to be worse than documented are still very
welcome.

## Supported versions

During 0.x, only the latest minor release receives security fixes.
