# Example 13: Signed Agents

Attribution to the agent, provable anywhere in the fleet. Two agents, an
`auditor` and a `clerk`, each hold a subordinate identity: an Ed25519 key of
their own that their peer certifies (SPEC §4.2). Every finding they write is
signed by the agent's key and travels with the peer's certificate, so a reader
anywhere in the fleet can attribute the entry to the agent itself, beyond the
peer that hosts it: `Attestation.AGENT_ATTESTED`. A third agent, the `desk`,
writes the way every earlier example has, under the peer key, and its findings
read as `Attestation.PEER_ASSERTED`. Nothing else about the space changes, and
a peer that has never heard of certificates replicates all three.

The first half seats each agent on a peer of its own and wires the identity by
hand. The second half is the one to copy: it puts both agents on one peer as
plain `@AgentSpec` classes bound through the `AgentSpaces` facade, with
a renewing subordinate key per agent (`identity.renewingSubordinate(name, ttl,
clock)`) as the facade's identity factory. The agent classes
contain no identity code at all.

## What it demonstrates

- **Certified agent keys.** `PeerIdentity.subordinate(name, issued, ttl)` mints
  a key for one agent and a certificate, signed by the peer, that binds the key
  to that `AgentId` for a lifetime.
- **Attestation on read.** `Space.readAllIssued(template, max)` returns each
  entry with its issuing `AgentId` and its `Attestation`, so any replica can tell
  a record the agent signed from one its peer vouched for.
- **Expiry that bites, history that lasts.** A certificate is judged at the signing
  time of what it certifies (SPEC v0.1.13): an agent whose certificate does not cover
  "now" refuses to sign, while records it signed while the certificate was valid stay
  mergeable everywhere, late joiners included. Renewing identities
  (`PeerIdentity.renewingSubordinate`) re-issue at half-life and never lapse.
- **Two agents, one peer, two identities.** With a subordinate identity factory,
  each bound bean receives a per-agent view of the shared replica through its
  `@SpaceRef`, so its writes are signed by its own key, and its AgentCard carries
  that public key (`card().attested()`).

## The API in this example

By hand, one line opts an agent in:

```java
AgentIdentity writer = identity.subordinate(name, Instant.now(), Duration.ofHours(1));
ReplicatedSpace findings = ReplicatedSpace.builder(runtime, FINDINGS, identity, name)
        .writer(writer)
        .settleWindow(Duration.ofMillis(150))
        .build();
```

Through the facade, the agents are plain classes:

```java
@AgentSpec(name = "auditor", description = "Audits releases", goals = {"audit"})
public static final class Auditor {
    @SpaceRef(FINDINGS) Space findings;

    public void conclude(String subject, String verdict) {
        findings.write(new Finding(subject, verdict), FINDING_LEASE);
    }
}

AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system(),
        name -> identity.renewingSubordinate(name, Duration.ofHours(24), InstantSource.system()));
AgentSpaces.GroupContext group = spaces.register("signed-agents", runtime.id(), runtime, null);
group.space(FINDINGS, findings);
AgentBinder.Bound auditorBound = group.bind(new Auditor());
AgentBinder.Bound clerkBound = group.bind(new Clerk());
```

Reading the ledger with attribution:

```java
for (Space.Issued<Finding> issued : reader.findings().readAllIssued(Template.of(Finding.class), 100)) {
    issued.entry();         // the Finding
    issued.issuer();        // the AgentId, for example <peer>/auditor
    issued.attestation();   // AGENT_ATTESTED or PEER_ASSERTED
}
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `PeerIdentity.subordinate(name[, issued, ttl])` | `agentspaces-identity` | A peer-certified key for one agent |
| `AgentIdentity`, `AgentCertificate` | `agentspaces-api` | The agent's key and the peer's certificate for it |
| `ReplicatedSpace.Builder.writer(AgentIdentity)` | `agentspaces-space` | Signs this handle's records with the agent key |
| `new AgentSpaces(identity, clock, name -> identity.renewingSubordinate(name, ttl, clock))` | `agentspaces-agent` | The identity factory: every bound agent gets its own key, re-certified at half-life |
| `Space.readAllIssued`, `Space.Issued`, `Space.Attestation` | `agentspaces-api` | Entries with their issuer and attestation |
| `AgentBinder.Bound.identity()`, `card()` | `agentspaces-agent` | The bound agent's identity and its published card |

Source: [SignedAgents.java](src/main/java/ai/badmonkey/agentspaces/examples/signed/SignedAgents.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), on ports 7701 to 7704:

```
mvn -q -pl examples/example-13-signed-agents exec:java
```

The demo prints the ledger as the desk reads it, with each finding's issuer
and attestation, then the certificates the peers issued, then the two
annotated agents' findings and whether their cards carry keys.

[SignedAgentsFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/signed/SignedAgentsFlowTest.java)
has three tests over real TCP:

- `eachFindingIsAttributedToItsAgentWithItsAttestationEverywhere`: every replica
  reads the auditor's and clerk's findings as `AGENT_ATTESTED` and the desk's as
  `PEER_ASSERTED`.
- `twoAnnotatedAgentsOnOnePeerAreTwoProvableIdentities`: the facade gives two
  beans on one peer two distinct keys and attested cards.
- `aLapsedCertificateCertifiesNothingSoItsFindingsAreRefusedElsewhere`: an agent
  whose certificate lapsed an hour ago cannot sign a finding at all, so none reaches
  the desk.

```
mvn -q -pl examples/example-13-signed-agents test
```

## Next steps

- **Turn it on with one property.** Under Spring Boot,
  `agentspaces.identity.agent-keys=subordinate` gives every bound agent its own
  certified key, with no change to any agent class.
- **Grant per agent.** Grant lists under `agentspaces.security.grants.*` accept
  AgentIDs (`peer/localName`) as well as PeerIDs. An AgentID grant puts the
  operation at AGENT granularity, which is how example 05's council counts one
  vote per granted agent on a single peer.
- **Audit agent actions end to end.** Take claims, ballots, and completions made
  through a subordinate identity carry the agent's attestation too, so an
  auditor can prove which agent confirmed a payment ([example 12](../example-12-exactly-once-desk/README.md))
  or which extractor produced a filed reading ([example 08](../example-08-intake-fleet/README.md)).
  For a fleet of LLM agents, this answers "which agent made this decision?"
  from the space alone; put the model name in the result record if the audit
  also needs to know which model the agent used.
- **Tie agents to your identity provider.** The `MTLS_OIDC` and `ZERO_TRUST`
  profiles bind each peer to an identity-provider JWT whose scopes decide what
  it may do. Under those profiles the OIDC authorizer speaks at PEER granularity,
  and every agent on the peer inherits its token's scopes (SPEC §11).
- **Embabel.** An Embabel `@Agent` that also carries `@SpaceTake` or
  `@SpaceNotify` methods binds through the space binder, so under
  `agent-keys=subordinate` its takes and writes sign with its own key. The cards
  the extension adopts for plain `@Agent` beans (`AgentBinder.adopt`) publish
  under the peer's identity.
