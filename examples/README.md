# examples

Fourteen graduated examples, each adding one layer or one coordination idea to the one before,
each with a flow test that drives it end to end over real TCP (`mvn -q -pl examples/<name> exec:java`
runs the live demo). The developer guide cuts its snippets from these sources.

| # | Example | Adds |
| --- | --- | --- |
| 01 | `hello-space` | `LocalSpace`: write, read, take, complete, leases |
| 02 | `research-fleet` | `ReplicatedSpace` over TCP; closing and reopening a space by name |
| 03 | `discovery-cards` | AgentCards and discovery |
| 04 | `auction` | The AUCTION strategy and `@BidFunction` (one bidder per space per peer) |
| 05 | `quorum` | Aggregate then vote: `@Ballot` members, an `@OnDecision` supervisor, and a council of seats on one peer counted per granted agent |
| 06 | `wan-rendezvous` | Rendezvous and relay roles across a WAN |
| 07 | `a2a-fleet` | The A2A gateway over a fleet |
| 08 | `intake-fleet` | Extraction as `@SpaceNotify`, a `@Ballot` voter, an `@OnDecision` auditor |
| 09 | `fleet-console` | The console and command-and-control |
| 10 | `data-fleet` | The connector SDK and semantic discovery |
| 11 | `quickstart` | The whole programming model in one file |
| 12 | `exactly-once-desk` | ORDERED takes: `OrderedTakes.over(...)` and an `@OrderedTake` clerk |
| 13 | `signed-agents` | Subordinate agent keys, attestation, two annotated agents on one peer |
| 14 | `gossip-learning` | Three local models converge on the fleet mean on the peer tick alone |

Example 15, a Spring AI orchestrator whose model calls fleet agents as tools,
lives with the Spring AI integration in `agentspaces-springai/examples/springai-fleet`
at the workspace root, beside the Spring AI patterns (`springai-patterns`).

## Notes

- Each module's own `README.md` covers what the example demonstrates, the API it
  uses, how to run and test it, and next steps into the Spring Boot starter,
  Embabel, Spring AI, the flagships, and the non-JVM clients.
- Every example module depends on `agentspaces-agent` with an explicit `${project.version}`;
  the examples parent manages the space and identity versions but not the agent's.
- `examples-guide.html` beside the modules is the narrative walk-through.
