# AgentSpaces documents

Written for: anyone who read the README and wants the rest of the story.

The README is short by design: what this is, the quick start, and how to add it to a build. Everything else lives here, one topic per document.

| Document | Written for | What it holds |
|---|---|---|
| [How to use AI with AgentSpaces](how-to-use-ai.md) | Java developers who build with AgentSpaces | Tested walk-throughs: an agent and the peer that hands it work, a crash that hands an order to another peer, and the same agent as a Spring Boot bean. |
| [About AgentSpaces](about.md) | Java developers who build with AgentSpaces | What AgentSpaces is, why it exists, how it works, what makes it different, and where it came from. |
| [Quick taste](quick-taste.md) | Java developers who build with AgentSpaces | Direct API: |
| [Add it to your build](add-it-to-your-build.md) | Java developers who build with AgentSpaces | Release artifacts are on Maven Central under the group `ai.badmonkey.agentspaces`, version `0.6.0` (in preparation; `0.5.0` is the current release). |
| [Let your agent be handed a beacon](beacons.md) | Java developers who build with AgentSpaces | An agent you write for one fleet will be asked to join another. |
| [What this is not](what-this-is-not.md) | Java developers who build with AgentSpaces | Honest boundaries: |
| [Architecture](architecture.md) | Java developers who build with AgentSpaces | Each layer depends only on the layers below it. |
| [Version 0.4.0 Capabilities](capabilities-by-version.md) | Java developers who build with AgentSpaces | Every specification layer (0 through 4) has a working implementation, and every clause the specification does not mark as planned has a pinning test. |
| [Build](build.md) | contributors who build and test the core | JDK 21+ and Maven are the only prerequisites. |
| [Modules](modules.md) | contributors and anyone choosing modules | Each module in two or three sentences. |
| [Extending AgentSpaces](extending.md) | developers who extend the core with a capability, a transport, or a host | The core stays small. |
| [Build environments](BUILD-ENVIRONMENTS.md) | contributors and release engineers | Which build runs where: the CI gate, the coverage and Spring profiles, and the local runs. |
| [The fleet console](CONSOLE.md) | operators of a fleet | The read-only console peer: what it shows, how to start it, and its settings. |
