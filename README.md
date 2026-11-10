# AgentSpaces

**A peer-to-peer coordination fabric for agents, services, and the people who work with them**, from Bad Monkey AI. Java developers use it to build fleets of agents that share a replicated space, take work under leases, vote, and recover from each other's failures, with no broker, registry, or scheduler to run.

## Quick start: your first agent

```java
@AgentSpec(name = "fulfiller", description = "Ships orders")
public class Fulfiller {

    @SpaceTake(lease = "10m")
    public Shipment ship(Order order) {
        return new Shipment(order.orderId(), order.item(), "fulfiller");
    }
}
```

Drop that in a Spring Boot app. Start a second instance. You now have competing consumers. Kill one mid-order and the order comes back for the other to pick up and run. That's the whole model: shared state across instances, time bound, with integrated failure handling. 


To run the examples from source, with JDK 21+ and Maven:

```
git clone https://github.com/badmonkeyai/AgentSpaces.git agentspaces
mvn -f agentspaces/pom.xml install -DskipTests
cd agentspaces && mvn -q -pl examples/example-11-quickstart exec:java
```

Run it in five minutes. The full quickstart lives in `examples/example-11-quickstart`, the whole experience in one file.

## What it is

- A replicated tuple space with `write`, `read`, `take`, and `notify`, signed end to end and leased so a crashed worker's task reappears.
- Annotations for agents (`@SpaceTake`, `@SpaceNotify`, `@Ballot`, `@SpaceJoin`, `@SpaceReduce`) and a binder; a Spring Boot starter.
- Capabilities for votes, aggregates, an ordered log, gossip learning, the content swarm, persistent values, and content stores.
- Peering over TCP, TLS 1.3, QUIC, DTLS, WebSocket, and WebRTC; Python, TypeScript, Ruby, Clojure, and Groovy peers on the same wire.
- Java 21; the release artifacts are on Maven Central under `ai.badmonkey.agentspaces`.

## Add it to your build

Add the Spring Boot starter from Maven Central, or `agentspaces-agent` for plain Java:

```xml
<dependency>
  <groupId>ai.badmonkey.agentspaces</groupId>
  <artifactId>agentspaces-spring-boot-starter</artifactId>
  <version>0.6.0</version>
</dependency>
```

[Add it to your build](docs/add-it-to-your-build.md) has the BOM, Gradle, and one dependency for each way an application is built, and [Build](docs/build.md) builds from a clone.

## Where to read next

1. **[The developer guide](https://www.badmonkey.ai/agentspaces/developer-guide.html)**: agents, the frameworks, the Space API, discovery, capabilities, large data, peering, security, and the other languages, chapter by chapter.
2. **[The examples](examples/)**: the numbered examples, one primitive each, from a first space to a database on a ref and a store for the fleet.
3. **[The documents](docs/README.md)**: everything this README leaves out, one topic per document, each with the reader it is written for.
4. **[The architecture guide](https://www.badmonkey.ai/agentspaces/architecture-guide.html)** and **[the workflow guide](https://www.badmonkey.ai/agentspaces/workflow-guide.html)**: how to size and shape a fleet, and the coordination patterns.

## License

Apache License 2.0, Copyright 2026 Bad Monkey, Inc. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Contributions are welcome under the Developer Certificate of Origin; see [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md). Report a vulnerability privately, as [SECURITY.md](SECURITY.md) describes.
