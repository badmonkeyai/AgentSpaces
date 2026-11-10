# How to use AI with AgentSpaces

Written for: Java developers who build with AgentSpaces.

At the end of this section you will have an agent that takes work from a shared space, a second peer that writes that work and reads the result back, a worker whose crash hands its order to a worker on another peer, and the same agent enrolled as a Spring Boot bean by the starter with no peer code of your own. 

**Build an agent.** Two plain classes are a fleet: a worker that takes orders under a lease and an auditor that reacts to every shipment. The group has one space, so no annotation names it; the `@SpaceRef` field gives the worker a handle for a mid-work progress entry.

```java
@AgentSpec(name = "fulfiller", description = "Ships orders from the shared space",
        goals = {"fulfill orders"})
public static class Fulfiller {

    @SpaceRef
    private Space space;

    @SpaceTake(lease = "10m", pollTimeout = "300ms")
    public Shipment ship(Order order) {
        space.write(new Progress(order.orderId(), "picking " + order.item()),
                Lease.of(Duration.ofMinutes(5)));
        return new Shipment(order.orderId(), order.item(), "fulfiller");
    }
}

@AgentSpec(name = "auditor", description = "Writes a receipt for every shipment",
        goals = {"account for shipments"})
public static class Auditor {

    @SpaceNotify
    public Receipt account(Shipment shipment) {
        return new Receipt(shipment.orderId(),
                shipment.item() + " shipped by " + shipment.by());
    }
}
```

Binding is three lines on any peer: `new AgentBinder(identity, runtime.id(), null, InstantSource.system())`, `binder.space("work", space)`, `binder.bind(new Fulfiller())`. Source: [`examples/example-11-quickstart/src/main/java/ai/badmonkey/agentspaces/examples/quickstart/Quickstart.java`](../examples/example-11-quickstart/src/main/java/ai/badmonkey/agentspaces/examples/quickstart/Quickstart.java); run it with `mvn -q -pl examples/example-11-quickstart exec:java` (two TCP peers on ports 7591 and 7592); `QuickstartTest` runs the same two agents over one `LocalSpace` with `mvn -q -pl examples/example-11-quickstart test`.

**Share work through the space.** The writer is another peer, and it binds no agent. It writes an `Order` into its replica, the worker on the other peer takes it, and the `Shipment` replicates back to the writer. `startPeer` is example 11's peer assembly on the in-JVM `mem` transport, and `Fulfiller` is the class at the top of this README.

```java
Peer desk = startPeer("desk", null);            // writes orders, binds no agent
Peer worker = startPeer("worker", "desk");      // seeds from the desk
worker.binder().bind(new Fulfiller());

desk.space().write(new Order("ord-1", "kite"), Lease.of(Duration.ofMinutes(10)));

assertThat(desk.space().read(Template.of(Shipment.class), Duration.ofSeconds(20)))
        .hasValueSatisfying(s -> assertThat(s.by()).isEqualTo("fulfiller"));
assertThat(desk.space().readAll(Template.of(Order.class), 10)).isEmpty();
```

**Survive a crash.** A worker that throws lets its take lease lapse, and closing its binder is the process dying: nothing on that peer renews or re-takes. The order reappears for the worker on a third peer, and the desk reads the shipment with no code for retries, dead-letter queues, or heartbeats.

```java
@AgentSpec(name = "crasher", description = "Dies mid-order")
public static class Crasher {
    final AtomicInteger attempts = new AtomicInteger();

    @SpaceTake(lease = "300ms", pollTimeout = "100ms")
    public Shipment ship(Order order) {
        attempts.incrementAndGet();
        throw new IllegalStateException("simulated crash");
    }
}

Peer desk = startPeer("desk", null);
Peer flaky = startPeer("flaky", "desk");
Peer steady = startPeer("steady", "desk");
Crasher crasher = new Crasher();
flaky.binder().bind(crasher);

desk.space().write(new Order("ord-2", "compass"), Lease.of(Duration.ofMinutes(10)));
await(() -> crasher.attempts.get() >= 1);       // flaky took the order and died
flaky.binder().close();                         // the process is gone; nothing renews its lease

steady.binder().bind(new Fulfiller());          // the lease lapses; the order reappears here
assertThat(desk.space().read(Template.of(Shipment.class), Duration.ofSeconds(40)))
        .hasValueSatisfying(s -> assertThat(s.orderId()).isEqualTo("ord-2"));
assertThat(desk.space().readAll(Template.of(Order.class), 10)).isEmpty();
```

Source for both: [`agentspaces-agent/src/test/java/ai/badmonkey/agentspaces/agent/ReadmeExamplesTest.java`](../agentspaces-agent/src/test/java/ai/badmonkey/agentspaces/agent/ReadmeExamplesTest.java), tests `anotherPeerWritesWhatTheWorkerTakes` and `aCrashedWorkersOrderReappearsForAWorkerOnAnotherPeer`; run with `mvn -q -o test -pl agentspaces-agent -Dtest=ReadmeExamplesTest -Dsurefire.failIfNoSpecifiedTests=false`. The same hand-off over three TCP peers, six tasks, and a worker that dies after its first completion is [`examples/example-02-research-fleet`](../examples/example-02-research-fleet/README.md); the exactly-once variant through the ordered log, where `@OrderedTake` replaces `@SpaceTake` and three clerks confirm each payment once, is [`examples/example-12-exactly-once-desk`](../examples/example-12-exactly-once-desk/README.md).

**Enroll it with the starter.** Under Spring Boot the peer, group, spaces, worker loop, and card publication arrive from properties, and a bean post-processor enrolls every annotated bean. The application's only AgentSpaces knowledge is one starter dependency, the properties, and the bean; the `@SpaceAgent` stereotype folds `@Component` and `@AgentSpec` into one annotation.

```java
@Component
@AgentSpec(name = "it-researcher", description = "Researches integration topics",
        goals = {"research"})
public static class Researcher {
    public final CopyOnWriteArrayList<String> worked = new CopyOnWriteArrayList<>();

    @SpaceTake(space = "tasks", lease = "PT10M", pollTimeout = "PT0.2S",
            resultSpace = "findings")
    public ItFinding research(ItTask task) {
        worked.add(task.topic());
        return new ItFinding(task.topic(), "done: " + task.topic());
    }
}
```

The test boots two such applications on free ports from the same keys an `application.yml` would carry, writes a task through the `AgentSpaces` facade of the one with no agent bean, and reads the finding back from it:

```java
SpringApplicationBuilder builder = new SpringApplicationBuilder(app)
        .properties(Map.of(
                "spring.main.web-application-type", "none",
                "agentspaces.bind", "127.0.0.1:" + port,
                "agentspaces.tick-millis", "200",
                "agentspaces.groups[0].name", "it-fleet",
                "agentspaces.groups[0].founding", "spring-it-fleet-v1",
                "agentspaces.groups[0].spaces[0].name", "tasks",
                "agentspaces.groups[0].spaces[0].settle-window-millis", "100",
                "agentspaces.groups[0].spaces[1].name", "findings",
                "agentspaces.groups[0].spaces[1].settle-window-millis", "100"));
if (seedPort > 0) {
    builder.properties(Map.of(
            "agentspaces.groups[0].seeds[0]", "127.0.0.1:" + seedPort));
}

AgentSpaces writer = first.getBean(AgentSpaces.class);
writer.group("it-fleet").space("tasks")
        .write(new ItTask("cross-context", 2),
                Lease.of(Duration.ofMinutes(10)));
assertThat(writer.group("it-fleet").space("findings")
        .read(Template.of(ItFinding.class), Duration.ofSeconds(60)))
        .hasValueSatisfying(finding -> assertThat(finding.summary())
                .isEqualTo("done: cross-context"));
```

Source: [`integration-tests/spring-boot-it/src/main/java/ai/badmonkey/agentspaces/it/FleetItApp.java`](../integration-tests/spring-boot-it/src/main/java/ai/badmonkey/agentspaces/it/FleetItApp.java) and [`SpringFleetIntegrationTest.twoSpringApplicationsFormOneFleetOverTcp`](../integration-tests/spring-boot-it/src/test/java/ai/badmonkey/agentspaces/it/SpringFleetIntegrationTest.java); run with `mvn -q -Pspring-it test -pl integration-tests/spring-boot-it -Dtest=SpringFleetIntegrationTest`. The [starter README](../agentspaces-spring-boot-starter/README.md) has the YAML form of the same keys. To put a model inside the worker, see [`agentspaces-springai`](https://github.com/badmonkeyai/agentspaces-springai) (Spring AI) or [`agentspaces-langchain4j`](https://github.com/badmonkeyai/agentspaces-langchain4j) (LangChain4j on any JVM host); the coordination and the model call stay orthogonal.
