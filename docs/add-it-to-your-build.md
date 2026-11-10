# Add it to your build

Written for: Java developers who build with AgentSpaces.

Release artifacts are on Maven Central under the group `ai.badmonkey.agentspaces`, version `0.6.0` (in preparation; `0.5.0` is the current release) ([central.sonatype.com/namespace/ai.badmonkey.agentspaces](https://central.sonatype.com/namespace/ai.badmonkey.agentspaces)). Maven and Gradle resolve Maven Central by default, so no repository configuration is needed. The libraries are Java 21 bytecode.

One dependency matches each way an application is built:

| Application | Dependency | What it brings |
| --- | --- | --- |
| Spring Boot | `agentspaces-spring-boot-starter` | the autoconfiguration, the capabilities, and the `@SpaceAgent` stereotype; the fabric boots with the application |
| Plain Java | `agentspaces-agent` | the annotation binder and the facade, with the fabric (`api`, `space`, `peering`, `discovery`, `capabilities`, `identity`) as transitive dependencies |
| Embabel | `embabel-agentspaces`, beside the starter | `@Agent` metadata as AgentCards and the fleet's cards as planner actions |
| Spring AI | [`agentspaces-springai`](https://github.com/badmonkeyai/agentspaces-springai), beside the starter | fleet tools, advisors, chat memory, and the model service on Spring AI's extension points |
| Micronaut | [`agentspaces-micronaut`](https://github.com/badmonkeyai/agentspaces-micronaut) | the same `agentspaces.*` properties and annotations for a Micronaut service |
| LangChain4j, any host | [`agentspaces-langchain4j`](https://github.com/badmonkeyai/agentspaces-langchain4j) | `FleetRuntime`, a peer from the same `agentspaces.*` configuration with no framework, and the fleet on LangChain4j's extension points; the Micronaut bridge is wiring over it |

The bill of materials, `agentspaces-dependencies`, manages the version of every module in this repository and of the third-party libraries the suite is tested against (Jackson, BouncyCastle, Netty QUIC, Nimbus). Import it once and leave the version off every AgentSpaces dependency. The Spring AI and Micronaut libraries release from their own repositories and carry their own version; see their READMEs.

## Maven (`pom.xml`)

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>ai.badmonkey.agentspaces</groupId>
      <artifactId>agentspaces-dependencies</artifactId>
      <version>0.6.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>ai.badmonkey.agentspaces</groupId>
    <artifactId>agentspaces-spring-boot-starter</artifactId>
  </dependency>
</dependencies>
```

A project that inherits from `spring-boot-starter-parent` imports the BOM the same way; Spring Boot's dependency management does not know the AgentSpaces artifacts, so either the import or an explicit `<version>0.6.0</version>` on each dependency is needed.

## Gradle Kotlin DSL (`build.gradle.kts`)

```kotlin
dependencies {
    implementation(platform("ai.badmonkey.agentspaces:agentspaces-dependencies:0.6.0"))
    implementation("ai.badmonkey.agentspaces:agentspaces-spring-boot-starter")
}
```

## Gradle Groovy DSL (`build.gradle`)

```groovy
dependencies {
    implementation platform('ai.badmonkey.agentspaces:agentspaces-dependencies:0.6.0')
    implementation 'ai.badmonkey.agentspaces:agentspaces-spring-boot-starter'
}
```

Without the BOM, write the version on the dependency: `implementation("ai.badmonkey.agentspaces:agentspaces-agent:0.6.0")`. The optional modules (`agentspaces-transport-quic`, `agentspaces-transport-dtls`, `agentspaces-console`, `agentspaces-a2a`, `agentspaces-connect-core`, `agentspaces-auth-oidc`) are added the same way and are all managed by the BOM.

## Unreleased versions

No snapshot repository is published. To build against the current source, install it into the local repository first and resolve from there (Gradle needs `mavenLocal()` in its `repositories` block):

```
git clone https://github.com/badmonkeyai/AgentSpaces.git agentspaces
mvn -f agentspaces/pom.xml install -DskipTests
```
