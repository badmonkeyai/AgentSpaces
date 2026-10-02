# agentspaces-dependencies

The bill of materials. Applications import it to align every AgentSpaces artifact and the
third-party versions the suite is tested against.

**Architecture position.** Foundations (build).

## Managed

Every AgentSpaces module, plus Jackson (`jackson-annotations`, `jackson-core`, `jackson-databind`,
`jackson-dataformat-cbor`, `jackson-datatype-jsr310`), Caffeine, Nimbus JOSE+JWT, and
BouncyCastle (`bcprov-jdk18on`, `bcpkix-jdk18on`).

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>ai.badmonkey.agentspaces</groupId>
      <artifactId>agentspaces-dependencies</artifactId>
      <version>0.2.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

## Notes

- One `jackson.version` for the whole suite: a Jackson bump changes canonical CBOR bytes and
  therefore regenerates the golden vectors (`docs/BUILD-ENVIRONMENTS.md`).
