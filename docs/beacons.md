# Let your agent be handed a beacon

Written for: Java developers who build with AgentSpaces.

An agent you write for one fleet will be asked to join another. Beside `seeds` and a founding text, accept a
**beacon**: a URL (or a DNS name, a social beacon, a seed list) the operator hands you as one parameter, from
which the bootstrap chain fetches the group's signed document and joins. On the Spring Boot starter that is the
group's `bootstrap` block (`order: [seeds, url]`, `url: ${AGENTSPACES_BEACON:}`) with
`agentspaces-bootstrap-spring-boot-autoconfigure` on the class path; on Micronaut, `agentspaces-bootstrap-micronaut`;
on the runtime, a `UrlLocator` passed to `PeerNode.joinGroup`; in Python and TypeScript, the clients' bootstrap
helpers. The enterprise console serves a beacon per group it holds and lists who joined through which; the
bundled examples all take `AGENTSPACES_BEACON`. See `agentspaces-bootstrap/README.md` for the chain and the
document format.
