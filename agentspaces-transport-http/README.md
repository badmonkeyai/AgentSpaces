# agentspaces-transport-http

The HTTP bindings of the `Transport` SPI: WebSocket (RFC 6455) as `ws` and `wss`, and the HTTP
exchange — a request/response carriage of batched frames for intermediaries that break
WebSocket — as `http` and `https`. All four carry the fabric's signed frames unchanged, so a
browser, a Node process, a Worker, or any HTTP client can be a member; a Java node that listens
here is what a browser dials. Netty's HTTP codec, one port per plaintext or TLS listener shared by
the WebSocket upgrade and the exchange routes (`HttpServerCore`).

**Architecture position.** Layer 1, Peering (a transport). SPEC §5.5 (the bindings), §5.6
(session attestation on an unattested link, which `ws` and `http` need), §9. TECH-SPEC §5.5.
Design of record: `srad/agentspaces-js.md` §3.3, §4.2, appendix C (the exchange, normative);
requirement record: `issues/ISSUE-Websocket.md`.

## Using it

Add the module; the BOM manages its version. The listener blocks follow the `tcp`/`tls` pair:
`ws` and `http` run in any profile like `tcp` and are refused by a node that requires
attestation; `wss` and `https` follow `tls`.

```yaml
agentspaces:
  transport:
    ws:
      enabled: true
      bind: 0.0.0.0:7501
      advertise: fleet.example:7501
      allowed-origins: [ "https://app.example" ]   # browsers; empty refuses every browser; "*" for demonstrations
    wss:
      enabled: true
      bind: 0.0.0.0:7443
      key-store: ...                               # the tls binding's identity modes
    http:
      enabled: true                                # same port as ws when the binds match
      bind: 0.0.0.0:7501
    https:
      enabled: true
      bind: 0.0.0.0:7443
```

Seeds name them as `ws://host:7501`, `wss://host:7443`, `http://host:7501/aspace`,
`https://host:7443/aspace`. In code: `node.listen(new WebSocketTransport(options), "0.0.0.0:7501", 1)`,
`new TlsWebSocketTransport(identity, ...)`, `new HttpExchangeTransport(options)`,
`new TlsHttpExchangeTransport(identity, ...)`. The TypeScript client dials all four with the
runtime's global `WebSocket` and `fetch` (`@agentspaces/client`); Python keeps `tcp` and skips them.

## The exchange (appendix C)

`POST /open` answers `201` with `Location: /s/<sid>`; `POST /s/<sid>/up` carries a CBOR batch of
frames under `Aspace-Seq`; `GET /s/<sid>/down` long-polls the server's batch under `Aspace-Ack`;
`DELETE /s/<sid>` ends the session, and a parked poll answers `204`. One session is one
`TransportConnection` to the node, unattested, so SESSION-mode peers run SPEC §5.6 session
attestation inside it exactly as on `ws`.

## Key files

| File | What it is |
| --- | --- |
| `WebSocketTransport`, `TlsWebSocketTransport` | `scheme() = "ws"` / `"wss"`; one binary message per frame, subprotocol `aspace.1`, the 8 MiB cap (1009 on an oversize message, 1003 on a text one); `wss` attests JVM dialers in the three identity modes and admits a dialer with no certificate as unattested. |
| `HttpExchangeTransport`, `TlsHttpExchangeTransport` | `scheme() = "http"` / `"https"`: the exchange of appendix C; `ExchangeSession` (server side), `ExchangeClient` (the JVM dialer), `FrameBatch` (the CBOR batch). |
| `HttpServerCore`, `NettyLoops`, `TlsSetup` | One Netty server per bind, shared by the WebSocket and exchange bindings on that port; the event loops; the TLS context from the identity modes. |
| `WebSocketBinding`, `ExchangeBinding`, `WsConnection`, `FrameConnection` | The route handlers and the `TransportConnection`s they produce. |
| `OriginPolicy` | The browser rule: a request without `Origin` passes, one with it only when listed, `*` admits any browser. |
| `HttpOptions`, `HttpCounters`, `HttpAddress` | The tunables (frame cap, idle, poll wait, per-source rates — bounds are per source or per message, never on members), the counters, and address parsing. |

## Intermediaries (ISSUE-Websocket §9.6, JS-H-08)

| Intermediary | Fabric effect | Attestation | Operator guidance |
|---|---|---|---|
| Layer-4 pass-through (TCP proxy, NAT) | none | survives | nothing to do |
| TLS passthrough on SNI (ingress with `ssl-passthrough`, L4 LB with SNI routing) | none | survives; the backend sees the dialer's channel certificate and the dialer sees the backend's | dial a hostname; this is the configuration for `require-attestation` fleets |
| TLS termination (ingress with its own certificate, L7 LB, CDN) | upgrade is forwarded; frames flow | **does not survive**: both ends read the link as unattested; in `SESSION` mode they establish session attestation (SPEC §5.6) inside it and otherwise keep every frame signed; a `require-attestation` node refuses the listener | run the backend as `ws`/`http` behind the terminator inside a network the operator trusts as far as the terminator, or use passthrough; never `require-attestation` behind termination |
| HTTP forward proxy (CONNECT) on the dialer's network | `wss`/`https` tunnel through CONNECT; `ws`/`http` may be rewritten | survives for `wss` (end-to-end TLS inside the tunnel) | the JVM's `https.proxyHost`; browsers use their own proxy settings |
| Idle-timing proxies | a silent link is cut (commonly at 60 s) | n/a | the 30 s ping keeps a WebSocket alive; the exchange's long-poll returns within 25 s |

## Requirements to tests

| Row (srad/agentspaces-js.md) | Test |
| --- | --- |
| JS-H-01 the module as the design of record of ISSUE-Websocket's `ws`/`wss` rows | `WebSocketTransportTest`, `Wire` (framing, subprotocol, the 8 MiB cap, close codes) |
| JS-H-02 one pipeline per plaintext port and per TLS port, routed by path | `HttpFleetTest` (`ws` and `http` on one bind) |
| JS-H-03 `Origin` refused with 403 unless listed, default empty | `WebSocketTransportTest` (`OriginPolicy`), `BrowserFleetTest` (a browser admitted from its listed origin, CORS on the exchange) |
| JS-H-04 `wss` attests JVM dialers, admits a certificate-less dialer as unattested | `WebSocketTransportTest` (TLS cases) |
| JS-H-05 session attestation on an unattested link, byte-identical in both languages | `SessionVectorsTest` (Java == TypeScript bytes), `TypeScriptClientTest`, `BrowserFleetTest` |
| JS-H-06 the TypeScript `ws`/`wss` dialers | `TypeScriptClientTest` (a Node worker over `ws`), the client's `test/session.test.ts` |
| JS-H-07 the exchange of appendix C | `HttpExchangeTransportTest`, `HttpFleetTest`, `HttpAddressTest`, `TypeScriptClientTest` and `BrowserFleetTest` over `http` |
| JS-H-08 unattested behind termination, the intermediary table | the table above; `TypeScriptClientTest` (an unattested `http` link runs session attestation) |
| JS-H-09 enablement like `tcp`/`tls`, refused under required attestation | `HttpBlocksAutoConfigurationTest` (autoconfigure) |
| JS-H-10 `agentspaces.transport.{ws,wss,http,https}.*` blocks with `path` and `allowed-origins` | `HttpBlocksAutoConfigurationTest`; Micronaut and the plain runtime share the starter's wiring |
| JS-H-11 the Node service host's `ws` listener | `@agentspaces/node` `test/ws-listener.test.ts` |
| JS-H-12 counters | `WebSocketTransportTest`, `BrowserFleetTest` (`HttpCounters`) |

Layering: main depends on api, common, identity, identity-tls, peering; tests on space and
test-support as well. The autoconfigure module's dependency on it is optional, like `dtls`.
