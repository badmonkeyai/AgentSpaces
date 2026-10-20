# agentspaces-connect-core

The open connector SDK behind the AssetCard model: how a data-provider peer describes assets,
answers queries for them through the space, and pushes source changes in as leased entries. The
SPI and protocol stay Apache 2 so anyone can build a connector.

**Architecture position.** Gateways and served interfaces. SPEC §6.1a, §6.3; TECH-SPEC §6.

## Key files

| File | What it is |
| --- | --- |
| `AssetProvider`, `TableAssetProvider` | The SPI a connector implements: enumerate assets, answer queries, optionally run an allowlisted operation (`invoke`, refusing by default); a table-shaped reference implementation. |
| `ConnectorRuntime` | Publishes leased `AssetCard`s and serves the `aspace:cap/data-query` protocol through the space; asks the `Authorizer` `CONNECTOR_SERVE` about itself before advertising an asset; serves `aspace:cap/asset-invoke` from the invocation space, running only operations the card's `invoke-ops` lists and, under an authorizer, only for a requester permitted `CONNECTOR_INVOKE` in `<asset>:<operation>` (srad/agentspaces-js.md JS-C-02, JS-C-03, JS-C-07). |
| `AssetInvocations` | The asset-invoke protocol (appendix F): `AssetInvocation` and `AssetInvocationResult` (with `v`, unknown fields ignored, golden `asset_invocation_cbor` / `asset_invocation_result_cbor`), `invoke(space, card, operation, params)` returning a future result, `retry` under the original id (JS-C-10). |
| `DataQueryClient`, `DataQueryEntries` | The consumer side: publish a query, await the correlated result (`Fetched`, with `result().rows()`), pull-once caching by canonical hash, trust only issuers the authorizer permits. |
| `MaterializingAssetProvider` | Pushes source changes into a space as leased entries. |
| `DataSpaces` | Wires the block exchange so bulk results travel content-addressed. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `agentspaces-identity`, `agentspaces-discovery`,
`agentspaces-space`, `agentspaces-peering`.

## Notes

- `example-10-data-fleet` is the worked example; the finished enterprise connectors and the
  console panels are the commercial layer (ENTERPRISE §6).
