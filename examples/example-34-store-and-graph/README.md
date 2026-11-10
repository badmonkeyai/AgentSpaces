# Example 34: a database on a ref and a store for the fleet

The example runs two halves over real TCP, each in its own group. In the graph
half, the superfriends graph lives on a `db` ref named `graph`: companies,
tickers, people, the relations between them, and findings about them, in the
schema of the superfriends graph peer. Two analysts add findings about Acme at
the same time, and the ref's linear mode makes them take turns. A Python
reader then joins, follows the ref space lazily, looks up
`[:entity/key "company:Acme"]`, and lists the findings about it from the few
index nodes those reads walk.

In the store half, a data agent publishes a 64 MiB table as the blob ref
`table` in a group that seals its nodes, and pins it on two content stores
with one signed pin of two copies. `store-a` holds the group's content key.
`store-b` is blind: it holds, serves, and proves the sealed nodes without
reading them. The data agent leaves. A late reader fetches the table from the
stores alone, reads it back to the writer's BLAKE3 identity, and challenges
both stores. Then `store-a` restarts over its pack files with one pack cut in
half, as a crash would leave it. It reconciles its held roots with `store-b`
by negentropy and fetches what it lost.

The libraries are `agentspaces-pdt` and `agentspaces-store`
(`srad/agentspaces-pdt2.md`). A `db` value is a DataScript database as a tree
of `dn` nodes in three indexes, and a transaction rides on the head as the
tail. A content store is a member that takes signed pins from the content
space, holds their roots, writes receipts, answers possession challenges
(tag 12), and reconciles with the other stores over the tree exchange.

```
cd examples/example-34-store-and-graph
mvn -q exec:java                 # the full sizes: a 64 MiB table, 300 companies, 900 findings
mvn -q exec:java -Dexec.args="7734 quick"
```

The run starts the graph peer and two analysts on loopback tcp from port 7734,
then the stores, the data agent, and the reader. The Python reader runs when
`agentspaces-python/tests/workers/pdt_db_worker.py` and an interpreter with
the client's `pdt` package are present (`$PYTHON`, or
`/tmp/aspace-venv4/bin/python`); otherwise the run says it skipped it. The
Python reader dials the graph peer alone, so the graph peer carries the RELAY
role and passes the reader's node requests to the analysts.

## What the steps show

| Step | What happens | Row |
|---|---|---|
| graph v0 | The graph peer folds every datom into the three indexes of the first version | P2-D-01, P2-D-03 |
| analysts | Two analysts add three to five findings each at once; every swap takes the head the one before it wrote, so the versions form one chain | PD-R-04, P2-D-03 |
| one transaction | A finding's datoms ride on the head in the tail: the new root and the tail node, under 2 KB | P2-Q-PRF-01 |
| Python lookup | The reader answers the lookup and the findings about Acme, the tail's included, from a few nodes of the graph | P2-D-04, P2-D-09, PD-Q-INT-01 |
| pin | The data agent pins the table for two copies; both stores take it and write receipts, one of them blind | P2-S-03, P2-S-08 |
| the writer leaves | The late reader fetches every byte from the two stores and reads back the writer's BLAKE3 identity | P2-Q-REL-02, P2-B-01 |
| challenges | The reader challenges each store with 16 picks; the blind store proves the sealed nodes without the key | P2-S-07, P2-Q-REL-03 |
| restart and reconcile | `store-a` restarts with a pack cut in half, finds the table incomplete, reconciles with `store-b`, and fetches what it lost | P2-K-03, P2-Q-REL-06 |

The stores follow the ref space lazily, so a store holds what a requester
pinned on it and nothing a ref happens to name. The reconcile is the only
repair the restarted store makes: it compares its held roots with the other
store's, and it fetches a root again only when it holds a pin for it.

## The tests

- `StoreAndGraphTest`: both halves over loopback tcp at the quick sizes (120
  companies, 400 findings, three findings per analyst, a 32 MiB table in 2 MiB
  packs). The graph test asserts one chain of swaps from v0, the first
  transaction's delta under 2 KB, one root on every Java peer and on the
  Python reader, the Python lookup and the findings about Acme, and the
  reader's nodes under a quarter of the graph's. The store test asserts two
  receipts with one blind, every byte of the late fetch from the stores, the
  identity read back, both challenges passed, the loss after the restart, the
  reconcile's repair, equal held sets, and a passed challenge after the
  repair. A missing Python interpreter skips the graph test.
- `PdtBindingsTest`: the store half on `dtls` with a 16 MiB table, every peer
  listening on the datagram binding alone, against the same rows.

A recorded run of `StoreAndGraphTest` on one laptop (2026-11-09): the graph of
5,540 datoms spanned 119 `dn` nodes of 545,594 bytes; the six analyst swaps
formed one chain, with deltas of 901 bytes for the first and 2,292 bytes for
the sixth, since the tail grows until a fold; the Python reader found Acme as
entity 1 with 138 findings about it and held 8 of the 119 nodes. The 32 MiB
table took two receipts, `store-b` blind; the late reader fetched it in 248 ms
from the two stores alone (15,925,483 and 17,693,111 bytes) in two round
trips; both challenges proved 16 of 16 picks; the restart cut pack 9 of 17
and lost 15 nodes; one reconcile round of 122 bytes named the table, and
`store-a` held it whole again 190 ms later, with the same nodes as `store-b`.
On `dtls` the 16 MiB table followed the same path, and the late fetch took
10.2 s.

A recorded run of `mvn -q exec:java` at the full sizes (2026-11-10): the
graph of 13,500 datoms spanned 278 `dn` nodes of 1,336,007 bytes; the ten
analyst swaps interleaved (analyst-2, analyst-1, analyst-1, analyst-2, ...)
and still formed one chain, with deltas of 902 to 3,414 bytes; the Python
reader found Acme with 315 findings about it and held 9 of the 278 nodes. The
64 MiB table (914 nodes, 67,237,424 sealed bytes) took two receipts; the late
reader fetched it in 235 ms from `store-a` (34,461,772 bytes) and `store-b`
(32,775,652 bytes); both challenges passed; the restart lost 25 nodes of
pack 9 of 17; and `store-a` held the table whole again 122 ms after one
reconcile round of 122 bytes. At the end of the run a tick of the restarted
`store-a` logs a `ClosedByInterruptException` while the run closes it: its
ticker is stopped in the middle of a pack read, after every figure is taken.
