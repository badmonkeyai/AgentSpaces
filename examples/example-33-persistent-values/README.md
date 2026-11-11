# Example 33: values that change across a fleet

A curator holds two persistent values as refs: `facts`, a map of 10,000 facts
of about 2 MiB, and `table`, a blob of generated rows. Three analysts revise
facts at the same time, the curator edits, appends to, and inserts into the
table, a late auditor fetches the table while an analyst dies, and the `plans`
space grows a reduce accumulator by delta. A Python reader joins the same group
over tcp and derefs the same roots. Every update prints what it cost: the
value's node bytes, the delta the writer produced, the bytes each reader
received and whether its delta came inline on the head or by a tree fetch, the
round trips of the same delta measured by a meter peer, and each reader's merge
conflicts.

The library is `agentspaces-pdt` (SPEC annex H): a value is a
tree of content-addressed nodes, a version moves as the nodes a receiver lacks,
and a ref is the set of live `PRef` heads of one name in the `pdt` space. The
nodes travel on the content swarm's tree exchange (`agentspaces-content`,
tags 8 to 11), and a space that opts in through
`ReplicatedSpace.Builder.bulk(pdt.payloads(), threshold)` escalates its large
payloads as trees, so a growing accumulator ships only its changed paths.

```
cd examples/example-33-persistent-values
mvn -q exec:java                 # the sizes of §7.3: a 256 MiB table, 50 plan steps
mvn -q exec:java -Dexec.args="7733 quick"
```

The run starts a curator, three analysts, and a meter on loopback tcp, then the
auditor for step 4. The Python reader runs when
`agentspaces-python/tests/workers/pdt_worker.py` and an interpreter with the
client's `pdt` package are present (`$PYTHON`, or
`/tmp/aspace-venv4/bin/python`); otherwise the run says it skipped it.

## What the steps show

| Step | What happens |
|---|---|
| 1 facts v0 | The curator builds the map; the analysts fetch its tree, the meter counts the round trips without a base |
| 1 facts one key | One key changes; every reader takes a few KiB inline on the head, and no tree node crosses a pipe |
| 2 round n | Three analysts swap ten revisions each; every reader, Python included, derefs one root from the merged heads |
| 2 curator folds heads | One swap persists the merge with every head as a base |
| 3 table v0, edit, append, insert | The blob and its edits by delta against the held base |
| 4 | The auditor fetches the table cold while analyst-3 dies part way |
| 5 | The `plans` space escalates a 3 MiB accumulator over 50 steps; each step ships about the delta |

The meter is a peer without refs: it holds the previous root and fetches each
new root explicitly, so its `TreeFetch.Progress` reports the round trips the
test bounds (two with the base held, the tree's depth plus one without). The
analysts' costs come from their facades' counters and the growth of their node
stores.

## The tests

- `PersistentValuesTest`: the composition over loopback tcp at the quick sizes
  (a 32 MiB table, three rounds, twelve plan steps), asserting the byte and
  round-trip bounds of the steps above at their as-built values, equal roots on every Java reader, the Python
  reader's root when it is present, the cold fetch's completion after the
  kill, and the delta bound per plan step.
- `PdtBindingsTest`: steps 1 to 3 on `dtls` with one holder behind a relay.
  The curator and analyst-1 listen on dtls; a relay with the
  RELAY role listens on dtls and tcp; analyst-2 is dial-only with a tcp
  transport alone and seeds the relay's tcp port, so it can dial no dtls
  listener and every head, inline delta, and tree node it exchanges crosses
  the relay on a pinned path.

A recorded run of `PersistentValuesTest` on one laptop (2026-11-08): the
one-key change shipped 4,764 bytes inline to each of three readers in two
round trips with zero pipe nodes; a one-byte edit of the 32 MiB table shipped
92,644 bytes (an 87,644-byte chunk plus its path) in two round trips; 1 MiB
appends shipped 1,053,137 to 1,067,575 bytes; 100-byte inserts shipped 72,508
to 121,898 bytes (median 82,393); the auditor's cold fetch completed in two
round trips with 36,724,128 bytes received for a 36,700,660-byte table after
analyst-3 died; every plan step's readers received exactly what the curator
wrote; and the Python reader's root equalled the Java readers' root after
every round, with three concurrent heads merged on read, and after the fold.
