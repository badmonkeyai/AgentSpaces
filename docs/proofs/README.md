# Proofs and models

Written for: contributors and reviewers who change replication rules and want the argument behind one.

Each proof here is a note in plain prose, with a TLA+ model beside it and a Java check in the module it is about.

| File | What it holds |
|---|---|
| [Why the per-entry horizon is safe](per-entry-horizon.md) | The claim, assumptions, argument and break conditions for collecting a withdrawn tombstone at L + max(5 min, MAX_DRIFT) under `wire.horizon=1`, with how it was checked. |
| [HorizonPerEntry.tla](HorizonPerEntry.tla) | The TLA+ model of that horizon: three replicas, one entry, lossy and reordering gossip, clock drift. Not yet model-checked. |
| [HorizonPerEntry.cfg](HorizonPerEntry.cfg) | TLC configuration of the main run: every invariant must hold. |
| [HorizonPerEntryReorder.cfg](HorizonPerEntryReorder.cfg) | Two copies in flight, so deliveries reorder, with a renewed entry collected while an older copy travels: every invariant must hold. |
| [HorizonPerEntryDrift.cfg](HorizonPerEntryDrift.cfg) | Clocks twice the grace apart, with the drift floor: every invariant and the step property `NoLateLive` must hold. |
| [HorizonPerEntryDriftNoFloor.cfg](HorizonPerEntryDriftNoFloor.cfg) | The same without the drift floor: the invariants hold, and TLC must find `NoLateLive` violated, the window the floor closes. |
| [HorizonPerEntryLegacyDrift.cfg](HorizonPerEntryLegacyDrift.cfg) | The legacy rule with 2 x maxLease shorter than the drift, with the floor: every invariant and `NoLateLive` must hold. |
| [HorizonPerEntryLegacyDriftNoFloor.cfg](HorizonPerEntryLegacyDriftNoFloor.cfg) | The same without the floor: TLC must find `NoLateLive` violated, the legacy rule's own window. |
| [HorizonPerEntryShortening.cfg](HorizonPerEntryShortening.cfg) | Break: a writer that shortens its lease. TLC must find a violation. |
| [HorizonPerEntryTwoIssuers.cfg](HorizonPerEntryTwoIssuers.cfg) | Break: one identity renewing from two nodes. TLC must find a violation. |
| [HorizonPerEntryCompletion.cfg](HorizonPerEntryCompletion.cfg) | Completions under the legacy rule: TLC must find a violation, which shows a completion's horizon rests on delivery. |
