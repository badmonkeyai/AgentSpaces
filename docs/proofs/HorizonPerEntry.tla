--------------------------- MODULE HorizonPerEntry ---------------------------
(***************************************************************************)
(* The per-entry tombstone horizon of a replicated space (SPEC section     *)
(* 7.3 under the hint wire.horizon=1). See per-entry-horizon.md beside     *)
(* this file for the argument in prose.                                    *)
(*                                                                         *)
(* Three replicas hold one entry. Replica 1 is the issuer: it writes the   *)
(* entry, renews it and cancels it. Replicas 2 and 3 may claim it and      *)
(* complete it. A copy travels as a whole state through a set of messages *)
(* that loses, reorders and delays them. Each replica reads its own clock, *)
(* and any two clocks differ by at most MAX_DRIFT. A replica collects a    *)
(* state once that state's horizon has passed on its own clock, and its    *)
(* door refuses a copy whose horizon has passed on that same clock.        *)
(*                                                                         *)
(* A partition needs no variable of its own: nothing ever forces a Send,   *)
(* so a run in which no copy crosses between two sides for any length of  *)
(* time is already a run of this model.                                    *)
(*                                                                         *)
(* NOT YET MODEL-CHECKED. No TLA+ tools were installed when this was       *)
(* written. The Java test HorizonModelCheckTest in agentspaces-space       *)
(* enumerates the same state space breadth-first and checks the same       *)
(* invariants; per-entry-horizon.md gives the TLC command and the state    *)
(* counts TLC should report.                                               *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    MAX_TIME,     \* bound on every local clock, which keeps the state space finite
    MAX_DRIFT,    \* bound on the difference between any two local clocks
    MAX_LEASE,    \* the longest extension a write or renewal asks for; the maxLease of the horizon
    GRACE,        \* the five-minute grace of SPEC section 7.3, in the same units
    MAX_STAMP,    \* bound on the lease writes (a shortening writer could otherwise renew for ever)
    MAX_MSGS,     \* bound on the copies in flight
    PER_ENTRY,    \* TRUE: every member advertises wire.horizon=1
    SHORTENING,   \* break 1: a renewal sets now + extension even when that is shorter
    TWO_ISSUERS,  \* break 2: the issuer's identity renews from replicas 1 and 2
    DRIFT_FLOOR   \* a withdrawal keeps at least MAX_DRIFT (SPEC 7.3, v0.1.35); FALSE only to show what it is for

ASSUME /\ MAX_TIME \in Nat /\ MAX_DRIFT \in Nat
       /\ MAX_LEASE \in Nat \ {0} /\ GRACE \in Nat \ {0}
       /\ MAX_STAMP \in Nat \ {0} /\ MAX_MSGS \in Nat
       /\ PER_ENTRY \in BOOLEAN /\ SHORTENING \in BOOLEAN /\ TWO_ISSUERS \in BOOLEAN
       /\ DRIFT_FLOOR \in BOOLEAN

Replicas == 1..3
Issuer == 1
Renewers == IF TWO_ISSUERS THEN {1, 2} ELSE {1}
Takers == Replicas \ {Issuer}

(* A state of the entry as a replica holds it: withdrawn (rm), completed   *)
(* (cp), the lease expiry (exp) and the stamp of the lease write that set  *)
(* it (stp). Stamps start at 1, so stp = 0 means the replica holds nothing.*)
States == [rm : BOOLEAN, cp : BOOLEAN, exp : 0..(MAX_TIME + MAX_LEASE), stp : 0..MAX_STAMP]
NoState == [rm |-> FALSE, cp |-> FALSE, exp |-> 0, stp |-> 0]

VARIABLES
    st,       \* st[r]: the state replica r holds, NoState when none
    gone,     \* gone[r]: every state replica r has collected
    knew,     \* knew[r]: replica r has held the entry withdrawn
    claimed,  \* claimed[r]: replica r holds a take claim
    clock,    \* clock[r]: replica r's clock
    msgs,     \* the copies in flight, each [to |-> r, st |-> s]
    L,        \* the expiry the issuer's replica held when it cancelled; 0 before
    ctr       \* the number of lease writes so far; the next one takes stamp ctr + 1

vars == <<st, gone, knew, claimed, clock, msgs, L, ctr>>

Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b

Held(r) == st[r].stp > 0
Present(s) == ~s.rm /\ ~s.cp
Live(r) == Held(r) /\ Present(st[r]) /\ st[r].exp > clock[r]

(* The CRDT merge: removal and completion are monotone, and the lease is   *)
(* a last-writer-wins register ordered by its stamp.                       *)
Merge(a, b) == [rm |-> a.rm \/ b.rm, cp |-> a.cp \/ b.cp,
                exp |-> IF a.stp >= b.stp THEN a.exp ELSE b.exp,
                stp |-> Max(a.stp, b.stp)]

(* The horizon rule under test: what a state keeps past its lease expiry. *)
(* A withdrawal keeps at least MAX_DRIFT under either rule (the drift     *)
(* floor, owner decision 22): when one replica collects at L + MAX_DRIFT  *)
(* by its clock, every other clock reads L or later.                      *)
(* A state both withdrawn and completed keeps the completed horizon and  *)
(* the floor too, so it never goes before a copy withdrawn alone.         *)
Floor(k) == IF DRIFT_FLOOR THEN Max(k, MAX_DRIFT) ELSE k
WithdrawnKeep == Floor(IF PER_ENTRY THEN GRACE ELSE 2 * MAX_LEASE)
Keep(s) == IF s.cp THEN (IF s.rm THEN Floor(2 * MAX_LEASE) ELSE 2 * MAX_LEASE)
           ELSE IF s.rm THEN WithdrawnKeep
           ELSE Min(2 * MAX_LEASE, GRACE)

(* Collection and the door read the same rule and the same local clock.   *)
Collectable(s, t) == (~Present(s) \/ s.exp <= t) /\ s.exp + Keep(s) <= t

-----------------------------------------------------------------------------

TypeOK == /\ st \in [Replicas -> States]
          /\ gone \in [Replicas -> SUBSET States]
          /\ knew \in [Replicas -> BOOLEAN]
          /\ claimed \in [Replicas -> BOOLEAN]
          /\ clock \in [Replicas -> 0..MAX_TIME]
          /\ msgs \subseteq [to : Replicas, st : States]
          /\ Cardinality(msgs) <= MAX_MSGS
          /\ L \in 0..(MAX_TIME + MAX_LEASE)
          /\ ctr \in 0..MAX_STAMP

Init == /\ st = [r \in Replicas |-> NoState]
        /\ gone = [r \in Replicas |-> {}]
        /\ knew = [r \in Replicas |-> FALSE]
        /\ claimed = [r \in Replicas |-> FALSE]
        /\ clock = [r \in Replicas |-> 0]
        /\ msgs = {}
        /\ L = 0
        /\ ctr = 0

(* The issuer writes the entry once. *)
Write == /\ ctr = 0
         /\ \E ext \in 1..MAX_LEASE :
               st' = [st EXCEPT ![Issuer] =
                        [rm |-> FALSE, cp |-> FALSE, exp |-> clock[Issuer] + ext, stp |-> 1]]
         /\ ctr' = 1
         /\ UNCHANGED <<gone, knew, claimed, clock, msgs, L>>

(* SPEC section 7.2: only the issuer renews, only an entry present and     *)
(* unexpired on its replica, and the new expiry is                         *)
(* max(current, now + extension). A renewal that changes nothing sends     *)
(* nothing, so it is no step here.                                          *)
Renew(r) == /\ r \in Renewers
            /\ Live(r)
            /\ ctr < MAX_STAMP
            /\ \E ext \in 1..MAX_LEASE :
                  LET asked == clock[r] + ext
                      new == IF SHORTENING THEN asked ELSE Max(st[r].exp, asked)
                  IN /\ new # st[r].exp
                     /\ st' = [st EXCEPT ![r].exp = new, ![r].stp = ctr + 1]
            /\ ctr' = ctr + 1
            /\ UNCHANGED <<gone, knew, claimed, clock, msgs, L>>

(* The issuer withdraws its present entry. A cancel writes no lease, so  *)
(* the withdrawn state carries the issuer's last lease: L.                 *)
Cancel == /\ Held(Issuer)
          /\ Present(st[Issuer])
          /\ L = 0
          /\ st' = [st EXCEPT ![Issuer].rm = TRUE]
          /\ knew' = [knew EXCEPT ![Issuer] = TRUE]
          /\ L' = st[Issuer].exp
          /\ UNCHANGED <<gone, claimed, clock, msgs, ctr>>

Claim(r) == /\ r \in Takers
            /\ Live(r)
            /\ ~claimed[r]
            /\ claimed' = [claimed EXCEPT ![r] = TRUE]
            /\ UNCHANGED <<st, gone, knew, clock, msgs, L, ctr>>

(* The holder completes; a completion writes no lease either. *)
Complete(r) == /\ claimed[r]
               /\ Held(r)
               /\ Present(st[r])
               /\ st' = [st EXCEPT ![r].cp = TRUE]
               /\ UNCHANGED <<gone, knew, claimed, clock, msgs, L, ctr>>

(* The sweep: a state whose horizon passed on this replica's clock goes,   *)
(* with any claim the replica held on it.                                  *)
Collect(r) == /\ Held(r)
              /\ Collectable(st[r], clock[r])
              /\ gone' = [gone EXCEPT ![r] = @ \cup {st[r]}]
              /\ st' = [st EXCEPT ![r] = NoState]
              /\ claimed' = [claimed EXCEPT ![r] = FALSE]
              /\ UNCHANGED <<knew, clock, msgs, L, ctr>>

(* A clock moves on, never more than MAX_DRIFT ahead of another, never back. *)
Tick(r) == /\ clock[r] < MAX_TIME
           /\ \A s \in Replicas : clock[r] + 1 - clock[s] <= MAX_DRIFT
           /\ clock' = [clock EXCEPT ![r] = @ + 1]
           /\ UNCHANGED <<st, gone, knew, claimed, msgs, L, ctr>>

(* Gossip and anti-entropy: any replica sends what it holds to any other, *)
(* at any time, while there is room in flight.                             *)
Send(r, s) == LET m == [to |-> s, st |-> st[r]] IN
              /\ r # s
              /\ Held(r)
              /\ m \notin msgs
              /\ Cardinality(msgs) < MAX_MSGS
              /\ msgs' = msgs \cup {m}
              /\ UNCHANGED <<st, gone, knew, claimed, clock, L, ctr>>

(* Delivery in any order, at the door of the receiving replica. *)
Deliver(m) == /\ msgs' = msgs \ {m}
              /\ IF Collectable(m.st, clock[m.to])
                    THEN UNCHANGED <<st, knew>>
                    ELSE LET r == m.to
                             new == IF Held(r) THEN Merge(st[r], m.st) ELSE m.st
                         IN /\ st' = [st EXCEPT ![r] = new]
                            /\ knew' = [knew EXCEPT ![r] = @ \/ new.rm]
              /\ UNCHANGED <<gone, claimed, clock, L, ctr>>

Lose(m) == /\ msgs' = msgs \ {m}
           /\ UNCHANGED <<st, gone, knew, claimed, clock, L, ctr>>

Next == \/ Write
        \/ Cancel
        \/ \E r \in Replicas : Renew(r) \/ Claim(r) \/ Complete(r) \/ Collect(r) \/ Tick(r)
        \/ \E r, s \in Replicas : Send(r, s)
        \/ \E m \in msgs : Deliver(m) \/ Lose(m)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

(* Every copy of the entry: what each replica holds and what is in flight. *)
Copies == {st[r] : r \in {x \in Replicas : Held(x)}} \cup {m.st : m \in msgs}

(* Merging c into k changes nothing: c says nothing k does not. *)
Subsumed(c, k) == (c.rm => k.rm) /\ (c.cp => k.cp) /\ c.stp <= k.stp

(* The safety claim of section 3.5: a replica that held the entry          *)
(* withdrawn never again holds it present and unexpired by its own clock.  *)
NoResurrection == \A r \in Replicas : knew[r] => ~Live(r)

(* A collected entry's re-offer is refused at every door: each copy that a *)
(* state a replica collected subsumes is refused there now.                *)
RefusedAtEveryDoor ==
    \A r \in Replicas : \A k \in gone[r] : \A c \in Copies :
        Subsumed(c, k) => Collectable(c, clock[r])

(* The lemma: once withdrawn, no copy anywhere carries an expiry past L.   *)
LeaseBound == L > 0 => \A c \in Copies : c.exp <= L

(* A replica has collected a withdrawal (not a completion).               *)
CollectedWithdrawal == \E q \in Replicas : \E k \in gone[q] : k.rm /\ ~k.cp

(* A step property: once some replica has collected a withdrawal, no step  *)
(* turns any replica live, one that never heard of the withdrawal          *)
(* included. It holds with DRIFT_FLOOR (every configuration but the two   *)
(* NoFloor ones). Without the floor it is expected to FAIL once MAX_DRIFT  *)
(* exceeds what a withdrawal keeps (HorizonPerEntryDriftNoFloor.cfg,       *)
(* HorizonPerEntryLegacyDriftNoFloor.cfg): a replica whose clock trails    *)
(* can be handed an old present copy that is unexpired by its own clock   *)
(* after every tombstone is gone. See per-entry-horizon.md, "A trailing   *)
(* clock, and the drift floor".                                            *)
NoLateLive == [][\A r \in Replicas : (CollectedWithdrawal /\ ~Live(r)) => ~Live(r)']_vars

(* Not claimed by section 3.5, and expected to FAIL under a partition: a   *)
(* replica that collected a completion never holds the entry live again.   *)
(* See per-entry-horizon.md, "Why completed entries keep 2 x maxLease".    *)
CompletionStaysDone == \A r \in Replicas : (\E k \in gone[r] : k.cp) => ~Live(r)

=============================================================================
