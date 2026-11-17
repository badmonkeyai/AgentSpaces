# agentspaces-spoilers

Test only. Never deployed, not in the BOM, and no shipped module may depend on it (a guard test fails the build if one
does). It is the Java harness of `srad/spoiler-peers.md`: a spoiler is a real member whose traffic follows a script, so
a race that load found at a rate reproduces every run.

## Writing a scenario

1. Build a fleet on `SimNetwork` (`RobustnessFleet` from the peering test-jar, `RefFleet` from the pdt test-jar), as
   the module's own tests do. Step writers on a barrier (`RefFleet.stepOnBarrier`) when the harness itself would
   otherwise let the scheduler shape the workload.
2. Write a `Script`: rules of trigger, action, release and ceiling.

   ```java
   Script s = Script.of("w2").seed(9400)
       .rule("hold-late-version")
           .when(Trigger.frame(Envelope.Kind.PIPE_DATA).to("l").item(ItemKind.NODE).id(xRoot))
           .act(Action.hold()).always()
           .until(Trigger.counter("the joiner read once", observed::get, 1))
           .ceiling(Ceiling.periods(800))
       .build();
   ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, Duration.ofMillis(250),
           PeerNode.ChannelAuth.SESSION, s);
   fleet.afterTick.add(runner::tick);
   ```

3. Trigger on effects an honest member shows (a frame or a decoded item on a link, a space event, a counter, fleet
   time), never on a wall-clock sleep. Frame triggers see every link of the fabric.
4. Use only the actions the link's channel-auth mode allows (§3.4.2 of the SRAD): a spoiler cannot duplicate or
   reorder what an honest member sends it. The runner refuses such a rule at load.
5. End with `runner.finish()` (it fails a rule that never fired, a hold whose release never fired, a passed ceiling)
   and the oracle: `FleetInvariants.baseline(probe)` before the script, `assertHolds(report.expectedDrops(), classA)`
   after. Run the same scenario once with `Script.honest(...)` as its negative control.

A failing run prints the seed (`-Dspoiler.seed` replays it), the rule, the last 32 frames and events, and the firing
sequence.

## The worked example

`SpoilerConcurrentHeadsTest` is SC-I4, the acceptance of phase 1: the negative control and hypothesis 2 on the
fleet of `ConcurrentHeadsTest` with its writers on a barrier, hypothesis 1 and its control on a fork built so the
joiner needs chain nodes, and the third hypothesis in its smallest fork, which found the late-joiner defect in
`Refs.heads`. `SpoilerLinearTakeTest` is the same defect on the write path of a linear swap. The barrier fleet is not
deterministic from its seed (random entry ids, writers on their own threads), so its tests assert relations inside one
run; the constructed forks are the ones to copy.
