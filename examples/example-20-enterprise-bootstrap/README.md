# Example 20: enterprise bootstrap

Three seeds behind one DNS name; a worker that knows the name and nothing
else. The worker's bootstrap chain expands the name to every address and
dials them all, re-asks the chain when its view of the group is empty and
the hint has aged, and keeps the trace of what each locator answered for the
console's traffic panel.

```
mvn -q exec:java -Dexec.args="seeds.corp.example:7500 7500 7501"
```

The name's A records must point at this host (two seeds on the loopback
ports). The flow test runs the same with an in-memory resolver. For the
records a beacon prints, SRV and DNS-SD locators, and the `agentspaces.bootstrap.*`
properties, see the `agentspaces-bootstrap` and `agentspaces-beacon` projects
and `srad/discovery-bootstrap-customization.md`.
