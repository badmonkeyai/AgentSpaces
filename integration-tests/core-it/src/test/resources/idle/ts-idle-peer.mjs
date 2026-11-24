// One idle TypeScript member of IdleCostIT (srad/agentspaces-event-loop.md EL-M-03): an agentspaces-typescript peer that
// dials one Java seed, as the client's quick start does, and then does nothing. It runs no SWIM prober of its own, so it
// stays in a Java member's view only while that member hears from it (EL-P-02). Read-only use of the TypeScript client.
//
//   node ts-idle-peer.mjs <agentspaces-typescript dir> <founding text> <seed port>
//
// One command per line on stdin, one line on stdout: "view" prints "VIEW <peer,...>", "quit" exits.
import { createInterface } from "node:readline";

const [base, founding, port] = process.argv.slice(2);
const { Peer } = await import(base + "/build/src/peer.js");
const { Identity, groupIdFromFounding } = await import(base + "/build/src/identity.js");
await import(base + "/build/src/node/index.js");

const identity = Identity.generate();
const peer = new Peer(identity, groupIdFromFounding(founding));
await peer.connect("127.0.0.1", Number(port));
console.log(`READY ${identity.peerId} ${port} ${process.pid}`);

const lines = createInterface({ input: process.stdin });
lines.on("line", (line) => {
  const command = line.trim();
  if (command === "view") {
    console.log(`VIEW ${[...peer.members.keys()].filter((p) => p !== identity.peerId).join(",")}`);
  } else if (command === "quit") {
    console.log("BYE");
    peer.close();
    process.exit(0);
  } else {
    console.log(`UNKNOWN ${command}`);
  }
});
lines.on("close", () => {
  peer.close();
  process.exit(0);
});
