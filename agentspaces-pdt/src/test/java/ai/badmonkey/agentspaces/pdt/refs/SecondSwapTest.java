/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.BlockStore;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A ref's second swap with a reader polling history and versions (the PDT second-swap ticket of 2026-11-15, phase 9b of
 * the replication-by-entry program): the writer swaps a four-key map of long text twice, a gossip minute apart, while a
 * second peer polls history(12) and at(root). On the released 0.6.0 a node then failed verification on one peer
 * ("bytes hash to X, which does not match the claimed CID"), every tick after that failed, and the reader could stop at
 * no head. Every store here verifies each write and records where a write that does not hash to its CID came from.
 */
class SecondSwapTest {

    /** A backend that checks each write of a CID-named node against its CID and keeps the stack of any that fails. */
    static final class Watching implements BlockStore {
        final BlockStore inner = new MemoryBlockStore();
        final List<String> bad = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Optional<byte[]> get(String cid, long nowMillis) {
            return inner.get(cid, nowMillis);
        }

        @Override
        public boolean put(String cid, byte[] bytes, long nowMillis) {
            if (cid.startsWith("baf")) {
                String actual = Cids.of(bytes, Cids.parse(cid).codec());
                if (!actual.equals(cid)) {
                    StringWriter where = new StringWriter();
                    new Throwable("write of " + cid + " whose bytes hash to " + actual).printStackTrace(new PrintWriter(where));
                    bad.add(where.toString());
                }
            }
            return inner.put(cid, bytes, nowMillis);
        }

        @Override
        public long remove(String cid) {
            return inner.remove(cid);
        }

        @Override
        public Set<String> cids() {
            return inner.cids();
        }

        @Override
        public long lastAccessMillis(String cid) {
            return inner.lastAccessMillis(cid);
        }

        @Override
        public int size() {
            return inner.size();
        }

        @Override
        public long bytesUsed() {
            return inner.bytesUsed();
        }

        @Override
        public String kind() {
            return "watching-memory";
        }

        /** The CIDs this backend holds whose bytes no longer hash to them. */
        List<String> unverifiable() {
            List<String> out = new ArrayList<>();
            for (String cid : inner.cids()) {
                if (cid.startsWith("baf")) {
                    byte[] bytes = inner.get(cid, 0).orElseThrow();
                    if (!Cids.of(bytes, Cids.parse(cid).codec()).equals(cid)) {
                        out.add(cid);
                    }
                }
            }
            return out;
        }
    }

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    /** The report's value: a long text with non-ASCII characters, repeated n times. */
    private static String text(int round, int n) {
        return ("Round " + round + ": favour assets tagged java, clojure — they overlap what targets care about.\n"
                + "Why: tag/interest overlap é… ").repeat(n);
    }

    @Test
    void aSecondSwapUnderAPollingReaderKeepsEveryNodeVerifiableOnBothPeers() throws Exception {
        Watching writerBlocks = new Watching();
        Watching readerBlocks = new Watching();
        RefFleet.Peer w = fleet.newPeer("w", 1, RefFleet.Spec.defaults().store(new NodeStore(writerBlocks, fleet.clock)));
        RefFleet.Peer r = fleet.newPeer("r", 2, RefFleet.Spec.defaults().store(new NodeStore(readerBlocks, fleet.clock)), "w");
        Ref<PMap<String, String>> onW = w.pdt().mapRef("strategy", RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onR = r.pdt().mapRef("strategy", RefFleet.STR, RefFleet.STR);
        List<String> readerFailures = new ArrayList<>();
        Runnable poll = () -> {
            try {
                for (var version : onR.history(12)) {
                    onR.at(version.root());
                }
            } catch (RuntimeException e) {
                readerFailures.add(String.valueOf(e.getMessage()));
            }
        };
        String last = null;
        for (int round = 1; round <= 2; round++) {
            int i = round;
            last = onW.swap(m -> m.assoc("today", text(i, 6)).assoc("next-week", text(i, 4))
                    .assoc("past-week", text(i, 3)).assoc("past-month", text(i, 3))).root();
            // a gossip minute of fleet time (240 ticks of the fleet clock, not of the wall), the reader polling on
            // every tick as the report's reader did
            fleet.runFor(Duration.ofSeconds(60), () -> {
                poll.run();
                return false;
            });
        }
        String head = last;
        boolean converged = fleet.runUntil(Duration.ofSeconds(60),
                () -> onR.heads().roots().equals(List.of(head)) && r.store().complete(head));

        assertThat(writerBlocks.bad).as("writes on the writer whose bytes do not hash to their CID").isEmpty();
        assertThat(readerBlocks.bad).as("writes on the reader whose bytes do not hash to their CID").isEmpty();
        assertThat(writerBlocks.unverifiable()).as("nodes the writer holds that fail verification").isEmpty();
        assertThat(readerBlocks.unverifiable()).as("nodes the reader holds that fail verification").isEmpty();
        assertThat(readerFailures).as("the reader's history and at reads").isEmpty();
        assertThat(converged).as("the reader holds the second version as the one head").isTrue();
    }
}
