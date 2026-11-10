/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.examples.storegraph;

import ai.badmonkey.agentspaces.agent.content.StoreReceipt;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.examples.storegraph.StoreGraphFleet.Peer;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import ai.badmonkey.agentspaces.store.ContentStore;
import ai.badmonkey.agentspaces.store.StorePolicy;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

/**
 * The store half of example 34 (srad/agentspaces-pdt2.md §7.3, P2-O-03; groups S and K, scenario 2 of §2.6): in a
 * group in the sealed-nodes mode, a data agent publishes a table as the blob ref {@value #REF} and asks two content
 * stores to hold it with one signed pin of two copies. {@code store-a} holds the group's content key;
 * {@code store-b} is blind and holds, serves, and proves the sealed nodes without reading them. The data agent
 * leaves; a late reader fetches the table from the stores alone, reads it back to the writer's BLAKE3 identity,
 * and challenges both stores. Then {@code store-a} restarts over its pack files with one pack cut in half, as a
 * crash would leave it, reconciles its held roots with {@code store-b} by negentropy, and fetches what it lost.
 */
public final class StoreHalf {

    /** The vault group's founding text. */
    public static final String FOUNDING = "store-and-graph-example-vault-v1";

    /** The blob ref the table lives on. */
    public static final String REF = "table";

    private static final Duration CONVERGE = Duration.ofSeconds(90);
    private static final Duration FETCH = Duration.ofSeconds(240);

    /**
     * The sizes of a run.
     *
     * @param blobBytes    the table's size, at most 64 MiB (the owner's ruling of 2026-11-09, §10.16)
     * @param maxPackBytes the pack size of the stores, so the table spans many packs and a restart loses one
     */
    public record Sizes(long blobBytes, long maxPackBytes) {

        public Sizes {
            if (blobBytes <= 0 || blobBytes > 64L << 20) {
                throw new IllegalArgumentException("the table is 1 byte to 64 MiB, got " + blobBytes);
            }
        }

        /** The demo: a 64 MiB table in 4 MiB packs. */
        public static Sizes full() {
            return new Sizes(64L << 20, 4L << 20);
        }

        /** The acceptance test: a 32 MiB table in 2 MiB packs. */
        public static Sizes quick() {
            return new Sizes(32L << 20, 2L << 20);
        }

        /** The dtls repeat: a 16 MiB table in 1 MiB packs. */
        public static Sizes bindings() {
            return new Sizes(16L << 20, 1L << 20);
        }
    }

    /** A store's receipt as the data agent read it. */
    public record Receipt(String store, boolean blind, long bytes) {
    }

    /**
     * The late reader's fetch of the table after the writer left.
     *
     * @param complete      whether the fetch completed
     * @param millis        its wall time
     * @param roundTrips    its round-trip chain
     * @param bytesBySource the bytes each source served, by peer name
     * @param identity      the BLAKE3 identity of the bytes the reader read back
     */
    public record Cold(boolean complete, long millis, int roundTrips, Map<String, Long> bytesBySource, String identity) {
    }

    /** One possession challenge (P2-S-07). */
    public record Challenge(String store, boolean passed, int picks, int proven, long millis, String reason) {
        static Challenge of(String store, Content.ChallengeResult r) {
            return new Challenge(store, r.passed(), r.picks(), r.proven(), r.elapsed(), r.reason());
        }
    }

    /**
     * The restart of {@code store-a} from a truncated pack and its reconcile with {@code store-b}.
     *
     * @param packs                the packs store-a held before the restart
     * @param truncatedPack        the pack cut in half
     * @param lostNodes            the nodes of the table the restart lost
     * @param completeAfterRestart whether store-a held the table whole after the restart
     * @param have                 roots store-a holds and store-b lacks, by the reconcile
     * @param need                 roots store-b holds and store-a lacks
     * @param repairing            the pinned roots store-a fetches again
     * @param rounds               the reconcile's rounds
     * @param bytes                the reconcile's bytes
     * @param repaired             whether store-a held the table whole again
     * @param repairMillis         from the reconcile to the repair
     * @param equalSets            whether both stores hold the same nodes under the root
     * @param challenge            a challenge of store-a after the repair
     */
    public record Restart(int packs, long truncatedPack, int lostNodes, boolean completeAfterRestart, int have, int need,
                          List<String> repairing, int rounds, long bytes, boolean repaired, long repairMillis, boolean equalSets,
                          Challenge challenge) {
    }

    /**
     * The run's figures.
     *
     * @param binding    the binding every peer listened on
     * @param blobBytes  the table's size
     * @param root       the table's root
     * @param identity   the writer's BLAKE3 identity of the table
     * @param treeNodes  the nodes of the table's tree, its outboard left out
     * @param treeBytes  their sealed bytes
     * @param receipts   the stores' receipts
     * @param cold       the late reader's fetch
     * @param challenges the late reader's challenges, store-a then store-b
     * @param restart    the restart and reconcile
     */
    public record Report(String binding, long blobBytes, String root, String identity, int treeNodes, long treeBytes,
                         List<Receipt> receipts, Cold cold, List<Challenge> challenges, Restart restart) {

        public String summary() {
            StringBuilder out = new StringBuilder();
            out.append(String.format("store half on %s: a %d MiB table, root %s, %d nodes of %d sealed bytes, identity %s%n",
                    binding, blobBytes >> 20, root, treeNodes, treeBytes, identity));
            for (Receipt r : receipts) {
                out.append(String.format("  receipt from %s: %d bytes%s%n", r.store(), r.bytes(), r.blind() ? ", blind" : ""));
            }
            out.append(String.format("  late reader after the writer left: %s in %d ms, %d round trips, by source %s, identity %s%n",
                    cold.complete() ? "complete" : "INCOMPLETE", cold.millis(), cold.roundTrips(), cold.bytesBySource(),
                    cold.identity().equals(identity) ? "equal" : "DIFFERS"));
            for (Challenge c : challenges) {
                out.append(String.format("  challenge of %s: %s, %d of %d picks proven in %d ms%n", c.store(),
                        c.passed() ? "passed" : "FAILED " + c.reason(), c.proven(), c.picks(), c.millis()));
            }
            Restart r = restart;
            out.append(String.format("  store-a restarted with pack %d of %d cut in half: %d nodes lost, %s%n", r.truncatedPack(),
                    r.packs(), r.lostNodes(), r.completeAfterRestart() ? "still complete" : "the table incomplete"));
            out.append(String.format("  reconcile with store-b: %d have, %d need, repairing %s, %d rounds, %d bytes; %s in %d ms;"
                            + " held sets %s; challenge %s%n", r.have(), r.need(), r.repairing(), r.rounds(), r.bytes(),
                    r.repaired() ? "repaired" : "NOT REPAIRED", r.repairMillis(), r.equalSets() ? "equal" : "DIFFER",
                    r.challenge().passed() ? "passed" : "FAILED"));
            return out.toString();
        }
    }

    private StoreHalf() {
    }

    /** A store's policy in the example: room for the table, open to every member of the group. */
    static StorePolicy policy(boolean blind) {
        return StorePolicy.of(1L << 30).acceptRequesters(StorePolicy.Accept.OPEN).blind(blind);
    }

    /**
     * Runs the store half.
     *
     * @param dir     a directory for the peers' stores and the table
     * @param binding {@code tcp}, {@code tls}, or {@code dtls}: what every peer listens on
     * @param ports   a fresh port of the binding per call
     * @param sizes   the sizes
     * @param out     where the per-step lines go
     * @return the figures
     * @throws Exception when a step fails
     */
    public static Report run(Path dir, String binding, IntSupplier ports, Sizes sizes, PrintStream out) throws Exception {
        GroupKey key = GroupKey.generate();
        int storeAPort = ports.getAsInt();
        String seed = binding.equals("tcp") ? "127.0.0.1:" + storeAPort : binding + "://127.0.0.1:" + storeAPort;
        PeerIdentity storeAIdentity = PeerIdentity.generate();
        StoreGraphFleet.Spec storeASpec = spec(binding, storeAPort, List.of(), dir.resolve("store-a"), sizes)
                .identity(storeAIdentity).sealedNodes(key).store("store-a", policy(false));
        List<Peer> running = new ArrayList<>();
        try {
            Peer storeA = started(running, StoreGraphFleet.start("store-a", storeASpec));
            int storeBPort = ports.getAsInt();
            Peer storeB = started(running, StoreGraphFleet.start("store-b", spec(binding, storeBPort, List.of(seed),
                    dir.resolve("store-b"), sizes).sealedNodes(null).store("store-b", policy(true))));
            Peer agent = started(running, StoreGraphFleet.start("data-agent", spec(binding, ports.getAsInt(), List.of(seed),
                    dir.resolve("data-agent"), sizes).sealedNodes(key)));
            require(StoreGraphFleet.awaitFleet(List.of(storeA, storeB, agent), CONVERGE), "the vault group forms");
            require(StoreGraphFleet.await(CONVERGE, () -> storeCards(agent).size() == 2), "the data agent sees both store cards");

            // the data agent publishes the table and pins it on two stores
            Path rows = dir.resolve("table.rows");
            writeRows(rows, sizes.blobBytes(), 34);
            Ref<PBlob> table = agent.pdt().blobRef(REF);
            PBlob blob = table.swap(b -> PBlob.of(rows, b.store()));
            Files.deleteIfExists(rows);
            String root = blob.root();
            String identity = blob.identity();
            int treeNodes = agent.store().reachable(root).size();
            long treeBytes = GraphHalf.bytesNotIn(agent.store(), root, null);
            out.printf("store half: the data agent wrote %s (%d nodes), identity %s%n", root, treeNodes, identity);
            agent.pdt().storeFor(root, StoreChooser.Need.of(0, 2), Duration.ofDays(1));
            require(StoreGraphFleet.await(FETCH, () -> agent.pdt().stores().receipts(root).size() == 2),
                    "two stores hold the table: " + storeA.contentStore().status() + " / " + storeB.contentStore().status());
            Map<PeerId, String> names = Map.of(storeA.id(), "store-a", storeB.id(), "store-b", agent.id(), "data-agent");
            List<Receipt> receipts = new ArrayList<>();
            for (StoreReceipt r : agent.pdt().stores().receipts(root)) {
                receipts.add(new Receipt(names.getOrDefault(r.holder(), r.holder().display()), r.blind(), r.bytes()));
            }
            out.printf("  receipts %s%n", receipts);

            // the data agent leaves; a late reader with the key fetches from the stores alone
            agent.close();
            running.remove(agent);
            Peer reader = started(running, StoreGraphFleet.start("reader", spec(binding, ports.getAsInt(), List.of(seed),
                    dir.resolve("reader"), sizes).sealedNodes(key)));
            require(StoreGraphFleet.awaitFleet(List.of(storeA, storeB, reader), CONVERGE), "the reader joins the stores");
            long started = System.nanoTime();
            TreeFetch fetch = reader.content().fetchTree(root, List.of(),
                    FetchOptions.defaults().withTimeout(FETCH).prefer(storeA.id(), storeB.id()));
            boolean complete = fetch.await(FETCH) && fetch.complete();
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            Map<String, Long> bySource = new LinkedHashMap<>();
            fetch.progress().bytesBySource().forEach((peer, bytes) -> {
                if (bytes > 0) {
                    bySource.put(names.getOrDefault(peer, peer.display()), bytes);
                }
            });
            String readIdentity = complete ? PBlob.load(reader.store(), root).identity() : "";
            Cold cold = new Cold(complete, millis, fetch.progress().roundTrips(), bySource, readIdentity);
            out.printf("  the reader fetched the table in %d ms from %s%n", millis, bySource);

            // a challenge of each store, from a member that holds the table (P2-S-07)
            List<Challenge> challenges = new ArrayList<>();
            challenges.add(Challenge.of("store-a", await(reader.pdt().challenge(root, storeA.id()))));
            challenges.add(Challenge.of("store-b", await(reader.pdt().challenge(root, storeB.id()))));

            // store-a restarts over its packs with one pack cut in half, then reconciles with store-b (P2-K-03)
            List<Long> packs = storeA.pack().packIds();
            int before = storeA.store().reachable(root).size();
            storeA.close();
            running.remove(storeA);
            long cut = packs.get(packs.size() / 2);
            Path packFile = dir.resolve("store-a").resolve("nodes").resolve(String.format("pack-%08d.pack", cut));
            try (FileChannel channel = FileChannel.open(packFile, StandardOpenOption.WRITE)) {
                channel.truncate(channel.size() / 2);
            }
            String storeBSeed = binding.equals("tcp") ? "127.0.0.1:" + storeBPort : binding + "://127.0.0.1:" + storeBPort;
            Peer storeA2 = started(running, StoreGraphFleet.start("store-a", storeASpec.listeners(Map.of(binding, ports.getAsInt()))
                    .seeds(List.of(storeBSeed))));
            int lost = before - countHeld(storeA2, storeB, root);
            boolean completeAfterRestart = storeA2.store().complete(root);
            require(StoreGraphFleet.awaitFleet(List.of(storeA2, storeB, reader), CONVERGE), "store-a rejoins");
            long reconcileStart = System.nanoTime();
            ContentStore.Reconciled reconciled = await(storeA2.contentStore().reconcile(storeB.id()));
            boolean repaired = StoreGraphFleet.await(FETCH, () -> storeA2.store().complete(root));
            long repairMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reconcileStart);
            boolean equal = storeA2.store().reachable(root).equals(storeB.store().reachable(root));
            Challenge after = Challenge.of("store-a", await(reader.pdt().challenge(root, storeA2.id())));
            Restart restart = new Restart(packs.size(), cut, lost, completeAfterRestart, reconciled.have(), reconciled.need(),
                    reconciled.repairing(), reconciled.rounds(), reconciled.bytes(), repaired, repairMillis, equal, after);
            return new Report(binding, sizes.blobBytes(), root, identity, treeNodes, treeBytes, receipts, cold, challenges, restart);
        } finally {
            for (Peer p : running) {
                p.close();
            }
        }
    }

    private static StoreGraphFleet.Spec spec(String binding, int port, List<String> seeds, Path dir, Sizes sizes) {
        return StoreGraphFleet.Spec.member(FOUNDING, port, seeds, dir).listeners(Map.of(binding, port))
                .maxPackBytes(sizes.maxPackBytes());
    }

    private static Peer started(List<Peer> running, Peer peer) {
        running.add(peer);
        return peer;
    }

    /** The nodes under the root a store holds, walked over the complete tree another store holds. */
    private static int countHeld(Peer store, Peer whole, String root) {
        int held = 0;
        for (String cid : whole.store().reachable(root)) {
            if (store.store().has(cid)) {
                held++;
            }
        }
        return held;
    }

    private static List<AssetCard> storeCards(Peer peer) {
        return peer.discovery().find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind")));
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(CONVERGE.toSeconds(), TimeUnit.SECONDS);
    }

    private static void require(boolean condition, String what) {
        if (!condition) {
            throw new IllegalStateException("example 34: " + what + " did not happen in time");
        }
    }

    /** Generated table rows of about 200 bytes, with a seeded text column so the chunker finds cut points. */
    static void writeRows(Path file, long bytes, long seed) throws IOException {
        Random random = new Random(seed);
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
        try (FileChannel out = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            long written = 0;
            long row = 0;
            char[] text = new char[180];
            while (written < bytes) {
                for (int c = 0; c < text.length; c++) {
                    text[c] = (char) ('a' + random.nextInt(26));
                }
                byte[] line = String.format("row-%010d|%s%n", row++, new String(text)).getBytes(StandardCharsets.US_ASCII);
                int n = (int) Math.min(line.length, bytes - written);
                if (buffer.remaining() < n) {
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        out.write(buffer);
                    }
                    buffer.clear();
                }
                buffer.put(line, 0, n);
                written += n;
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                out.write(buffer);
            }
        }
    }
}
