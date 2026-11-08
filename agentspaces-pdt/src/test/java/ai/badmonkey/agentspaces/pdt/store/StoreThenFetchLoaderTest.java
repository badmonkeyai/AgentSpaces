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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link StoreThenFetchLoader} (PD-V-06): store first, then the fetcher within the deadline, else a named failure. */
class StoreThenFetchLoaderTest {

    private static final Random RANDOM = new Random(42);

    /** A fetcher over a map of nodes that counts its calls and names its sources. */
    private static final class MapFetcher implements Fetcher {
        final Map<String, byte[]> nodes;
        final AtomicInteger calls = new AtomicInteger();

        MapFetcher(Map<String, byte[]> nodes) {
            this.nodes = nodes;
        }

        @Override
        public CompletableFuture<byte[]> fetch(String cid) {
            calls.incrementAndGet();
            byte[] bytes = nodes.get(cid);
            if (bytes == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("nobody holds " + cid));
            }
            return CompletableFuture.completedFuture(bytes);
        }

        @Override
        public List<String> sourcesTried(String cid) {
            return List.of("peer-a", "peer-b");
        }
    }

    @Test
    void readsTheStoreFirstWithoutFetching() {
        NodeStore store = NodeStore.inMemory();
        byte[] bytes = TreeFixtures.chunk(RANDOM, 50);
        String cid = store.put(bytes, Cids.RAW);
        MapFetcher fetcher = new MapFetcher(Map.of());
        StoreThenFetchLoader loader = new StoreThenFetchLoader(store, fetcher);
        assertThat(loader.deadline()).isEqualTo(Duration.ofSeconds(10));
        assertThat(loader.load(cid)).isEqualTo(bytes);
        assertThat(loader.loadIfHeld(cid)).contains(bytes);
        assertThat(fetcher.calls.get()).isZero();
        assertThat(loader.store()).isSameAs(store);
        assertThat(loader.fetcher()).isSameAs(fetcher);
    }

    @Test
    void fetchesAMissingNodeVerifiesItAndStoresIt() {
        NodeStore store = NodeStore.inMemory();
        byte[] bytes = TreeFixtures.chunk(RANDOM, 50);
        String cid = Cids.of(bytes, Cids.RAW);
        MapFetcher fetcher = new MapFetcher(Map.of(cid, bytes));
        StoreThenFetchLoader loader = new StoreThenFetchLoader(store, fetcher, Duration.ofSeconds(1));
        assertThat(loader.loadIfHeld(cid)).isEmpty();
        assertThat(loader.load(cid)).isEqualTo(bytes);
        assertThat(store.has(cid)).isTrue();
        assertThat(loader.load(cid)).isEqualTo(bytes);
        assertThat(fetcher.calls.get()).as("the second load is local").isEqualTo(1);
    }

    @Test
    void aFetchThatFailsNamesTheCidAndTheSources() {
        NodeStore store = NodeStore.inMemory();
        String cid = Cids.of(TreeFixtures.chunk(RANDOM, 10), Cids.RAW);
        StoreThenFetchLoader loader = new StoreThenFetchLoader(store, new MapFetcher(Map.of()), Duration.ofSeconds(1));
        assertThatThrownBy(() -> loader.load(cid))
                .isInstanceOf(NodeUnavailableException.class)
                .hasMessageContaining(cid)
                .hasMessageContaining("peer-a, peer-b")
                .hasMessageContaining("nobody holds")
                .satisfies(e -> {
                    NodeUnavailableException unavailable = (NodeUnavailableException) e;
                    assertThat(unavailable.cid()).isEqualTo(cid);
                    assertThat(unavailable.sources()).containsExactly("peer-a", "peer-b");
                    assertThat(unavailable.getCause()).isInstanceOf(IllegalStateException.class);
                });
        assertThat(store.has(cid)).isFalse();
    }

    @Test
    void theDeadlinePassesOnAFetchThatNeverCompletes() {
        NodeStore store = NodeStore.inMemory();
        String cid = Cids.of(TreeFixtures.chunk(RANDOM, 10), Cids.RAW);
        List<CompletableFuture<byte[]>> futures = new ArrayList<>();
        Fetcher stalled = c -> {
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            futures.add(future);
            return future;
        };
        StoreThenFetchLoader loader = new StoreThenFetchLoader(store, stalled, Duration.ofMillis(100));
        long started = System.nanoTime();
        assertThatThrownBy(() -> loader.load(cid))
                .isInstanceOf(NodeUnavailableException.class)
                .hasMessageContaining(cid)
                .hasMessageContaining("deadline of 100 ms")
                .hasMessageContaining("sources tried: none");
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(Duration.ofMillis(90));
        assertThat(futures).hasSize(1);
        assertThat(futures.get(0).isCancelled()).as("the stalled fetch is cancelled").isTrue();
    }

    /** PD-V-06 end to end: a map built in one store opens in an empty store through the loader, path by path. */
    @Test
    void aValueOpensInAnEmptyStoreThroughTheLoaderAndNamesWhatItCannotFetch() {
        NodeStore a = NodeStore.inMemory();
        ai.badmonkey.agentspaces.pdt.values.Codec<String> strings = ai.badmonkey.agentspaces.pdt.values.Codec.of(String.class);
        ai.badmonkey.agentspaces.pdt.values.Editor<String, String> editor =
                ai.badmonkey.agentspaces.pdt.values.PMap.empty(a, strings, strings).editor();
        for (int i = 0; i < 5000; i++) {
            editor.assoc("key-" + i, "value-" + i);
        }
        String root = editor.done().value().root();
        Map<String, byte[]> held = new java.util.HashMap<>();
        for (String cid : a.reachable(root)) {
            held.put(cid, a.get(cid).orElseThrow());
        }
        NodeStore b = NodeStore.inMemory();
        MapFetcher fetcher = new MapFetcher(held);
        StoreThenFetchLoader loader = new StoreThenFetchLoader(b, fetcher, Duration.ofSeconds(1));
        ai.badmonkey.agentspaces.pdt.values.PMap<String, String> map =
                ai.badmonkey.agentspaces.pdt.values.PMap.load(b, loader, root, strings, strings);
        assertThat(map.count()).isEqualTo(5000);
        assertThat(map.get("key-4242")).contains("value-4242");
        int fetched = fetcher.calls.get();
        assertThat(fetched).as("the root and the path to one key").isBetween(2, 8);
        assertThat(b.size()).as("B holds exactly the nodes the path needed").isEqualTo(fetched);
        assertThat(b.complete(root)).isFalse();
        assertThat(map.get("key-4242")).contains("value-4242");
        assertThat(fetcher.calls.get()).as("the second get is local").isEqualTo(fetched);

        // Drop one child from the fetcher's holdings: a get that needs it names the CID and the sources.
        String missing = null;
        for (String cid : a.reachable(root)) {
            if (!cid.equals(root) && !b.has(cid)) {
                missing = cid;
                break;
            }
        }
        held.remove(missing);
        String absent = missing;
        List<String> failing = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            String key = "key-" + i;
            try {
                map.get(key);
            } catch (NodeUnavailableException e) {
                assertThat(e.cid()).isEqualTo(absent);
                assertThat(e.getMessage()).contains(absent).contains("peer-a, peer-b");
                failing.add(key);
            }
        }
        assertThat(failing).isNotEmpty();
        assertThat(b.has(absent)).isFalse();
    }

    @Test
    void bytesThatDoNotHashToTheCidAreRefused() {
        NodeStore store = NodeStore.inMemory();
        String cid = Cids.of(TreeFixtures.chunk(RANDOM, 10), Cids.RAW);
        byte[] wrong = TreeFixtures.chunk(RANDOM, 10);
        StoreThenFetchLoader loader = new StoreThenFetchLoader(store, c -> CompletableFuture.completedFuture(wrong), Duration.ofSeconds(1));
        assertThatThrownBy(() -> loader.load(cid))
                .isInstanceOf(NodeUnavailableException.class)
                .hasMessageContaining("does not match the claimed CID");
        assertThat(store.size()).isZero();
    }

    @Test
    void aFetcherThatThrowsOrReturnsNothingIsReportedAsUnavailable() {
        NodeStore store = NodeStore.inMemory();
        String cid = Cids.of(TreeFixtures.chunk(RANDOM, 10), Cids.RAW);
        StoreThenFetchLoader throwing = new StoreThenFetchLoader(store, c -> {
            throw new IllegalArgumentException("refused outright");
        }, Duration.ofSeconds(1));
        assertThatThrownBy(() -> throwing.load(cid)).isInstanceOf(NodeUnavailableException.class).hasMessageContaining("refused outright");
        StoreThenFetchLoader nothing = new StoreThenFetchLoader(store, c -> null, Duration.ofSeconds(1));
        assertThatThrownBy(() -> nothing.load(cid)).isInstanceOf(NodeUnavailableException.class).hasMessageContaining("nothing");
        StoreThenFetchLoader nullBytes = new StoreThenFetchLoader(store, c -> CompletableFuture.completedFuture(null), Duration.ofSeconds(1));
        assertThatThrownBy(() -> nullBytes.load(cid)).isInstanceOf(NodeUnavailableException.class).hasMessageContaining("no bytes");
        StoreThenFetchLoader none = new StoreThenFetchLoader(store, Fetcher.none(), Duration.ofSeconds(1));
        assertThatThrownBy(() -> none.load(cid)).isInstanceOf(NodeUnavailableException.class).hasMessageContaining("no fetcher");
        assertThatThrownBy(() -> new StoreThenFetchLoader(store, Fetcher.none(), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
