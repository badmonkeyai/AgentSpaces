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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example 34: a database on a ref and a store for the fleet (srad/agentspaces-pdt2.md §7.3, P2-O-03). The graph
 * half ({@link GraphHalf}) and the store half ({@link StoreHalf}) run one after the other on loopback tcp, each
 * in its own group. The first argument is the base port (7734 by default); the second is {@code full} (a
 * 64 MiB table, the larger graph) or {@code quick} (the acceptance test's sizes).
 */
public final class StoreAndGraph {

    private StoreAndGraph() {
    }

    public static void main(String[] args) throws Exception {
        int base = args.length > 0 ? Integer.parseInt(args[0]) : 7734;
        boolean quick = args.length > 1 && args[1].equals("quick");
        AtomicInteger next = new AtomicInteger(base);
        Path root = Files.createTempDirectory("example-34");
        try {
            GraphHalf.Report graph = GraphHalf.run(root.resolve("graph"), next::getAndIncrement,
                    quick ? GraphHalf.Sizes.quick() : GraphHalf.Sizes.full(), true, System.out);
            System.out.print(graph.summary());
            StoreHalf.Report store = StoreHalf.run(root.resolve("vault"), "tcp", next::getAndIncrement,
                    quick ? StoreHalf.Sizes.quick() : StoreHalf.Sizes.full(), System.out);
            System.out.print(store.summary());
        } finally {
            try (var walk = Files.walk(root)) {
                walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }
}
