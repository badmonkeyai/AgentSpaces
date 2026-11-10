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

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 1, P2-V-03 (srad/agentspaces-pdt2.md §3.10): the pins of a {@link NodeStore} and
 * the retained versions of a {@link Retention} persist as meta records in the packs of a
 * {@link PackNodeStore}, written on {@code flush}, and a store reopened over the directory
 * starts with them.
 */
class PersistedMetaTest {

    private static String root(String text) {
        return Cids.of(text.getBytes(StandardCharsets.UTF_8), Cids.RAW);
    }

    @Test
    void pinsSurviveARestart(@TempDir Path dir) throws IOException {
        String a = root("a");
        String b = root("b");
        String c = root("c");
        try (PackNodeStore pack = PackNodeStore.open(dir)) {
            NodeStore store = new NodeStore(pack, InstantSource.system());
            store.pin(a);
            store.pin(a);
            store.pin(b);
            store.pin(c);
            store.unpin(c);
            store.flushMeta();
            store.pin(c);
            store.unpin(c);
        }
        try (PackNodeStore pack = PackNodeStore.open(dir)) {
            NodeStore store = new NodeStore(pack, InstantSource.system());
            assertThat(store.pinned()).containsExactlyInAnyOrder(a, b);
            assertThat(store.pinCount(a)).isEqualTo(2);
            assertThat(store.unpin(a)).isEqualTo(1);
            store.flushMeta();
        }
        try (PackNodeStore pack = PackNodeStore.open(dir)) {
            assertThat(new NodeStore(pack, InstantSource.system()).pinCount(a)).isEqualTo(1);
        }
    }

    @Test
    void retainedVersionsSurviveARestart(@TempDir Path dir) throws IOException {
        AtomicLong now = new AtomicLong(1_000_000);
        InstantSource clock = () -> java.time.Instant.ofEpochMilli(now.get());
        List<Retention.Version> before;
        try (PackNodeStore pack = PackNodeStore.open(dir)) {
            Retention retention = new Retention(2, Duration.ZERO, clock, pack);
            retention.recordVersion("ledger", root("1"), 10);
            retention.recordVersion("ledger", root("2"), 20);
            retention.recordVersion("ledger", root("3"), 30);
            retention.recordVersion("naïve ref", root("4"), 40);
            retention.recordVersion("gone", root("5"), 50);
            retention.forget("gone");
            retention.flush();
            before = retention.versions("ledger");
            assertThat(before).extracting(Retention.Version::root).containsExactly(root("2"), root("3"));
        }
        try (PackNodeStore pack = PackNodeStore.open(dir)) {
            Retention retention = new Retention(2, Duration.ZERO, clock, pack);
            assertThat(retention.refs()).containsExactlyInAnyOrder("ledger", "naïve ref");
            assertThat(retention.versions("ledger")).isEqualTo(before);
            assertThat(retention.retainedRoots()).containsExactlyInAnyOrder(root("2"), root("3"), root("4"));
        }
    }

    /** PDT 2 phase 1, P2-V-03 (gate): a ref name of {@link PRef#MAX_NAME_BYTES} bytes names a meta record too. */
    @Test
    void theVersionsOfARefWithTheLongestNamePersist(@TempDir Path dir) throws IOException {
        String longest = "r".repeat(PRef.MAX_NAME_BYTES);
        try (PackNodeStore pack = PackNodeStore.open(dir)) {
            Retention retention = new Retention(2, Duration.ZERO, InstantSource.system(), pack);
            retention.recordVersion(longest, root("1"), 10);
            retention.flush();
        }
        try (PackNodeStore pack = PackNodeStore.open(dir)) {
            Retention retention = new Retention(2, Duration.ZERO, InstantSource.system(), pack);
            assertThat(retention.versions(longest)).extracting(Retention.Version::root).containsExactly(root("1"));
        }
    }
}
