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
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link Retention} (PD-S-03): the last 64 versions or every version under an hour, whichever keeps more. */
class RetentionTest {

    private static final long HOUR = 3_600_000L;

    private static String root(String name) {
        return Cids.of(name.getBytes(StandardCharsets.UTF_8), Cids.RAW);
    }

    private static InstantSource clockAt(AtomicLong millis) {
        return () -> java.time.Instant.ofEpochMilli(millis.get());
    }

    @Test
    void defaultsAreSixtyFourVersionsAndOneHour() {
        Retention retention = Retention.defaults();
        assertThat(retention.versions()).isEqualTo(64);
        assertThat(retention.age()).isEqualTo(Duration.ofHours(1));
        assertThat(Retention.DEFAULT_VERSIONS).isEqualTo(64);
        assertThat(Retention.DEFAULT_AGE).isEqualTo(Duration.ofHours(1));
    }

    @Test
    void keepsTheLastSixtyFourVersionsWhenAllAreOld() {
        AtomicLong now = new AtomicLong(0);
        Retention retention = Retention.defaults(clockAt(now));
        List<String> roots = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String root = root("v" + i);
            roots.add(root);
            retention.recordVersion("ref", root, i * 1000L);
        }
        now.set(10 * HOUR);
        assertThat(retention.retainedRoots()).containsExactlyElementsOf(roots.subList(36, 100));
        assertThat(retention.versions("ref")).hasSize(64);
        assertThat(retention.versions("ref").get(0).root()).isEqualTo(roots.get(36));
    }

    @Test
    void keepsEveryVersionYoungerThanAnHourEvenPastTheCount() {
        AtomicLong now = new AtomicLong(0);
        Retention retention = Retention.defaults(clockAt(now));
        List<String> roots = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            String root = root("y" + i);
            roots.add(root);
            retention.recordVersion("ref", root, i * 1000L);
        }
        now.set(200_000L);
        assertThat(retention.retainedRoots()).as("all 200 are under an hour").containsExactlyElementsOf(roots);
        now.set(100_000L + HOUR);
        assertThat(retention.retainedRoots()).as("99 young ones, which beats 64").hasSize(99)
                .containsExactlyElementsOf(roots.subList(101, 200));
        now.set(3 * HOUR);
        assertThat(retention.retainedRoots()).as("none young, so the last 64").containsExactlyElementsOf(roots.subList(136, 200));
    }

    @Test
    void whicheverKeepsMoreWins() {
        AtomicLong now = new AtomicLong(0);
        Retention retention = new Retention(3, Duration.ofMinutes(10), clockAt(now));
        retention.recordVersion("r", root("a"), 0);
        retention.recordVersion("r", root("b"), 1000);
        retention.recordVersion("r", root("c"), 2000);
        retention.recordVersion("r", root("d"), 3000);
        retention.recordVersion("r", root("e"), 4000);
        now.set(5000);
        assertThat(retention.retainedRoots()).as("age keeps all five").hasSize(5);
        now.set(3000 + Duration.ofMinutes(10).toMillis());
        assertThat(retention.retainedRoots()).as("age keeps e; count keeps c, d, e").containsExactly(root("c"), root("d"), root("e"));
        assertThat(retention.retainedRoots(3999 + Duration.ofMinutes(10).toMillis())).as("explicit now").containsExactly(root("c"), root("d"), root("e"));
    }

    @Test
    void refsAreIndependentAndCanBeForgotten() {
        AtomicLong now = new AtomicLong(10 * HOUR);
        Retention retention = new Retention(2, Duration.ZERO, clockAt(now));
        retention.recordVersion("a", root("a1"), 0);
        retention.recordVersion("a", root("a2"), 0);
        retention.recordVersion("a", root("a3"), 0);
        retention.recordVersion("b", root("b1"), 0);
        assertThat(retention.refs()).containsExactlyInAnyOrder("a", "b");
        assertThat(retention.retainedRoots()).containsExactlyInAnyOrder(root("a2"), root("a3"), root("b1"));
        assertThat(retention.versions("a")).extracting(Retention.Version::root).containsExactly(root("a2"), root("a3"));
        assertThat(retention.versions("zzz")).isEmpty();
        assertThat(retention.forget("a")).isTrue();
        assertThat(retention.forget("a")).isFalse();
        assertThat(retention.retainedRoots()).containsExactly(root("b1"));
    }

    @Test
    void pruneForgetsWhatNeitherRuleKeeps() {
        AtomicLong now = new AtomicLong(0);
        Retention retention = new Retention(2, Duration.ofSeconds(10), clockAt(now));
        for (int i = 0; i < 50; i++) {
            retention.recordVersion("r", root("p" + i), i * 1000L);
        }
        now.set(49_000L);
        assertThat(retention.versions("r")).as("the ten young plus the count overlap").hasSize(10);
        assertThat(retention.versions("r").get(0).root()).isEqualTo(root("p40"));
        now.set(1_000_000L);
        assertThat(retention.versions("r")).extracting(Retention.Version::root).containsExactly(root("p48"), root("p49"));
    }

    @Test
    void sameRootRecordedTwiceCountsOnceInTheSet() {
        Retention retention = new Retention(5, Duration.ZERO, InstantSource.system());
        retention.recordVersion("r", root("same"), 1);
        retention.recordVersion("r", root("same"), 2);
        assertThat(retention.versions("r")).hasSize(2);
        assertThat(retention.retainedRoots()).containsExactly(root("same"));
    }

    @Test
    void refusesBadLimits() {
        assertThatThrownBy(() -> new Retention(0, Duration.ZERO, InstantSource.system())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Retention(1, Duration.ofSeconds(-1), InstantSource.system())).isInstanceOf(IllegalArgumentException.class);
    }
}
