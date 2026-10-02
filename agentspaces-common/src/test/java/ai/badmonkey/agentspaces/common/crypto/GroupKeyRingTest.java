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
package ai.badmonkey.agentspaces.common.crypto;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SPEC §11a.3 v0.1.13 (TODO-9-10-11 D2): the content-key ring. */
class GroupKeyRingTest {

    private final Instant now = Instant.parse("2026-10-01T12:00:00Z");

    @Test
    void writersSealUnderTheNewestEpochWhoseCutoverHasPassed() {
        GroupKeyRing ring = GroupKeyRing.of(GroupKey.generate());
        assertThat(ring.writeEpoch(now)).isZero();
        ring.install(1, "rotator", GroupKey.generate(), now.plusSeconds(60));
        assertThat(ring.writeEpoch(now)).as("before the cutover").isZero();
        assertThat(ring.writeEpoch(now.plusSeconds(60))).as("at the cutover").isEqualTo(1);
        assertThat(ring.epochs()).containsExactly(0L, 1L);
    }

    @Test
    void racingRotatorsLeaveBothKeysReadableAndOneCanonicalForWriting() {
        GroupKeyRing first = GroupKeyRing.of(GroupKey.generate());
        GroupKeyRing second = GroupKeyRing.of(GroupKey.generate());
        GroupKey a = GroupKey.generate();
        GroupKey b = GroupKey.generate();
        first.install(1, "zAlpha", a, now);
        first.install(1, "zBravo", b, now);
        second.install(1, "zBravo", b, now);
        second.install(1, "zAlpha", a, now);
        assertThat(first.openingKeys(1)).hasSize(2);
        assertThat(first.sealingKey(1).orElseThrow().rawBytes())
                .as("every replica picks the same canonical key, whatever the arrival order")
                .isEqualTo(second.sealingKey(1).orElseThrow().rawBytes());
    }

    @Test
    void aMissingEpochIsReportedAndThrottled() {
        GroupKeyRing ring = GroupKeyRing.of(GroupKey.generate());
        List<Long> missing = new CopyOnWriteArrayList<>();
        ring.onMissingEpoch(missing::add);
        assertThat(ring.openingKeys(3)).isEmpty();
        assertThat(ring.openingKeys(3)).isEmpty();
        assertThat(missing).as("reported once within the throttle window").containsExactly(3L);
        ring.announce(4, now);
        assertThat(missing).containsExactly(3L, 4L);
    }

    @Test
    void aWriterIsBoundedOnceANewerEpochCutsOverUnheld() {
        GroupKeyRing ring = GroupKeyRing.of(GroupKey.generate());
        ring.writerGrace(Duration.ofMinutes(10));
        ring.announce(1, now);
        assertThat(ring.writeEpoch(now.plus(Duration.ofMinutes(10)))).as("within grace").isZero();
        assertThatThrownBy(() -> ring.writeEpoch(now.plus(Duration.ofMinutes(11))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("awaiting epoch 1");
        ring.install(1, "r", GroupKey.generate(), now);
        assertThat(ring.writeEpoch(now.plus(Duration.ofMinutes(11)))).isEqualTo(1);
    }

    @Test
    void epochNumbersAreNonNegativeAndTheRingIsBounded() {
        GroupKeyRing ring = GroupKeyRing.empty();
        assertThatThrownBy(() -> ring.writeEpoch(now)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> ring.install(-1, "r", GroupKey.generate(), now))
                .isInstanceOf(IllegalArgumentException.class);
        GroupKey shared = GroupKey.generate();
        for (int i = 0; i < GroupKeyRing.MAX_EPOCHS; i++) {
            ring.install(i, "r", shared, now);
        }
        assertThatThrownBy(() -> ring.install(GroupKeyRing.MAX_EPOCHS, "r", shared, now))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("epochs");
    }

    @Test
    void anEpochBoundCiphertextDoesNotOpenUnderAnotherEpochsBinding() {
        GroupKey key = GroupKey.generate();
        byte[] sealed = key.encrypt("x".getBytes(StandardCharsets.UTF_8),
                "space|entry|1".getBytes(StandardCharsets.UTF_8));
        assertThat(key.decrypt(sealed, "space|entry".getBytes(StandardCharsets.UTF_8))).isEmpty();
        assertThat(key.decrypt(sealed, "space|entry|2".getBytes(StandardCharsets.UTF_8))).isEmpty();
        assertThat(key.decrypt(sealed, "space|entry|1".getBytes(StandardCharsets.UTF_8))).isPresent();
    }
}
