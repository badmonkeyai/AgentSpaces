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

package ai.badmonkey.agentspaces.common.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The frame pump that decouples a transport reader from its receiver (the TCP and TLS reader deadlock fix): frames
 * arrive in order, a full queue blocks a blocking offer and refuses a non-blocking one until relief, a throwing
 * receiver stops the pump and reports the failure, and an ended pump drains then reports the end.
 */
class FramePumpTest {

    static void await(BooleanSupplier check) throws InterruptedException {
        Instant end = Instant.now().plus(Duration.ofSeconds(5));
        while (Instant.now().isBefore(end)) {
            if (check.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        assertThat(check.getAsBoolean()).isTrue();
    }

    @Test
    @Timeout(20)
    void framesArriveInOrderAndTheEndIsReported() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        AtomicInteger failed = new AtomicInteger();
        CountDownLatch ended = new CountDownLatch(1);
        FramePump pump = new FramePump("pump-order", () -> frame -> seen.add(new String(frame)), failed::incrementAndGet, ended::countDown, FramePump.DEFAULT_CAP_BYTES);
        for (int i = 0; i < 50; i++) {
            assertThat(pump.offer(("f" + i).getBytes())).isTrue();
        }
        await(() -> seen.size() == 50);
        assertThat(seen.get(0)).isEqualTo("f0");
        assertThat(seen.get(49)).isEqualTo("f49");
        assertThat(pump.queuedBytes()).isZero();
        assertThat(pump.stopped()).isFalse();
        pump.end();
        assertThat(ended.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(pump.stopped()).as("an ended pump stops").isTrue();
        assertThat(pump.offer("late".getBytes())).as("a stopped pump refuses").isFalse();
        assertThat(pump.offerNow("late".getBytes())).isFalse();
        assertThat(failed.get()).isZero();
    }

    @Test
    @Timeout(20)
    void aThrowingReceiverStopsThePumpAndReportsTheFailure() throws Exception {
        AtomicInteger failed = new AtomicInteger();
        CountDownLatch ended = new CountDownLatch(1);
        FramePump pump = new FramePump("pump-fail", () -> frame -> {
            throw new IllegalStateException("bad frame");
        }, failed::incrementAndGet, ended::countDown, FramePump.DEFAULT_CAP_BYTES);
        assertThat(pump.offer("x".getBytes())).isTrue();
        assertThat(ended.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(failed.get()).isEqualTo(1);
        assertThat(pump.stopped()).isTrue();
        assertThat(pump.queuedBytes()).isZero();
    }

    @Test
    @Timeout(20)
    void theCapBlocksABlockingOfferAndRefusesANonBlockingOneUntilRelief() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger delivered = new AtomicInteger();
        AtomicInteger relief = new AtomicInteger();
        FramePump pump = new FramePump("pump-cap", () -> frame -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            delivered.incrementAndGet();
        }, () -> { }, () -> { }, 100);
        pump.onRelief(relief::incrementAndGet);
        byte[] big = new byte[60];
        assertThat(pump.offer(big)).isTrue();            // taken by the receiver, which now blocks
        await(() -> pump.queuedBytes() == 0);
        assertThat(pump.offer(big)).isTrue();            // queued: 60 of 100
        assertThat(pump.offerNow(big)).as("120 over the cap of 100: queued but over the cap").isFalse();
        assertThat(pump.queuedBytes()).isEqualTo(120);
        Thread blocked = Thread.ofVirtual().start(() -> {
            try {
                pump.offer(big);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread.sleep(100);
        assertThat(blocked.isAlive()).as("a blocking offer waits for room").isTrue();
        release.countDown();
        await(() -> delivered.get() >= 4 && pump.queuedBytes() == 0);
        blocked.join(5000);
        assertThat(blocked.isAlive()).isFalse();
        assertThat(relief.get()).as("relief fired once the queue fell to half the cap").isGreaterThanOrEqualTo(1);
        pump.end();
    }
}
