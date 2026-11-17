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
package ai.badmonkey.agentspaces.peering.gossip;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The paces of replication (srad/replication-by-entry.md RE-F-03, RE-Q-REL-09): each has a floor, a ceiling and a
 * runtime setter that refuses a value outside them by name; a raise takes effect at once; a lowering waits until the
 * raised value has served one hold, the last lowering asked for winning; and the bus and the membership view keep
 * their paces this way, so a pace that moved up stays up for at least one period before it moves down.
 */
class PaceTest {

    private final AtomicLong now = new AtomicLong(1_000_000);

    @Test
    void aRaiseIsAtOnceALoweringWaitsOneHoldAndTheLastLoweringWins() {
        Pace pace = new Pace("demo", 10, 100, 50, now::get, () -> 1_000);
        assertThat(pace.set(80)).as("up at once").isEqualTo(80);
        assertThat(pace.set(20)).as("down waits for the hold").isEqualTo(80);
        assertThat(pace.snapshot().pending()).isEqualTo(20L);
        now.addAndGet(500);
        pace.set(30); // a second lowering inside the hold replaces the first
        assertThat(pace.get()).isEqualTo(80);
        now.addAndGet(499);
        assertThat(pace.get()).as("one millisecond short of the hold").isEqualTo(80);
        now.addAndGet(1);
        assertThat(pace.get()).as("the hold served, the last lowering takes effect").isEqualTo(30);
        Pace.Snapshot after = pace.snapshot();
        assertThat(after.pending()).isNull();
        assertThat(after.raises()).isEqualTo(1);
        assertThat(after.lowerings()).isEqualTo(1);
        assertThat(after.deferred()).isEqualTo(2);
        assertThat(pace.set(15)).as("no hold running: down at once").isEqualTo(15);
    }

    @Test
    void askingForTheValueInEffectCancelsAWaitingLowering() {
        Pace pace = new Pace("demo", 10, 100, 50, now::get, () -> 1_000);
        pace.set(90);
        pace.set(20);
        pace.set(90);
        now.addAndGet(5_000);
        assertThat(pace.get()).isEqualTo(90);
    }

    @Test
    void aValueOutsideTheBoundsIsRefusedByNameAndChangesNothing() {
        Pace pace = new Pace("probe-interval", 100, 60_000, 50, now::get, () -> 1_000);
        assertThat(pace.get()).as("a start outside the bounds is kept").isEqualTo(50);
        assertThatThrownBy(() -> pace.set(99)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("probe-interval").hasMessageContaining("100").hasMessageContaining("60000");
        assertThatThrownBy(() -> pace.set(60_001)).isInstanceOf(IllegalArgumentException.class);
        assertThat(pace.get()).isEqualTo(50);
    }

    @Test
    void theBusKeepsItsPacesBoundedSettableAndHeldForOnePeriod() {
        GossipBus bus = new GossipBus(n -> List.of(), sender(), CborCodec.defaultCodec(), 3, 6, now::get,
                new Random(1));
        bus.antiEntropyBase(1_000);
        assertThat(bus.antiEntropyPeriod()).isEqualTo(Duration.ofSeconds(1));
        bus.antiEntropyPeriod(Duration.ofSeconds(4));
        bus.antiEntropyPeriod(Duration.ofSeconds(1));
        assertThat(bus.antiEntropyPeriod()).as("the raised period serves one period of its own").hasSeconds(4);
        now.addAndGet(3_999);
        assertThat(bus.antiEntropyPeriod()).hasSeconds(4);
        now.addAndGet(1);
        assertThat(bus.antiEntropyPeriod()).hasSeconds(1);
        assertThatThrownBy(() -> bus.antiEntropyPeriod(Duration.ofMillis(99)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("anti-entropy-period");
        assertThatThrownBy(() -> bus.antiEntropyPeriod(Duration.ofMinutes(2)))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(bus.antiEntropyStretch()).isEqualTo(32);
        bus.antiEntropyStretch(1);
        assertThat(bus.antiEntropyStretch()).as("no raise before: down at once").isEqualTo(1);
        assertThatThrownBy(() -> bus.antiEntropyStretch(33)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("anti-entropy-stretch");
        assertThatThrownBy(() -> bus.antiEntropyStretch(0)).isInstanceOf(IllegalArgumentException.class);

        bus.auditBudget(4L << 20);
        bus.auditBudget(64L << 10);
        assertThat(bus.auditBudget()).as("a raised budget is held for one anti-entropy period").isEqualTo(4L << 20);
        now.addAndGet(1_000);
        assertThat(bus.auditBudget()).isEqualTo(64L << 10);
        assertThatThrownBy(() -> bus.auditBudget(4095)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("audit-budget");
        bus.auditSessions(1);
        assertThat(bus.auditSessions()).isEqualTo(1);
        assertThatThrownBy(() -> bus.auditSessions(5)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("audit-sessions");

        assertThat(bus.paces()).extracting(Pace.Snapshot::name).containsExactly("anti-entropy-period",
                "anti-entropy-stretch", "audit-budget", "audit-sessions", "fetch-window", "fetch-wait-floor");
        assertThat(bus.paces()).allSatisfy(p -> assertThat(p.floor()).isLessThanOrEqualTo(p.ceiling()));
    }

    @Test
    void theMembershipViewKeepsTheProbePacesTheSameWay() {
        InstantSource clock = () -> Instant.ofEpochMilli(now.get());
        GroupMembership view = new GroupMembership(PeerIdentity.generate().peerId(), clock, new Random(1),
                GroupMembership.Config.defaults());
        long base = GroupMembership.Config.defaults().probeInterval().toMillis();
        assertThat(view.probeIntervalMillis()).isEqualTo(base);
        view.probeInterval(Duration.ofMillis(base * 4));
        view.probeInterval(Duration.ofMillis(base));
        assertThat(view.probeIntervalMillis()).as("held for one interval at its raised value").isEqualTo(base * 4);
        now.addAndGet(base * 4);
        assertThat(view.probeIntervalMillis()).isEqualTo(base);
        assertThatThrownBy(() -> view.probeInterval(Duration.ofMillis(50))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("probe-interval");
        assertThat(view.probeStretch()).isEqualTo(GroupMembership.MAX_QUIET_STRETCH);
        view.probeStretch(0);
        assertThat(view.probeStretch()).isZero();
        assertThatThrownBy(() -> view.probeStretch(GroupMembership.MAX_QUIET_STRETCH + 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("probe-stretch");
        assertThat(view.paces()).extracting(Pace.Snapshot::name).containsExactly("probe-interval", "probe-stretch");
    }

    private static GossipBus.FrameSender sender() {
        return new GossipBus.FrameSender() {
            @Override
            public void sendRumor(PeerId to, Bodies.Rumor rumor) {
            }

            @Override
            public void sendDigest(PeerId to, Bodies.Digest digests) {
            }

            @Override
            public void sendPullResp(PeerId to, Bodies.PullResp deltas) {
            }
        };
    }
}
