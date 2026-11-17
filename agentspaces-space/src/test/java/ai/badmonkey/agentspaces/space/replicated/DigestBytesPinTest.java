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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The digest's bytes did not move (reviews/IDLE-WORK-2026-11-13.md items 3 and 4): the expiry index, the hash cache,
 * and the skipped re-merge change when the digest is computed and what is cached, never what it says. A fixed state
 * (fixed ids, dots, stamps, leases) is pinned by the SHA-256 of its digest, a value the 0.6.0 jar computed for the same
 * state before this change, and by the line format spelled out here. The digest then keeps those bytes across the
 * sweeps that change nothing in it (a lapse within the grace), and moves exactly as the full sweep moved it when the
 * grace ends.
 */
class DigestBytesPinTest {

    /**
     * SHA-256 of the digest of {@link #pinnedState} at the clock's start, and seven minutes on once the two lapsed
     * entries are collected: computed on 2026-11-13 by the released 0.6.0 jars (see {@link #FULL_PINNED_SHA256}) over
     * the same state with an issuer key per entry, as a merge files one, so the withdrawn entry is listed as the
     * tombstone it is. (A first pin set the crdt alone, which left the withdrawal out of the digest; the same one-off
     * run reproduces that pin's values, 46a6af30... and 4e6bdbbe..., from the crdt alone, which is how it is known to
     * set the state the way the old replica held it.)
     */
    static final String PINNED_SHA256 = "e869e6093c6e45baf27c3593b7f12781e3e7967ad36e3f08ce56a2ad42993408";
    static final String COLLECTED_SHA256 = "b975b1735d194b23c569f2c4aed19371bae25c5994327604a45511c8da44906b";

    /**
     * SHA-256 of the digest of {@link #fullState} at the clock's start, two minutes on (after the sweep that lapses two
     * leases, lapses one claim and drops the orphaned claim), and seven minutes on (after the two lapsed entries are
     * collected). Computed on 2026-11-13 by the released 0.6.0 jars (agentspaces-dependencies/target/central-staging,
     * dated 2026-11-11, before this change) over the same state, set on the old replica by reflection: its crdt, an
     * issuer key per entry, the claims, and the refused-and-acknowledged lines, then {@code digest()} at each time.
     */
    static final String FULL_PINNED_SHA256 = "e86feaf702c4af9bbd151eeac5ffd8224ed4dcf892f7264416b9813270f60977";
    static final String FULL_SWEPT_SHA256 = "b71653b03e031e05c2f89d9d7de2d95391fe26c63547d311761670aac50627e8";
    static final String FULL_COLLECTED_SHA256 = "1da7304e8bb79249d4005bf226dc09a7018e1b59802de2515cf9b8d182702b8c";

    /** Refused-and-acknowledged lines of the full fixture (ASF-047): one refused state, one refused claim. */
    static final List<String> REFUSED_LINES = List.of(
            "e:00000000-0000-4000-8000-000000000009=1a2b3c4d",
            "c:00000000-0000-4000-8000-00000000000a=3:0000000000001-0000-x:0.0:1000000090000");

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final GroupId groupId = GroupId.of("zPin");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zPin", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "pin",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private PeerIdentity identity;
    private PeerNode node;
    private ReplicatedSpace space;

    @BeforeEach
    void setUp() throws Exception {
        identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        node.listen(network.register("pin"), "pin");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        space = ReplicatedSpace.builder(runtime, "pinned", identity, "worker")
                .clock(clock)
                .settleWindow(Duration.ZERO)
                .maxLease(Duration.ofDays(1)) // so a lapse keeps the five-minute grace
                .build();
    }

    @AfterEach
    void tearDown() {
        node.close();
    }

    /**
     * Six entries: four present with leases of one minute, ten minutes, one hour and one day, one already withdrawn,
     * and one present with a renewed lease. Everything is fixed, so the digest is the same on every run.
     */
    static SpaceStateCrdt pinnedState(SpaceId spaceId, AgentId issuer, long start) {
        long[] leases = {60_000, 600_000, 3_600_000, 86_400_000, 60_000, 120_000};
        SpaceStateCrdt state = SpaceStateCrdt.empty();
        for (int i = 0; i < leases.length; i++) {
            EntryId entryId = EntryId.of("00000000-0000-4000-8000-00000000000" + i);
            HlcTimestamp stamp = new HlcTimestamp(start + i, i, "pin-node");
            LeaseInfo lease = new LeaseInfo(issuer, start + leases[i], LeaseKind.WRITE);
            EntryRecord record = new EntryRecord(entryId, spaceId, "pin#1", new byte[] {(byte) i}, null, issuer,
                    stamp, lease, Map.of(), new byte[0]);
            EntryState entry = EntryState.added(record, new Dot("pin-replica", i + 1), new LwwRegister<>(stamp, lease));
            if (i == 4) {
                entry = entry.withObservedRemoved();
            }
            if (i == 5) {
                entry = entry.withLease(new LwwRegister<>(new HlcTimestamp(start + 50, 0, "pin-node"),
                        new LeaseInfo(issuer, start + 90_000, LeaseKind.WRITE)));
            }
            state = state.merge(SpaceStateCrdt.of(entryId, entry));
        }
        return state;
    }

    static EntryId pinId(int i) {
        return EntryId.of("00000000-0000-4000-8000-00000000000" + i);
    }

    /**
     * The full fixture: the six entries of {@link #pinnedState}, plus a completed entry (6) whose claim lapsed, a
     * present entry (7) under a claim that lapses before its lease, and (in {@link #fullClaims}) a claim for an entry
     * this replica never held (8). With the refused lines, the digest carries every kind of line: present entries,
     * a withdrawal and a completion as {@code e:} tombstones, {@code c:} claims, and {@code a:} acknowledgements.
     */
    static SpaceStateCrdt fullState(SpaceId spaceId, AgentId issuer, long start) {
        SpaceStateCrdt state = pinnedState(spaceId, issuer, start);
        for (int i = 6; i <= 7; i++) {
            EntryId entryId = pinId(i);
            HlcTimestamp stamp = new HlcTimestamp(start + i, i, "pin-node");
            LeaseInfo lease = new LeaseInfo(issuer, start + 3_600_000, LeaseKind.WRITE);
            EntryRecord record = new EntryRecord(entryId, spaceId, "pin#1", new byte[] {(byte) i}, null, issuer,
                    stamp, lease, Map.of(), new byte[0]);
            EntryState entry = EntryState.added(record, new Dot("pin-replica", i + 1), new LwwRegister<>(stamp, lease));
            if (i == 6) {
                entry = entry.withCompleted();
            }
            state = state.merge(SpaceStateCrdt.of(entryId, entry));
        }
        return state;
    }

    /** The full fixture's claims: entry 6 (lapsed at 30 s), entry 7 (lapses at 60 s), and the orphan 8 (30 s). */
    static Map<EntryId, SpaceWire.SignedClaim> fullClaims(SpaceId spaceId, AgentId holder, long start) {
        Map<EntryId, SpaceWire.SignedClaim> claims = new java.util.HashMap<>();
        long[][] spec = {{6, 1, 30_000}, {7, 2, 60_000}, {8, 1, 30_000}};
        for (long[] c : spec) {
            EntryId entryId = pinId((int) c[0]);
            TakeClaim claim = new TakeClaim(entryId, spaceId, c[1], new HlcTimestamp(start + 100 + c[0], 0, "pin-node"),
                    holder, 0.0, start + c[2]);
            claims.put(entryId, new SpaceWire.SignedClaim(claim, new byte[32], new byte[64]));
        }
        return claims;
    }

    /** The digest lines of the given entries, spelled out: {@code e:<id>=<hex of the state hash>}, sorted. */
    private static String expectedDigest(SpaceStateCrdt state, java.util.Set<EntryId> keep) {
        java.util.TreeMap<String, String> lines = new java.util.TreeMap<>();
        for (EntryId entryId : keep) {
            EntryState s = state.state(entryId).orElseThrow();
            lines.put("e:" + entryId.value(), Integer.toHexString(Objects.hash(s.adds(), s.removes(),
                    s.lease().stamp(), s.completed())));
        }
        StringBuilder sb = new StringBuilder();
        lines.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        return sb.toString();
    }

    /**
     * Every kind of digest line, spelled out: {@code e:} for each entry the replica knows (present or tombstone),
     * {@code c:<id>=<epoch>:<stamp>:<bid>:<expiry>} for each held claim, sorted together, then {@code a:} for each
     * refused-and-acknowledged line, sorted.
     */
    private static String expectedFullDigest(SpaceStateCrdt state, java.util.Set<EntryId> entries,
                                             Map<EntryId, SpaceWire.SignedClaim> claims) {
        java.util.TreeMap<String, String> lines = new java.util.TreeMap<>();
        for (EntryId entryId : entries) {
            EntryState s = state.state(entryId).orElseThrow();
            lines.put("e:" + entryId.value(), Integer.toHexString(Objects.hash(s.adds(), s.removes(),
                    s.lease().stamp(), s.completed())));
        }
        claims.forEach((entryId, signed) -> {
            TakeClaim c = signed.claim();
            lines.put("c:" + entryId.value(), c.epoch() + ":" + c.stamp().encoded() + ":" + c.bid() + ":"
                    + c.expiresAtMillis());
        });
        StringBuilder sb = new StringBuilder();
        lines.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        new java.util.TreeSet<>(REFUSED_LINES).forEach(line -> sb.append("a:").append(line).append('\n'));
        return sb.toString();
    }

    /**
     * The full fixture (reviews/IDLE-WORK-2026-11-13.md, review of the change): the parts of the digest the expiry
     * index, the collection timing and the claim bookkeeping touch, which are the tombstone, claim and
     * acknowledgement lines, keep the bytes the 0.6.0 jar wrote for the same state at each of three times.
     */
    @Test
    void everyKindOfDigestLineSaysWhatItSaidBefore() {
        long start = clock.instant().toEpochMilli();
        AgentId issuer = identity.agentIdentity("worker").id();
        SpaceStateCrdt state = fullState(space.id(), issuer, start);
        Map<EntryId, SpaceWire.SignedClaim> claims = fullClaims(space.id(), issuer, start);
        space.mergeForTest(state, claims, new java.util.HashSet<>(REFUSED_LINES));

        java.util.Set<EntryId> entries = new java.util.HashSet<>(state.entryIds());
        Map<EntryId, SpaceWire.SignedClaim> held = new java.util.HashMap<>(claims);
        byte[] first = space.digest();
        String text = new String(first, StandardCharsets.UTF_8);
        assertThat(text).isEqualTo(expectedFullDigest(state, entries, held));
        assertThat(text).as("a withdrawal and a completion are listed as tombstones")
                .contains("e:" + pinId(4).value() + "=", "e:" + pinId(6).value() + "=");
        assertThat(text).contains("c:" + pinId(6).value() + "=", "c:" + pinId(7).value() + "=",
                "c:" + pinId(8).value() + "=", "a:e:", "a:c:");
        assertThat(HexFormat.of().formatHex(Digests.sha256(first))).isEqualTo(FULL_PINNED_SHA256);

        // Two minutes: leases 0 and 5 lapse (EXPIRED), 7's claim lapses (REAPPEARED), the orphaned claim 8 goes.
        clock.advance(Duration.ofMinutes(2));
        byte[] swept = space.digest();
        ReplicatedSpace.SweepActions actions = space.lastSweepActions();
        assertThat(actions.expired()).containsExactlyInAnyOrder(pinId(0), pinId(5));
        assertThat(actions.reappeared()).containsExactly(Map.entry(pinId(7), 2L));
        assertThat(actions.orphanClaims()).containsExactly(pinId(8));
        held.remove(pinId(8));
        assertThat(new String(swept, StandardCharsets.UTF_8)).isEqualTo(expectedFullDigest(state, entries, held));
        assertThat(HexFormat.of().formatHex(Digests.sha256(swept))).isEqualTo(FULL_SWEPT_SHA256);

        // The grace ends: 0 and 5 are collected; the withdrawal and the completion stay for two maximum leases.
        clock.advance(Duration.ofMillis(SpaceStateCrdt.LAPSED_GRACE_MILLIS));
        byte[] collected = space.digest();
        entries.remove(pinId(0));
        entries.remove(pinId(5));
        assertThat(new String(collected, StandardCharsets.UTF_8))
                .isEqualTo(expectedFullDigest(state, entries, held));
        assertThat(HexFormat.of().formatHex(Digests.sha256(collected))).isEqualTo(FULL_COLLECTED_SHA256);
        assertThat(space.digestHash()).isEqualTo(Digests.sha256(collected));
    }

    @Test
    void theDigestSaysWhatItSaidBeforeTheIndexAndTheCaches() {
        long start = clock.instant().toEpochMilli();
        SpaceStateCrdt state = pinnedState(space.id(), identity.agentIdentity("worker").id(), start);
        space.mergeForTest(state);

        byte[] first = space.digest();
        // Every entry the replica knows: the five present and the withdrawn one, a tombstone line.
        java.util.Set<EntryId> present = new java.util.HashSet<>(state.entryIds());
        assertThat(present).hasSize(6);
        assertThat(new String(first, StandardCharsets.UTF_8)).isEqualTo(expectedDigest(state, present));
        assertThat(HexFormat.of().formatHex(Digests.sha256(first))).isEqualTo(PINNED_SHA256);
        assertThat(space.digestHash()).isEqualTo(Digests.sha256(first));

        // Two lapses within the grace: EXPIRED fires, nothing is collected, and the digest keeps its bytes.
        clock.advance(Duration.ofMinutes(2));
        space.sweepNow();
        assertThat(space.lastSweepActions().expired()).hasSize(2);
        byte[] lapsed = space.digest();
        assertThat(lapsed).isEqualTo(first);
        assertThat(space.digestHash()).isEqualTo(Digests.sha256(first));

        // The grace ends: the two lapsed entries go, and the digest is exactly the remaining lines.
        clock.advance(Duration.ofMillis(SpaceStateCrdt.LAPSED_GRACE_MILLIS));
        byte[] collected = space.digest();
        present.remove(EntryId.of("00000000-0000-4000-8000-000000000000"));
        present.remove(EntryId.of("00000000-0000-4000-8000-000000000005"));
        assertThat(new String(collected, StandardCharsets.UTF_8)).isEqualTo(expectedDigest(state, present));
        assertThat(HexFormat.of().formatHex(Digests.sha256(collected))).isEqualTo(COLLECTED_SHA256);
        assertThat(space.digestHash()).isEqualTo(Digests.sha256(collected));
    }
}
