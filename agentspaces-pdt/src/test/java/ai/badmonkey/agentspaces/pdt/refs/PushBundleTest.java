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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.pdt.golden.PushVectors;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link PushBundle} record (PDT 2 phase 5, P2-P-01, §6.10.4, rule 16): a receiver reads it strictly, and it
 * refuses a bundle outside its bounds by name; the announcing attribute {@code pdt.push} counts in canonical decimal.
 * {@code PdtVectorsConformanceTest} checks its golden vector {@code pdt_push_bundle_cbor} ({@code PushVectors}).
 */
class PushBundleTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private static PushBundle vector() {
        return PushVectors.pushBundle();
    }

    @Test
    void theBundleOfTheGoldenVectorReadsBackStrictlyAndKeepsItsBytes() {
        byte[] bytes = CODEC.toBytes(vector());
        PushBundle read = CODEC.fromWire(bytes, PushBundle.class);
        assertThat(read.violation()).isEmpty();
        assertThat(read.root()).isEqualTo(vector().root());
        assertThat(read.nodes()).singleElement().isEqualTo(vector().nodes().get(0));
        assertThat(CODEC.toBytes(read)).isEqualTo(bytes);
    }
    @Test
    void aBundleOutsideItsSchemaOrBoundsIsRefused() {
        PushBundle good = vector();
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("v", 1);
        extra.put("root", good.root());
        extra.put("seq", 3L);
        extra.put("part", 0);
        extra.put("parts", 1);
        extra.put("nodes", good.nodes());
        extra.put("x", 1);
        assertThatThrownBy(() -> CODEC.fromWire(CODEC.toBytes(extra), PushBundle.class)).as("a field the schema does not name")
                .isInstanceOf(RuntimeException.class);
        byte[] node = good.nodes().get(0);
        assertThat(new PushBundle(2, good.root(), 3, 0, 1, List.of(node)).violation()).get().asString().contains("version");
        assertThat(new PushBundle(1, "root", 3, 0, 1, List.of(node)).violation()).get().asString().contains("CID");
        assertThat(new PushBundle(1, good.root(), -1, 0, 1, List.of(node)).violation()).get().asString().contains("seq");
        assertThat(new PushBundle(1, good.root(), 3, 1, 1, List.of(node)).violation()).get().asString().contains("part 1 of 1");
        assertThat(new PushBundle(1, good.root(), 3, 0, PushBundle.MAX_PARTS + 1, List.of(node)).violation()).isPresent();
        assertThat(new PushBundle(1, good.root(), 3, 0, 1, List.of()).violation()).get().asString().contains("at least one");
        assertThat(new PushBundle(1, good.root(), 3, 0, 1, List.of(new byte[PushBundle.MAX_BYTES + 1])).violation())
                .get().asString().contains("at most " + PushBundle.MAX_BYTES);
        assertThat(PushPublisher.announced(good(Map.of(PushPublisher.ATTR, "3")))).isEqualTo(3);
        assertThat(PushPublisher.announced(good(Map.of(PushPublisher.ATTR, "03")))).as("not canonical decimal").isZero();
        assertThat(PushPublisher.announced(good(Map.of(PushPublisher.ATTR, "65")))).as("past the part bound").isZero();
        assertThat(PushPublisher.announced(good(Map.of()))).isZero();
    }

    /**
     * R1 probe P10 (reviews/RBE-REGRESSION-MATRIX.md §3; replication by entry phase 1, RE-M-08): a follower holds the
     * pushed bundles of two deltas, a head and its successor, before it holds either head, and the two heads then reach
     * it in one {@code PULL_RESP} (its rumour handler for the ref space drops every item, so only anti-entropy brings
     * them), whose WRITTEN events fire after both are in, in the order the pull carried them. Each head announces its
     * push, the held bundles complete both roots, and the follower sends no {@code TREE_WANT} and falls back on none,
     * whichever head's event fires first.
     */
    @Test
    void heldBundlesCompleteAHeadAndItsSuccessorThatArriveInOnePullResp() throws Exception {
        // On the text form of §5.3 the pull carries the two heads in the map's order, not the order written: fresh
        // fleets until both orders ran. On the audit of annex I.6 (replication by entry, phase 4) the follower fetches
        // them by id and the answer carries them newest first (RE-I-06), so the successor's event always fires first.
        java.util.Set<String> orders = new java.util.HashSet<>();
        for (int run = 0; run < 16 && orders.size() < 2; run++) {
            orders.add(heldBundlesThenHeadsInOnePull(4200 + 10L * run, false));
        }
        assertThat(orders).as("the successor's event fired first in some run, and the head's in another")
                .containsExactlyInAnyOrder("first", "second");
        for (int run = 0; run < 2; run++) {
            assertThat(heldBundlesThenHeadsInOnePull(4200 + 500 + 10L * run, true)).as("newest first under the audit")
                    .isEqualTo("second");
        }
    }

    /** One run of the push half of P10 on a fresh fleet; returns which head's WRITTEN event fired first. */
    private static String heldBundlesThenHeadsInOnePull(long seed, boolean audit) throws Exception {
        RefFleet fleet = new RefFleet().audit(audit);
        try {
            RefFleet.Spec pushing = RefFleet.Spec.defaults().pdt(b -> b.push(PushPublisher.Config.on()));
            // the writer cancels its displaced head only after a long grace, so both heads are live when the pull runs
            RefFleet.Peer writer = fleet.newPeer("w", seed,
                    RefFleet.Spec.defaults().pdt(b -> b.push(PushPublisher.Config.on()).cancelGrace(Duration.ofMinutes(5))));
            RefFleet.Peer follower = fleet.newPeer("f", seed + 1, pushing, "w");
            fleet.settle(10);
            String name = "plans";
            Ref<PMap<String, String>> ref = writer.pdt().mapRef(name, RefFleet.STR, RefFleet.STR);
            Ref<PMap<String, String>> onFollower = follower.pdt().mapRef(name, RefFleet.STR, RefFleet.STR);
            List<String> eventRoots = new CopyOnWriteArrayList<>();
            List<Integer> headsAtEvent = new CopyOnWriteArrayList<>();
            follower.refSpace().notify(Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq(name)), event -> {
                if (event.kind() == SpaceEvent.Kind.WRITTEN) {
                    eventRoots.add(event.entry().root());
                    headsAtEvent.add(RefFleet.headEntries(follower, name).size());
                }
            }, Lease.of(Duration.ofHours(1)));
            follower.runtime().gossip().replaceHandler("space:" + RefFleet.REF_SPACE, (from, id, payload) -> { });
            long pullsBefore = follower.runtime().gossip().pacingSignals().pullRespReceived();

            PMap<String, String> first = ref.swap(m -> grow(m, 3 * PRef.INLINE_DELTA_BYTES, seed));
            PMap<String, String> second = ref.swap(m -> grow(m, 3 * PRef.INLINE_DELTA_BYTES, seed + 1));
            PushPublisher.Stats pushed = writer.pdt().refs().pushStats();
            assertThat(pushed.deltasPushed()).as("both deltas pass the inline bound and are pushed").isEqualTo(2);
            assertThat(fleet.runUntil(Duration.ofSeconds(20),
                    () -> follower.pdt().refs().pushStats().bundlesReceived() >= pushed.bundlesSent()))
                    .as("the follower holds every bundle").isTrue();
            assertThat(RefFleet.headEntries(follower, name)).as("and neither head yet").isEmpty();

            assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> onFollower.heads().roots().equals(List.of(second.root()))
                    && follower.store().complete(second.root()))).as("the follower ends on the successor and holds it").isTrue();
            fleet.settle(8);

            assertThat(follower.runtime().gossip().pacingSignals().pullRespReceived()).isGreaterThan(pullsBefore);
            assertThat(eventRoots).as("a WRITTEN per head").containsExactlyInAnyOrder(first.root(), second.root());
            assertThat(headsAtEvent).as("both heads were in when the first event fired: one batch").containsExactly(2, 2);
            assertThat(follower.store().complete(first.root())).as("the first head's push completed it too").isTrue();
            assertThat(onFollower.deref().root()).isEqualTo(second.root());
            PushPublisher.Stats received = follower.pdt().refs().pushStats();
            assertThat(follower.pdt().stats().deltasFetched()).as("no TREE_WANT: the held bundles completed both roots").isZero();
            assertThat(received.fallbacks()).as("no fallback").isZero();
            assertThat(received.bytesStored()).as("each delta stored once").isEqualTo(pushed.bytesPushed());
            assertThat(writer.content().stats().nodesServed()).as("the writer served no node").isZero();
            String firstEvent = eventRoots.get(0).equals(first.root()) ? "first" : "second";
            System.out.printf("P10 held bundles, heads in one PULL_RESP (seed %d): %s head's event first; %d bundles, %d bytes"
                    + " stored, %d fetched%n", seed, firstEvent, received.bundlesReceived(), received.bytesStored(),
                    follower.pdt().stats().deltasFetched());
            return firstEvent;
        } finally {
            fleet.close();
        }
    }

    /** A value with about {@code bytes} bytes of fresh entries over {@code base}. */
    private static PMap<String, String> grow(PMap<String, String> base, int bytes, long seed) {
        Random random = new Random(seed);
        PMap<String, String> out = base;
        for (int i = 0; i * 120 < bytes; i++) {
            out = out.assoc("plan-" + seed + "-" + i, RefFleet.value(100, random));
        }
        return out;
    }

    private ai.badmonkey.agentspaces.agent.pdt.PRef good(Map<String, ?> attrs) {
        return new ai.badmonkey.agentspaces.agent.pdt.PRef(1, "n", "map", vector().root(), List.of(), 1, 1, null,
                ai.badmonkey.agentspaces.agent.pdt.PRef.MODE_MULTI, ai.badmonkey.agentspaces.agent.pdt.PRef.SEALING_NONE, null,
                attrs.isEmpty() ? null : attrs);
    }
}
