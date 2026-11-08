/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.constraints;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** FR-7 to FR-11, SR-1: the gate over local spaces. */
class ConstraintGateTest {

    record HumanApproval(String claimId, String by) {
    }

    record PaymentOrder(String claimId, long amount) {
    }

    record Quote(String claimId, String vendor) {
    }

    record Approval(String claimId, String by) {
    }

    /** The platform's shape of a record written for a type the writer has no class of: the gate knows the shape only. */
    record MappedRecord(int v, String type, String by, String key, Map<String, Object> fields) {
    }

    static final PeerIdentity ID = PeerIdentity.generate();

    static Space space(String name) {
        return LocalSpace.builder(name, ID.agent("t")).build();
    }

    static void publish(Space constraints, Constraint... cs) {
        Vocabulary v = new Vocabulary(1, "rb-claims", 1, "h1", Map.of("big", "exists(PaymentOrder where amount > 50000)"));
        constraints.write(v, Lease.of(Duration.ofDays(1)), v.tags());
        for (Constraint c : cs) {
            constraints.write(c, Lease.of(Duration.ofDays(1)), c.tags("h1"));
        }
    }

    static Constraint precedence() {
        return new Constraint(1, "rb-claims", 1, "k1", Constraint.PRECEDENCE, "HumanApproval", List.of(), "PaymentOrder",
                List.of("amount>50000"), 0, "", "field:claimId", List.of("gate", "monitor"), Map.of());
    }

    @Test
    void aPrecedenceRefusesTheClaimAndWritesViolated() throws Exception {
        Space constraints = space("constraints");
        Space payments = space("payments");
        publish(constraints, precedence());
        try (ConstraintGate gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "gate-1")) {
            gate.watch("payments", payments);
            assertThat(gate.loaded()).hasSize(1);
            payments.write(new PaymentOrder("c-1", 60000), Lease.of(Duration.ofMinutes(5)));
            Thread.sleep(100);
            Space.Entry<PaymentOrder> candidate = payments.readAllEntries(Template.of(PaymentOrder.class), 1).get(0);
            ConstraintGate.Decision d = gate.beforeClaim(candidate);
            assertThat(d.admitted()).isFalse();
            assertThat(d.unit()).isEqualTo("k1");
            assertThat(d.key()).isEqualTo("c-1");
            assertThat(constraints.readAll(Template.of(ConstraintViolated.class), 10)).singleElement()
                    .satisfies(v -> {
                        assertThat(v.event()).isEqualTo(ConstraintViolated.CLAIM);
                        assertThat(v.by()).isEqualTo("gate-1");
                    });
            // the same refusal once more writes nothing new
            gate.beforeClaim(candidate);
            assertThat(constraints.readAll(Template.of(ConstraintViolated.class), 10)).hasSize(1);
            // a small order is outside the guard: admitted
            payments.write(new PaymentOrder("c-2", 100), Lease.of(Duration.ofMinutes(5)));
            Thread.sleep(100);
            Space.Entry<PaymentOrder> small = payments.readAllEntries(Template.of(PaymentOrder.class), 10).stream()
                    .filter(e -> e.value().claimId().equals("c-2")).findFirst().orElseThrow();
            assertThat(gate.beforeClaim(small).admitted()).isTrue();
            // after an approval for c-1 the order is enabled
            Space approvals = space("approvals");
            gate.watch("approvals", approvals);
            approvals.write(new HumanApproval("c-1", "alice"), Lease.of(Duration.ofMinutes(5)));
            Thread.sleep(100);
            assertThat(gate.beforeClaim(candidate).admitted()).as("events=" + gate.events("c-1") + " marking="
                    + gate.marking("rb-claims", "c-1")).isTrue();
            assertThat(gate.marking("rb-claims", "c-1").conditions()).containsEntry("big", true);
            // a result write of a gated type is judged the same way
            assertThat(gate.beforeResult(new PaymentOrder("c-3", 70000), null, Map.of()).admitted()).isFalse();
            assertThat(gate.beforeResult(new PaymentOrder("c-1", 70000), null, Map.of()).admitted()).isTrue();
        }
    }

    @Test
    void aCardinalityAdmitsNThenRefuses() throws Exception {
        Space constraints = space("constraints");
        Space quotes = space("quotes");
        publish(constraints, new Constraint(1, "rb-claims", 1, "k3", Constraint.CARDINALITY, "Quote", List.of(), "", List.of(), 2, "",
                "field:claimId", List.of("gate"), Map.of()));
        try (ConstraintGate gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "g")) {
            gate.watch("quotes", quotes);
            assertThat(gate.beforeResult(new Quote("c-9", "a"), null, Map.of()).admitted()).isTrue();
            quotes.write(new Quote("c-9", "a"), Lease.of(Duration.ofMinutes(5)));
            quotes.write(new Quote("c-9", "b"), Lease.of(Duration.ofMinutes(5)));
            Thread.sleep(100);
            assertThat(gate.events("c-9")).hasSize(2);
            ConstraintGate.Decision third = gate.beforeResult(new Quote("c-9", "c"), null, Map.of());
            assertThat(third.admitted()).isFalse();
            assertThat(third.reason()).contains("Quote");
            assertThat(gate.beforeResult(new Quote("c-10", "a"), null, Map.of()).admitted()).isTrue();
        }
    }

    @Test
    void aJuriedUnitRefusesWithoutADecisionAndAdmitsWithOne() throws Exception {
        Space constraints = space("constraints");
        Space payments = space("payments");
        publish(constraints, new Constraint(1, "rb-claims", 1, "k2", Constraint.PRECEDENCE, "DamageCheck", List.of(), "PaymentOrder",
                List.of(), 0, "", "field:claimId", List.of("jury"), Map.of("quorum", 2)));
        try (ConstraintGate gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "g")) {
            gate.watch("payments", payments);
            assertThat(gate.beforeResult(new PaymentOrder("c-1", 10), null, Map.of()).admitted()).isFalse();
            constraints.write(new JuryDecision(1, "rb-claims", 1, "k2", "c-1", "m-1", JuryDecision.ALLOW, List.of(), java.time.Instant.now()),
                    Lease.of(Duration.ofDays(7)));
            Thread.sleep(100);
            assertThat(gate.beforeResult(new PaymentOrder("c-1", 10), null, Map.of()).admitted()).isTrue();
            assertThat(gate.beforeResult(new PaymentOrder("c-2", 10), null, Map.of()).admitted()).isFalse();
        }
    }

    @Test
    void failsOpenWhenNothingIsLoadedWhenTheSpaceIsGoneAndWhenTheEngineThrows() throws Exception {
        Space constraints = space("constraints");
        try (ConstraintGate gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "g")) {
            assertThat(gate.loaded()).isEmpty();
            assertThat(gate.beforeResult(new PaymentOrder("c-1", 1), null, Map.of()).admitted()).isTrue();
            // a constraint with a template the engine does not know, and a malformed guard: still admitted
            Vocabulary v = new Vocabulary(1, "rb", 1, "h2", Map.of());
            constraints.write(v, Lease.of(Duration.ofDays(1)), v.tags());
            Constraint odd = new Constraint(1, "rb", 1, "x", "banana", "PaymentOrder", List.of("!!"), "", List.of(), 0, "", "field:claimId",
                    List.of("gate"), Map.of());
            constraints.write(odd, Lease.of(Duration.ofDays(1)), odd.tags("h2"));
            Thread.sleep(100);
            gate.reload();
            assertThat(gate.loaded()).hasSize(1);
            assertThat(gate.beforeResult(new PaymentOrder("c-1", 1), null, Map.of()).admitted()).isTrue();
            assertThat(gate.beforeClaim(null).admitted()).isTrue();
        }
        // the space failing underneath: still admitted
        Space failing = (Space) java.lang.reflect.Proxy.newProxyInstance(Space.class.getClassLoader(), new Class<?>[]{Space.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("name")) {
                        return "constraints-failing";
                    }
                    throw new IllegalStateException("space is gone");
                });
        ConstraintGate gate = ConstraintGate.attach(failing, null, "constraints", InstantSource.system(), "g");
        assertThat(gate.loaded()).isEmpty();
        assertThat(gate.beforeResult(new PaymentOrder("c-1", 1), null, Map.of()).admitted()).isTrue();
        gate.close();
    }

    @Test
    void ignoresAVersionFromAnUnpermittedIssuerAndAHalfPresentVersion() throws Exception {
        Space constraints = space("constraints");
        Authorizer nobody = new Authorizer() {
            @Override
            public boolean permits(PeerId peer, Operation operation, String scope) {
                return false;
            }
        };
        publish(constraints, precedence());
        try (ConstraintGate gate = ConstraintGate.attach(constraints, nobody, "constraints", InstantSource.system(), "g")) {
            assertThat(gate.loaded()).as("the issuer may not write the scope").isEmpty();
            assertThat(gate.beforeResult(new PaymentOrder("c-1", 60000), null, Map.of()).admitted()).isTrue();
        }
        Space half = space("constraints-half");
        Constraint orphan = precedence();
        half.write(orphan, Lease.of(Duration.ofDays(1)), orphan.tags("h-missing"));
        try (ConstraintGate gate = ConstraintGate.attach(half, null, "constraints", InstantSource.system(), "g")) {
            assertThat(gate.loaded()).as("a constraint without its vocabulary is not loaded").isEmpty();
        }
    }

    @Test
    void recordsRoundTripThroughCborWithTheirTags() {
        var codec = ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();
        Vocabulary v = new Vocabulary(1, "rb-claims", 7, "sha256:abc", Map.of("c1", "exists(Claim where amount > 50000)"));
        assertThat(codec.fromBytes(codec.toBytes(v), Vocabulary.class)).isEqualTo(v);
        Constraint c = precedence();
        assertThat(codec.fromBytes(codec.toBytes(c), Constraint.class)).isEqualTo(c);
        assertThat(c.tags("sha256:abc")).containsEntry(RuleTags.UNIT, "k1@sha256:abc").containsEntry(RuleTags.CHARTER, "rb-claims");
        Goal g = new Goal(1, "rb-claims", 7, "g1", "c-1", "Payment", List.of(), java.time.Instant.EPOCH, "rule-agent", null);
        assertThat(codec.fromBytes(codec.toBytes(g), Goal.class).state()).isEqualTo(Goal.OPEN);
        ConstraintViolated cv = new ConstraintViolated(1, "rb-claims", 7, "k1", "c-1", java.time.Instant.EPOCH, "gate", ConstraintViolated.CLAIM);
        assertThat(codec.fromBytes(codec.toBytes(cv), ConstraintViolated.class)).isEqualTo(cv);
        assertThat(AgentId.class).isNotNull();
    }

    /** A flow step's result rides as a MappedRecord; the gate types it by the logical type it carries, so a rule book can name it. */
    @Test
    void aMappedRecordOnALaneMarksUnderItsLogicalType() throws Exception {
        Space constraints = space("constraints");
        Space assessments = space("assessments");
        publish(constraints, new Constraint(1, "rb-claims", 1, "k5", Constraint.PRECEDENCE, "Assessment", List.of(), "Approval",
                List.of(), 0, "", "field:claimId", List.of("gate", "monitor"), Map.of()));
        try (ConstraintGate gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "g")) {
            gate.watch("assessments", assessments);
            assertThat(gate.beforeResult(new Approval("c-1", "alice"), null, Map.of()).admitted()).as("no assessment yet").isFalse();
            MappedRecord assessment = new MappedRecord(1, "acme.Assessment", "assess", "c-1", Map.of("claimId", "c-1", "verdict", "approve"));
            assessments.write(assessment, Lease.of(Duration.ofMinutes(5)), Map.of("type", "acme.Assessment", "key", "c-1", "by", "assess"));
            Thread.sleep(100);
            assertThat(gate.events("c-1")).singleElement().satisfies(e -> {
                assertThat(e.type()).isEqualTo("Assessment");
                assertThat(e.wireType()).contains("MappedRecord");
                assertThat(e.fields()).containsEntry("verdict", "approve").containsEntry("key", "c-1");
            });
            assertThat(gate.beforeResult(new Approval("c-1", "alice"), null, Map.of()).admitted()).as("the assessment enables the approval").isTrue();
            // an approval that rides as a mapped record is judged by its logical type too
            MappedRecord approval = new MappedRecord(1, "acme.Approval", "approve", "c-2", Map.of("claimId", "c-2"));
            assertThat(gate.beforeResult(approval, null, Map.of()).admitted()).as("no assessment for c-2").isFalse();
            assessments.write(new MappedRecord(1, "acme.Assessment", "assess", "c-2", Map.of("claimId", "c-2")), Lease.of(Duration.ofMinutes(5)));
            Thread.sleep(100);
            assertThat(gate.beforeResult(approval, null, Map.of()).admitted()).isTrue();
            Space.Entry<MappedRecord> claimed = assessments.readAllEntries(Template.of(MappedRecord.class), 10).get(0);
            assertThat(gate.narrow(Template.of(MappedRecord.class), claimed)).as("narrowed to the key the logical type names")
                    .isNotSameAs(Template.of(MappedRecord.class));
        }
    }

    /** A geo view's shape: one unit's newest position and the last forty points of its track. */
    record Tracked(String key, double lat, double lon, List<Map<String, Object>> track) {
    }

    static Tracked tracked(int unit, int sweep) {
        List<Map<String, Object>> track = new java.util.ArrayList<>();
        for (int i = 0; i < 40; i++) {
            track.add(Map.of("lat", 40.0 + i / 1000.0, "lon", -75.0 - i / 1000.0, "at", "2026-11-08T14:" + sweep + ":" + i));
        }
        return new Tracked("truck-" + unit, 40.0 + sweep / 1000.0, -75.0, track);
    }

    /**
     * FIT 2026-11-08 (geo watch, live heaps of the host, the feed, and the summary grew 80 to 110 MB in three minutes, each
     * histogram holding exactly 4096 constraint Events): a gate with no book watched the views lane and kept every View
     * the view agent wrote, a forty-point track each, in its list for a type nothing names yet, up to the 4096 it caps.
     * Many sweeps of short-leased views of a type no book names leave the gate holding nothing.
     */
    @Test
    void manySweepsOfATypeNothingNamesLeaveTheGateHoldingNothing() throws Exception {
        Space constraints = space("constraints");
        Space views = space("views");
        try (ConstraintGate gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "g")) {
            gate.watch("views", views);
            for (int sweep = 0; sweep < 600; sweep++) {
                for (int unit = 0; unit < 12; unit++) {
                    views.write(tracked(unit, sweep), Lease.of(Duration.ofSeconds(30)), Map.of("kind", "view", "key", "truck-" + unit));
                }
                gate.reload();   // a constraints notification between sweeps replays what the gate keeps
            }
            assertThat(gate.heldEvents()).as("events held after 7200 views of a type nothing names").isZero();
            assertThat(gate.keys()).isEmpty();
        }
    }

    /**
     * FIT 2026-11-08: the gate keeps no event of a type nothing names, so a book published after the events reads the
     * lanes again for the types it names; a take, which the lane can no longer show, is kept and still counts.
     */
    @Test
    void aBookPublishedAfterTheEventsCountsTheLanesEntriesAndTheTakesBeforeIt() throws Exception {
        Space constraints = space("constraints");
        Space approvals = space("approvals");
        try (ConstraintGate gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "g")) {
            gate.watch("approvals", approvals);
            approvals.write(new HumanApproval("c-2", "bob"), Lease.of(Duration.ofMinutes(5)));
            assertThat(approvals.take(Template.of(HumanApproval.class), Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(1))).isPresent();
            approvals.write(new HumanApproval("c-1", "alice"), Lease.of(Duration.ofMinutes(5)));
            Thread.sleep(100);
            publish(constraints, precedence());
            Thread.sleep(100);
            assertThat(gate.loaded()).hasSize(1);
            assertThat(gate.beforeResult(new PaymentOrder("c-1", 60000), null, Map.of()).admitted()).as("the approval the lane holds").isTrue();
            assertThat(gate.beforeResult(new PaymentOrder("c-2", 60000), null, Map.of()).admitted()).as("the approval taken before the book").isTrue();
            assertThat(gate.beforeResult(new PaymentOrder("c-3", 60000), null, Map.of()).admitted()).as("no approval").isFalse();
            assertThat(gate.events("c-1")).hasSize(1);
        }
    }

    /** FIT 2026-11-08: an interest named after the lane's entries were written reads them from the lane. */
    @Test
    void anInterestNamedAfterTheEntriesReadsThemFromTheLane() throws Exception {
        Space constraints = space("constraints");
        Space quotes = space("quotes");
        try (ConstraintGate gate = ConstraintGate.attach(constraints, null, "constraints", InstantSource.system(), "g")) {
            gate.watch("quotes", quotes);
            quotes.write(new Quote("c-5", "a"), Lease.of(Duration.ofMinutes(5)));
            quotes.write(new Quote("c-5", "b"), Lease.of(Duration.ofMinutes(5)));
            Thread.sleep(100);
            assertThat(gate.heldEvents()).as("nothing names Quote yet").isZero();
            gate.interest("Quote", "field:claimId");
            assertThat(gate.events("c-5")).hasSize(2);
            gate.interest("Quote", "field:claimId");   // the same interest again reads nothing twice
            assertThat(gate.events("c-5")).hasSize(2);
        }
    }
}
