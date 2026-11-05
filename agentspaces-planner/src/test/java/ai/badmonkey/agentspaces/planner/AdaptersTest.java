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
package ai.badmonkey.agentspaces.planner;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.constraints.Event;
import ai.badmonkey.agentspaces.constraints.Marking;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** ISSUE-CardContract FR-4, FR-5: actions read from cards (contract or not), states read from events and markings. */
class AdaptersTest {

    @Test
    void actionsComeFromCardsWithOrWithoutContracts() {
        PeerId peer = PeerId.fromPublicKey(new byte[32]);
        GroupId group = GroupId.of("zPlan");
        CardAction pay = new CardAction("pay", "", List.of("claims.PaymentOrder#v1"), List.of("claims.Payment#v1"), "payments", CardAction.TAKE);
        CardAction schedule = new CardAction("schedule", "", List.of("claims.Claim#v1"), List.of("claims.SiteVisit#v1"), null, CardAction.TAKE,
                new CardAction.Contract(List.of("adjuster-assigned"), List.of("visit-scheduled"), "PT2H", List.of()));
        CardAction ballot = new CardAction("judge", "", List.of("Proposal"), List.of("Ballot"), "votes", CardAction.BALLOT);
        AgentCard card = new AgentCard("aspace://zPlan/agent/p", peer, group, Instant.EPOCH, Duration.ofMinutes(15), new AgentId(peer, "payer"), "",
                List.of(), List.of("claims.PaymentOrder#v1", "claims.Claim#v1"), List.of("claims.Payment#v1"), Map.of(), Map.of("claims.Claim#v1", "intake"), null,
                List.of(pay, schedule, ballot));
        List<PlanAction> actions = PlanAction.of(card);
        assertThat(actions).extracting(PlanAction::name).containsExactly("pay", "schedule");   // a ballot is never invocable
        assertThat(actions.get(0)).satisfies(a -> {
            assertThat(a.card()).isEqualTo("payer");
            assertThat(a.consumes()).containsExactly("PaymentOrder");
            assertThat(a.produces()).containsExactly("Payment");
            assertThat(a.space()).isEqualTo("payments");
            assertThat(a.pre()).isEmpty();
        });
        assertThat(actions.get(1)).satisfies(a -> {
            assertThat(a.pre()).containsExactly("adjuster-assigned");
            assertThat(a.effects()).containsExactly("SiteVisit", "visit-scheduled");
            assertThat(a.cost()).isEqualTo("PT2H");
            assertThat(a.space()).as("from the card's space bindings").isEqualTo("intake");
        });
        assertThat(SearchPlanner.costOf(actions.get(1))).isEqualTo(120.0);
    }

    @Test
    void stateComesFromEventsAndTheMarking() {
        Event written = new Event("Claim", "claims.Claim#v1", "e1", Instant.EPOCH, Map.of("claimId", "c-1"), Map.of(), Event.Kind.WRITTEN);
        Event claimed = new Event("PaymentOrder", "claims.PaymentOrder#v1", "e2", Instant.EPOCH, Map.of(), Map.of(), Event.Kind.CLAIMED);
        Marking marking = new Marking(Map.of(), Set.of(), Set.of(), Set.of(), List.of(), Map.of("high-value", true, "quiet", false));
        PlanningState state = PlanningState.of(List.of(written, claimed), marking);
        assertThat(state.types()).containsExactly("Claim");
        assertThat(state.situations()).containsExactly("high-value");
        assertThat(state.holds(List.of("claims.Claim#v1", "high-value"))).isTrue();
        assertThat(state.has("quiet")).isFalse();
        PlanAction pay = new PlanAction("pay", "payer", List.of("Claim"), List.of("Payment"), List.of(), List.of("paid"), "", "");
        PlanningState after = state.after(pay);
        assertThat(after.types()).contains("Payment");
        assertThat(after.situations()).contains("paid");
    }
}
