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
package ai.badmonkey.agentspaces.connect.hitl;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement.MembershipPolicy;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HITL conventions (SPEC §6.1a, v0.1.38): a desk's card read and checked, the arguments' hash that binds a decision
 * to one call, the trust rule of a requester resolving {@code #name} (an impostor desk in an open group refused, a
 * pinned issuer trusted, two issuers refused unless they are one deployment), the ask's parameters, and the source gate
 * (no approval, pending, refused, other arguments, expired, approved once, then used up).
 */
class HitlTest {

    private static final GroupId GROUP = GroupId.of("zDesks");
    private static final Instant NOW = Instant.parse("2026-11-25T12:00:00Z");

    static AssetCard desk(PeerId issuer, String name, Map<String, String> extra) {
        Map<String, String> access = new LinkedHashMap<>(Map.of("mode", "invoke", "invoke", AssetInvocations.CAPABILITY, "invoke-ops", "ask,status,cancel,pending,decide",
                "deciders", "role:finance-lead", "quorum", "1", "decide-within", "PT4H"));
        access.putAll(extra);
        return new AssetCard("aspace://" + GROUP.value() + "/asset/" + issuer.value() + "/" + name, issuer, GROUP, NOW, Duration.ofMinutes(15), name,
                "desk://" + name, "Finance leads approve refunds over 100", "hitl:approval", "PT5M", Map.of(), access);
    }

    static AssetCard tool(PeerId issuer, Map<String, String> extra) {
        Map<String, String> access = new LinkedHashMap<>(Map.of("mode", "invoke", "invoke", AssetInvocations.CAPABILITY, "invoke-ops", "refund"));
        access.putAll(extra);
        return new AssetCard("aspace://" + GROUP.value() + "/asset/" + issuer.value() + "/orders", issuer, GROUP, NOW, Duration.ofMinutes(15), "orders",
                "shop://orders", "Orders", "table", "PT5M", Map.of(), access);
    }

    @Test
    void aDesksCardIsReadAndChecked() {
        PeerId a = PeerIdentity.generate().peerId();
        AssetCard d = desk(a, "finance-approvals", Map.of());
        assertThat(Hitl.isDesk(d)).isTrue();
        assertThat(Hitl.kindOf(d)).isEqualTo("approval");
        assertThat(Hitl.spaceOf(d)).isEqualTo("data");
        assertThat(Hitl.decideWithin(d)).contains(Duration.ofHours(4));
        assertThat(Hitl.problems(d)).isEmpty();
        assertThat(Hitl.problems(desk(a, "x", Map.of("quorum", "0", "decide-within", "soon", "invoke-ops", "status"))))
                .containsExactlyInAnyOrder("quorum is at least 1", "decide-within is an ISO 8601 duration, such as PT4H", "the desk's invoke-ops list ask");
        assertThat(Hitl.isDesk(tool(a, Map.of()))).isFalse();
    }

    @Test
    void theArgumentsHashBindsOneCall() {
        assertThat(Hitl.argumentsOf(Map.of("arguments", "{\"amount\":250}"))).isEqualTo("{\"amount\":250}");
        Map<String, String> params = new HashMap<>(Map.of("order", "o-7", "amount", "250", "approval", "inv-1"));
        assertThat(Hitl.argumentsOf(params)).as("sorted, without the approval").isEqualTo("{\"amount\":\"250\",\"order\":\"o-7\"}");
        String h = Hitl.argsHash("orders#refund", Hitl.argumentsOf(params));
        assertThat(h).hasSize(64).isEqualTo(Hitl.argsHash("orders#refund", "{\"amount\":\"250\",\"order\":\"o-7\"}"))
                .isNotEqualTo(Hitl.argsHash("orders#refund", "{\"amount\":\"2500\",\"order\":\"o-7\"}"));
    }

    @Test
    void theTrustRuleRefusesADeskAnAttackerCouldStandUp() {
        PeerId owner = PeerIdentity.generate().peerId();
        PeerId impostor = PeerIdentity.generate().peerId();
        AssetCard real = desk(owner, "finance-approvals", Map.of());
        AssetCard fake = desk(impostor, "finance-approvals", Map.of());
        // one issuer, a group that is not open: the name resolves
        assertThat(HitlClient.resolve("#finance-approvals", List.of(real), MembershipPolicy.INVITE, HitlClient.Trust.openCore()).desk()).isEqualTo(real);
        // an open group: anyone could offer the name
        assertThat(HitlClient.resolve("#finance-approvals", List.of(real), MembershipPolicy.OPEN, HitlClient.Trust.openCore()).refusal()).contains("pin");
        // two issuers: ambiguous
        assertThat(HitlClient.resolve("#finance-approvals", List.of(real, fake), MembershipPolicy.INVITE, HitlClient.Trust.openCore()).refusal()).contains("2 issuers");
        // pinned: only the trusted issuer, even in an open group with an impostor
        assertThat(HitlClient.resolve("#finance-approvals", List.of(fake, real), MembershipPolicy.OPEN, HitlClient.Trust.pinned(Set.of(owner))).desk()).isEqualTo(real);
        // a platform's proof: two issuers of one deployment are replicas of one desk
        PeerId replica = PeerIdentity.generate().peerId();
        HitlClient.Trust platform = new HitlClient.Trust(Set.of(), p -> !p.equals(impostor), (x, y) -> true);
        assertThat(HitlClient.resolve("#finance-approvals", List.of(real, desk(replica, "finance-approvals", Map.of()), fake), MembershipPolicy.OPEN, platform).ok()).isTrue();
        assertThat(HitlClient.resolve("#finance-approvals", List.of(real, desk(replica, "finance-approvals", Map.of())), MembershipPolicy.OPEN,
                new HitlClient.Trust(Set.of(owner, replica), null, null)).refusal()).contains("not one desk");
        // the full card id names that issuer's card alone
        assertThat(HitlClient.resolve(fake.id(), List.of(real, fake), MembershipPolicy.OPEN, HitlClient.Trust.openCore()).desk()).isEqualTo(fake);
        assertThat(HitlClient.resolve("#nobody", List.of(real), MembershipPolicy.INVITE, HitlClient.Trust.openCore()).ok()).isFalse();
    }

    @Test
    void anAskCarriesTheHashAndAWaitTheDeskAllows() {
        AssetCard d = desk(PeerIdentity.generate().peerId(), "finance-approvals", Map.of());
        AssetInvocation ask = HitlClient.ask(d, new HitlClient.Ask("orders#refund", "{\"amount\":250}", "Refund 250 on o-7?", "", "support-bot", "t-42", Duration.ofHours(12), "thread t-42"));
        assertThat(ask.operation()).isEqualTo("ask");
        assertThat(ask.asset()).isEqualTo("finance-approvals");
        assertThat(ask.params()).containsEntry("argsHash", Hitl.argsHash("orders#refund", "{\"amount\":250}"))
                .containsEntry("within", "PT4H").containsEntry("onBehalfOf", "t-42").doesNotContainKey("summary");
    }

    @Test
    void theSourceGateAdmitsOneApprovedCallWithItsOwnArguments() {
        PeerId owner = PeerIdentity.generate().peerId();
        AssetCard d = desk(owner, "finance-approvals", Map.of());
        AssetCard orders = tool(PeerIdentity.generate().peerId(), Map.of("hitl.refund", "#finance-approvals"));
        Map<String, AssetInvocationResult> written = new HashMap<>();
        Instant[] now = {NOW};
        ApprovalGate gate = new ApprovalGate(() -> List.of(d, orders), () -> MembershipPolicy.INVITE, HitlClient.Trust.openCore(),
                (desk, id) -> Optional.ofNullable(written.get(id)).map(Hitl.Decision::of), () -> now[0]);
        Map<String, String> args = Map.of("order", "o-7", "amount", "250");
        String hash = Hitl.argsHash("orders#refund", Hitl.argumentsOf(args));
        AssetInvocation bare = AssetInvocation.of("orders", "refund", args);
        assertThat(gate.refusal(orders, bare)).hasValueSatisfying(r -> assertThat(r).startsWith("approval-required"));
        AssetInvocation withId = AssetInvocation.of("orders", "refund", with(args, "approval", "inv-1"));
        assertThat(gate.refusal(orders, withId)).hasValueSatisfying(r -> assertThat(r).startsWith("approval-pending"));
        written.put("inv-1", decision("inv-1", "rejected", hash, NOW.plusSeconds(3600)));
        assertThat(gate.refusal(orders, withId)).hasValueSatisfying(r -> assertThat(r).startsWith("approval-refused"));
        written.put("inv-1", decision("inv-1", "approved", Hitl.argsHash("orders#refund", "{\"amount\":\"9999\",\"order\":\"o-7\"}"), NOW.plusSeconds(3600)));
        assertThat(gate.refusal(orders, withId)).hasValueSatisfying(r -> assertThat(r).startsWith("approval-mismatch"));
        written.put("inv-1", decision("inv-1", "approved", hash, NOW.plusSeconds(3600)));
        now[0] = NOW.plusSeconds(7200);
        assertThat(gate.refusal(orders, withId)).hasValueSatisfying(r -> assertThat(r).startsWith("approval-expired"));
        now[0] = NOW;
        assertThat(gate.refusal(orders, withId)).as("approved, these arguments, in time").isEmpty();
        assertThat(gate.refusal(orders, withId)).hasValueSatisfying(r -> assertThat(r).startsWith("approval-used"));
        // an operation that names no desk, or names the chat step, passes the gate
        assertThat(gate.refusal(tool(owner, Map.of("hitl.refund", "chat")), bare)).isEmpty();
        assertThat(gate.refusal(tool(owner, Map.of()), bare)).isEmpty();
    }

    private static AssetInvocationResult decision(String id, String verdict, String hash, Instant until) {
        return new AssetInvocationResult(1, id, "finance-approvals", "ask", true, Map.of("verdict", verdict, "by", "person:ana", "proof", "oidc:https://idp",
                "argsHash", hash, "decidedAt", NOW.toString(), "validUntil", until.toString()), null);
    }

    private static Map<String, String> with(Map<String, String> m, String k, String v) {
        Map<String, String> out = new HashMap<>(m);
        out.put(k, v);
        return out;
    }
}
