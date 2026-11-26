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
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The source gate (SPEC §6.1a conventions, v0.1.38): an operation whose own card says {@code hitl.<operation> = <desk>}
 * runs only with a person's approval, whatever agent calls it. The caller passes {@code approval=<invocationId>}; the
 * gate reads the decision by that id from the named desk's issuer and admits the call only when it approves, its
 * arguments' hash is this call's, it is still valid, and it was not used before. A value of {@code chat} or {@code alert}
 * names a step another part of the connector checks, not a desk, and passes here.
 */
public final class ApprovalGate {

    /** The parameter a call carries its approval in. */
    public static final String PARAM = "approval";
    /** The access key prefix of an operation's required approval: {@code hitl.<operation>}. */
    public static final String KEY = "hitl.";

    private final Supplier<Collection<AssetCard>> cards;
    private final Supplier<GroupAdvertisement.MembershipPolicy> policy;
    private final HitlClient.Trust trust;
    private final BiFunction<AssetCard, String, Optional<Hitl.Decision>> decisions;
    private final InstantSource clock;
    /** The approvals already used, until a little past their validity, so one approval runs one call. */
    private final Map<String, Instant> used = new ConcurrentHashMap<>();

    /**
     * @param cards   the group's live AssetCards, the desks among them
     * @param policy  the group's membership policy, for the open-core trust rule
     * @param trust   the issuers this connector trusts for {@code #name} desks
     * @param decisions reads a desk's decision on an ask by its id, as its issuer wrote it; see {@link #reader}
     */
    public ApprovalGate(Supplier<Collection<AssetCard>> cards, Supplier<GroupAdvertisement.MembershipPolicy> policy, HitlClient.Trust trust,
                        BiFunction<AssetCard, String, Optional<Hitl.Decision>> decisions, InstantSource clock) {
        this.cards = Objects.requireNonNull(cards, "cards");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.trust = trust == null ? HitlClient.Trust.openCore() : trust;
        this.decisions = Objects.requireNonNull(decisions, "decisions");
        this.clock = clock == null ? InstantSource.system() : clock;
    }

    /** The decision reader over spaces: a desk's decisions are read from the space it takes asks in ({@link Hitl#spaceOf}). */
    public static BiFunction<AssetCard, String, Optional<Hitl.Decision>> reader(Function<AssetCard, Space> spaceOf) {
        return (desk, id) -> HitlClient.decided(spaceOf.apply(desk), desk, id);
    }

    /** The desk an operation's card requires, or empty when it requires none (or names a step this gate does not check). */
    public static Optional<String> required(AssetCard card, String operation) {
        String ref = card.access().get(KEY + operation);
        if (ref == null || ref.isBlank()) {
            return Optional.empty();
        }
        String r = ref.strip();
        return r.startsWith("#") || r.startsWith("aspace://") ? Optional.of(r) : Optional.empty();
    }

    /**
     * Why the call may not run, or empty when it may: no approval is required, or the one it carries holds. A call
     * that passes uses its approval up.
     */
    public synchronized Optional<String> refusal(AssetCard card, AssetInvocation call) {
        Optional<String> desk = required(card, call.operation());
        if (desk.isEmpty()) {
            return Optional.empty();
        }
        String id = call.params().get(PARAM);
        if (id == null || id.isBlank()) {
            return Optional.of("approval-required: " + call.asset() + "#" + call.operation() + " runs only with an approval of " + desk.get()
                    + "; ask the desk and pass approval=<its invocation id>");
        }
        Instant now = clock.instant();
        used.values().removeIf(t -> t.isBefore(now));
        if (used.containsKey(id)) {
            return Optional.of("approval-used: the approval " + id + " ran a call already; one approval runs one call");
        }
        HitlClient.Resolution r = HitlClient.resolve(desk.get(), cards.get(), policy.get(), trust);
        if (!r.ok()) {
            return Optional.of("approval-desk: " + r.refusal());
        }
        Optional<Hitl.Decision> decision = decisions.apply(r.desk(), id);
        if (decision.isEmpty()) {
            return Optional.of("approval-pending: " + desk.get() + " has not decided " + id);
        }
        Hitl.Decision d = decision.get();
        String hash = Hitl.argsHash(call.asset() + "#" + call.operation(), Hitl.argumentsOf(call.params()));
        if (!d.approves()) {
            return Optional.of("approval-refused: " + desk.get() + " decided " + d.verdict() + (d.reason().isEmpty() ? "" : " (" + d.reason() + ")"));
        }
        if (!d.argsHash().equals(hash)) {
            return Optional.of("approval-mismatch: the approval was for other arguments");
        }
        if (d.validUntil() != null && !now.isBefore(d.validUntil())) {
            return Optional.of("approval-expired: the approval was valid until " + d.validUntil());
        }
        used.put(id, (d.validUntil() == null ? now.plus(Hitl.DEFAULT_VALIDITY) : d.validUntil()).plus(Duration.ofMinutes(5)));
        return Optional.empty();
    }
}
