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
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * Asks a desk of people (SPEC §6.1a conventions, v0.1.38): resolves a reference to one desk's card under the trust
 * rule, asks it with an {@code ask} asset call, reads its decision, and cancels an ask. A decision counts only when the
 * desk's own issuer wrote it, as every asset call's result does.
 *
 * <p><b>The trust rule.</b> A decision can move money, so a requester never accepts a desk an attacker could stand up.
 * A full card id ({@code aspace://<group>/asset/<issuer>/<name>}) names that issuer's live card or nothing. A
 * {@code #name} resolves only among the issuers the requester trusts: its pinned list, or, where a platform proves which
 * issuers run its enrolled deployments, those. With neither, {@code #name} resolves when exactly one live issuer offers
 * the name and the group's membership policy is not {@code OPEN}. Several trusted issuers of one name are replicas of one
 * desk only when they prove the same deployment; otherwise the name is ambiguous and refused.
 */
public final class HitlClient {

    /** Which issuers a requester trusts for {@code #name} desks. */
    public record Trust(Set<PeerId> pinned, Predicate<PeerId> provesDeployment, BiPredicate<PeerId, PeerId> sameDeployment) {

        public Trust {
            pinned = pinned == null ? Set.of() : Set.copyOf(pinned);
        }

        /** The open core's default: no pinned issuer and no platform proof. */
        public static Trust openCore() {
            return new Trust(Set.of(), null, null);
        }

        /** A requester's own pinned issuers. */
        public static Trust pinned(Set<PeerId> issuers) {
            return new Trust(issuers, null, null);
        }

        boolean trusts(PeerId issuer) {
            return pinned.contains(issuer) || provesDeployment != null && provesDeployment.test(issuer);
        }

        boolean names() {
            return !pinned.isEmpty() || provesDeployment != null;
        }
    }

    /** A reference resolved: the desk's card, or the reason it was refused. */
    public record Resolution(AssetCard desk, String refusal) {
        public boolean ok() {
            return desk != null;
        }
    }

    /**
     * What an agent asks a person.
     *
     * @param action     {@code <asset>#<operation>}, or a free description of a non-tool action
     * @param arguments  the arguments as text, as {@link Hitl#argumentsOf} gives them for a call
     * @param question   the question rendered from the arguments by the requester's own template, never by a model
     * @param summary    the model's own explanation, shown labelled as the model's; may be empty
     * @param requester  the asking agent's name
     * @param onBehalfOf the person the agent acts for, when known; may be empty
     * @param within     the wait the requester accepts; the desk's {@code decide-within} caps it
     * @param context    a link back: the thread, the page, the record key; may be empty
     */
    public record Ask(String action, String arguments, String question, String summary, String requester, String onBehalfOf,
                      Duration within, String context) {
        public Ask {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(question, "question");
            arguments = arguments == null ? "{}" : arguments;
            summary = summary == null ? "" : summary;
            requester = requester == null ? "" : requester;
            onBehalfOf = onBehalfOf == null ? "" : onBehalfOf;
            context = context == null ? "" : context;
        }
    }

    private HitlClient() {
    }

    /**
     * Resolves a reference among the live cards of the group.
     *
     * @param reference a full card id, or {@code #name}
     * @param cards     the group's live AssetCards
     * @param policy    the group's membership policy; null reads as {@code OPEN}
     * @param trust     the requester's trust
     */
    public static Resolution resolve(String reference, Collection<AssetCard> cards, GroupAdvertisement.MembershipPolicy policy, Trust trust) {
        if (reference == null || reference.isBlank()) {
            return new Resolution(null, "no desk named");
        }
        String ref = reference.strip();
        List<AssetCard> desks = cards.stream().filter(Hitl::isDesk).toList();
        if (ref.startsWith("aspace://")) {
            return desks.stream().filter(c -> ref.equals(c.id())).findFirst().map(c -> new Resolution(c, null))
                    .orElse(new Resolution(null, "no live desk card " + ref));
        }
        if (!ref.startsWith("#") || ref.length() < 2) {
            return new Resolution(null, "a desk is named #name or by its full card id, not " + ref);
        }
        String name = ref.substring(1);
        List<AssetCard> named = desks.stream().filter(c -> name.equals(c.asset())).toList();
        if (named.isEmpty()) {
            return new Resolution(null, "no live desk named " + ref);
        }
        Trust t = trust == null ? Trust.openCore() : trust;
        if (t.names()) {
            List<AssetCard> trusted = named.stream().filter(c -> t.trusts(c.issuer())).toList();
            if (trusted.isEmpty()) {
                return new Resolution(null, "no trusted issuer offers " + ref + "; pin the desk's card id");
            }
            AssetCard first = trusted.get(0);
            boolean replicas = trusted.stream().allMatch(c -> c.issuer().equals(first.issuer())
                    || t.sameDeployment() != null && t.sameDeployment().test(first.issuer(), c.issuer()));
            return replicas ? new Resolution(first, null) : new Resolution(null, "two trusted issuers offer " + ref + " and are not one desk; pin the card id");
        }
        Set<PeerId> issuers = named.stream().map(AssetCard::issuer).collect(java.util.stream.Collectors.toSet());
        if (issuers.size() != 1) {
            return new Resolution(null, issuers.size() + " issuers offer " + ref + "; pin the desk's card id");
        }
        if (policy == null || policy == GroupAdvertisement.MembershipPolicy.OPEN) {
            return new Resolution(null, "the group is open, so anyone could offer " + ref + "; pin the desk's card id");
        }
        return new Resolution(named.get(0), null);
    }

    /** The ask as an asset call, its id minted once; the parameters carry the arguments' hash that binds the decision. */
    public static AssetInvocation ask(AssetCard desk, Ask a) {
        Duration within = a.within();
        Optional<Duration> cap = Hitl.decideWithin(desk);
        if (within == null || cap.isPresent() && within.compareTo(cap.get()) > 0) {
            within = cap.orElse(within == null ? Duration.ofHours(4) : within);
        }
        Map<String, String> params = new LinkedHashMap<>();
        params.put(Hitl.ACTION, a.action());
        params.put(Hitl.ARGUMENTS, a.arguments());
        params.put(Hitl.ARGS_HASH, Hitl.argsHash(a.action(), a.arguments()));
        params.put(Hitl.QUESTION, a.question());
        if (!a.summary().isEmpty()) {
            params.put(Hitl.SUMMARY, a.summary());
        }
        params.put(Hitl.REQUESTER, a.requester());
        if (!a.onBehalfOf().isEmpty()) {
            params.put(Hitl.ON_BEHALF_OF, a.onBehalfOf());
        }
        params.put(Hitl.WITHIN, within.toString());
        if (!a.context().isEmpty()) {
            params.put(Hitl.CONTEXT, a.context());
        }
        return AssetInvocation.of(desk.asset(), "ask", params);
    }

    /**
     * Writes the ask into the desk's space and completes when the desk decides, or fails with a timeout after
     * {@code wait}. A requester that should not hold a thread for a person checks {@link #decided} on its next pass
     * instead, with the invocation's id.
     */
    public static CompletableFuture<Hitl.Decision> request(Space space, AssetCard desk, AssetInvocation ask, Duration wait) {
        return AssetInvocations.retry(space, desk, ask, wait).thenApply(Hitl.Decision::of);
    }

    /** The desk's decision on an ask, when its issuer wrote one. */
    public static Optional<Hitl.Decision> decided(Space space, AssetCard desk, String invocationId) {
        return decided(space, desk.issuer(), invocationId);
    }

    /** The decision the issuer wrote for an ask, when it wrote one; any other writer's result is ignored. */
    public static Optional<Hitl.Decision> decided(Space space, PeerId deskIssuer, String invocationId) {
        Template<AssetInvocationResult> results = Template.of(AssetInvocationResult.class).where("invocationId", eq(invocationId));
        return space.readAllIssued(results, 16).stream().filter(i -> i.issuer().peer().equals(deskIssuer)).findFirst()
                .map(i -> Hitl.Decision.of(i.entry()));
    }

    /** Withdraws an ask from every channel of the desk. */
    public static CompletableFuture<AssetInvocationResult> cancel(Space space, AssetCard desk, String invocationId, Duration wait) {
        return AssetInvocations.invoke(space, desk, "cancel", Map.of("invocationId", invocationId), wait);
    }
}
