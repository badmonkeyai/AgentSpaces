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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

/**
 * The worker side of command-and-control: a worker attaches a gate to the
 * control space and consults it before taking work, so a PAUSE, RESUME, or
 * DRAIN directive the console broadcasts takes effect. The gate is a thin,
 * lease-aware subscription; a worker loop reads {@link #paused()} and
 * {@link #draining()} and needs no other C2 knowledge.
 *
 * <p>Directives are ordinary replicated entries, so a gate honors the most
 * recent directive addressed to its worker (by {@code issuedMillis}), and a
 * DRAIN is terminal: once seen, the gate stays draining.
 *
 * <p>The gate authorizes by writer identity (ASF-008): it honors only
 * directives whose entry was written by a peer its {@link Authorizer} permits
 * {@code DIRECTIVE_ISSUER} in the gate's scope (TODO-EFG §4), using the
 * authenticated issuer the space surfaces on every event — never a field inside
 * the directive payload. {@link #attach(Space, String, PeerId)} is the one-peer
 * form: an authorizer granting exactly the console peer. Any other admitted
 * peer's directive is ignored, so a compromised worker cannot pause or drain
 * the fleet. Directives stamped in the future beyond a small skew allowance are
 * also ignored, so a hostile {@code issuedMillis} cannot wedge the gate against
 * all later directives.
 */
public final class DirectiveGate implements AutoCloseable {

    /**
     * How far ahead of this worker's clock a directive's {@code issuedMillis}
     * may run before the directive is discarded as forged or clock-broken.
     */
    static final long MAX_STAMP_SKEW_MILLIS = Duration.ofMinutes(5).toMillis();

    private final String worker;
    private final Authorizer authorizer;
    private final String scope;
    private final InstantSource clock;
    private final Subscription subscription;
    private volatile boolean paused;
    private volatile boolean draining;
    private volatile long lastSeenMillis;

    private DirectiveGate(String worker, Authorizer authorizer, String scope,
                          Space controlSpace, InstantSource clock) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.subscription = controlSpace.notify(Template.of(Directive.class),
                event -> onDirective(event), Lease.of(Duration.ofDays(365)));
    }

    /** The one-peer grant behind the console-peer form. */
    private static Authorizer onlyPeer(PeerId console) {
        Objects.requireNonNull(console, "console");
        return (peer, operation, scope) -> console.equals(peer);
    }

    /**
     * Attaches a gate for a worker to the control space, honoring directives
     * written by the given console peer only.
     *
     * @param controlSpace the console's control space
     * @param worker       this worker's name
     * @param console      the peer whose directives this gate obeys
     * @return the gate
     */
    public static DirectiveGate attach(Space controlSpace, String worker, PeerId console) {
        return attach(controlSpace, worker, console, InstantSource.system());
    }

    /**
     * Attaches a gate with an explicit time source (deterministic tests).
     *
     * @param controlSpace the console's control space
     * @param worker       this worker's name
     * @param console      the peer whose directives this gate obeys
     * @param clock        the time source for the future-stamp clamp
     * @return the gate
     */
    public static DirectiveGate attach(Space controlSpace, String worker, PeerId console,
                                       InstantSource clock) {
        return attach(controlSpace, worker, onlyPeer(console), "", clock);
    }

    /**
     * Attaches a gate whose obedience is decided by the fleet's
     * {@link Authorizer} (TODO-EFG §4, TODO item 6): a directive takes effect
     * when {@code authorizer.permits(issuer.peer(), DIRECTIVE_ISSUER, scope)}
     * holds for the entry's space-authenticated issuer. Under the Spring
     * starter's membership profiles the console peer is granted that operation
     * automatically; under the OIDC profiles the identity provider's
     * {@code aspace:directive-issuer[:group]} scope decides.
     *
     * @param controlSpace the console's control space
     * @param worker       this worker's name
     * @param authorizer   decides {@code DIRECTIVE_ISSUER} per directive issuer
     * @param scope        the authorizer's scope, conventionally the group id
     * @return the gate
     */
    public static DirectiveGate attach(Space controlSpace, String worker,
                                       Authorizer authorizer, String scope) {
        return attach(controlSpace, worker, authorizer, scope, InstantSource.system());
    }

    /**
     * Attaches an authorizer-decided gate with an explicit time source
     * (deterministic tests).
     *
     * @param controlSpace the console's control space
     * @param worker       this worker's name
     * @param authorizer   decides {@code DIRECTIVE_ISSUER} per directive issuer
     * @param scope        the authorizer's scope, conventionally the group id
     * @param clock        the time source for the future-stamp clamp
     * @return the gate
     */
    public static DirectiveGate attach(Space controlSpace, String worker,
                                       Authorizer authorizer, String scope,
                                       InstantSource clock) {
        Objects.requireNonNull(controlSpace, "controlSpace");
        return new DirectiveGate(worker, authorizer, scope, controlSpace, clock);
    }

    private void onDirective(SpaceEvent<Directive> event) {
        if (event.kind() != SpaceEvent.Kind.WRITTEN) {
            return;
        }
        if (!authorizer.permits(event.issuer().peer(),
                Authorizer.Operation.DIRECTIVE_ISSUER, scope)) {
            return; // not a permitted issuer: an admitted peer may not command workers
        }
        Directive directive = event.entry();
        if (directive.issuedMillis() > clock.millis() + MAX_STAMP_SKEW_MILLIS) {
            return; // future-stamped: would wedge the gate against real directives
        }
        if (!directive.addresses(worker) || directive.issuedMillis() < lastSeenMillis) {
            return;
        }
        lastSeenMillis = directive.issuedMillis();
        switch (directive.action()) {
            case Directive.PAUSE -> paused = true;
            case Directive.RESUME -> paused = false;
            case Directive.DRAIN -> {
                draining = true;
                paused = true;
            }
            default -> {
                // Unknown verb: ignore rather than guess.
            }
        }
    }

    /** Whether this worker should hold off taking new work. */
    public boolean paused() {
        return paused;
    }

    /** Whether this worker should finish current work and stop for good. */
    public boolean draining() {
        return draining;
    }

    /** Stops honoring directives. */
    @Override
    public void close() {
        try {
            subscription.close();
        } catch (Exception e) {
            // Closing an already-lapsed subscription is fine.
        }
    }
}
