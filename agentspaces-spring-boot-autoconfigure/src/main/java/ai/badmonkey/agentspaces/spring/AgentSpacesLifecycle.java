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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Objects;

/**
 * Drives the fabric with the application's lifecycle: on context start, the
 * node begins ticking (membership probes, anti-entropy, self-advertisement
 * refresh) and bound agents' cards refresh on their leased cadence (P2); on
 * context stop, both stop. Runs late in the start order so agent beans are
 * bound before the fleet learns about this peer.
 */
public class AgentSpacesLifecycle implements SmartLifecycle {

    private static final System.Logger LOG =
            System.getLogger(AgentSpacesLifecycle.class.getName());

    private final PeerNode node;
    private final AgentSpaces spaces;
    private final Duration tickPeriod;
    private final Duration cardRefreshPeriod;
    /** Runs after each card refresh (TODO-EFG §4: the OIDC token-file re-read). */
    private final Runnable onCardRefresh;
    /** The next card refresh: a one-shot on the node's time source, re-armed after each (EL-T-12, EL-T-13). */
    private volatile ai.badmonkey.agentspaces.common.time.TimeSource.Scheduled refresher;
    private volatile boolean running;

    /**
     * Creates the lifecycle.
     *
     * @param node              the peer node to tick
     * @param spaces            the facade whose bound cards refresh
     * @param tickPeriod        the protocol tick period
     * @param cardRefreshPeriod the AgentCard re-publish period
     */
    public AgentSpacesLifecycle(PeerNode node, AgentSpaces spaces,
                                Duration tickPeriod, Duration cardRefreshPeriod) {
        this(node, spaces, tickPeriod, cardRefreshPeriod, () -> { });
    }

    /**
     * Creates the lifecycle with a hook run on every card-refresh tick, after
     * the cards themselves (TODO-EFG §4: the starter re-reads
     * {@code agentspaces.security.oidc.token-file} here so a rotated token
     * reaches the fleet on the same cadence as other leased state).
     *
     * @param node              the peer node to tick
     * @param spaces            the facade whose bound cards refresh
     * @param tickPeriod        the protocol tick period
     * @param cardRefreshPeriod the AgentCard re-publish period
     * @param onCardRefresh     runs after each card refresh; exceptions are logged
     */
    public AgentSpacesLifecycle(PeerNode node, AgentSpaces spaces,
                                Duration tickPeriod, Duration cardRefreshPeriod,
                                Runnable onCardRefresh) {
        this.node = Objects.requireNonNull(node, "node");
        this.spaces = Objects.requireNonNull(spaces, "spaces");
        this.tickPeriod = Objects.requireNonNull(tickPeriod, "tickPeriod");
        this.cardRefreshPeriod = Objects.requireNonNull(cardRefreshPeriod,
                "cardRefreshPeriod");
        this.onCardRefresh = Objects.requireNonNull(onCardRefresh, "onCardRefresh");
    }

    /** One refresh tick: cards first, then the hook; neither may kill the schedule. */
    private void refresh() {
        try {
            spaces.refreshCards();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "card refresh failed", e);
        }
        try {
            onCardRefresh.run();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "card-refresh hook failed", e);
        }
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        node.startTicking(tickPeriod);
        running = true;
        armRefresh();
    }

    /**
     * Arms the next card refresh on the node's time source (srad/agentspaces-event-loop.md EL-T-12, EL-T-13): a
     * one-shot re-armed from the end of each refresh, on a virtual thread, so the {@code agentspaces-card-refresh}
     * platform thread is gone and a late refresh never runs twice back to back.
     */
    private synchronized void armRefresh() {
        if (!running) {
            return;
        }
        refresher = node.time().schedule(cardRefreshPeriod, () -> {
            try {
                refresh();
            } finally {
                armRefresh();
            }
        });
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (refresher != null) {
            refresher.cancel();
            refresher = null;
        }
        spaces.close();
        node.close();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Late start, early stop: agents bind before the fleet sees this peer.
        return Integer.MAX_VALUE - 1024;
    }
}
