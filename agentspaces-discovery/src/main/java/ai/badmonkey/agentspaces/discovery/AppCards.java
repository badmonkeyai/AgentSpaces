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
package ai.badmonkey.agentspaces.discovery;

import ai.badmonkey.agentspaces.api.ad.AppCard;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;

import java.time.Instant;
import java.util.Objects;

/**
 * The AppCard level as an issuer applies it (SPEC §6.1a.2, v0.1.32; ISSUE-AppCardOriginFormats FR-7; AppCard origin
 * and formats, 2026-11-09). An issuer writes {@code origin}, {@code formats}, a {@code host} whose {@code signOn} is
 * {@code idp}, and {@code spill} only while every member of its view advertises {@code wire.appcard=2}; otherwise it
 * issues the card without them, which a v0.1.31 reader decodes.
 */
public final class AppCards {

    private AppCards() {
    }

    /**
     * Whether this node and every member of the group's current view read the gated fields.
     *
     * @param runtime the group
     * @return true when the view is at {@link PeerNode#APPCARD_LEVEL}
     */
    public static boolean gateOpen(GroupRuntime runtime) {
        return Objects.requireNonNull(runtime, "runtime").viewAtAppCardLevel(PeerNode.APPCARD_LEVEL);
    }

    /**
     * The card a view may receive: unchanged while the gate is open, and otherwise without {@code origin},
     * {@code formats}, and an {@code idp} host. A host whose {@code signOn} is {@code none} reads at level 1 and stays.
     *
     * @param card     a full card
     * @param gateOpen whether the view reads the gated fields
     * @return the card for the view
     */
    public static AppCard forView(AppCard card, boolean gateOpen) {
        Objects.requireNonNull(card, "card");
        if (card.spill() != null) {
            throw new IllegalArgumentException("the gate applies to a full card; a stub exists only behind an open gate");
        }
        if (gateOpen) {
            return card;
        }
        AppCard stripped = card.withOrigin(null).withFormats(null);
        return card.host() != null && "idp".equals(card.host().signOn()) ? stripped.withHost(null) : stripped;
    }

    /** The card under another issue instant, every other field unchanged. */
    static AppCard issuedAt(AppCard c, Instant issued) {
        return new AppCard(c.id(), c.issuer(), c.group(), issued, c.ttl(), c.name(), c.kind(), c.shape(), c.owner(), c.reads(),
                c.actions(), c.idpClient(), c.url(), c.pkg(), c.version(), c.views(), c.ui(), c.host(), c.origin(), c.formats(),
                c.spill());
    }
}
