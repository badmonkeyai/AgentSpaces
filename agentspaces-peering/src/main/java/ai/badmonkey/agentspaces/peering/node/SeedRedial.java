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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * One group's seed redial (owner decisions of 2026-11-23, the same rules as every client): the redial table, the
 * hold that resets it, the staggered round of seed dials, and the seed link a round found.
 *
 * <ul>
 *   <li><b>The table.</b> With nobody in view, the seeds are dialled again after 5, 10, 20 and 30 s, then 5 s again
 *       ({@link #TABLE_MILLIS}), each wait spread uniformly by +/-10 % ({@link #SPREAD}). The table is static; the
 *       position is this node's, per group. It never gives up.</li>
 *   <li><b>The reset.</b> The position goes back to the first wait only when a link that held at least 5 s ends
 *       (exactly 5 s resets), and then the seeds are dialled at once; or on a wake. A link that ends sooner waits the
 *       next step and moves the position on. For a member, the link is the view: it holds while somebody is in it.</li>
 *   <li><b>The round.</b> The seeds start in list order, {@link #STAGGER_MILLIS} apart (RFC 8305); a refused dial starts
 *       the next at once; at most {@link #DIALS_IN_FLIGHT_MAX} links wait for an answer at once; the first seed to
 *       answer wins, the seeds not yet started are never dialled, and the links still waiting are closed. The seed
 *       whose link was lost last is dialled last, in every round.</li>
 * </ul>
 *
 * <p>Every time here is on the node's monotonic base ({@code localMillis}), so a sleep or a wall step moves none of
 * it. The node holds {@link #lock} for every change but the volatile reads.
 */
final class SeedRedial {

    /** The waits of the table, in order; the position wraps to the first after the last. */
    static final long[] TABLE_MILLIS = {5_000, 10_000, 20_000, 30_000};
    /** The spread on each wait: its table value times a factor in [1 - spread, 1 + spread). */
    static final double SPREAD = 0.10;
    /** A link that held this long resets the table when it ends: the table's first wait. */
    static final long HOLD_MILLIS = TABLE_MILLIS[0];
    /** The gap between two seed dials of one round while the earlier one has not answered (RFC 8305). */
    static final long STAGGER_MILLIS = 250;
    /** At most this many seed links wait for an answer at once; with every slot taken, a failure starts the next. */
    static final int DIALS_IN_FLIGHT_MAX = 16;

    final Object lock = new Object();
    /** The index of the next wait in {@link #TABLE_MILLIS}. */
    int position;
    /** When the next round is due while nobody is in view; {@link Long#MAX_VALUE} for none. */
    long nextRoundMillis = Long.MAX_VALUE;
    /** When the view last went from empty to held, or -1 while it is empty. */
    long linkSinceMillis = -1;
    /** A round is wanted at once: a wake with nobody in view, a fail-over, new seeds from the locator. */
    volatile boolean roundWanted;
    /** The locator answered new seeds: a round under way takes them on (set without the lock, from its thread). */
    volatile boolean seedsChanged;
    /** A wake was seen: the next run resets the table (set without the lock, from the wall-step thread). */
    volatile boolean wakeReset;
    /** The key of the seed whose link was lost last, dialled last in every round; null for none. */
    String lostSeed;
    /** The seed a round found, and the member that answered on it; null when no round found one yet. */
    String seedKey;
    volatile PeerId seedPeer;
    /** The round in progress, or null. */
    Round round;
    /** The last round's links, kept past its end so a late answer still names the seed link. */
    final Map<TransportConnection, String> lastRoundLinks = new LinkedHashMap<>();

    /** One round of seed dials. */
    static final class Round {
        final List<PeerAdvertisement.Endpoint> order;
        int next;
        /** When the last dial started, or {@link Long#MIN_VALUE} before the first. */
        long lastStartMillis = Long.MIN_VALUE;
        /** The last dial started, or null. */
        Dial lastDial;
        /** Dials started whose outcome is not known yet: each runs on a virtual thread of its own, off the loop. */
        int dialing;
        /** Set when a seed answered: a dial of this round that completes later is closed at once. */
        boolean won;
        /** The links of this round waiting for an answer, with their seed's key. */
        final Map<TransportConnection, String> waiting = new LinkedHashMap<>();

        Round(List<PeerAdvertisement.Endpoint> order) {
            this.order = order;
        }
    }

    /**
     * One seed dial of a round (SPEC v0.1.37 §9.4): it runs on a virtual thread of its own, so a connect that gets no
     * answer (a blackholed address) holds that thread for the transport's connect timeout and never the loop. Its
     * outcome is noted under {@link #lock}: a refusal starts the next seed at once, a link joins the round's waiting
     * links. Guarded by {@link #lock}.
     */
    static final class Dial {
        final String key;
        final Round round;
        boolean done;
        /** The link the dial opened, or null while it runs or when it was refused. */
        TransportConnection connection;

        Dial(String key, Round round) {
            this.key = key;
            this.round = round;
        }
    }

    /**
     * The next wait of the table, spread, and the position moves on (wrapping after the last).
     *
     * @param random the node's random
     * @return the wait in milliseconds
     */
    long nextWait(Random random) {
        long wait = TABLE_MILLIS[position];
        position = (position + 1) % TABLE_MILLIS.length;
        return Math.round(wait * (1 + SPREAD * (2 * random.nextDouble() - 1)));
    }

    /** Back to the first wait: a link that held at least 5 s ended, or the node woke. */
    void reset() {
        position = 0;
    }

    /** A seed's key: its scheme and address. */
    static String key(PeerAdvertisement.Endpoint endpoint) {
        return endpoint.transport() + "|" + endpoint.address();
    }

    /**
     * The seeds in a round's order: list order (ascending priority, stable for ties), with the seed lost last moved
     * to the end.
     *
     * @param seeds    the seeds in priority order
     * @param lostSeed the lost seed's key, or null
     * @return the order
     */
    static List<PeerAdvertisement.Endpoint> order(List<PeerAdvertisement.Endpoint> seeds, String lostSeed) {
        List<PeerAdvertisement.Endpoint> order = new ArrayList<>(seeds.size());
        List<PeerAdvertisement.Endpoint> last = new ArrayList<>(1);
        for (PeerAdvertisement.Endpoint seed : seeds) {
            (key(seed).equals(lostSeed) ? last : order).add(seed);
        }
        order.addAll(last);
        return order;
    }
}
