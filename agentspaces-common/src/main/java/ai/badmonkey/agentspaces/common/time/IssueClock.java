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
package ai.badmonkey.agentspaces.common.time;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Issue instants for the cards and advertisements one publisher signs, each in a millisecond strictly later than the
 * last one it issued under the same id (srad/agentspaces-event-loop.md §10.14 defect 4, §10.16).
 *
 * <p>A receiver's advertisement cache keeps the version it holds on an equal issue instant, and the gossip item that
 * carries a card is named by its id and issue <em>millisecond</em>, which every peer has already seen. So a second card
 * issued in the millisecond of the first (two changes, or a change and the refresh) reached nobody until the next
 * refresh, while the publisher believed it published. With change-driven republishing (phase 2) two issues in one
 * millisecond are ordinary. An instant inside the millisecond after the last one counts as that millisecond: the
 * comparison is on milliseconds, not on the instant, whose sub-millisecond part the item id drops.
 *
 * <p>Thread-safe: the choice for one id is one atomic step, so a refresh thread and a node loop never pick the same
 * millisecond. One entry per id this publisher ever issued; withdrawing an id may {@link #forget} it.
 */
public final class IssueClock {

    private final Map<String, Long> lastIssuedMillis = new ConcurrentHashMap<>();

    /**
     * The issue instant for the next card under an id: {@code now} when its millisecond is past the last one issued
     * under the id, else the millisecond after that one.
     *
     * @param id  the card's id
     * @param now the issue instant the publisher would use
     * @return the instant to issue at
     */
    public Instant next(String id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        long described = now.toEpochMilli();
        long issued = lastIssuedMillis.merge(id, described, (last, fresh) -> Math.max(fresh, last + 1));
        return issued == described ? now : Instant.ofEpochMilli(issued);
    }

    /**
     * Notes an issue made elsewhere (a card restored or signed by another path), so the next one is later still.
     *
     * @param id     the card's id
     * @param issued its issue instant
     */
    public void issued(String id, Instant issued) {
        lastIssuedMillis.merge(id, issued.toEpochMilli(), Math::max);
    }

    /**
     * Forgets an id, once nothing is issued under it any more.
     *
     * @param id the card's id
     */
    public void forget(String id) {
        lastIssuedMillis.remove(id);
    }
}
