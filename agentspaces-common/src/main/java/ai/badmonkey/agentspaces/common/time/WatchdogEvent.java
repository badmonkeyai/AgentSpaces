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

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Label;
import jdk.jfr.Name;

/**
 * A watchdog detection, re-initialisation or escalation as a JFR event (srad/agentspaces-event-loop.md EL-H-15), so a
 * recording shows it beside the carrier samples of EL-M-08.
 */
@Name("ai.badmonkey.agentspaces.Watchdog")
@Label("AgentSpaces watchdog")
@Category({"AgentSpaces", "Event loop"})
@Description("A loop the watchdog found faulty, and what it did")
final class WatchdogEvent extends jdk.jfr.Event {

    @Label("Loop")
    String loop;

    @Label("Kind")
    String kind;

    @Label("Participant")
    String participant;

    @Label("Duration (ms)")
    long durationMillis;

    static void commit(String loop, String kind, String participant, long durationMillis) {
        WatchdogEvent event = new WatchdogEvent();
        if (!event.isEnabled()) {
            return;
        }
        event.loop = loop;
        event.kind = kind;
        event.participant = participant;
        event.durationMillis = durationMillis;
        event.commit();
    }
}
