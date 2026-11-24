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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.common.time.Watchdog;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import java.nio.file.Path;

/**
 * The reactor's end-of-build read of the watchdog (srad/agentspaces-event-loop.md EL-Q-REL-11, §8 phase 1 gate): when
 * a test JVM's plan ends, it prints this process's {@link Watchdog#totalDetections()} and
 * {@link Watchdog#totalReinits()} (faults found by watchdogs not marked scripted) and records them in the
 * measurements store as {@code core.watchdog.detections} and {@code core.watchdog.reinits} (goal 0), with the module
 * as a parameter. A JVM in which nothing ever registered with a watchdog records nothing.
 */
public final class WatchdogTotalsListener implements TestExecutionListener {

    /** Made by the JUnit launcher. */
    public WatchdogTotalsListener() {
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        System.out.print(line(Path.of(System.getProperty("user.dir"))));
        long registrations = Watchdog.totalRegistrations();
        if (registrations == 0) {
            return;
        }
        long detections = Watchdog.totalDetections();
        long reinits = Watchdog.totalReinits();
        Path dir = Path.of(System.getProperty("user.dir"));
        String module = dir.getFileName() == null ? "?" : dir.getFileName().toString();
        try {
            Recorder.record(module, detections, reinits);
        } catch (LinkageError e) {
            // agentspaces-test-measure is not on this module's test classpath: the printed line is the record.
        }
    }

    /**
     * The end-of-plan text: the watchdog line (EL-Q-REL-11) and, when any node loop ran in this JVM, the backstop line
     * (EL-Q-REL-03: backstop runs that found work over every unscripted loop, goal 0, named by participant when not
     * 0). Empty when neither ran.
     *
     * @param dir the module directory
     * @return the lines, each ending in a newline
     */
    static String line(Path dir) {
        String module = dir.getFileName() == null ? "?" : dir.getFileName().toString();
        StringBuilder out = new StringBuilder();
        long registrations = Watchdog.totalRegistrations();
        if (registrations > 0) {
            out.append("[watchdog] ").append(module).append(": detections=").append(Watchdog.totalDetections())
                    .append(" reinits=").append(Watchdog.totalReinits()).append(" (registrations ")
                    .append(registrations).append("; srad/agentspaces-event-loop.md EL-Q-REL-11, goal 0)\n");
        }
        long loops = ai.badmonkey.agentspaces.common.time.LoopTotals.loops();
        if (loops > 0) {
            long found = ai.badmonkey.agentspaces.common.time.LoopTotals.backstopFoundWork();
            out.append("[backstop] ").append(module).append(": found-work=").append(found).append(" (loops ")
                    .append(loops).append("; srad/agentspaces-event-loop.md EL-Q-REL-03, goal 0)");
            if (found > 0) {
                out.append(" by ").append(ai.badmonkey.agentspaces.common.time.LoopTotals.foundBy());
            }
            out.append('\n');
        }
        return out.toString();
    }

    /** Kept apart so a missing measurements helper fails only here, as a LinkageError. */
    private static final class Recorder {
        static void record(String module, long detections, long reinits) {
            ai.badmonkey.agentspaces.measure.Rec rec = ai.badmonkey.agentspaces.measure.Rec.with()
                    .params(ai.badmonkey.agentspaces.measure.Params.of("module", module))
                    .kind(ai.badmonkey.agentspaces.measure.Kind.COUNT);
            ai.badmonkey.agentspaces.measure.Measure.record("core.watchdog.detections",
                    ai.badmonkey.agentspaces.measure.Unit.COUNT, ai.badmonkey.agentspaces.measure.Direction.LOWER,
                    detections, rec);
            ai.badmonkey.agentspaces.measure.Measure.record("core.watchdog.reinits",
                    ai.badmonkey.agentspaces.measure.Unit.COUNT, ai.badmonkey.agentspaces.measure.Direction.LOWER,
                    reinits, rec);
        }
    }
}
