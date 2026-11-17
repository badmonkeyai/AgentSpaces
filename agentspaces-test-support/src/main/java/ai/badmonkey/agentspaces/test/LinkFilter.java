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

/**
 * Decides what happens to one frame on a {@link SimNetwork} link (srad/spoiler-peers.md §3.4.1,
 * SP-N-01): pass, drop, hold under a named gate until {@link SimNetwork#releaseGate}, delay by fleet
 * clock, or duplicate. Filters act on the in-process fabric only; nothing here reaches a real transport.
 *
 * <p>A filter runs on the sender's thread, as the fabric delivers. It must not block, and it must not
 * send or release from inside {@link #decide}: work that would deliver frames goes in
 * {@link Verdict#then}, which the fabric runs after this frame's own delivery and outside every lock,
 * so a release never delivers into a receiver while another thread waits on the filter.
 */
@FunctionalInterface
public interface LinkFilter {

    /**
     * The verdict for one frame.
     *
     * @param frame the frame and its link
     * @return what to do with it; {@link Verdict#pass()} leaves the wire as it was
     */
    Verdict decide(LinkFrame frame);

    /**
     * What a filter does with a frame. When several filters see one frame the most restrictive wins:
     * drop beats hold, hold beats delay, delay beats pass; the largest copy count applies to whatever
     * passes, and every {@link #then} runs.
     *
     * @param action      the action
     * @param gate        the gate a held frame waits under; null unless held
     * @param delayMillis the fleet-clock span a delayed frame waits; 0 unless delayed
     * @param copies      how many times a passing or delayed frame is delivered, at least 1
     * @param then        work to run after the frame's own delivery, or null
     */
    record Verdict(Action action, String gate, long delayMillis, int copies, Runnable then) {

        /** The actions, least restrictive first. */
        public enum Action { PASS, DELAY, HOLD, DROP }

        private static final Verdict PASS = new Verdict(Action.PASS, null, 0, 1, null);
        private static final Verdict DROP = new Verdict(Action.DROP, null, 0, 1, null);

        /** Checks the fields. */
        public Verdict {
            java.util.Objects.requireNonNull(action, "action");
            if (action == Action.HOLD && gate == null) {
                throw new IllegalArgumentException("a hold names its gate");
            }
            if (delayMillis < 0 || copies < 1) {
                throw new IllegalArgumentException("delay " + delayMillis + ", copies " + copies);
            }
        }

        /** Delivers the frame as sent. */
        public static Verdict pass() {
            return PASS;
        }

        /** Loses the frame silently, as the wire does. */
        public static Verdict drop() {
            return DROP;
        }

        /** Queues the frame under a gate until {@link SimNetwork#releaseGate} names it. */
        public static Verdict hold(String gate) {
            return new Verdict(Action.HOLD, gate, 0, 1, null);
        }

        /** Delivers the frame on the first {@link SimNetwork#pump} at or past this span of fleet clock. */
        public static Verdict delay(long millis) {
            return new Verdict(Action.DELAY, null, millis, 1, null);
        }

        /** Delivers the frame {@code copies} times. */
        public static Verdict duplicate(int copies) {
            return new Verdict(Action.PASS, null, 0, copies, null);
        }

        /** This verdict with work to run after the frame's delivery; earlier work runs first. */
        public Verdict then(Runnable after) {
            if (after == null) {
                return this;
            }
            Runnable before = then;
            return new Verdict(action, gate, delayMillis, copies, before == null ? after : () -> {
                before.run();
                after.run();
            });
        }

        /** The most restrictive of two verdicts, with the larger copy count and both follow-ups. */
        public Verdict combine(Verdict other) {
            Verdict stricter = other.action.ordinal() > action.ordinal() ? other : this;
            long delay = Math.max(delayMillis, other.delayMillis);
            Verdict combined = new Verdict(stricter.action, stricter.gate,
                    stricter.action == Action.DELAY ? delay : stricter.delayMillis,
                    Math.max(copies, other.copies), null);
            return combined.then(then).then(other.then);
        }
    }
}
