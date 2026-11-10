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
package ai.badmonkey.agentspaces.content.tree;

/**
 * A {@link TreeStore} refusing a node it was handed, a quota for one (PDT 2 phase 7, P2-X-08; PD-S-05). The tree
 * exchange ends the fetch with the refusal, keeps what the store already held, and strikes nobody, since the
 * sender did nothing wrong.
 */
public class StoreRefusedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int reason;

    /**
     * A refusal.
     *
     * @param message what was refused and why
     * @param reason  the exchange's reject reason that names it, such as
     *                {@link ai.badmonkey.agentspaces.content.Messages#REASON_OVER_BUDGET}
     */
    public StoreRefusedException(String message, int reason) {
        super(message);
        this.reason = reason;
    }

    /** The exchange's reject reason that names the refusal. */
    public int reason() {
        return reason;
    }
}
