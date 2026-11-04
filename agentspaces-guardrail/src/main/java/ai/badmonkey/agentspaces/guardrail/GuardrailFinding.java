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


package ai.badmonkey.agentspaces.guardrail;

import java.time.Instant;

/** One hit (FR-4): the policy version, the check, where it applied, the subject (a request id, an entry id), the action taken, the detail. */
public record GuardrailFinding(String group, long policy, String check, String where, String subject, String action,
                               String detail, Instant at) {

    public GuardrailFinding {
        group = group == null ? "" : group;
        check = check == null ? "" : check;
        where = where == null ? "" : where;
        subject = subject == null ? "" : subject;
        action = action == null ? "" : action;
        detail = detail == null ? "" : detail;
    }
}
