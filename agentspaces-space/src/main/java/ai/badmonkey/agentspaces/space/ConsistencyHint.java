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
package ai.badmonkey.agentspaces.space;

/**
 * How current a read must be (spec §7.2). Matching always runs against the
 * local replica, so reads never block on the network (P7); {@link #FRESH}
 * triggers exactly one anti-entropy pull toward one random group member
 * before the local match, which is the cheap "make sure I am current" a late
 * joiner or an operator console wants without changing the local-first
 * default.
 */
public enum ConsistencyHint {
    /** Match against the local replica as it stands. The default. */
    LOCAL,
    /** Perform one anti-entropy pull from one random member, then match locally. */
    FRESH
}
