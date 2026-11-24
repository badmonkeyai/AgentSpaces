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

package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.codec.WireRecord;

import java.util.List;
import java.util.Objects;

/**
 * A conversation an agent answers (SPEC §6.1, v0.1.38): the space it watches, the thread key, and how far the key
 * reaches. An AgentCard names its threads so a reader knows which channel the agent chats on without its configuration.
 *
 * @param space the space the agent watches for chat lines, 1 to {@link #MAX_SPACE} characters with no white space
 * @param key   the thread key; empty exactly when {@code scope} is {@link #ALL}, otherwise 1 to {@link #MAX_KEY}
 *              characters with no white space
 * @param scope {@link #EXACT} (that thread), {@link #TREE} (the thread and every {@code key:<sub>} thread under it), or
 *              {@link #ALL} (every thread of the space)
 */
@WireRecord
public record ChatThread(String space, String key, String scope) {

    /** The one thread the key names. */
    public static final String EXACT = "exact";
    /** The thread the key names and every {@code key:<sub>} thread under it. */
    public static final String TREE = "tree";
    /** Every thread of the space; the key is empty. */
    public static final String ALL = "all";
    /** The scopes a reader accepts; any other refuses the card. */
    public static final List<String> SCOPES = List.of(EXACT, TREE, ALL);
    /** The longest space name. */
    public static final int MAX_SPACE = 128;
    /** The longest thread key. */
    public static final int MAX_KEY = 128;

    public ChatThread {
        Objects.requireNonNull(space, "thread.space");
        Objects.requireNonNull(key, "thread.key");
        Objects.requireNonNull(scope, "thread.scope");
        if (space.isEmpty() || space.length() > MAX_SPACE || space.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("thread.space is 1 to " + MAX_SPACE + " characters with no white space: " + space);
        }
        if (!SCOPES.contains(scope)) {
            throw new IllegalArgumentException("thread.scope is one of " + SCOPES + ": " + scope);
        }
        if (ALL.equals(scope) ? !key.isEmpty() : key.isEmpty() || key.length() > MAX_KEY || key.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException(ALL.equals(scope) ? "thread.key is empty when the scope is all: " + key
                    : "thread.key is 1 to " + MAX_KEY + " characters with no white space: " + key);
        }
    }

    /** Whether this thread takes a line written with the key in the space. */
    public boolean takes(String lineSpace, String lineKey) {
        if (!space.equals(lineSpace) || lineKey == null) {
            return false;
        }
        return switch (scope) {
            case ALL -> true;
            case TREE -> lineKey.equals(key) || lineKey.startsWith(key + ":");
            default -> lineKey.equals(key);
        };
    }

    /** Whether this thread and another may take the same line: both in one space, and their keys reach each other. */
    public boolean overlaps(ChatThread other) {
        if (!space.equals(other.space)) {
            return false;
        }
        if (ALL.equals(scope) || ALL.equals(other.scope)) {
            return true;
        }
        return takes(other.space, other.key) || other.takes(space, key);
    }
}
