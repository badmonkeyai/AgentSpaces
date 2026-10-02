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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.List;

/** Wire messages of the {@code aspace:cap/ordered-log} Raft protocol. */
public final class RaftMessages {

    private RaftMessages() {
    }

    /** The multiplexing envelope: exactly one field is set. */
    public record Frame(RequestVote requestVote, VoteReply voteReply,
                        AppendEntries appendEntries, AppendReply appendReply,
                        ClientSubmit clientSubmit) {

        static Frame of(RequestVote m) {
            return new Frame(m, null, null, null, null);
        }

        static Frame of(VoteReply m) {
            return new Frame(null, m, null, null, null);
        }

        static Frame of(AppendEntries m) {
            return new Frame(null, null, m, null, null);
        }

        static Frame of(AppendReply m) {
            return new Frame(null, null, null, m, null);
        }

        static Frame of(ClientSubmit m) {
            return new Frame(null, null, null, null, m);
        }
    }

    /**
     * One replicated log entry.
     *
     * @param term    the leader term that appended it
     * @param command the opaque command bytes
     */
    public record LogEntry(long term, byte[] command) {
    }

    /**
     * A candidate's vote request.
     *
     * @param term         the candidate's term
     * @param candidate    the candidate
     * @param lastLogIndex the candidate's last log index
     * @param lastLogTerm  the candidate's last log term
     */
    public record RequestVote(long term, PeerId candidate, long lastLogIndex, long lastLogTerm) {
    }

    /**
     * A vote reply.
     *
     * @param term    the voter's term
     * @param granted whether the vote was granted
     */
    public record VoteReply(long term, boolean granted) {
    }

    /**
     * Log replication (and heartbeat when {@code entries} is empty).
     *
     * @param term         the leader's term
     * @param leader       the leader
     * @param prevLogIndex index preceding the shipped entries
     * @param prevLogTerm  term of the preceding index
     * @param entries      the entries to append
     * @param leaderCommit the leader's commit index
     */
    public record AppendEntries(long term, PeerId leader, long prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) {
    }

    /**
     * A follower's replication reply.
     *
     * @param term       the follower's term
     * @param follower   the follower
     * @param success    whether the append matched
     * @param matchIndex the follower's highest matching index on success
     */
    public record AppendReply(long term, PeerId follower, boolean success, long matchIndex) {
    }

    /**
     * A client's command forwarded to the leader.
     *
     * @param command the opaque command bytes
     */
    public record ClientSubmit(byte[] command) {
    }
}
