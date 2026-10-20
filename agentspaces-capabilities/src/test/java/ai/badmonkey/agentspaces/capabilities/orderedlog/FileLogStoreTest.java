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

import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.LogEntry;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The file-backed {@link LogStore} (srad/agentspaces-js.md JS-N-06) on its
 * own: an empty directory is term 0; term, vote, and entries round-trip
 * through a fresh instance; an append at an earlier index drops the tail
 * first; a record torn by a crash mid-append is dropped on load and the file
 * trimmed behind it.
 */
class FileLogStoreTest {

    @TempDir
    Path directory;

    private static LogEntry entry(long term, String command) {
        return new LogEntry(term, command.getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> commands(List<LogEntry> entries) {
        return entries.stream().map(e -> e.term() + ":" + new String(e.command(), StandardCharsets.UTF_8))
                .toList();
    }

    @Test
    void anEmptyDirectoryLoadsAsTermZeroNoVoteNoEntries() throws Exception {
        LogStore.State state = new FileLogStore(directory.resolve("fresh")).load();
        assertThat(state.term()).isZero();
        assertThat(state.votedFor()).isNull();
        assertThat(state.entries()).isEmpty();
    }

    @Test
    void termVoteAndEntriesSurviveAFreshInstance() throws Exception {
        PeerId voted = PeerId.of("zVoter");
        FileLogStore store = new FileLogStore(directory);
        store.saveTermVote(3, voted);
        store.append(1, List.of(entry(1, "one"), entry(2, "two")));
        store.append(3, List.of(entry(3, "three")));

        LogStore.State state = new FileLogStore(directory).load();
        assertThat(state.term()).isEqualTo(3);
        assertThat(state.votedFor()).isEqualTo(voted);
        assertThat(commands(state.entries())).containsExactly("1:one", "2:two", "3:three");

        // A vote cleared by a term bump reads back as none.
        store.saveTermVote(4, null);
        assertThat(new FileLogStore(directory).load().votedFor()).isNull();
        assertThat(new FileLogStore(directory).load().term()).isEqualTo(4);
    }

    @Test
    void anAppendAtAnEarlierIndexReplacesTheTail() throws Exception {
        FileLogStore store = new FileLogStore(directory);
        store.append(1, List.of(entry(1, "a"), entry(1, "b"), entry(1, "c")));
        store.append(2, List.of(entry(2, "B")));
        assertThat(commands(new FileLogStore(directory).load().entries())).containsExactly("1:a", "2:B");

        store.truncateFrom(2);
        assertThat(commands(new FileLogStore(directory).load().entries())).containsExactly("1:a");

        // A gap is refused: index 5 of a one-entry log.
        assertThatThrownBy(() -> store.append(5, List.of(entry(2, "x"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aTornTailRecordIsDroppedOnLoadAndTrimmed() throws Exception {
        FileLogStore store = new FileLogStore(directory);
        store.append(1, List.of(entry(1, "whole"), entry(1, "also-whole")));
        Path entries = directory.resolve("entries");
        long wholeLength = Files.size(entries);
        // A crash mid-append: a third record's term and length landed, its command did not.
        byte[] torn = new byte[]{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 9, 't', 'o', 'r'};
        Files.write(entries, torn, java.nio.file.StandardOpenOption.APPEND);
        assertThat(Files.size(entries)).isGreaterThan(wholeLength);

        LogStore.State state = new FileLogStore(directory).load();
        assertThat(commands(state.entries())).containsExactly("1:whole", "1:also-whole");
        assertThat(Files.size(entries)).as("the torn record is trimmed").isEqualTo(wholeLength);

        // And the store appends cleanly after the trim.
        FileLogStore again = new FileLogStore(directory);
        again.load();
        again.append(3, List.of(entry(2, "after")));
        assertThat(commands(new FileLogStore(directory).load().entries()))
                .containsExactly("1:whole", "1:also-whole", "2:after");
    }
}
