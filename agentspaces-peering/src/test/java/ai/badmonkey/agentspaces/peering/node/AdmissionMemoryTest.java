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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** The admission memory on its own (ISSUE-PeerRobustness-2, R2-1.4 to R2-1.6, R2-S2). */
class AdmissionMemoryTest {

    private final TestClock clock = TestClock.create();
    private final GroupId group = GroupId.of("zMemory");

    private static PeerId peer() {
        return PeerIdentity.generate().peerId();
    }

    /** R2-1.6: past the bound, the entry least recently seen goes first. */
    @Test
    void boundEvictsTheLeastRecentlySeen() {
        AdmissionMemory memory = new AdmissionMemory(null, 3, clock);
        PeerId oldest = peer();
        PeerId middle = peer();
        PeerId newest = peer();
        memory.record(group, oldest, "a");
        clock.advance(Duration.ofSeconds(1));
        memory.record(group, middle, "b");
        clock.advance(Duration.ofSeconds(1));
        memory.record(group, newest, "c");
        clock.advance(Duration.ofSeconds(1));
        memory.touch(group, oldest); // seen again: no longer the oldest
        clock.advance(Duration.ofSeconds(1));

        memory.record(group, peer(), "d");

        assertThat(memory.forGroup(group)).hasSize(3).containsKeys(oldest, newest).doesNotContainKey(middle);
        assertThat(memory.holds(group, oldest, "a")).isTrue();
        assertThat(memory.holds(group, oldest, "other")).as("the identical record only").isFalse();
        assertThat(memory.holds(group, middle, "b")).isFalse();
    }

    /** R2-1.5 and R2-S2: one JSON file per group, owner-only, holding peer ids and hashes, rewritten only when dirty and due, and reloaded. */
    @Test
    void persistsOwnerOnlyAndReloads(@TempDir Path state) throws Exception {
        AdmissionMemory memory = new AdmissionMemory(state, 10, clock);
        memory.load(group);
        PeerId a = peer();
        PeerId b = peer();
        memory.record(group, a, "aa,dd");
        memory.record(group, b, "bb");
        Path file = state.resolve(AdmissionMemory.DIRECTORY).resolve(group.value() + ".json");
        assertThat(file).doesNotExist();

        memory.flushIfDue(group, clock.millis(), 1_000);
        assertThat(file).exists();
        String json = Files.readString(file, StandardCharsets.UTF_8);
        assertThat(json).contains("\"v\":1").contains("\"group\":\"" + group.value() + "\"")
                .contains(a.value()).contains("\"record\":\"aa,dd\"").contains("\"lastSeen\":");
        if (Files.getFileStore(file).supportsFileAttributeView("posix")) {
            assertThat(Files.getPosixFilePermissions(file))
                    .containsExactlyInAnyOrder(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
        }
        long written = Files.getLastModifiedTime(file).toMillis();

        // Not dirty: no rewrite. Dirty but inside the period: no rewrite. Dirty and due: rewritten.
        memory.touch(group, a);
        memory.flushIfDue(group, clock.millis() + 5_000, 1_000);
        memory.remove(group, b);
        memory.flushIfDue(group, clock.millis() + 500, 1_000);
        assertThat(Files.readString(file, StandardCharsets.UTF_8)).contains(b.value());
        Thread.sleep(5);
        memory.flushIfDue(group, clock.millis() + 2_000, 1_000);
        assertThat(Files.readString(file, StandardCharsets.UTF_8)).doesNotContain(b.value());
        assertThat(Files.getLastModifiedTime(file).toMillis()).isGreaterThanOrEqualTo(written);

        AdmissionMemory reloaded = new AdmissionMemory(state, 10, clock);
        reloaded.load(group);
        assertThat(reloaded.forGroup(group)).containsExactly(java.util.Map.entry(a, "aa,dd"));
        assertThat(reloaded.holds(group, a, "aa,dd")).isTrue();

        reloaded.drop(group);
        assertThat(file).as("leaving deletes the file").doesNotExist();
        assertThat(reloaded.forGroup(group)).isEmpty();
    }

    /** A foreign or corrupt file leaves the group empty rather than refusing to start. */
    @Test
    void ignoresAForeignOrCorruptFile(@TempDir Path state) throws Exception {
        Path directory = state.resolve(AdmissionMemory.DIRECTORY);
        Files.createDirectories(directory);
        Path file = directory.resolve(group.value() + ".json");
        Files.writeString(file, "{\"v\":1,\"group\":\"zSomeoneElse\",\"peers\":{\"" + peer().value()
                + "\":{\"record\":\"x\",\"lastSeen\":1}}}");
        AdmissionMemory memory = new AdmissionMemory(state, 10, clock);
        memory.load(group);
        assertThat(memory.forGroup(group)).isEmpty();

        Files.writeString(file, "not json");
        memory.load(group);
        assertThat(memory.forGroup(group)).isEmpty();
        assertThat(new AdmissionMemory(null, 10, clock).persistent()).isFalse();
        assertThat(memory.persistent()).isTrue();
    }
}
