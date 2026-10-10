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
package ai.badmonkey.agentspaces.api.space;

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #16 §9.2 / §10.1: the additions to {@link Space} are defaults that
 * throw, naming the method, so a third-party implementation compiled against
 * 0.2.0 keeps compiling and fails loudly only when the new call is made.
 */
class SpaceDefaultsTest {

    /** The smallest Space: every abstract method, nothing else. */
    private static final class Minimal implements Space {
        @Override public String name() { return "minimal"; }
        @Override public SpaceId id() { return SpaceId.local("minimal"); }
        @Override public <T> EntryHandle write(T entry, Lease lease) { return null; }
        @Override public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
            return null;
        }
        @Override public <T> Optional<T> read(Template<T> template) { return Optional.empty(); }
        @Override public <T> Optional<T> read(Template<T> template, Duration timeout) {
            return Optional.empty();
        }
        @Override public <T> List<T> readAll(Template<T> template, int limit) { return List.of(); }
        @Override public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
            return List.of();
        }
        @Override public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease,
                                                          Duration timeout) {
            return Optional.empty();
        }
        @Override public void complete(TakenEntry<?> taken) { }
        @Override public <R> EntryHandle complete(TakenEntry<?> taken, R result,
                                                  Lease resultLease) {
            return null;
        }
        @Override public <T> Subscription notify(Template<T> template, SpaceListener<T> listener,
                                                 Lease lease) {
            return null;
        }
    }

    @Test
    void readAllEntriesAndTaggedCompleteThrowByDefaultNamingTheMethod() {
        Space space = new Minimal();
        TakenEntry<String> taken = new TakenEntry<>() {
            @Override public String entry() { return "x"; }
            @Override public ai.badmonkey.agentspaces.api.entry.EntryId entryId() { return null; }
            @Override public void renew(Duration extension) { }
        };

        assertThatThrownBy(() -> space.readAllEntries(Template.of(String.class), 10))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("readAllEntries");
        assertThatThrownBy(() -> space.complete(taken, "result", Lease.of(Duration.ofMinutes(1)),
                Map.of("k", "v")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("complete");
    }

    @Test
    void writerStaysEmptyByDefault() {
        org.assertj.core.api.Assertions.assertThat(new Minimal().writer())
                .isEqualTo(Optional.<AgentId>empty());
    }
}
