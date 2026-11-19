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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.content.tree.Cids;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One directory serves one store (the PDT second-swap ticket, 2026-11-17). Every peer of a group on one host defaulted
 * to the same pack directory; two stores on it each appended at offsets only they tracked and rewrote the shared index,
 * so reads came back short or as bytes that did not hash to their CID. A second store on a held directory is refused.
 */
class PackNodeStoreLockTest {

    @TempDir
    Path dir;

    @Test
    void aSecondStoreOnAHeldDirectoryIsRefusedWithTheReason() throws IOException {
        try (PackNodeStore first = PackNodeStore.open(dir)) {
            assertThatThrownBy(() -> PackNodeStore.open(dir))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("another node store holds")
                    .hasMessageContaining("give each peer its own store path");
            // the first store is untouched by the refused open
            byte[] node = "a node".getBytes(StandardCharsets.UTF_8);
            String cid = Cids.of(node, Cids.RAW);
            assertThat(first.put(cid, node, 0)).isTrue();
            assertThat(first.get(cid, 0)).hasValueSatisfying(b -> assertThat(b).isEqualTo(node));
        }
    }

    @Test
    void aClosedStoreLeavesItsDirectoryFreeAndItsNodesHeld() throws IOException {
        byte[] node = "kept".getBytes(StandardCharsets.UTF_8);
        String cid = Cids.of(node, Cids.RAW);
        try (PackNodeStore first = PackNodeStore.open(dir)) {
            first.put(cid, node, 0);
        }
        try (PackNodeStore again = PackNodeStore.open(dir)) {
            assertThat(again.get(cid, 0)).hasValueSatisfying(b -> assertThat(b).isEqualTo(node));
        }
    }

    @Test
    void aCrashedStoreLeavesItsDirectoryFreeAsAProcessExitDoes() throws IOException {
        PackNodeStore crashed = PackNodeStore.open(dir);
        crashed.abandon();
        try (PackNodeStore again = PackNodeStore.open(dir)) {
            assertThat(again.dir()).isEqualTo(dir);
        }
    }
}
