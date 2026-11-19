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
package ai.badmonkey.agentspaces.spring;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The PDT pack directory (the second-swap ticket, 2026-11-17): every peer of a group on one host defaulted to
 * {@code ~/.agentspaces/pdt/<group>}, where their stores corrupted each other. Under the default path each peer now
 * keeps its own directory; a path set explicitly is used as given.
 */
class PdtPackDirTest {

    private static final String DEFAULT = AgentSpacesProperties.PersistentTrees.Store.DEFAULT_PATH;

    @Test
    void underTheDefaultPathTwoPeersOfOneGroupGetTwoDirectories() {
        Path a = Pdts.packDir(DEFAULT, "marketing", "zPeerA");
        Path b = Pdts.packDir(DEFAULT, "marketing", "zPeerB");
        assertThat(a).isNotEqualTo(b);
        assertThat(a).isEqualTo(Path.of(DEFAULT, "marketing", "zPeerA"));
    }

    @Test
    void anExplicitPathIsTheOnePeersStoreAsGiven() {
        assertThat(Pdts.packDir("/data/pdt", "marketing", "zPeerA")).isEqualTo(Path.of("/data/pdt", "marketing"));
    }

    @Test
    void aGroupOrPeerNameIsMadeSafeForTheFileSystem() {
        assertThat(Pdts.packDir(DEFAULT, "a/b", "z:1").getFileName().toString()).isEqualTo("z_1");
        assertThat(Pdts.packDir("/data", "a/b", null)).isEqualTo(Path.of("/data", "a_b"));
    }
}
