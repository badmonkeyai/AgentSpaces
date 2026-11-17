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
package ai.badmonkey.agentspaces.test;

import java.util.Optional;

/**
 * Reads the level-1 header of a frame for a {@link LinkFilter} (srad/spoiler-peers.md §3.3, SP-N-01).
 * {@link EnvelopeHeaders} reads the SPEC §9 envelope; a test may plug another.
 */
@FunctionalInterface
public interface FrameDecoder {

    /**
     * The frame's header.
     *
     * @param frame the bytes on the wire
     * @return the header, or empty when the bytes are not a frame this decoder reads
     */
    Optional<FrameHeader> header(byte[] frame);
}
