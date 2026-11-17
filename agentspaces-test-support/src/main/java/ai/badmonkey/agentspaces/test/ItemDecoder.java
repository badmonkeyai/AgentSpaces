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

import java.util.List;

/**
 * Decodes the items inside a frame for the level-2 matcher (srad/spoiler-peers.md §3.3, SP-C-06). The
 * implementation that reads space deltas, content messages and PDT nodes needs the peering, space and
 * pdt modules, so it lives downstream in {@code agentspaces-spoilers}; this module holds the contract.
 */
@FunctionalInterface
public interface ItemDecoder {

    /**
     * The items a frame carries, looking inside the frames it wraps.
     *
     * @param frame the bytes on the wire
     * @return the items, empty when none decode
     */
    List<FrameItem> items(byte[] frame);
}
