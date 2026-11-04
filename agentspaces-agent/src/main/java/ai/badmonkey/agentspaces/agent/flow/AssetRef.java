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


package ai.badmonkey.agentspaces.agent.flow;

import ai.badmonkey.agentspaces.api.ad.AssetCard;

/** A resolved asset reference (ISSUE-FlowManifest FR-5): the card's name, URI, and shape at bind time. */
public record AssetRef(String asset, String uri, String shape, AssetCard card) {

    public static AssetRef of(AssetCard card) {
        return new AssetRef(card.asset(), card.uri(), card.shape(), card);
    }
}
