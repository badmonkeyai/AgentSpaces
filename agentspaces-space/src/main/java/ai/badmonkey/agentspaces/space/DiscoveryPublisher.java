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
package ai.badmonkey.agentspaces.space;

import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;

/**
 * The sink a space publishes its signed {@code SpaceAdvertisement} into
 * (spec §7.5). The space module cannot depend on the discovery module, so
 * this is the seam: {@code DiscoveryService::publish} satisfies it directly,
 * and a test can record what was published.
 */
@FunctionalInterface
public interface DiscoveryPublisher {

    /**
     * Publishes one signed advertisement.
     *
     * @param signed the advertisement, signed by the founding peer
     */
    void publish(SignedAdvertisement<?> signed);
}
