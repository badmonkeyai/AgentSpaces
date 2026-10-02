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
package ai.badmonkey.agentspaces.api.spi;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.Map;

/**
 * The pluggable admission check behind {@code POLICY} groups (spec §5.1): DID and
 * verifiable-credential checks, allowlists, and organization-specific rules all
 * implement this SPI.
 */
@FunctionalInterface
public interface MembershipValidator {

    /**
     * Decides whether a candidate peer may join a group.
     *
     * @param candidate   the joining peer
     * @param group       the group's founding advertisement
     * @param credentials credentials presented by the candidate
     * @return {@code true} to admit the candidate
     */
    boolean admit(PeerId candidate, GroupAdvertisement group, Map<String, String> credentials);
}
