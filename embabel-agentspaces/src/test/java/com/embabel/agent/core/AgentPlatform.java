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
package com.embabel.agent.core;

/**
 * Test stub mirroring the reflective surface {@code EmbabelRemoteActions}
 * touches on Embabel's platform: a single-argument {@code deploy}. The
 * profile-gated CI integration test exercises the real Embabel API; this stub
 * keeps the adapter honest in the plain build.
 */
public interface AgentPlatform {

    /**
     * Deploys an agent.
     *
     * @param agent the agent (annotated instance or platform metadata)
     */
    void deploy(Object agent);
}
