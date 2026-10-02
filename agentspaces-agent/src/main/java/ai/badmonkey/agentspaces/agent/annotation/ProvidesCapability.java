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
package ai.badmonkey.agentspaces.agent.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@code CapabilityProvider} implementation as a capability service
 * the fleet should advertise (spec §10.5): when such a bean is bound through
 * the {@code AgentSpaces} facade, it is registered on the group's
 * {@code CapabilityRuntime}, which starts it, signs and publishes its
 * {@code CapabilityAdvertisement}, and keeps the advertisement fresh on every
 * card refresh. Consumers reach the capability through a typed client:
 * {@code spaces.group("g").capability(VoteClient.class)}.
 *
 * <pre>{@code
 * @ProvidesCapability("aspace:cap/vote")
 * public class FleetVotes implements CapabilityProvider { ... }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProvidesCapability {

    /**
     * The capability type URI, e.g. {@code aspace:cap/vote}. Informational for
     * discovery tooling; the provider's own {@code capabilityType()} governs
     * registration.
     *
     * @return the capability type URI
     */
    String value();

    /**
     * The group to advertise into, by name or GroupId value; empty means every
     * registered group that has a capability runtime (networked groups).
     *
     * @return the group name, or empty for all
     */
    String group() default "";
}
