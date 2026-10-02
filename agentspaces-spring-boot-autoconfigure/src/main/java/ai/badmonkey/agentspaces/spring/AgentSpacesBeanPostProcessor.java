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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.BidFunction;
import ai.badmonkey.agentspaces.agent.annotation.ProvidesCapability;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Enrolls annotated Spring beans in the fleet (spec §10.3, §10.4, §10.5): any
 * bean carrying {@link AgentSpec} or space-bound methods ({@link SpaceTake},
 * {@link SpaceNotify}, {@link BidFunction}) binds after initialization, which
 * starts its worker loops, wires its bids, and publishes its AgentCard; a
 * {@link ProvidesCapability} bean implementing {@code CapabilityProvider} is
 * registered on its groups' capability runtimes and advertised. Plain beans
 * pass through untouched.
 *
 * <p>Group selection is delegated to {@link AgentSpaces#bind(Object)}: the
 * bean binds into every joined group whose registered spaces satisfy its
 * annotations (one card per group, §10.4), and a {@code group} attribute on an
 * annotation pins that method to one group (§10.3). A bean referencing spaces
 * no group covers, or a group that is not joined, fails fast at startup, which
 * is the moment a configuration mistake should surface.
 */
public class AgentSpacesBeanPostProcessor implements BeanPostProcessor {

    private final Supplier<AgentSpaces> spaces;
    private final Supplier<List<String>> groupOrder;

    /**
     * Creates the post-processor over a built facade.
     *
     * @param spaces     the facade beans bind through
     * @param groupOrder the configured group names, in configuration order
     */
    public AgentSpacesBeanPostProcessor(AgentSpaces spaces, List<String> groupOrder) {
        Objects.requireNonNull(spaces, "spaces");
        List<String> order = List.copyOf(Objects.requireNonNull(groupOrder, "groupOrder"));
        this.spaces = () -> spaces;
        this.groupOrder = () -> order;
    }

    /**
     * Creates the post-processor over a facade resolved lazily, on the first
     * bean that enrolls. A post-processor is created before other beans, so
     * resolving the facade eagerly would build the whole fabric before the rest
     * of the context's post-processors are registered; Spring Boot reports that
     * as a {@code BeanPostProcessorChecker} warning per fabric bean.
     *
     * @param spaces     supplies the facade beans bind through
     * @param groupOrder supplies the configured group names, in configuration order
     */
    public AgentSpacesBeanPostProcessor(Supplier<AgentSpaces> spaces, Supplier<List<String>> groupOrder) {
        this.spaces = Objects.requireNonNull(spaces, "spaces");
        this.groupOrder = Objects.requireNonNull(groupOrder, "groupOrder");
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        Class<?> type = bean.getClass();
        boolean enrolls = AgentBinder.isAgentType(type) || AgentBinder.hasBindings(type)
                || type.isAnnotationPresent(ProvidesCapability.class);
        if (!enrolls) {
            return bean;
        }
        try {
            spaces.get().bind(bean);
        } catch (IllegalArgumentException | IllegalStateException e) {
            // Only a space-coverage failure gets the space-coverage message.
            // A bean that binds no spaces failed for some other reason -- a
            // @ProvidesCapability without the interface, say (QA4 A4-1) -- and
            // blaming the group wiring for it sends the reader the wrong way.
            if (referencedSpaces(type).isEmpty()) {
                throw new IllegalStateException("bean '" + beanName + "' could not be enrolled: "
                        + e.getMessage(), e);
            }
            throw new IllegalStateException("bean '" + beanName + "' references spaces "
                    + referencedSpaces(type) + " but no configured group registers them all"
                    + " (" + e.getMessage() + "); groups: " + groupOrder.get(), e);
        }
        return bean;
    }

    /** The space names a bean's annotations reference; inferred (empty) names excluded. */
    private static Set<String> referencedSpaces(Class<?> type) {
        Set<String> referenced = new LinkedHashSet<>();
        for (Method method : type.getMethods()) {
            SpaceTake take = method.getAnnotation(SpaceTake.class);
            if (take != null) {
                referenced.add(take.space());
                referenced.add(take.resultSpace());
            }
            SpaceNotify notify = method.getAnnotation(SpaceNotify.class);
            if (notify != null) {
                referenced.add(notify.space());
                referenced.add(notify.resultSpace());
            }
            BidFunction bid = method.getAnnotation(BidFunction.class);
            if (bid != null) {
                referenced.add(bid.space());
            }
        }
        for (Class<?> at = type; at != null && at != Object.class; at = at.getSuperclass()) {
            for (java.lang.reflect.Field field : at.getDeclaredFields()) {
                SpaceRef ref = field.getAnnotation(SpaceRef.class);
                if (ref != null) {
                    referenced.add(ref.value());
                }
            }
        }
        referenced.remove(""); // inferred: resolved by the binder inside the group
        return referenced;
    }
}
