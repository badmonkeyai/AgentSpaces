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
 * Injects a persistent-data-tree ref handle by name, the sibling of {@link SpaceRef} for the
 * values of {@code agentspaces-pdt} (srad/agentspaces-pdt.md PD-R-11; PDT 2 phase 7, P2-X-03).
 * The declared generic type picks the ref's kind and codecs, for example
 * {@code Ref<PMap<String, Fact>>}, {@code Ref<PVector<Event>>}, or {@code Ref<PBlob>}; the
 * group's {@link ai.badmonkey.agentspaces.agent.pdt.RefResolver} builds the handle, so the
 * group needs its persistent trees ({@code agentspaces.pdt.enabled=true} in the hosts).
 *
 * <p>On a field the binder sets the handle at bind time. On a parameter of a bound method
 * ({@code @SpaceTake}, {@code @SpaceNotify}, and the other verbs) the handle follows the
 * method's own parameters and is resolved once at bind; every call receives it.
 *
 * <pre>{@code
 * @PdtRef("facts")
 * Ref<PMap<String, Fact>> facts;
 *
 * @SpaceTake(space = "findings")
 * public void record(Finding finding, @PdtRef("ledger") Ref<PVector<Finding>> ledger) {
 *     ledger.swap(v -> v.conj(finding));
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER})
public @interface PdtRef {

    /**
     * The ref name.
     *
     * @return the name
     */
    String value();

    /**
     * The group whose facade holds the ref, by the application-facing group name or the GroupId
     * value; empty means any binding group (the last one binding wins, as for
     * {@link CapabilityRef}). A binder for another group skips the field. On a parameter the
     * method's own binding names the group, and this must stay empty.
     *
     * @return the group, or empty
     */
    String group() default "";
}
