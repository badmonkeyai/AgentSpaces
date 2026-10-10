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
package ai.badmonkey.agentspaces.api.space;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static ai.badmonkey.agentspaces.api.space.Matchers.contains;
import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static ai.badmonkey.agentspaces.api.space.Matchers.gt;
import static ai.badmonkey.agentspaces.api.space.Matchers.gte;
import static ai.badmonkey.agentspaces.api.space.Matchers.in;
import static ai.badmonkey.agentspaces.api.space.Matchers.isNull;
import static ai.badmonkey.agentspaces.api.space.Matchers.lt;
import static ai.badmonkey.agentspaces.api.space.Matchers.lte;
import static ai.badmonkey.agentspaces.api.space.Matchers.ne;
import static ai.badmonkey.agentspaces.api.space.Matchers.notNull;
import static ai.badmonkey.agentspaces.api.space.Matchers.predicate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TemplateTest {

    record Task(String kind, int priority, String assignee) {
    }

    static class Bean {
        public String getName() {
            return "bean";
        }

        public boolean isReady() {
            return true;
        }
    }

    @Test
    void bareTemplateMatchesAnyInstanceOfItsType() {
        Template<Task> template = Template.of(Task.class);

        assertThat(template.matches(new Task("summarize", 3, null))).isTrue();
        assertThat(template.matches("a string")).isFalse();
        assertThat(template.matches(null)).isFalse();
    }

    @Test
    void conditionsNarrowTheMatch() {
        Template<Task> template = Template.of(Task.class)
                .where("kind", eq("summarize"))
                .where("priority", gte(3));

        assertThat(template.matches(new Task("summarize", 3, "a"))).isTrue();
        assertThat(template.matches(new Task("summarize", 5, "a"))).isTrue();
        assertThat(template.matches(new Task("summarize", 2, "a"))).isFalse();
        assertThat(template.matches(new Task("translate", 5, "a"))).isFalse();
    }

    @Test
    void orderingAndMembershipMatchers() {
        assertThat(Template.of(Task.class).where("priority", gt(2))
                .matches(new Task("k", 3, null))).isTrue();
        assertThat(Template.of(Task.class).where("priority", lt(2))
                .matches(new Task("k", 3, null))).isFalse();
        assertThat(Template.of(Task.class).where("priority", lte(3))
                .matches(new Task("k", 3, null))).isTrue();
        assertThat(Template.of(Task.class).where("kind", in("a", "b"))
                .matches(new Task("b", 1, null))).isTrue();
        assertThat(Template.of(Task.class).where("kind", ne("a"))
                .matches(new Task("b", 1, null))).isTrue();
        assertThat(Template.of(Task.class).where("kind", contains("umm"))
                .matches(new Task("summarize", 1, null))).isTrue();
    }

    @Test
    void nullnessAndCustomPredicateMatchers() {
        assertThat(Template.of(Task.class).where("assignee", isNull())
                .matches(new Task("k", 1, null))).isTrue();
        assertThat(Template.of(Task.class).where("assignee", notNull())
                .matches(new Task("k", 1, null))).isFalse();
        assertThat(Template.of(Task.class)
                .where("priority", predicate(v -> ((Integer) v) % 2 == 1))
                .matches(new Task("k", 3, null))).isTrue();
    }

    @Test
    void typeMismatchedComparisonFailsInsteadOfThrowing() {
        Template<Task> template = Template.of(Task.class).where("kind", gte(3));

        assertThat(template.matches(new Task("summarize", 1, null))).isFalse();
    }

    @Test
    void unknownFieldFailsFastAtConstruction() {
        assertThatThrownBy(() -> Template.of(Task.class).where("priorty", eq(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("priorty")
                .hasMessageContaining("priority");
    }

    @Test
    void beanStyleAccessorsResolve() {
        assertThat(Template.of(Bean.class).where("name", eq("bean"))
                .matches(new Bean())).isTrue();
        assertThat(Template.of(Bean.class).where("ready", eq(true))
                .matches(new Bean())).isTrue();
    }

    @Test
    void whereReturnsANewTemplate() {
        Template<Task> base = Template.of(Task.class);
        Template<Task> narrowed = base.where("priority", gte(5));

        assertThat(base.matches(new Task("k", 1, null))).isTrue();
        assertThat(narrowed.matches(new Task("k", 1, null))).isFalse();
        assertThat(base).isNotSameAs(narrowed);
    }

    /** Issue #16 §9.2: tag predicates are evaluated against the record's tags. */
    @Test
    void tagConditionsMatchTheRecordsTags() {
        assertThat(Template.of(Task.class).whereTag("region", eq("eu"))
                .matchesTags(Map.of("region", "eu"))).isTrue();
        assertThat(Template.of(Task.class).whereTag("region", eq("eu"))
                .matchesTags(Map.of("region", "us"))).isFalse();
        assertThat(Template.of(Task.class).whereTag("region", in("eu", "uk"))
                .matchesTags(Map.of("region", "uk", "tier", "gold"))).isTrue();
        assertThat(Template.of(Task.class).whereTag("rdf:type", contains("#Claim"))
                .matchesTags(Map.of("rdf:type", "https://example.org/ns#Claim"))).isTrue();
        assertThat(Template.of(Task.class)
                .whereTag("region", eq("eu")).whereTag("tier", eq("gold"))
                .matchesTags(Map.of("region", "eu"))).isFalse();
    }

    @Test
    void hasTagRequiresTheKeyWhateverItsValue() {
        Template<Task> template = Template.of(Task.class).hasTag("rdf:type");

        assertThat(template.matchesTags(Map.of("rdf:type", "x"))).isTrue();
        assertThat(template.matchesTags(Map.of("rdf:type", ""))).isTrue();
        assertThat(template.matchesTags(Map.of("other", "x"))).isFalse();
        assertThat(template.matchesTags(Map.of())).isFalse();
    }

    /** A missing key is presented to the matcher as null, so ordinary matchers fail on it. */
    @Test
    void aTagConditionOnAMissingKeyFails() {
        assertThat(Template.of(Task.class).whereTag("region", eq("eu")).matchesTags(Map.of()))
                .isFalse();
        assertThat(Template.of(Task.class).whereTag("region", in("eu", "us"))
                .matchesTags(Map.of("tier", "gold"))).isFalse();
        assertThat(Template.of(Task.class).whereTag("region", contains("e")).matchesTags(Map.of()))
                .isFalse();
        // The one matcher that accepts an absent key, by design: isNull selects untagged entries.
        assertThat(Template.of(Task.class).whereTag("region", isNull()).matchesTags(Map.of()))
                .isTrue();
    }

    @Test
    void matchesTagsWithNoTagConditionsIsTrue() {
        assertThat(Template.of(Task.class).matchesTags(Map.of())).isTrue();
        assertThat(Template.of(Task.class).where("priority", gte(5))
                .matchesTags(Map.of("region", "eu"))).isTrue();
    }

    /** matches(Object) stays field-and-type only; the space applies the tag conditions. */
    @Test
    void tagConditionsAreExposedAndDoNotAffectFieldMatching() {
        Template<Task> base = Template.of(Task.class).where("priority", gte(3));
        Template<Task> tagged = base.whereTag("region", eq("eu")).hasTag("rdf:type");

        assertThat(tagged.matches(new Task("k", 3, null))).isTrue();
        assertThat(tagged.matches(new Task("k", 1, null))).isFalse();
        assertThat(base.tagConditions()).isEmpty();
        assertThat(tagged.tagConditions()).extracting(Template.TagCondition::key)
                .containsExactly("region", "rdf:type");
        assertThat(tagged.type()).isEqualTo(Task.class);
        assertThat(base).isNotSameAs(tagged);
        assertThatThrownBy(() -> Template.of(Task.class).whereTag(null, eq("x")))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Template.of(Task.class).whereTag("k", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void anUnknownFieldStillFailsFastOnATaggedTemplate() {
        assertThatThrownBy(() -> Template.of(Task.class).hasTag("x").where("priorty", eq(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("priorty");
    }
}
