/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Read-time criteria widening broadens a type-filtering {@link EventCriteria} so the storage
 * layer still returns every event the chain can transform into one of the queried types.
 * Exact and declared-predicate {@code from} types are added to the type filter (transitively,
 * to a fixed point); a predicate-{@code from} transformation that declares no types drops the type
 * filter entirely because its source names cannot be enumerated. Tags are always preserved.
 */
final class EventTransformerChainCriteriaWideningTest {

    private static final QualifiedName CURRENT = new QualifiedName("com.example.CourseCreated");
    private static final QualifiedName LEGACY = new QualifiedName("com.example.LegacyCourseCreated");
    private static final QualifiedName OLDEST = new QualifiedName("com.example.OldestCourseCreated");
    private static final QualifiedName UNRELATED = new QualifiedName("com.example.StudentRegistered");
    private static final MessageType CURRENT_V2 = new MessageType(CURRENT, "2.0.0");
    private static final MessageType LEGACY_V2 = new MessageType(LEGACY, "2.0.0");
    private static final Tag COURSE_TAG = Tag.of("courseId", "course-1");

    private static final Predicate<MessageType> ANY = type -> true;

    private static EventTransformation declaredFromTransformation(QualifiedName declaredFrom, MessageType to) {
        return declaredFromTransformation(to, declaredFrom);
    }

    // A predicate mapping that declares specific from-types, so it contributes a widening edge from each declared
    // type to the target. (Not a rename: this exercises the widening graph, not the read-time transform.)
    private static EventTransformation declaredFromTransformation(MessageType to, QualifiedName... declaredFrom) {
        return EventTransformation.from(ANY)
                                  .declaringFromTypes(declaredFrom)
                                  .to(to)
                                  .transform(JsonNode.class, (in, ctx) -> in);
    }

    private static EventTransformation wildcardTransformation(MessageType to) {
        return EventTransformation.from(ANY).to(to).transform(JsonNode.class, (in, ctx) -> in);
    }

    @Nested
    final class NoWidening {

        @Test
        void emptyChainLeavesCriteriaUnchanged() {
            // given a chain with no transformations
            EventTransformerChain chain = EventTransformerChain.builder().build();
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT);

            // when / then the criteria is returned untouched (same instance, zero-cost)
            assertThat(chain.widen(criteria)).isSameAs(criteria);
        }

        @Test
        void versionOnlyChainLeavesCriteriaUnchanged() {
            // given a chain whose only transformation changes the version, not the qualified name
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.from(new MessageType(CURRENT, "1.0.0"))
                                                                                             .to(CURRENT_V2)
                                                                                             .transform(JsonNode.class, (in, ctx) -> in))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT);

            // when / then version-only transforms widen nothing (the type filter is version-less)
            assertThat(chain.widen(criteria)).isSameAs(criteria);
        }

        @Test
        void matchAllCriteriaIsLeftUnchanged() {
            // given a chain that can widen
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(declaredFromTransformation(LEGACY, CURRENT_V2))
                                                               .build();

            // when / then a match-everything criteria carries no type filter to widen
            EventCriteria matchAll = EventCriteria.havingAnyTag();
            assertThat(chain.widen(matchAll)).isSameAs(matchAll);
        }

        @Test
        void criterionUnrelatedToTheChainIsLeftUnchanged() {
            // given a chain that widens CourseCreated reads only
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(declaredFromTransformation(LEGACY, CURRENT_V2))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(UNRELATED);

            // when / then a query for an unrelated type is returned untouched
            assertThat(chain.widen(criteria)).isSameAs(criteria);
        }

        @Test
        void tagOnlyCriterionIsLeftUnchangedByAnActiveWidener() {
            // given a chain that can widen, but a criterion carrying tags only (no type filter)
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(declaredFromTransformation(LEGACY, CURRENT_V2))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingTags(COURSE_TAG);

            // when / then there is no queried type to broaden, so the criteria is returned untouched
            assertThat(chain.widen(criteria)).isSameAs(criteria);
        }
    }

    @Nested
    final class DeclaredFromTypeWidening {

        @Test
        void declaredFromTypesAreAddedToTheTypeFilter() {
            // given a transformation declaring it consumes the legacy type into the current type
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(declaredFromTransformation(LEGACY, CURRENT_V2))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT);

            // when widening a read for the current type
            EventCriteria widened = chain.widen(criteria);

            // then both the queried and the declared source type are matched
            assertThat(widened.flatten())
                    .singleElement()
                    .satisfies(criterion -> {
                        assertThat(criterion.types()).containsExactlyInAnyOrder(CURRENT, LEGACY);
                        assertThat(criterion.tags()).isEmpty();
                    });
        }

        @Test
        void transitiveDeclaredFromTypesResolveToFixedPoint() {
            // given a two-hop rename graph: oldest -> legacy -> current
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(declaredFromTransformation(LEGACY, CURRENT_V2))
                                                               .register(declaredFromTransformation(OLDEST, LEGACY_V2))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT);

            // when widening a read for the current type
            EventCriteria widened = chain.widen(criteria);

            // then every transitive ancestor type is included
            assertThat(widened.flatten())
                    .singleElement()
                    .satisfies(criterion ->
                                       assertThat(criterion.types()).containsExactlyInAnyOrder(CURRENT, LEGACY, OLDEST));
        }

        @Test
        void convergingRenamePathsResolveEachAncestorOnce() {
            // given a diamond graph: current <- {legacy, oldest} and legacy <- oldest,
            // so oldest is reachable from current along two distinct paths
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(declaredFromTransformation(CURRENT_V2, LEGACY, OLDEST))
                                                               .register(declaredFromTransformation(LEGACY_V2, OLDEST))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT);

            // when widening a read for the current type
            EventCriteria widened = chain.widen(criteria);

            // then every ancestor appears exactly once despite being reachable via multiple paths
            assertThat(widened.flatten())
                    .singleElement()
                    .satisfies(criterion ->
                                       assertThat(criterion.types()).containsExactlyInAnyOrder(CURRENT, LEGACY, OLDEST));
        }

        @Test
        void tagsArePreservedWhenTypesAreWidened() {
            // given a chain that widens the current type
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(declaredFromTransformation(LEGACY, CURRENT_V2))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingTags(COURSE_TAG).andBeingOneOfTypes(CURRENT);

            // when widening a tag-and-type read
            EventCriteria widened = chain.widen(criteria);

            // then the tag restriction survives alongside the widened types
            assertThat(widened.flatten())
                    .singleElement()
                    .satisfies(criterion -> {
                        assertThat(criterion.types()).containsExactlyInAnyOrder(CURRENT, LEGACY);
                        assertThat(criterion.tags()).containsExactly(COURSE_TAG);
                    });
        }

        @Test
        void eachCriterionInAnOrCombinationIsWidenedIndependently() {
            // given a chain that widens the current type but not the unrelated type
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(declaredFromTransformation(LEGACY, CURRENT_V2))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT)
                                                  .or(EventCriteria.havingAnyTag().andBeingOneOfTypes(UNRELATED));

            // when widening the combined criteria
            EventCriteria widened = chain.widen(criteria);

            // then the current-type criterion gains the legacy type while the other is left alone
            assertThat(widened.flatten())
                    .anySatisfy(criterion -> assertThat(criterion.types()).containsExactlyInAnyOrder(CURRENT, LEGACY))
                    .anySatisfy(criterion -> assertThat(criterion.types()).containsExactly(UNRELATED));
        }
    }

    @Nested
    final class WildcardPredicateDropsTypeFilter {

        @Test
        void taggedCriterionCollapsesToTagsOnly() {
            // given a predicate-from transformation that declares no from types
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(wildcardTransformation(CURRENT_V2))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingTags(COURSE_TAG).andBeingOneOfTypes(CURRENT);

            // when widening a read whose type is produced by the wildcard transformation
            EventCriteria widened = chain.widen(criteria);

            // then the type filter is dropped but the tag restriction is preserved
            assertThat(widened.flatten())
                    .singleElement()
                    .satisfies(criterion -> {
                        assertThat(criterion.types()).isEmpty();
                        assertThat(criterion.tags()).containsExactly(COURSE_TAG);
                    });
        }

        @Test
        void taglessCriterionCollapsesToMatchEverything() {
            // given a wildcard transformation producing the current type
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(wildcardTransformation(CURRENT_V2))
                                                               .build();
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT);

            // when widening a tagless read for that type
            EventCriteria widened = chain.widen(criteria);

            // then the read matches everything: no type and no tag restriction remains
            assertThat(widened.hasCriteria()).isFalse();
        }
    }
}
