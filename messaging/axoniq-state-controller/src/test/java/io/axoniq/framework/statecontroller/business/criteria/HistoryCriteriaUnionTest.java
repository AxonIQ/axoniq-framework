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

package io.axoniq.framework.statecontroller.business.criteria;

import io.axoniq.framework.statecontroller.history.History;
import io.axoniq.framework.statecontroller.history.HistoryFactory;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Value-level acceptance tests for the multi-term {@link History} {@code EventCriteria} builder, complementing
 * {@code HistoryCriteriaTest} with the builder shapes and union reads it does not cover: a three-term
 * {@code or(...)} chain, a tagless {@code or(Class...)} term combined with a tag term, repeated {@code and(...)}
 * calls accumulating types onto the same term, and the full read vocabulary exercised <em>across</em> the union —
 * {@code latest} / {@code first} / {@code lastWas} ordering spanning terms, {@code total(...)} summing across the
 * union, and {@code entry(...)} returning a timestamp.
 * <p>
 * Setup mirrors {@code HistoryCriteriaTest}: events are appended through an in-memory
 * {@link StorageEngineBackedEventStore} over an {@link InMemoryEventStorageEngine} and read back through a
 * {@link History} minted by {@link HistoryFactory#rootHistoryFor(ProcessingContext)} on a
 * {@link StubProcessingContext} exposing the {@link EventStore} and a {@link MessageTypeResolver}.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class HistoryCriteriaUnionTest {

    private InMemoryEventStorageEngine engine;
    private EventStore eventStore;
    private ProcessingContext processingContext;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEventStorageEngine();
        eventStore = new StorageEngineBackedEventStore(engine, new SimpleEventBus(), e -> Set.of());
        MessageTypeResolver typeResolver = new ClassBasedMessageTypeResolver();
        processingContext = new StubProcessingContext(new ApplicationContext() {
            @Override
            public <C> C component(Class<C> type, @Nullable String name) {
                if (type == MessageTypeResolver.class) {
                    return type.cast(typeResolver);
                }
                if (type == EventStore.class) {
                    return type.cast(eventStore);
                }
                throw new ComponentNotFoundException(type, name);
            }
        });
    }

    private History rootHistory() {
        return HistoryFactory.rootHistoryFor(processingContext);
    }

    private <P> void seed(P payload, Set<Tag> tags) {
        var message = new GenericEventMessage(new MessageType(payload.getClass()), payload);
        TaggedEventMessage<?> tagged = new GenericTaggedEventMessage<>(message, tags);
        @SuppressWarnings("unchecked")
        AppendTransaction<ConsistencyMarker> tx = (AppendTransaction<ConsistencyMarker>)
                engine.appendEvents(AppendCondition.none(), null, List.of(tagged)).join();
        tx.commit().thenCompose(tx::afterCommit).join();
    }

    private static Set<Tag> courseTag(String courseId) {
        return Set.of(new Tag("courseId", courseId));
    }

    private static Set<Tag> studentTag(String studentId) {
        return Set.of(new Tag("studentId", studentId));
    }

    private static Set<Tag> facultyTag(String facultyId) {
        return Set.of(new Tag("facultyId", facultyId));
    }

    @Nested
    class ThreeTermOr {

        // Three OR-ed tag-then-types terms: courseId OR studentId OR facultyId.
        private History triUnion(String courseId, String studentId, String facultyId) {
            return rootHistory()
                    .of("courseId", courseId)
                    .and(CourseCreated.class, CourseCapacityChanged.class)
                    .or("studentId", studentId)
                    .and(StudentEnrolledInFaculty.class)
                    .or("facultyId", facultyId)
                    .and(FacultyFunded.class);
        }

        @Test
        void seesTheUnionOfAllThreeTerms() {
            // given — one event satisfying each of the three terms, each under its own tag
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));
            seed(new FacultyFunded("law", BigDecimal.valueOf(1000)), facultyTag("law"));

            // when — the three-term union scoped to c1 / s1 / law
            History scope = triUnion("c1", "s1", "law");

            // then — every term contributes, so all three event types are visible
            assertThat(scope.has(CourseCreated.class)).isTrue();
            assertThat(scope.has(StudentEnrolledInFaculty.class)).isTrue();
            assertThat(scope.has(FacultyFunded.class)).isTrue();
            assertThat(scope.count(CourseCreated.class, StudentEnrolledInFaculty.class, FacultyFunded.class))
                    .isEqualTo(3L);
        }

        @Test
        void excludesEventsMatchingNoneOfTheThreeTerms() {
            // given — each event lives under a tag that none of the three terms select
            seed(new CourseCreated("c2"), courseTag("c2"));
            seed(new StudentEnrolledInFaculty("s2", "law"), studentTag("s2"));
            seed(new FacultyFunded("arts", BigDecimal.valueOf(500)), facultyTag("arts"));

            // when — the union is scoped to c1 / s1 / law
            History scope = triUnion("c1", "s1", "law");

            // then — nothing matches any term
            assertThat(scope.count(CourseCreated.class, StudentEnrolledInFaculty.class, FacultyFunded.class))
                    .isZero();
        }
    }

    @Nested
    class TaglessOrTermCombinedWithTagTerm {

        @Test
        void taglessOrTermMatchesItsTypesAcrossAllTagsWhileTagTermStaysScoped() {
            // given — a tag-term event under c1, and a tagless-term type under an unrelated tag
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new FacultyFunded("arts", BigDecimal.valueOf(700)), facultyTag("arts"));
            // and the same tagless type under yet another tag — still matched (tagless = across all tags)
            seed(new FacultyFunded("law", BigDecimal.valueOf(300)), studentTag("s9"));

            // when — a scoped courseId term OR a tagless type term
            History scope = rootHistory()
                    .of("courseId", "c1").and(CourseCreated.class)
                    .or(FacultyFunded.class);

            // then — the courseId event plus both FacultyFunded events (any tag) are visible
            assertThat(scope.has(CourseCreated.class)).isTrue();
            assertThat(scope.count(FacultyFunded.class)).isEqualTo(2L);
            assertThat(scope.count(CourseCreated.class, FacultyFunded.class)).isEqualTo(3L);
        }

        @Test
        void tagTermStillExcludesForeignTagsForItsOwnTypes() {
            // given — a CourseCreated under a foreign courseId (term one is scoped to c1), plus a tagless match
            seed(new CourseCreated("c2"), courseTag("c2"));
            seed(new FacultyFunded("law", BigDecimal.valueOf(300)), facultyTag("law"));

            // when
            History scope = rootHistory()
                    .of("courseId", "c1").and(CourseCreated.class)
                    .or(FacultyFunded.class);

            // then — the foreign-tagged CourseCreated is not selected by either term
            assertThat(scope.has(CourseCreated.class)).isFalse();
            // but the tagless FacultyFunded term still matches
            assertThat(scope.has(FacultyFunded.class)).isTrue();
        }
    }

    @Nested
    class RepeatedAndAccumulatesTypesOnSameTerm {

        // Repeated and(...) accumulate types onto a single tagless of(Class...) term; the type restriction is what
        // narrows the scope here, since the tagless term matches across all tags.

        @Test
        void successiveAndCallsWidenTheSameTermToAllListedTypes() {
            // given — three distinct types under one courseId tag
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new CourseCapacityChanged("c1", 30), courseTag("c1"));
            seed(new StudentSubscribedToCourse("c1", "s1"), courseTag("c1"));

            // when — the single tagless term is widened by chaining and(...) twice after of(Class...)
            History scope = rootHistory()
                    .of(CourseCreated.class)
                    .and(CourseCapacityChanged.class)
                    .and(StudentSubscribedToCourse.class);

            // then — all three accumulated types are matched on that one term
            assertThat(scope.has(CourseCreated.class)).isTrue();
            assertThat(scope.has(CourseCapacityChanged.class)).isTrue();
            assertThat(scope.has(StudentSubscribedToCourse.class)).isTrue();
            assertThat(scope.count(CourseCreated.class, CourseCapacityChanged.class,
                                   StudentSubscribedToCourse.class)).isEqualTo(3L);
        }

        @Test
        void typesNotAccumulatedOntoTheTermAreExcluded() {
            // given — a type the chained and(...) calls never list, alongside one they do
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new StudentUnsubscribedFromCourse("c1", "s1"), courseTag("c1"));

            // when — only CourseCreated and CourseCapacityChanged are accumulated on the tagless term
            History scope = rootHistory()
                    .of(CourseCreated.class)
                    .and(CourseCapacityChanged.class);

            // then — the unlisted StudentUnsubscribedFromCourse is excluded
            assertThat(scope.has(StudentUnsubscribedFromCourse.class)).isFalse();
            assertThat(scope.count(CourseCreated.class, CourseCapacityChanged.class)).isEqualTo(1L);
        }

        @Test
        void duplicateTypesAcrossAndCallsCollapse() {
            // given — a single matching event
            seed(new CourseCreated("c1"), courseTag("c1"));

            // when — the same type is listed twice across of(Class...) and a following and(...)
            History scope = rootHistory()
                    .of(CourseCreated.class)
                    .and(CourseCreated.class);

            // then — the event is still counted once, not twice
            assertThat(scope.count(CourseCreated.class)).isEqualTo(1L);
        }
    }

    @Nested
    class ReadVocabularyAcrossTheUnion {

        // courseId term (course lifecycle) OR studentId term (faculty enrolment).
        private History union(String courseId, String studentId) {
            return rootHistory()
                    .of("courseId", courseId)
                    .and(CourseCreated.class, CourseCapacityChanged.class)
                    .or("studentId", studentId)
                    .and(StudentEnrolledInFaculty.class, FacultyFunded.class);
        }

        @Test
        void lastWasReflectsTheNewestEventRegardlessOfWhichTermItCameFrom() {
            // given — a course event first, then a later student-term event
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));

            // when
            History scope = union("c1", "s1");

            // then — the newest event across the union is the student-term enrolment
            assertThat(scope.lastWas(StudentEnrolledInFaculty.class)).isTrue();
            assertThat(scope.lastWas(CourseCreated.class)).isFalse();
        }

        @Test
        void firstSelectsTheEarliestEventOfATypeAcrossTheUnion() {
            // given — a student-term event precedes the course-term event of the queried type
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new CourseCapacityChanged("c1", 30), courseTag("c1"));

            // when
            History scope = union("c1", "s1");

            // then — first(StudentEnrolledInFaculty) reaches back across the union to the earliest seeded event
            assertThat(scope.first(StudentEnrolledInFaculty.class))
                    .hasValueSatisfying(e -> assertThat(e.faculty()).isEqualTo("law"));
            // and first(CourseCreated) is the course-term event that followed it
            assertThat(scope.first(CourseCreated.class))
                    .hasValueSatisfying(e -> assertThat(e.courseId()).isEqualTo("c1"));
        }

        @Test
        void latestSelectsTheNewestEventOfATypeAcrossTheUnion() {
            // given — two faculty-funding events under the studentId term, the second carrying a higher amount
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));
            seed(new FacultyFunded("law", BigDecimal.valueOf(100)), studentTag("s1"));
            seed(new FacultyFunded("law", BigDecimal.valueOf(250)), studentTag("s1"));

            // when
            History scope = union("c1", "s1");

            // then — the newest FacultyFunded across the union wins
            assertThat(scope.latest(FacultyFunded.class))
                    .hasValueSatisfying(e -> assertThat(e.amount()).isEqualByComparingTo("250"));
        }

        @Test
        void totalSumsAFieldAcrossEventsDrawnFromBothTerms() {
            // given — FacultyFunded events under BOTH the courseId term and the studentId term
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));
            seed(new FacultyFunded("law", BigDecimal.valueOf(40)), studentTag("s1"));
            // FacultyFunded is also accumulated on the courseId term below, so a course-tagged one counts too
            seed(new FacultyFunded("law", BigDecimal.valueOf(60)), courseTag("c1"));

            // when — both terms list FacultyFunded, so the union spans course-tagged and student-tagged fundings
            History scope = rootHistory()
                    .of("courseId", "c1")
                    .and(CourseCreated.class, FacultyFunded.class)
                    .or("studentId", "s1")
                    .and(StudentEnrolledInFaculty.class, FacultyFunded.class);

            // then — total sums the amount across both terms
            assertThat(scope.total(FacultyFunded.class, FacultyFunded::amount)).isEqualByComparingTo("100");
        }

        @Test
        void entryCarriesTheLatestPayloadAndItsTimestampAcrossTheUnion() {
            // given — a single matching event in the student term
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));

            // when
            History scope = union("c1", "s1");

            // then — entry returns the payload paired with a recorded timestamp
            assertThat(scope.entry(StudentEnrolledInFaculty.class))
                    .hasValueSatisfying(entry -> {
                        assertThat(entry.payload().faculty()).isEqualTo("law");
                        assertThat(entry.occurredAt()).isNotNull();
                    });
        }
    }

    // ----------------------------------------------------------------------
    // Test fixtures
    // ----------------------------------------------------------------------

    record CourseCreated(String courseId) {
    }

    record CourseCapacityChanged(String courseId, int capacity) {
    }

    record StudentEnrolledInFaculty(String studentId, String faculty) {
    }

    record StudentSubscribedToCourse(String courseId, String studentId) {
    }

    record StudentUnsubscribedFromCourse(String courseId, String studentId) {
    }

    record FacultyFunded(String facultyId, BigDecimal amount) {
    }
}
