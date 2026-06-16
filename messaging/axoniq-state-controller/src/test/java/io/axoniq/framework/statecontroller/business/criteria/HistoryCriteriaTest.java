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

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Value-level acceptance tests pinning down the fluent multi-term {@link History} {@code EventCriteria} builder
 * against real, seeded events.
 * <p>
 * Events are appended through an in-memory {@link StorageEngineBackedEventStore} over an
 * {@link InMemoryEventStorageEngine} and read back through a {@link History} minted by
 * {@link HistoryFactory#rootHistoryFor(ProcessingContext)} on a {@link StubProcessingContext} that exposes the
 * {@link EventStore} and a {@link MessageTypeResolver}. The tests cover the tagless type-first start
 * ({@code of(Class...)} + {@code and(...)}), the real DCB target folding two tag-then-types terms into an
 * {@code either(...)}, the bare {@code of(tag)} regression that still loads all event types for the tag, and the
 * builder guards rejecting {@code and(...)} on the bare root, mutation after a read, and an empty type list.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class HistoryCriteriaTest {

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

    @Nested
    class TaglessTypeFirst {

        @Test
        void seesEventsOfTheGivenTypesRegardlessOfTag() {
            // given — the two requested types live under unrelated, distinct tags
            seed(new StudentSubscribedToCourse("c1", "s1"), courseTag("c1"));
            seed(new CourseCreated("c2"), courseTag("c2"));

            // when — a tagless term restricted to both types, matching across all tags
            History scope = rootHistory().of(StudentSubscribedToCourse.class).and(CourseCreated.class);

            // then — both events are visible despite living under different tags
            assertThat(scope.has(StudentSubscribedToCourse.class)).isTrue();
            assertThat(scope.has(CourseCreated.class)).isTrue();
            assertThat(scope.count(StudentSubscribedToCourse.class, CourseCreated.class)).isEqualTo(2L);
        }

        @Test
        void excludesEventTypesOutsideTheTerm() {
            // given — a type the term does not list, plus a listed type
            seed(new CourseCapacityChanged("c1", 30), courseTag("c1"));
            seed(new CourseCreated("c1"), courseTag("c1"));

            // when — the term lists only the two types below
            History scope = rootHistory().of(StudentSubscribedToCourse.class).and(CourseCreated.class);

            // then — the unlisted type is excluded, the listed one is seen
            assertThat(scope.has(CourseCapacityChanged.class)).isFalse();
            assertThat(scope.has(CourseCreated.class)).isTrue();
            assertThat(scope.count(StudentSubscribedToCourse.class, CourseCreated.class)).isEqualTo(1L);
        }
    }

    @Nested
    class RealTargetEither {

        // The real DCB target: OR of two tag+type terms.
        //   history.of("courseId", c)
        //          .and(CourseCreated, CourseCapacityChanged, StudentSubscribedToCourse, StudentUnsubscribedFromCourse)
        //          .or("studentId", s)
        //          .and(StudentEnrolledInFaculty, StudentSubscribedToCourse, StudentUnsubscribedFromCourse)
        private History target(String courseId, String studentId) {
            return rootHistory()
                    .of("courseId", courseId)
                    .and(CourseCreated.class, CourseCapacityChanged.class,
                         StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class)
                    .or("studentId", studentId)
                    .and(StudentEnrolledInFaculty.class,
                         StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class);
        }

        @Test
        void seesTheUnionOfBothTerms() {
            // given — events satisfying each term: one under the courseId tag, one under the studentId tag
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));

            // when — the either(...) of both terms
            History scope = target("c1", "s1");

            // then — both terms contribute, so the union is visible
            assertThat(scope.has(CourseCreated.class)).isTrue();
            assertThat(scope.has(StudentEnrolledInFaculty.class)).isTrue();
            assertThat(scope.count(CourseCreated.class, StudentEnrolledInFaculty.class)).isEqualTo(2L);
        }

        @Test
        void countsEventsMatchingEitherTermAcrossBothTags() {
            // given — first-term events under courseId, second-term events under studentId
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new CourseCapacityChanged("c1", 30), courseTag("c1"));
            seed(new StudentSubscribedToCourse("c1", "s1"), courseTag("c1"));
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));
            seed(new StudentUnsubscribedFromCourse("c9", "s1"), studentTag("s1"));

            // when
            History scope = target("c1", "s1");

            // then — all five seeded events match one of the two terms
            assertThat(scope.count(CourseCreated.class, CourseCapacityChanged.class,
                                   StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class,
                                   StudentEnrolledInFaculty.class)).isEqualTo(5L);
        }

        @Test
        void latestSelectsTheNewestEventAcrossTheUnion() {
            // given — a course event, then a later student event
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new StudentEnrolledInFaculty("s1", "law"), studentTag("s1"));

            // when
            History scope = target("c1", "s1");

            // then — the newest event in the union is the student enrolment
            assertThat(scope.lastWas(StudentEnrolledInFaculty.class)).isTrue();
            assertThat(scope.latest(StudentEnrolledInFaculty.class))
                    .hasValueSatisfying(e -> assertThat(e.faculty()).isEqualTo("law"));
        }

        @Test
        void excludesEventsMatchingNeitherTerm() {
            // given — a CourseCapacityChanged under a foreign courseId (wrong tag for term one),
            // and only listed by term one anyway, so it must not leak into the studentId term
            seed(new CourseCapacityChanged("c2", 30), courseTag("c2"));
            // and a StudentEnrolledInFaculty under a foreign studentId (wrong tag for term two)
            seed(new StudentEnrolledInFaculty("s2", "law"), studentTag("s2"));

            // when — the target is scoped to c1 / s1
            History scope = target("c1", "s1");

            // then — neither foreign-tagged event matches either term
            assertThat(scope.has(CourseCapacityChanged.class)).isFalse();
            assertThat(scope.has(StudentEnrolledInFaculty.class)).isFalse();
            assertThat(scope.count(CourseCreated.class, CourseCapacityChanged.class,
                                   StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class,
                                   StudentEnrolledInFaculty.class)).isZero();
        }

        @Test
        void excludesEventTypesNotListedInTheTermForItsTag() {
            // given — CourseCreated is listed only by the courseId term; seed it under the studentId tag instead
            seed(new CourseCreated("c1"), studentTag("s1"));

            // when
            History scope = target("c1", "s1");

            // then — CourseCreated under the studentId tag matches neither term (term two does not list it)
            assertThat(scope.has(CourseCreated.class)).isFalse();
        }
    }

    @Nested
    class BareOfTagLoadsAllTypes {

        @Test
        void ofTagWithoutAndStillSeesEveryEventTypeForThatTag() {
            // given — three distinct event types under one courseId tag
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new CourseCapacityChanged("c1", 30), courseTag("c1"));
            seed(new StudentSubscribedToCourse("c1", "s1"), courseTag("c1"));

            // when — bare of(...) with no .and(...) narrowing
            History scope = rootHistory().of("courseId", "c1");

            // then — every event type tagged with that courseId is visible
            assertThat(scope.has(CourseCreated.class)).isTrue();
            assertThat(scope.has(CourseCapacityChanged.class)).isTrue();
            assertThat(scope.has(StudentSubscribedToCourse.class)).isTrue();
            assertThat(scope.count(CourseCreated.class, CourseCapacityChanged.class,
                                   StudentSubscribedToCourse.class)).isEqualTo(3L);
        }

        @Test
        void ofTagWithoutAndExcludesOtherTags() {
            // given — events under two different courseId tags
            seed(new CourseCreated("c1"), courseTag("c1"));
            seed(new CourseCreated("c2"), courseTag("c2"));

            // when — narrow to c1 only
            History scope = rootHistory().of("courseId", "c1");

            // then — the c2 event is outside the scope
            assertThat(scope.count(CourseCreated.class)).isEqualTo(1L);
        }
    }

    @Nested
    class BuilderGuards {

        @Test
        void andOnTheBareRootFailsFast() {
            // given — the unbound root, no term started
            History root = rootHistory();

            // when / then — and(...) before any of(...) is a programming error
            assertThatThrownBy(() -> root.and(CourseCreated.class))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void orOnTheBareRootFailsFast() {
            // given — the unbound root, no term started
            History root = rootHistory();

            // when / then — or(...) before any of(...) is equally rejected
            assertThatThrownBy(() -> root.or("studentId", "s1"))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.or(CourseCreated.class))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void andAfterAReadFailsFast() {
            // given — a builder that has already been read (its criteria is sealed)
            seed(new CourseCreated("c1"), courseTag("c1"));
            History scope = rootHistory().of(CourseCreated.class);
            assertThat(scope.has(CourseCreated.class)).isTrue();

            // when / then — extending a sealed builder is a programming error
            assertThatThrownBy(() -> scope.and(CourseCapacityChanged.class))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void orAfterAReadFailsFast() {
            // given — a builder that has already been read (its criteria is sealed)
            seed(new CourseCreated("c1"), courseTag("c1"));
            History scope = rootHistory().of("courseId", "c1");
            assertThat(scope.has(CourseCreated.class)).isTrue();

            // when / then — beginning a new term after a read is rejected
            assertThatThrownBy(() -> scope.or("studentId", "s1"))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> scope.or(CourseCapacityChanged.class))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void emptyAndVarargsFailsFast() {
            // given — a builder under construction
            History scope = rootHistory().of("courseId", "c1");

            // when / then — and(...) requires at least one type
            assertThatThrownBy(scope::and)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void emptyOfTypesVarargsFailsFast() {
            // given — the root

            // when / then — of(Class...) requires at least one type
            assertThatThrownBy(() -> rootHistory().of())
                    .isInstanceOf(IllegalArgumentException.class);
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
}
