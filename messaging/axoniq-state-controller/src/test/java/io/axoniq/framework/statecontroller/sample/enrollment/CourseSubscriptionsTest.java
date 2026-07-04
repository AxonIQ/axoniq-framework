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

package io.axoniq.framework.statecontroller.sample.enrollment;

import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the union-scope builder driving a real cross-entity DCB decision, written against
 * {@link AxonTestFixture} on a full {@link EventSourcingConfigurer} setup: in-memory event store,
 * annotation-based tag resolution off the {@code @EventTag} fields of the sample events, and the State
 * Controller's ServiceLoader-discovered handler enhancer and {@code History} parameter resolver — no test
 * doubles anywhere in the path.
 * <p>
 * Prior events are seeded through the fixture's given-phase (auto-tagged by the configured
 * {@code AnnotationBasedTagResolver}), a {@link SubscribeStudentToCourse} command is dispatched, and the appended
 * event or thrown rejection is asserted. Because {@link CourseSubscriptions#subscribe} reads a union scope
 * spanning a {@code courseId} branch and a {@code studentId} branch, the seeding deliberately mixes
 * {@code courseId}-tagged course and seat events with {@code studentId}-tagged eligibility events — and, in the
 * per-branch restriction cases, events that carry <em>both</em> tags.
 */
class CourseSubscriptionsTest {

    private AxonTestFixture fixture;

    @BeforeEach
    void setUp() {
        var configurer = EventSourcingConfigurer.create().registerCommandHandlingModule(
                CommandHandlingModule.named("CourseSubscriptions")
                                     .commandHandlers(handlers -> handlers.autodetectedCommandHandlingComponent(
                                             c -> new CourseSubscriptions())));
        fixture = AxonTestFixture.with(configurer);
    }

    @Nested
    class HappyPath {

        @Test
        void anEnrolledStudentSubscribesToAnOpenCourseWithRemainingCapacity() {
            // given a created course with capacity for two and an enrolled student, no seats taken yet
            fixture.given()
                   .events(new CourseCreated("c1"),
                           new CourseCapacityChanged("c1", 2),
                           new StudentEnrolledInFaculty("s1", "law"))
                   // when the student subscribes
                   .when()
                   .command(new SubscribeStudentToCourse("c1", "s1"))
                   // then the subscription is recorded
                   .then()
                   .events(new StudentSubscribedToCourse("c1", "s1"));
        }

        @Test
        void aStudentMayResubscribeAfterUnsubscribing() {
            // given the student subscribed and then unsubscribed, so they are not currently subscribed
            fixture.given()
                   .events(new CourseCreated("c1"),
                           new CourseCapacityChanged("c1", 2),
                           new StudentEnrolledInFaculty("s1", "law"),
                           new StudentSubscribedToCourse("c1", "s1"),
                           new StudentUnsubscribedFromCourse("c1", "s1"))
                   // when the student subscribes again
                   .when()
                   .command(new SubscribeStudentToCourse("c1", "s1"))
                   // then the fresh subscription is recorded
                   .then()
                   .events(new StudentSubscribedToCourse("c1", "s1"));
        }

        @Test
        void aSubscriptionToAnotherCourseDoesNotOccupyThisCoursesSeatOrCountAsDuplicate() {
            // given the enrolled student already subscribes to a different course; the union's per-branch type
            //       restriction keeps that foreign subscription out of this course's branch, and the focused
            //       per-student duplicate check is per (student, course) pair only through the latest event
            fixture.given()
                   .events(new CourseCreated("c1"),
                           new CourseCapacityChanged("c1", 1),
                           new StudentEnrolledInFaculty("s1", "law"),
                           new StudentSubscribedToCourse("c2", "s1"),
                           new StudentUnsubscribedFromCourse("c2", "s1"))
                   // when the student subscribes to this course
                   .when()
                   .command(new SubscribeStudentToCourse("c1", "s1"))
                   // then the seat is granted: the c2 events neither fill c1 nor mark the student subscribed
                   .then()
                   .events(new StudentSubscribedToCourse("c1", "s1"));
        }
    }

    @Nested
    class Rejections {

        @Test
        void aStudentNotEnrolledInAnyFacultyIsRejected() {
            // given a created course but no faculty enrolment for the student
            fixture.given()
                   .events(new CourseCreated("c1"),
                           new CourseCapacityChanged("c1", 2))
                   // when the unenrolled student tries to subscribe
                   .when()
                   .command(new SubscribeStudentToCourse("c1", "s1"))
                   // then the command is rejected and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> assertThat(t)
                           .isInstanceOf(CommandExecutionException.class)
                           .hasMessageContaining("student not enrolled in faculty"))
                   .noEvents();
        }

        @Test
        void subscribingToACourseThatWasNeverCreatedIsRejected() {
            // given an enrolled student but a course that was never created
            fixture.given()
                   .events(new StudentEnrolledInFaculty("s1", "law"))
                   // when the student tries to subscribe to the missing course
                   .when()
                   .command(new SubscribeStudentToCourse("c1", "s1"))
                   // then the command is rejected and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> assertThat(t)
                           .isInstanceOf(CommandExecutionException.class)
                           .hasMessageContaining("course does not exist"))
                   .noEvents();
        }

        @Test
        void subscribingWhenAlreadySubscribedIsRejected() {
            // given an enrolled student already subscribed (subscription newer than any unsubscription)
            fixture.given()
                   .events(new CourseCreated("c1"),
                           new CourseCapacityChanged("c1", 5),
                           new StudentEnrolledInFaculty("s1", "law"),
                           new StudentSubscribedToCourse("c1", "s1"))
                   // when the student subscribes again
                   .when()
                   .command(new SubscribeStudentToCourse("c1", "s1"))
                   // then the command is rejected and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> assertThat(t)
                           .isInstanceOf(CommandExecutionException.class)
                           .hasMessageContaining("already subscribed"))
                   .noEvents();
        }

        @Test
        void subscribingToAFullCourseIsRejected() {
            // given a course with capacity one already taken by another student, plus our enrolled student
            fixture.given()
                   .events(new CourseCreated("c1"),
                           new CourseCapacityChanged("c1", 1),
                           new StudentSubscribedToCourse("c1", "other"),
                           new StudentEnrolledInFaculty("s1", "law"))
                   // when the enrolled student tries to take the last, already-filled seat
                   .when()
                   .command(new SubscribeStudentToCourse("c1", "s1"))
                   // then the command is rejected and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> assertThat(t)
                           .isInstanceOf(CommandExecutionException.class)
                           .hasMessageContaining("course full"))
                   .noEvents();
        }

        @Test
        void aFreedSeatFromAnUnsubscriptionAllowsADifferentStudentToSubscribe() {
            // given capacity one, one seat taken then freed by an unsubscription, and our enrolled student
            fixture.given()
                   .events(new CourseCreated("c1"),
                           new CourseCapacityChanged("c1", 1),
                           new StudentSubscribedToCourse("c1", "other"),
                           new StudentUnsubscribedFromCourse("c1", "other"),
                           new StudentEnrolledInFaculty("s1", "law"))
                   // when the enrolled student takes the freed seat
                   .when()
                   .command(new SubscribeStudentToCourse("c1", "s1"))
                   // then the subscription is recorded
                   .then()
                   .events(new StudentSubscribedToCourse("c1", "s1"));
        }
    }
}
