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

package io.axoniq.framework.statecontroller.business.enrollment;

import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Headline test for the fluent multi-term {@link io.axoniq.framework.statecontroller.history.History History}
 * builder driving a real cross-entity DCB decision, written against Axon Framework's {@link AxonTestFixture}.
 * Prior events are seeded through the configured {@link EventSink} (where {@link AnnotationBasedTagResolver}
 * auto-tags each off the {@link org.axonframework.eventsourcing.annotation.EventTag @EventTag} on its id field(s)),
 * a {@link SubscribeStudentToCourse} command is dispatched, and the appended event / thrown rejection is asserted.
 * <p>
 * The fixture configures one annotated {@link CourseSubscriptions} component on top of an in-memory event store and
 * lets the {@code @Decide} handler enhancer and {@code History} parameter-resolver factory (both
 * ServiceLoader-discovered) wire the decision dispatch path. {@code subscribe} narrows a unioned scope spanning a
 * {@code courseId} term and a {@code studentId} term — the cross-entity DCB payoff — so the seeding mixes
 * {@code courseId}-tagged course/seat events with {@code studentId}-tagged eligibility events.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class CourseSubscriptionsTest {

    private AxonTestFixture fixture;

    @BeforeEach
    void setUp() {
        var configurer = MessagingConfigurer.create().registerCommandHandlingModule(
                CommandHandlingModule.named("CourseSubscriptions")
                                     .commandHandlers(ch ->
                                                              ch.autodetectedCommandHandlingComponent(
                                                                      c -> new CourseSubscriptions()
                                                              )));
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
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("student not enrolled in faculty");
                   })
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
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("course does not exist");
                   })
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
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("already subscribed");
                   })
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
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("course full");
                   })
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

    // Imported as static methods so the inline lambda assertions read cleanly.
    private static org.assertj.core.api.AbstractThrowableAssert<?, ? extends Throwable> assertThat(Throwable t) {
        return org.assertj.core.api.Assertions.assertThat(t);
    }

    private static org.assertj.core.api.AbstractStringAssert<?> assertThat(String s) {
        return org.assertj.core.api.Assertions.assertThat(s);
    }
}
