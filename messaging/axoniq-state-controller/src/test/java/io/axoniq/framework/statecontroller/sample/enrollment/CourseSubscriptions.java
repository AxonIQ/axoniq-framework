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

import io.axoniq.framework.statecontroller.History;
import io.axoniq.framework.statecontroller.Outcome;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;

import static io.axoniq.framework.statecontroller.Outcome.accept;
import static io.axoniq.framework.statecontroller.Outcome.reject;

/**
 * Course-enrollment decision demonstrating the union-scope builder driving a real cross-entity DCB decision: a
 * plain {@link CommandHandler @CommandHandler} whose {@link History} parameter and {@link Outcome} return type
 * opt it into the State Controller.
 * <p>
 * The union scope is built with {@code history.of("courseId", c).and(...).or("studentId", s).and(...)}:
 * <ul>
 *     <li>the {@code courseId} branch carries the course lifecycle ({@link CourseCreated},
 *         {@link CourseCapacityChanged}) plus every {@link StudentSubscribedToCourse} /
 *         {@link StudentUnsubscribedFromCourse} for that course — the seats taken;</li>
 *     <li>the {@code studentId} branch carries the student's {@link StudentEnrolledInFaculty} — whether the
 *         student is eligible to subscribe.</li>
 * </ul>
 * The per-branch {@code and(...)} type restriction is load-bearing: subscription events carry <em>both</em> tags,
 * and the restriction is what says they belong to the course branch of this union — the student branch reads
 * enrolments only, keeping foreign courses' subscriptions of the same student out of the seat count.
 * <p>
 * Whether <em>this</em> student already holds an active subscription is a per-(student, course) question the
 * union cannot isolate — the course branch carries other students' subscriptions too — so it is read from a
 * focused {@code studentId} scope restricted to the student's own subscription events. All conditions — across
 * the union and the focused scope — are declared before the first {@code resolve()}, so both sourced reads run
 * concurrently in one batch, and their combined criteria form the decision's consistency boundary.
 */
public class CourseSubscriptions {

    @CommandHandler
    public Outcome subscribe(SubscribeStudentToCourse cmd, History history) {
        History union = history.of("courseId", cmd.courseId())
                               .and(CourseCreated.class, CourseCapacityChanged.class,
                                    StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class)
                               .or("studentId", cmd.studentId())
                               .and(StudentEnrolledInFaculty.class);
        History student = history.of("studentId", cmd.studentId())
                                 .and(StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class);

        var notEnrolled = union.never(StudentEnrolledInFaculty.class);
        var courseMissing = union.never(CourseCreated.class);
        // The student holds an active subscription when their newest sub/unsub event is a subscription.
        var subscription = student.latestOf(StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class);
        // Seats taken = course subscriptions net of unsubscriptions; full when that meets the latest capacity.
        var seatsTaken = union.count(StudentSubscribedToCourse.class)
                              .minus(union.count(StudentUnsubscribedFromCourse.class));
        var full = union.latest(CourseCapacityChanged.class)
                        .combine(seatsTaken, (capacity, taken) ->
                                capacity.map(c -> taken >= c.capacity()).orElse(false));

        if (notEnrolled.resolve()) {
            return reject("student not enrolled in faculty");
        }
        if (courseMissing.resolve()) {
            return reject("course does not exist");
        }
        if (subscription.resolve() instanceof StudentSubscribedToCourse) {
            return reject("already subscribed");
        }
        if (full.resolve()) {
            return reject("course full");
        }
        return accept(new StudentSubscribedToCourse(cmd.courseId(), cmd.studentId()));
    }
}
