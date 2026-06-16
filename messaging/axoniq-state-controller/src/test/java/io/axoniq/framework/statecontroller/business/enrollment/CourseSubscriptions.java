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

import io.axoniq.framework.statecontroller.decisions.Decide;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.history.History;

import static io.axoniq.framework.statecontroller.decisions.Decision.accept;
import static io.axoniq.framework.statecontroller.decisions.Decision.reject;

/**
 * Course-enrollment decision in the business-first {@code @Decide} / {@link History} API, demonstrating the fluent
 * multi-term builder driving a real cross-entity DCB decision. {@link #subscribe(SubscribeStudentToCourse, History)
 * subscribe} narrows one unioned scope spanning a {@code courseId} term and a {@code studentId} term in a single
 * builder chain, then admits the subscription only when every business rule holds.
 * <p>
 * The unioned scope is built with {@code history.of("courseId", c).and(...).or("studentId", s).and(...)}:
 * <ul>
 *     <li>the {@code courseId} term carries the course lifecycle ({@link CourseCreated}, {@link
 *         CourseCapacityChanged}) plus every {@link StudentSubscribedToCourse} / {@link
 *         StudentUnsubscribedFromCourse} for that course — the seats taken;</li>
 *     <li>the {@code studentId} term carries the student's {@link StudentEnrolledInFaculty} — whether the student is
 *         eligible to subscribe.</li>
 * </ul>
 * From this one cross-entity slice the decision rejects when the student is not enrolled in any faculty, when the
 * course was never created, and when the course is full (subscriptions net of unsubscriptions meet the latest
 * declared capacity). Whether <em>this</em> student already holds an active subscription is a per-(student, course)
 * fact the unioned slice cannot isolate — both terms carry foreign subscriptions for the same course — so it is read
 * from a focused {@code studentId} scope of the student's own subscription events. When all rules hold the decision
 * emits a {@link StudentSubscribedToCourse}.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public class CourseSubscriptions {

    @Decide
    Decision subscribe(SubscribeStudentToCourse cmd, History history) {
        History union = history.of("courseId", cmd.courseId())
                               .and(CourseCreated.class, CourseCapacityChanged.class,
                                    StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class)
                               .or("studentId", cmd.studentId())
                               .and(StudentEnrolledInFaculty.class);

        if (union.never(StudentEnrolledInFaculty.class)) {
            return reject("student not enrolled in faculty");
        }
        if (union.never(CourseCreated.class)) {
            return reject("course does not exist");
        }

        History student = history.of("studentId", cmd.studentId())
                                 .and(StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class);
        if (alreadySubscribed(student)) {
            return reject("already subscribed");
        }
        if (full(union)) {
            return reject("course full");
        }
        return accept(new StudentSubscribedToCourse(cmd.courseId(), cmd.studentId()));
    }

    // The student holds an active subscription when their newest sub/unsub event is a subscription.
    private static boolean alreadySubscribed(History student) {
        return student.latestOf(StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class)
                instanceof StudentSubscribedToCourse;
    }

    // Seats taken = course subscriptions net of unsubscriptions; full when that meets the latest declared capacity.
    private static boolean full(History union) {
        long taken = union.count(StudentSubscribedToCourse.class) - union.count(StudentUnsubscribedFromCourse.class);
        return union.latest(CourseCapacityChanged.class)
                    .map(c -> taken >= c.capacity())
                    .orElse(false);
    }
}
