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

import io.axoniq.framework.statecontroller.conditions.BooleanCondition;
import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import io.axoniq.framework.statecontroller.conditions.OptionalCondition;
import io.axoniq.framework.statecontroller.decisions.Decide;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import io.axoniq.framework.statecontroller.eventstream.EventCondition;
import io.axoniq.framework.statecontroller.eventstream.EventStream;

import static io.axoniq.framework.statecontroller.decisions.Decision.accept;
import static io.axoniq.framework.statecontroller.decisions.Decision.reject;

/**
 * The lazy, batched sibling of the eager {@link CourseSubscriptions}: the same cross-entity subscription rules and
 * rejection messages, written against the {@link DecisionContext} surface instead of
 * {@link io.axoniq.framework.statecontroller.history.History History}. Both carry {@link Decide @Decide} and
 * produce identical events and rejection messages — this class exists to prove the lazy path is behaviourally
 * equivalent to the eager one while showcasing the one-pass batching that is the {@code DecisionContext} payoff.
 * <p>
 * Where {@link CourseSubscriptions} reads from one eager unioned slice
 * ({@code history.of("courseId", c).and(...).or("studentId", s).and(...)}), this decision narrows <em>two</em>
 * lazy scopes — a {@code courseId} scope carrying the course lifecycle and seat events, and a {@code studentId}
 * scope carrying the student's faculty enrolment and their own subscription history. The lazy
 * {@link DecisionContext#scope(String, Object) scope(...)} surface expresses a per-tag slice, so the cross-entity
 * read is two scopes rather than one union; both scopes share a single event-store transaction and contribute to
 * the same DCB consistency marker, so the decision still spans both entities inside one boundary.
 * <p>
 * The batching is the point: on each scope every question is declared as a lazy
 * {@link io.axoniq.framework.statecontroller.conditions.Condition Condition} <em>before</em> any is forced, so the
 * first {@link io.axoniq.framework.statecontroller.conditions.Condition#resolve() resolve()} on a scope satisfies
 * the whole batch in one coordinated read. The {@code courseId} scope batches the course-exists check, the seat
 * counts, and the latest capacity; the {@code studentId} scope batches the faculty-enrolment check and the
 * student's latest subscription state. {@link EventStream} has no {@code never(...)}; "absent" is expressed as
 * {@code contains(...).resolve()} negated.
 * <p>
 * Reading the student's own subscription state from a {@code studentId} scope (rather than the cross-entity slice)
 * mirrors the eager decision's reason for a focused {@code studentId} read: a course-wide slice cannot isolate
 * <em>this</em> student's subscriptions from other students' subscriptions to the same course, whereas the
 * student's own {@code studentId} scope carries exactly their sub/unsub events.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public class CourseSubscriptionsViaContext {

    @Decide
    Decision subscribe(SubscribeStudentToCourse cmd, DecisionContext ctx) {
        EventStream course = ctx.scope("courseId", cmd.courseId());
        EventStream student = ctx.scope("studentId", cmd.studentId());

        // Declare every question on both scopes up front so each scope loads its whole batch in one read.
        BooleanCondition enrolled = student.contains(StudentEnrolledInFaculty.class);
        BooleanCondition created = course.contains(CourseCreated.class);
        EventCondition latestSubscription =
                student.latestOf(StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class);
        NumericCondition<Long> subscriptions = course.count(StudentSubscribedToCourse.class);
        NumericCondition<Long> unsubscriptions = course.count(StudentUnsubscribedFromCourse.class);
        OptionalCondition<CourseCapacityChanged> capacity = course.latest(CourseCapacityChanged.class);

        if (!enrolled.resolve()) {
            return reject("student not enrolled in faculty");
        }
        if (!created.resolve()) {
            return reject("course does not exist");
        }
        // The student holds an active subscription when their newest sub/unsub event is a subscription.
        if (latestSubscription.resolve().orElse(null) instanceof StudentSubscribedToCourse) {
            return reject("already subscribed");
        }
        // Seats taken = course subscriptions net of unsubscriptions; full when that meets the latest capacity.
        long taken = subscriptions.resolve() - unsubscriptions.resolve();
        boolean full = capacity.resolve()
                               .map(c -> taken >= c.capacity())
                               .orElse(false);
        if (full) {
            return reject("course full");
        }
        return accept(new StudentSubscribedToCourse(cmd.courseId(), cmd.studentId()));
    }
}
