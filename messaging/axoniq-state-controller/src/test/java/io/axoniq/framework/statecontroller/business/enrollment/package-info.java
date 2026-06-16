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

/**
 * Course-enrollment sample exercising the fluent multi-term {@code History} builder inside a real cross-entity
 * {@code @Decide} decision. The {@code CourseSubscriptions} decision reads one unioned slice spanning a
 * {@code courseId} term (course lifecycle plus subscription events for that course) OR a {@code studentId} term
 * (the student's faculty enrolment) and decides whether to admit a subscription:
 * <ul>
 *     <li>reject when the student is not enrolled in any faculty;</li>
 *     <li>reject when the course was never created;</li>
 *     <li>reject when the student is already subscribed (a subscription newer than any unsubscription);</li>
 *     <li>reject when the course is full (subscriptions net of unsubscriptions meet the latest capacity);</li>
 *     <li>otherwise accept a {@code StudentSubscribedToCourse}.</li>
 * </ul>
 * Course events tag their {@code courseId} as {@code courseId}; faculty enrolment tags its {@code studentId} as
 * {@code studentId}; the subscription events tag <em>both</em> ids, so a {@code StudentSubscribedToCourse} lands
 * in the course term and the student term at once, keeping both entities inside the decision's DCB boundary.
 */
@NullMarked
package io.axoniq.framework.statecontroller.business.enrollment;

import org.jspecify.annotations.NullMarked;
