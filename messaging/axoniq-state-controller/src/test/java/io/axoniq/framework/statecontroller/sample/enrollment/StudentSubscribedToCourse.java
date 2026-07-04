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

import org.axonframework.eventsourcing.annotation.EventTag;

/**
 * Event: the student identified by {@code studentId} subscribed to the course identified by {@code courseId}.
 * Both ids are tagged, so the event belongs to the course's history and the student's history at once. Which
 * branch of a union scope reads it is decided by the branch's type declaration — in the subscription decision it
 * is a course-branch event (a taken seat), while the focused per-student scope reads it to detect a duplicate
 * subscription.
 */
public record StudentSubscribedToCourse(@EventTag(key = "courseId") String courseId,
                                        @EventTag(key = "studentId") String studentId) {
}
