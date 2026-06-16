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

import org.axonframework.eventsourcing.annotation.EventTag;

/**
 * Event: the student identified by {@code studentId} was enrolled in {@code faculty}. The {@code studentId} is
 * tagged {@code studentId} so the enrolment lands in that student's term of the subscription decision's unioned
 * scope, which the decision requires before admitting a course subscription.
 */
public record StudentEnrolledInFaculty(@EventTag(key = "studentId") String studentId, String faculty) {

}
