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
 * Event: the student identified by {@code studentId} unsubscribed from the course identified by {@code courseId}.
 * Both ids are tagged, mirroring {@link StudentSubscribedToCourse}: the course branch counts it as a freed seat,
 * and the focused per-student scope treats a subscription as active only when it is newer than any
 * unsubscription.
 */
public record StudentUnsubscribedFromCourse(@EventTag(key = "courseId") String courseId,
                                            @EventTag(key = "studentId") String studentId) {
}
