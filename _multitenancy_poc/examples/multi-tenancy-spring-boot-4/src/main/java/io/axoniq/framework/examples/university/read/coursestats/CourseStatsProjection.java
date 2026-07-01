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

package io.axoniq.framework.examples.university.read.coursestats;

import io.axoniq.framework.examples.university.event.CourseCreated;
import org.axonframework.messaging.core.annotation.SequencingPolicy;
import org.axonframework.messaging.core.sequencing.PropertySequencingPolicy;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.springframework.stereotype.Component;

/**
 * Projection that stores course stats per tenant.
 */
@Component
@SequencingPolicy(type = PropertySequencingPolicy.class, parameters = {"courseId"})
class CourseStatsProjection {

    @EventHandler
    void handle(CourseCreated event, CourseStatsRepository repository) {
        repository.save(new CourseStats(event.courseId(), event.name(), event.capacity()));
    }
}
