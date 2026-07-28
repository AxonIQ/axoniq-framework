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

package io.axoniq.framework.examples.faculty.read.coursestats;

import io.axoniq.framework.examples.faculty.events.CourseCreated;
import org.axonframework.messaging.core.annotation.SequencingPolicy;
import org.axonframework.messaging.core.sequencing.PropertySequencingPolicy;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;

import static io.axoniq.framework.examples.faculty.read.coursestats.CourseStatsConfiguration.logger;

@SequencingPolicy(type = PropertySequencingPolicy.class, parameters = {"courseId"})
class CoursesStatsProjection {

    @EventHandler
    void handle(CourseCreated event, CourseStatsRepository repository) {
        logger.info("Handling CourseCreated event for course {}", event.courseId());
        CoursesStatsReadModel readModel = new CoursesStatsReadModel(
                event.courseId(),
                event.name(),
                event.capacity(),
                0
        );
        repository.save(readModel);

        logger.info("Saved read model {}", repository);
    }

//    @EventHandler
//    void handle(CourseRenamed event) {
//        CoursesStatsReadModel readModel = repository.findByIdOrThrow(event.courseId());
//        var updatedReadModel = readModel.name(event.name());
//        repository.save(updatedReadModel);
//    }
//
//    @EventHandler
//    void handle(CourseCapacityChanged event) {
//        CoursesStatsReadModel readModel = repository.findByIdOrThrow(event.courseId());
//        var updatedReadModel = readModel.capacity(event.capacity());
//        repository.save(updatedReadModel);
//    }
//
//    @EventHandler
//    void handle(StudentSubscribedToCourse event) {
//        CoursesStatsReadModel readModel = repository.findByIdOrThrow(event.courseId());
//        var updatedReadModel = readModel.subscribedStudents(readModel.subscribedStudents() + 1);
//        repository.save(updatedReadModel);
//    }
//
//    @EventHandler
//    void handle(StudentUnsubscribedFromCourse event) {
//        CoursesStatsReadModel readModel = repository.findByIdOrThrow(event.courseId());
//        var updatedReadModel = readModel.subscribedStudents(readModel.subscribedStudents() - 1);
//        repository.save(updatedReadModel);
//    }

}
