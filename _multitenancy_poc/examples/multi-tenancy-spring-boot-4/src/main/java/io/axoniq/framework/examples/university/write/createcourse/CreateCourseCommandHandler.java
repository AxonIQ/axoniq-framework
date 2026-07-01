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

package io.axoniq.framework.examples.university.write.createcourse;

import io.axoniq.framework.examples.university.event.CourseCreated;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.InjectEntity;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Handles course creation commands.
 */
@Component
class CreateCourseCommandHandler {

    @CommandHandler
    void handle(
            CreateCourse command,
            @InjectEntity(idProperty = "courseId") State state,
            EventAppender eventAppender
    ) {
        if (state.created) {
            throw new IllegalStateException("Course with id [%s] already exists".formatted(command.courseId()));
        }
        eventAppender.append(List.of(new CourseCreated(
                command.courseId(),
                command.name(),
                command.capacity()
        )));
    }

    @EventSourcedEntity(tagKey = "courseId")
    static final class State {

        private boolean created;

        @EntityCreator
        private State() {
            this.created = false;
        }

        @EventSourcingHandler
        private State apply(CourseCreated event) {
            this.created = true;
            return this;
        }
    }
}
