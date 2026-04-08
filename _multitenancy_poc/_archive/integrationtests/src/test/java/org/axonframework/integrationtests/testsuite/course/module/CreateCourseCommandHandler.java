/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.integrationtests.testsuite.course.module;

import org.axonframework.integrationtests.testsuite.course.commands.CreateCourse;
import org.axonframework.integrationtests.testsuite.course.events.CourseCreated;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.modelling.annotation.InjectEntity;

import java.util.List;

class CreateCourseCommandHandler {

    @CommandHandler
    void handle(
            CreateCourse command,
            @InjectEntity(idProperty = "courseId") State state,
            EventAppender eventAppender
    ) {
        var events = decide(command, state);
        eventAppender.append(events);
    }

    private List<CourseCreated> decide(CreateCourse command, State state) {
        if (state.created) {
            return List.of();
        }
        return List.of(new CourseCreated(command.courseId()));
    }

    @EventSourcedEntity(tagKey = "Course")
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
