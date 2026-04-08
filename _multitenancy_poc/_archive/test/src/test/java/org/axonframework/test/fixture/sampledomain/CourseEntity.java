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

package org.axonframework.test.fixture.sampledomain;

import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

@EventSourcedEntity
public class CourseEntity {

    public static EventSourcingConfigurer configurer() {
        return EventSourcingConfigurer.create()
                .registerEntity(
                        EventSourcedEntityModule.autodetected(Integer.class, CourseEntity.class)
                );
    }

    @CommandHandler
    static void handle(CreateCourse cmd, EventAppender eventAppender) {
        eventAppender.append(new CourseCreated(cmd.courseId(), cmd.name()));
    }

    private final int courseId;
    private final String name;

    @EntityCreator
    public CourseEntity(CourseCreated event) {
        this.courseId = event.courseId();
        this.name = event.name();
    }
}
