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

package org.axonframework.modelling.entity.domain.development.state;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.entity.domain.development.events.TaskAssigned;
import org.axonframework.modelling.entity.domain.development.events.TaskCompleted;
import org.axonframework.modelling.entity.domain.development.events.TaskCreated;

/**
 * Example for polymorphy without sealed interface.
 */
public interface TaskState {

    @CommandHandler
    default void handle(CreatedTask cmd, EventAppender appender) {
        // command handling logic
    }

    record InitialTask() implements TaskState {

        @EventHandler
        private CreatedTask on(TaskCreated event) {
            return new CreatedTask(event.taskId());
        }
    }

    record CreatedTask(String taskId) implements TaskState {

        @EventHandler
        private AssignedTask on(TaskAssigned event) {
            return new AssignedTask(event.taskId(), event.assignee());
        }
    }

    record AssignedTask(String taskId, String assignee) implements TaskState {

        @EventHandler
        private CompletedTask on(TaskCompleted event) {
            return new CompletedTask(event.taskId(), event.resolution());
        }
    }

    record CompletedTask(String taskId, String resolution) implements TaskState {

    }
}
