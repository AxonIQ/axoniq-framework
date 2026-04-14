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

package io.axoniq.framework.integrationtests.testsuite.student;

import org.axonframework.integrationtests.testsuite.AbstractAxonServerIT;
import org.axonframework.integrationtests.testsuite.student.AbstractCommandHandlingStudentIT;
import org.axonframework.integrationtests.testsuite.student.commands.ChangeStudentNameCommand;
import org.axonframework.integrationtests.testsuite.student.events.StudentNameChangedEvent;
import org.axonframework.integrationtests.testsuite.student.state.Student;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.annotation.InjectEntity;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests whether stateful command handling components can process commands with a single entity.
 *
 * @author Mitchell Herrijgers
 */
class SingleEntityCommandHandlingComponentIT extends AbstractCommandHandlingStudentIT {

    private final String student1 = AbstractAxonServerIT.createId("student-1");
    private final String student2 = AbstractAxonServerIT.createId("student-2");

    @Test
    void canHandleCommandThatTargetsOneEntityUsingStateManager() {
        registerCommandHandlers(handlerPhase -> handlerPhase.commandHandler(
                new QualifiedName(ChangeStudentNameCommand.class),
                c -> (command, context) -> {
                    EventAppender eventAppender = EventAppender.forContext(context);
                    ChangeStudentNameCommand payload =
                            command.payloadAs(ChangeStudentNameCommand.class);
                    StateManager state = context.component(StateManager.class);
                    Student student = state.loadEntity(Student.class, payload.id(), context).join();
                    eventAppender.append(new StudentNameChangedEvent(student.getId(), payload.name()));
                    // Entity through magic of repository automatically updated
                    assertThat(student.getName()).isEqualTo(payload.name());
                    return MessageStream.just(SUCCESSFUL_COMMAND_RESULT).cast();
                }
        ));
        startApp();

        changeStudentName(student1, "name-1");
        verifyStudentName(student1, "name-1");
        changeStudentName(student1, "name-2");
        verifyStudentName(student1, "name-2");
        changeStudentName(student1, "name-3");
        verifyStudentName(student1, "name-3");
        changeStudentName(student1, "name-4");
        verifyStudentName(student1, "name-4");

        changeStudentName(student2, "name-5");
        verifyStudentName(student1, "name-4");
        verifyStudentName(student2, "name-5");
    }

    @Test
    void canHandleCommandThatTargetsOneModelViaStateManagerParameter() {
        registerCommandHandlers(handlerPhase -> handlerPhase.autodetectedCommandHandlingComponent(
                c -> new SingleModelAnnotatedCommandHandler()
        ));
        startApp();

        changeStudentName(student1, "name-1");
        verifyStudentName(student1, "name-1");

        changeStudentName(student1, "name-2");
        verifyStudentName(student1, "name-2");
    }

    static class SingleModelAnnotatedCommandHandler {

        @CommandHandler
        public void handle(ChangeStudentNameCommand command,
                           @InjectEntity Student student,
                           EventAppender eventAppender) {
            // Change name through event
            eventAppender.append(new StudentNameChangedEvent(student.getId(), command.name()));
            // Entity through magic of repository automatically updated
            assertThat(student.getName()).isEqualTo(command.name());
        }
    }

    private void verifyStudentName(String id, String name) {
        UnitOfWork uow = unitOfWorkFactory.create();
        uow.executeWithResult(context -> context.component(StateManager.class)
                                                .repository(Student.class, String.class)
                                                .load(id, context)
                                                .thenAccept(student -> assertThat(student.entity().getName())
                                                                                    .isEqualTo(name)))
           .join();
    }
}