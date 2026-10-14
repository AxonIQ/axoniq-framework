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

package io.axoniq.framework.integrationtests.testsuite.student;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.eventsourcing.annotation.AppendCriteriaBuilder;
import org.axonframework.eventsourcing.commandhandling.AppendCriteriaResolvingCommandHandlingComponent;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;
import org.axonframework.integrationtests.testsuite.student.AbstractCommandHandlingStudentIT;
import org.axonframework.integrationtests.testsuite.student.events.StudentEnrolledEvent;
import org.axonframework.integrationtests.testsuite.student.events.StudentNameChangedEvent;
import org.axonframework.integrationtests.testsuite.student.state.Course;
import org.axonframework.integrationtests.testsuite.student.state.Student;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.SimpleCommandHandlingComponent;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.annotation.InjectEntity;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests validating append criteria for commands routed through Axon Server, which delivers them to the
 * handling component with a serialized payload.
 * <p>
 * An {@link AppendCriteriaBuilder} receives the payload converted to the handler's declared type, so it can switch over
 * a sealed command supertype. A declaratively registered resolver decorates the command-handling component, runs before
 * that conversion, and therefore converts the payload itself.
 *
 * @author Mateusz Nowak
 */
class AppendCriteriaAxonServerIT extends AbstractCommandHandlingStudentIT {

    // AxonServer-only: payload conversion is a distributed-bus concern.
    // SimpleCommandBus passes the payload through without serialization, so there is no in-memory equivalent.
    private static final TestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    private static final String CONSISTENCY_CONDITION_NOT_MET = "Consistency condition is not met";

    private final String studentId = createId("student");
    private final String courseId = createId("course");

    @Override
    protected TestInfrastructure testInfrastructure() {
        return INFRASTRUCTURE;
    }

    @Test
    void annotatedBuilderSwitchesOverSealedCommandsConvertedFromAxonServerPayload() {
        // given
        SealedCommandHandler.receivedCommands.clear();
        SealedCommandHandler.resolvedCriteria.clear();
        registerCommandHandlers(handlerPhase -> handlerPhase.autodetectedCommandHandlingComponent(
                c -> new SealedCommandHandler()
        ));
        startApp();

        // when
        sendCommand(new Enroll(studentId, courseId));
        sendCommand(new Rename(studentId, "Jane"));

        // then
        assertThat(SealedCommandHandler.receivedCommands)
                .containsExactly(new Enroll(studentId, courseId), new Rename(studentId, "Jane"));
        // Enroll narrows the union of both injected entities, Rename keeps its sourcing criteria
        assertThat(SealedCommandHandler.resolvedCriteria.get(0).flatten())
                .containsExactlyInAnyOrderElementsOf(enrollmentOnly().flatten());
        assertThat(SealedCommandHandler.resolvedCriteria.get(1))
                .isEqualTo(EventCriteria.havingTags(new Tag("Student", studentId)));
    }

    @Test
    void declarativeResolverReceivesTheUnconvertedAxonServerPayload() {
        // given
        List<Object> receivedPayloads = new CopyOnWriteArrayList<>();
        List<EventCriteria> resolvedCriteria = new CopyOnWriteArrayList<>();
        registerCommandHandlers(handlerPhase -> handlerPhase.commandHandlingComponent(
                AppendCriteriaResolvingCommandHandlingComponent.withAppendCriteria(
                        c -> SimpleCommandHandlingComponent
                                .create("declarative-enrollment")
                                .subscribe(new QualifiedName(Enroll.class), (command, context) -> {
                                    Enroll enroll = command.payloadAs(Enroll.class,
                                                                      context.component(MessageConverter.class));
                                    StateManager state = context.component(StateManager.class);
                                    state.loadEntity(Student.class, enroll.studentId(), context)
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join();
                                    state.loadEntity(Course.class, enroll.courseId(), context)
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join();
                                    EventAppender.forContext(context).append(
                                            new StudentEnrolledEvent(enroll.studentId(), enroll.courseId())
                                    );
                                    return MessageStream.just(SUCCESSFUL_COMMAND_RESULT).cast();
                                }),
                        c -> (command, context, sourcingCriteria) -> {
                            receivedPayloads.add(command.payload());
                            command.payloadAs(Enroll.class, context.component(MessageConverter.class));
                            EventCriteria criteria = sourcingCriteria.replaceEventTypes(StudentEnrolledEvent.class);
                            resolvedCriteria.add(criteria);
                            return criteria;
                        }
                )
        ));
        startApp();

        // when
        sendCommand(new Enroll(studentId, courseId));

        // then
        // the resolver runs before the handler converts, so it sees the payload as Axon Server delivered it
        assertThat(receivedPayloads).singleElement().isInstanceOf(byte[].class);
        assertThat(resolvedCriteria).singleElement()
                                    .satisfies(criteria -> assertThat(criteria.flatten())
                                            .containsExactlyInAnyOrderElementsOf(enrollmentOnly().flatten()));
    }

    @Test
    void axonServerRejectsSecondCommandMatchingCriteriaOfACommandThatSourcedNothing() {
        // given
        registerCommandHandlers(handlerPhase -> handlerPhase.autodetectedCommandHandlingComponent(
                c -> new RegistrationCommandHandler()
        ));
        startApp();
        sendCommand(new Register(studentId, "Jane"));

        // when / then
        // nothing was sourced, so the criteria are checked from the start of the store and match the first registration
        assertThatThrownBy(() -> sendCommand(new Register(studentId, "John")))
                .isInstanceOf(CommandExecutionException.class)
                .hasMessageContaining(CONSISTENCY_CONDITION_NOT_MET);
    }

    private EventCriteria enrollmentOnly() {
        return EventCriteria.havingTags(new Tag("Student", studentId))
                            .or(EventCriteria.havingTags(new Tag("Course", courseId)))
                            .replaceEventTypes(StudentEnrolledEvent.class);
    }

    sealed interface StudentCommand permits Enroll, Rename {

    }

    record Enroll(String studentId, String courseId) implements StudentCommand {

    }

    record Rename(String studentId, String name) implements StudentCommand {

    }

    record Register(String studentId, String name) {

    }

    private static class SealedCommandHandler {

        private static final List<StudentCommand> receivedCommands = new CopyOnWriteArrayList<>();
        private static final List<EventCriteria> resolvedCriteria = new CopyOnWriteArrayList<>();

        @CommandHandler
        void handle(Enroll command,
                    @InjectEntity(idProperty = "studentId") Student student,
                    @InjectEntity(idProperty = "courseId") Course course,
                    EventAppender eventAppender) {
            eventAppender.append(new StudentEnrolledEvent(command.studentId(), command.courseId()));
        }

        @CommandHandler
        void handle(Rename command,
                    @InjectEntity(idProperty = "studentId") Student student,
                    EventAppender eventAppender) {
            eventAppender.append(new StudentNameChangedEvent(command.studentId(), command.name()));
        }

        @AppendCriteriaBuilder
        static EventCriteria appendCriteria(StudentCommand command, EventCriteria sourcingCriteria) {
            receivedCommands.add(command);
            EventCriteria criteria = switch (command) {
                case Enroll ignored -> sourcingCriteria.replaceEventTypes(StudentEnrolledEvent.class);
                case Rename ignored -> sourcingCriteria;
            };
            resolvedCriteria.add(criteria);
            return criteria;
        }
    }

    private static class RegistrationCommandHandler {

        @CommandHandler
        void handle(Register command, EventAppender eventAppender) {
            eventAppender.append(new StudentNameChangedEvent(command.studentId(), command.name()));
        }

        @AppendCriteriaBuilder
        static EventCriteria appendCriteria(Register command, EventCriteria sourcingCriteria) {
            return EventCriteria.havingTags(new Tag("Student", command.studentId()));
        }
    }
}
