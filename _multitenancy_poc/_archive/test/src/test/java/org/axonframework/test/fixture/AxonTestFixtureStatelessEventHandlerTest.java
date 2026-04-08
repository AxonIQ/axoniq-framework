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

package org.axonframework.test.fixture;

import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.test.fixture.sampledomain.SendNotificationCommand;
import org.axonframework.test.fixture.sampledomain.StudentNameChangedEvent;
import org.junit.jupiter.api.*;

import java.time.Duration;

public class AxonTestFixtureStatelessEventHandlerTest {

    @Test
    void givenEventsThenAwaitCommands_Success() {
        var configurer = whenEventThenCommandConfig();

        var fixture = AxonTestFixture.with(configurer);

        var studentNameChanged = new StudentNameChangedEvent("my-studentId-1", "name-1", 1);
        var expectedCommand = new SendNotificationCommand("my-studentId-1", "Name changed");
        fixture.given()
               .events(studentNameChanged)
               .then()
               .await(r -> r.commands(expectedCommand), Duration.ofMillis(500));
    }

    @Test
    void givenEventsWhenNothingThenAwaitCommands_Success() {
        var configurer = whenEventThenCommandConfig();

        var fixture = AxonTestFixture.with(configurer);

        var studentNameChanged = new StudentNameChangedEvent("my-studentId-1", "name-1", 1);
        var expectedCommand = new SendNotificationCommand("my-studentId-1", "Name changed");
        fixture.given()
               .events(studentNameChanged)
               .when()
               .nothing()
               .then()
               .await(r -> r.commands(expectedCommand));
    }

    private static EventSourcingConfigurer whenEventThenCommandConfig() {
        var configurer = EventSourcingConfigurer.create();
        configurer.messaging(cr -> cr.eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(
                EventProcessorModule
                        .pooledStreaming("test-given-event-then-command")
                        .eventHandlingComponents(c -> c.declarative(
                                "notificationHandler",
                                cfg -> SimpleEventHandlingComponent.create("test").subscribe(
                                        new QualifiedName(StudentNameChangedEvent.class),
                                        AxonTestFixtureStatelessEventHandlerTest::handleStudentNameChanged
                                )
                        )).notCustomized()
        ))));
        return configurer;
    }

    private static MessageStream.Empty<Message> handleStudentNameChanged(EventMessage e, ProcessingContext ctx) {
        var commandDispatcher = ctx.component(CommandGateway.class);
        var payload = e.payloadAs(StudentNameChangedEvent.class);
        commandDispatcher.send(new SendNotificationCommand(
                payload.id(),
                "Name changed"
        ), ctx);
        return MessageStream.empty();
    }
}
