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

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DefaultAxonApplication;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.decorateWithTenantResolver;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Timeout(60)
class TenantIdCorrelationIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private AxonServerTestInfrastructure.ContextManager contextManager;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
    }

    @AfterEach
    void tearDown() {
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void aTenantIdCanBeResolvedFromCommandAndIsPropagatedToEventStore() {

        CommandHandlingModule testCommandModule = CommandHandlingModule
                .named("test")
                .commandHandlers(ch -> ch.commandHandler(
                        new QualifiedName("TestCommand"),
                        (command, context) -> {
                            EventAppender.forContext(context)
                                         .append(new GenericEventMessage(
                                                 new MessageType(
                                                         "TestEvent"),
                                                 "TestEventPayload"
                                         ));
                            return MessageStream.just(
                                    new GenericCommandResultMessage(
                                            new MessageType(
                                                    "TestCommandResult"),
                                            "ok"
                                    )
                            );
                        }
                ))
                .build();

        AxonConfiguration axon = new DefaultAxonApplication()
                .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                .componentRegistry(decorateWithTenantResolver(new MetadataBasedTenantResolver()))
                .componentRegistry(cr -> cr.registerModule(testCommandModule))
                .start();

        var commandGateway = axon.getComponent(CommandGateway.class);
        var eventStore = axon.getComponent(EventStore.class);

        var cmd = new GenericCommandMessage(
                new MessageType("TestCommand"),
                "TestPayload"
        ).andMetadata(Map.of("tenantId", DEFAULT_CONTEXT));

        commandGateway.sendAndWait(cmd);

        await().untilAsserted(() -> {
            MessageStream.Entry<?> storedEvent = requireNonNull(
                    eventStore.open(StreamingCondition.startingFrom(TrackingToken.FIRST), null)
                              .first()
                              .asCompletableFuture()
                              .orTimeout(5, TimeUnit.SECONDS)
                              .join()
            );

            assertThat(storedEvent.message().metadata())
                    .containsEntry(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY, DEFAULT_CONTEXT);
            assertThat(storedEvent.message().type().name()).isEqualTo("TestEvent");
            assertThat(storedEvent.message().payload()).isEqualTo("TestEventPayload".getBytes());
        });
    }
}
