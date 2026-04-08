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

package org.axonframework.test.aggregate;

import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.modelling.command.AggregateCreationPolicy;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.modelling.command.CreationPolicy;
import org.junit.jupiter.api.*;

import java.util.concurrent.Executor;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

/**
 * @author Allard Buijze
 */
class FixtureTest_Resources {

    private FixtureConfiguration<AggregateWithResources> fixture;

    @BeforeEach
    void setUp() {
        fixture = new AggregateTestFixture<>(AggregateWithResources.class);
    }

    @Test
    @Disabled("TODO #3073 - Revisit Aggregate Test Fixture")
    void resourcesAreScopedToSingleTest_ConstructorPartOne() {
        // executing the same test should pass, as resources are scoped to a single test only
        final Executor resource = mock(Executor.class);
        fixture.registerInjectableResource(resource)
               .given()
               .when(new CreateAggregateCommand("id"));

        verify(resource).execute(isA(Runnable.class));
        verifyNoMoreInteractions(resource);
    }

    @Test
    @Disabled("TODO #3073 - Revisit Aggregate Test Fixture")
    void resourcesAreScopedToSingleTest_ConstructorPartTwo() {
        resourcesAreScopedToSingleTest_ConstructorPartOne();
    }

    @Test
    @Disabled("TODO #3195 - Migration Module")
    void resourcesAreScopedToSingleTest_MethodPartOne() {
        // executing the same test should pass, as resources are scoped to a single test only
        final Executor resource = mock(Executor.class);
        fixture.registerInjectableResource(resource)
               .given(new MyEvent("id", 1))
               .when(new TestCommand("id"))
               .expectResultMessagePayload(fixture.getCommandBus());

        verify(resource).execute(isA(Runnable.class));
        verifyNoMoreInteractions(resource);
    }

    @Test
    @Disabled("TODO #3195 - Migration Module")
    void resourcesAreScopedToSingleTest_MethodPartTwo() {
        resourcesAreScopedToSingleTest_MethodPartOne();
    }

    public static class AggregateWithResources {

        @AggregateIdentifier
        private String id;

        @CommandHandler
        @CreationPolicy(AggregateCreationPolicy.ALWAYS)
        public void handle(CreateAggregateCommand cmd, Executor resource, CommandBus commandBus) {
            apply(new MyEvent(cmd.getAggregateIdentifier(), 1));
            resource.execute(() -> fail("Should not really be executed"));
        }

        @CommandHandler
        public CommandBus handleCommand(TestCommand cmd, Executor resource, CommandBus commandBus) {
            resource.execute(() -> fail("Should not really be executed"));
            return commandBus;
        }

        @EventSourcingHandler
        void handle(MyEvent event, Executor resource) {
            assertNotNull(resource);
            this.id = event.getAggregateIdentifier().toString();
        }

        public AggregateWithResources() {
        }
    }
}
