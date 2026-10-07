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

package org.axonframework.test.saga;

import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.junit.jupiter.api.*;

/**
 * Test class validating a {@link org.axonframework.messaging.ScopeDescriptor}, specifically an {@link
 * org.axonframework.modelling.command.AggregateScopeDescriptor}, can be resolved on Aggregate's message handling
 * functions.
 *
 * @author Steven van Beelen
 */
class FixtureScopeDescriptorTest {

    private FixtureConfiguration fixture;

    @BeforeEach
    void setUp() {
        fixture = new SagaTestFixture<>(TestSaga.class);
    }

    @Test
    void resolvesScopeDescriptor() {
        fixture.givenNoPriorActivity()
               .whenPublishingA(new SagaStartEvent("some-identifier"))
//               .expectDispatchedCommandsMatching(payloadsMatching(sequenceOf(matches(
//                       command -> ScopeDescriptorCommand.class.isAssignableFrom(command.getClass()) &&
//                               SagaScopeDescriptor.class.isAssignableFrom(
//                                       ((ScopeDescriptorCommand) command).scopeDescriptor.getClass()
//                               )
//               ))))
        ;
    }

    private static class SagaStartEvent {

        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final String identifier;

        private SagaStartEvent(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return identifier;
        }
    }

    private static class ScopeDescriptorCommand {

        private final ScopeDescriptor scopeDescriptor;

        private ScopeDescriptorCommand(ScopeDescriptor scopeDescriptor) {
            this.scopeDescriptor = scopeDescriptor;
        }
    }

    @SuppressWarnings("unused")
    public static class TestSaga {

        @StartSaga
        @SagaEventHandler(associationProperty = "identifier")
        public void on(SagaStartEvent event, ScopeDescriptor scopeDescriptor, CommandGateway commandGateway, ProcessingContext context) {
            commandGateway.send(new ScopeDescriptorCommand(scopeDescriptor), context);
        }
    }
}
