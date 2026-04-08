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

package org.axonframework.modelling.configuration;

import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.common.configuration.ApplicationConfigurerTestSuite;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ModuleBuilder;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ModellingConfigurer}.
 *
 * @author Steven van Beelen
 */
class ModellingConfigurerTest extends ApplicationConfigurerTestSuite<ModellingConfigurer> {

    @Override
    public ModellingConfigurer createConfigurer() {
        return testSubject == null ? ModellingConfigurer.create() : testSubject;
    }

    @Test
    void messagingDelegatesTasks() {
        TestComponent result =
                testSubject.componentRegistry(cr -> cr.registerComponent(
                                   TestComponent.class,
                                   c -> TEST_COMPONENT
                           ))
                           .build()
                           .getComponent(TestComponent.class);

        assertEquals(TEST_COMPONENT, result);
    }

    @Test
    void componentRegistryDelegatesTasks() {
        TestComponent result =
                testSubject.componentRegistry(cr -> cr.registerComponent(TestComponent.class, c -> TEST_COMPONENT))
                           .build()
                           .getComponent(TestComponent.class);

        assertEquals(TEST_COMPONENT, result);
    }

    @Test
    void registerEntityModuleAddsAModuleConfiguration() {
        StateBasedEntityModule<String, Object> testEntityBuilder =
                StateBasedEntityModule.declarative(String.class, Object.class)
                                      .loader(c -> (id, context) -> null)
                                      .persister(c -> (id, entity, context) -> null)
                                      .build();

        Configuration configuration = testSubject.registerEntity(testEntityBuilder).build();

        assertThat(configuration.getModuleConfiguration(
                "SimpleStateBasedEntityModule<java.lang.String, java.lang.Object>")).isPresent();
    }

    @Test
    void registerCommandHandlingModuleAddsAModuleConfiguration() {
        ModuleBuilder<CommandHandlingModule> statefulCommandHandlingModule =
                CommandHandlingModule.named("test")
                                     .commandHandlers(commandHandlerPhase -> commandHandlerPhase.commandHandler(
                                             new QualifiedName(String.class),
                                             (command, context) -> MessageStream.empty().cast()
                                     ));

        Configuration configuration =
                testSubject.registerCommandHandlingModule(statefulCommandHandlingModule)
                           .build();

        assertThat(configuration.getModuleConfiguration("test")).isPresent();
    }

    @Test
    void registerQueryHandlingModuleAddsAModuleConfiguration() {
        ModuleBuilder<QueryHandlingModule> statefulCommandHandlingModule =
                QueryHandlingModule.named("test")
                                   .queryHandlers(handlerPhase -> handlerPhase.queryHandler(
                                           new QualifiedName(String.class),
                                           (query, context) -> MessageStream.empty().cast()
                                   ));

        Configuration configuration =
                testSubject.registerQueryHandlingModule(statefulCommandHandlingModule)
                           .build();

        assertThat(configuration.getModuleConfiguration("test")).isPresent();
    }
}
