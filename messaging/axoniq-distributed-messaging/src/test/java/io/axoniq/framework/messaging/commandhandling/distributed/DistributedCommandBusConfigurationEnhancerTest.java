/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.commandhandling.distributed;

import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.interception.InterceptingCommandBus;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link DistributedCommandBusConfigurationEnhancer}.
 *
 * @author Jens Mayer
 */
class DistributedCommandBusConfigurationEnhancerTest {

    @Test
    void enhancesComponentRegistryWithDistributedCommandBus() {

        CommandBusConnector mockConnector = mock(CommandBusConnector.class);

        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(CommandBusConnector.class, c -> mockConnector);
                                       cr.registerEnhancer(new DistributedCommandBusConfigurationEnhancer());
                                   })
                                   .build();

        CommandBus commandBus = config.getComponent(CommandBus.class);
        assertThat(commandBus).isInstanceOf(InterceptingCommandBus.class);
        assertThat(config.hasComponent(DistributedCommandBusConfiguration.class)).isTrue();

        // Verify that the delegate of InterceptingCommandBus is a DistributedCommandBus
        assertThat(extractDelegate(commandBus)).isInstanceOf(DistributedCommandBus.class);
    }

    @Test
    void noEnhancementsWhenNoCommandBusConnectorPresent() {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerEnhancer(
                                           new DistributedCommandBusConfigurationEnhancer()
                                   ))
                                   .build();

        CommandBus commandBus = config.getComponent(CommandBus.class);
        // Intercepting at all times, since we have a default CorrelationDataInterceptor.
        assertThat(commandBus).isInstanceOf(InterceptingCommandBus.class);
        assertThat(config.hasComponent(DistributedCommandBusConfiguration.class)).isFalse();
    }

    private static CommandBus extractDelegate(CommandBus commandBus) {
        try {
            Field delegateField = InterceptingCommandBus.class.getDeclaredField("delegate");
            delegateField.setAccessible(true);
            return (CommandBus) delegateField.get(commandBus);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to extract delegate from InterceptingCommandBus", e);
        }
    }
}