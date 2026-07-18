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
 * Test class validating the {@link LocalShortcutCommandBusConnectorConfigurationEnhancer}.
 *
 * @author Allard Buijze
 */
class LocalShortcutCommandBusConnectorConfigurationEnhancerTest {

    private final CommandBusConnector mockConnector = mock(CommandBusConnector.class);
    private final LocalCommandDispatchPredicate predicate = (command, context) -> true;

    @Test
    void decoratesConnectorWhenConnectorAndPredicateArePresent() {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(CommandBusConnector.class, c -> mockConnector);
                                       cr.registerComponent(LocalCommandDispatchPredicate.class, c -> predicate);
                                       cr.registerEnhancer(new LocalShortcutCommandBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        CommandBusConnector connector = config.getComponent(CommandBusConnector.class);
        assertThat(connector).isInstanceOf(LocalShortcutCommandBusConnector.class);
    }

    @Test
    void doesNotDecorateConnectorWhenNoPredicateIsPresent() {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(CommandBusConnector.class, c -> mockConnector);
                                       cr.registerEnhancer(new LocalShortcutCommandBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        CommandBusConnector connector = config.getComponent(CommandBusConnector.class);
        assertThat(connector).isSameAs(mockConnector);
    }

    @Test
    void noEnhancementsWhenNoCommandBusConnectorPresent() {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(LocalCommandDispatchPredicate.class, c -> predicate);
                                       cr.registerEnhancer(new LocalShortcutCommandBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        assertThat(config.hasComponent(CommandBusConnector.class)).isFalse();
    }

    @Test
    void distributedCommandBusReceivesTheLocalShortcutConnector() {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(CommandBusConnector.class, c -> mockConnector);
                                       cr.registerComponent(LocalCommandDispatchPredicate.class, c -> predicate);
                                       cr.registerEnhancer(new DistributedCommandBusConfigurationEnhancer());
                                       cr.registerEnhancer(new LocalShortcutCommandBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        CommandBus commandBus = config.getComponent(CommandBus.class);
        assertThat(commandBus).isInstanceOf(InterceptingCommandBus.class);

        Object distributedCommandBus = extractField(commandBus, InterceptingCommandBus.class, "delegate");
        assertThat(distributedCommandBus).isInstanceOf(DistributedCommandBus.class);

        Object connector = extractField(distributedCommandBus, DistributedCommandBus.class, "connector");
        assertThat(connector).isInstanceOf(LocalShortcutCommandBusConnector.class);
    }

    private static Object extractField(Object target, Class<?> declaringType, String fieldName) {
        try {
            Field field = declaringType.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to extract field [" + fieldName + "]", e);
        }
    }
}
