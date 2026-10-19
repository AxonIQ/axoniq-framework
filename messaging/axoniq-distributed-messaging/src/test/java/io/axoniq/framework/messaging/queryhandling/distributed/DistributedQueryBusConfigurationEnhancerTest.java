/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.interception.InterceptingQueryBus;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link DistributedQueryBusConfigurationEnhancer}.
 *
 * @author Mateusz Nowak
 */
class DistributedQueryBusConfigurationEnhancerTest {

    @Test
    void enhancesComponentRegistryWithDistributedQueryBus() {
        // given
        QueryBusConnector mockConnector = mock(QueryBusConnector.class);

        // when
        Configuration config =
                MessagingConfigurer.create()
                        .componentRegistry(cr -> {
                            cr.registerComponent(QueryBusConnector.class, c -> mockConnector);
                            cr.registerEnhancer(new DistributedQueryBusConfigurationEnhancer());
                        })
                        .build();

        // then
        QueryBus queryBus = config.getComponent(QueryBus.class);
        assertThat(queryBus).isInstanceOf(InterceptingQueryBus.class);
        assertThat(config.hasComponent(DistributedQueryBusConfiguration.class)).isTrue();

        // Verify that the delegate of InterceptingQueryBus is a DistributedQueryBus
        assertThat(extractDelegate(queryBus)).isInstanceOf(DistributedQueryBus.class);
    }

    @Test
    void noEnhancementsWhenNoQueryBusConnectorPresent() {
        // given / when
        Configuration config =
                MessagingConfigurer.create()
                        .componentRegistry(cr -> cr.registerEnhancer(
                                new DistributedQueryBusConfigurationEnhancer()
                        ))
                        .build();

        // then
        QueryBus queryBus = config.getComponent(QueryBus.class);
        // Intercepting at all times, since we have a default CorrelationDataInterceptor.
        assertThat(queryBus).isInstanceOf(InterceptingQueryBus.class);
        assertThat(config.hasComponent(DistributedQueryBusConfiguration.class)).isFalse();
    }

    private static QueryBus extractDelegate(QueryBus queryBus) {
        try {
            Field delegateField = InterceptingQueryBus.class.getDeclaredField("delegate");
            delegateField.setAccessible(true);
            return (QueryBus) delegateField.get(queryBus);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to extract delegate from InterceptingQueryBus", e);
        }
    }
}
