/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.distributed.tracing;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DistributedTracingConfigurationEnhancerTest {

    @Test
    void absentSpanFactoryLeavesRegisteredConnectorUnchanged() {
        // given
        CommandBusConnector connector = mock(CommandBusConnector.class);
        AxonConfiguration configuration = MessagingConfigurer.create()
                .componentRegistry(registry ->
                                           registry.registerComponent(CommandBusConnector.class, c -> connector))
                .build();

        // when
        CommandBusConnector configuredConnector = configuration.getComponent(CommandBusConnector.class);

        // then
        assertThat(configuredConnector).isSameAs(connector);
    }
}
