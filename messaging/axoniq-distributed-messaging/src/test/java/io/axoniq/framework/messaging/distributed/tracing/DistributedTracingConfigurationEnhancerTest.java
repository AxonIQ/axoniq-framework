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
