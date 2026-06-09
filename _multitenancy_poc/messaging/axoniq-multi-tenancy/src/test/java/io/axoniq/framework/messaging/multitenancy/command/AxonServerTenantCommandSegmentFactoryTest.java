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

package io.axoniq.framework.messaging.multitenancy.command;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBus;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AxonServerTenantCommandSegmentFactoryTest {

    private static final TenantDescriptor TENANT = TenantDescriptor.tenantWithId("tenant-1");
    private static final String CLIENT_ID = "client-id";
    private static final String COMPONENT_NAME = "component-name";

    @Nested
    class GivenDefaultTenantContextMapping {

        @Test
        void applyCreatesDistributedCommandBusForTenantContext() {
            // given
            AxonServerConnectionManager connectionManager = mock(AxonServerConnectionManager.class);
            AxonServerConnection connection = mock(AxonServerConnection.class);
            CommandChannel commandChannel = mock(CommandChannel.class);
            MessageConverter messageConverter = mock(MessageConverter.class);
            AxonServerConfiguration serverConfiguration = new AxonServerConfiguration();
            serverConfiguration.setClientId(CLIENT_ID);
            serverConfiguration.setComponentName(COMPONENT_NAME);
            when(connectionManager.getConnection(TENANT.tenantId())).thenReturn(connection);
            when(connection.commandChannel()).thenReturn(commandChannel);
            when(commandChannel.sendCommand(any())).thenReturn(CompletableFuture.completedFuture(
                    CommandResponse.newBuilder().build()
            ));

            AxonServerTenantCommandSegmentFactory testSubject = new AxonServerTenantCommandSegmentFactory(
                    connectionManager,
                    serverConfiguration,
                    messageConverter
            );

            // when
            DistributedCommandBus tenantCommandBus = (DistributedCommandBus) testSubject.apply(TENANT);
            CompletableFuture<?> result = tenantCommandBus.dispatch(commandFor("CreateOrder"), null);

            // then
            assertThat(result).isCompleted();
            verify(connectionManager).getConnection(TENANT.tenantId());
            verify(commandChannel).sendCommand(any());
        }
    }

    private static CommandMessage commandFor(String commandName) {
        return new GenericCommandMessage(
                new GenericMessage(
                        "message-id",
                        new MessageType(commandName),
                        new byte[] {1, 2, 3},
                        Map.of()
                )
        );
    }
}
