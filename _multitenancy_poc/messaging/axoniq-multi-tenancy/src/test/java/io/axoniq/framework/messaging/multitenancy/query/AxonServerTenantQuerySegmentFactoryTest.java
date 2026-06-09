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

package io.axoniq.framework.messaging.multitenancy.query;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.query.TenantQuerySegmentFactory;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBus;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AxonServerTenantQuerySegmentFactoryTest {

    private static final TenantDescriptor TENANT = TenantDescriptor.tenantWithId("tenant-1");
    private static final String CLIENT_ID = "client-id";
    private static final String COMPONENT_NAME = "component-name";

    @Nested
    class GivenDefaultTenantContextMapping {

        @Test
        void applyCreatesDistributedQueryBusForTenantContext() {
            // given
            AxonServerConnectionManager connectionManager = mock(AxonServerConnectionManager.class);
            AxonServerConnection connection = mock(AxonServerConnection.class);
            QueryChannel queryChannel = mock(QueryChannel.class);
            Registration registration = mock(Registration.class);
            MessageConverter messageConverter = mock(MessageConverter.class);
            AxonServerConfiguration serverConfiguration = new AxonServerConfiguration();
            serverConfiguration.setClientId(CLIENT_ID);
            serverConfiguration.setComponentName(COMPONENT_NAME);

            when(connectionManager.getConnection(TENANT.tenantId())).thenReturn(connection);
            when(connection.queryChannel()).thenReturn(queryChannel);
            when(queryChannel.registerQueryHandler(any(), any())).thenReturn(registration);
            when(registration.onAck(any(Runnable.class))).thenAnswer(invocation -> {
                Runnable callback = invocation.getArgument(0, Runnable.class);
                callback.run();
                return null;
            });

            AxonServerTenantQuerySegmentFactory testSubject = new AxonServerTenantQuerySegmentFactory(
                    connectionManager,
                    serverConfiguration,
                    messageConverter
            );

            // when
            DistributedQueryBus tenantQueryBus = (DistributedQueryBus) testSubject.apply(TENANT);
            QueryHandler handler = (query, context) -> MessageStream.<org.axonframework.messaging.queryhandling.QueryResponseMessage>fromIterable(
                    () -> List.of(response("ok")).iterator()
            );
            tenantQueryBus.subscribe(new QualifiedName("FindOrder"), handler);
            var result = tenantQueryBus.query(queryFor("FindOrder", "request"), null);

            // then
            assertThat(result.next()).isPresent()
                                     .get()
                                     .satisfies(entry -> assertThat(entry.message().payloadAs(String.class)).isEqualTo("ok"));

            verify(connectionManager).getConnection(TENANT.tenantId());
            verify(queryChannel).registerQueryHandler(any(), any());
        }
    }

    private static GenericQueryMessage queryFor(String queryName, String payload) {
        return new GenericQueryMessage(
                new GenericMessage(
                        "query-" + queryName,
                        new MessageType(queryName),
                        payload,
                        Map.of()
                )
        );
    }

    private static org.axonframework.messaging.queryhandling.QueryResponseMessage response(String payload) {
        return new GenericQueryResponseMessage(
                new GenericMessage(
                        "response-" + payload,
                        new MessageType(String.class),
                        payload,
                        Map.of()
                )
        );
    }
}
