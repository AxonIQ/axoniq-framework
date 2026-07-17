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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.axonserver.grpc.admin.ContextUpdateType;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.util.RecordingTenantAwareComponent;
import org.axonframework.common.Registration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AxonServerTenantProviderTest {

    @Mock
    private AxonServerConnectionManager connectionManager;

    @Mock
    private AxonServerConnection connection;

    @Mock
    private AdminChannel adminChannel;

    @Mock
    private ResultStream<ContextUpdate> contextUpdates;

    @Nested
    class ContextUpdates {

        @Test
        void recordsCreatedTenantAndRemovesDeletedTenant() {
            // given
            when(connectionManager.getConnection(ADMIN_CONTEXT)).thenReturn(connection);
            when(connection.adminChannel()).thenReturn(adminChannel);
            when(adminChannel.getAllContexts()).thenReturn(CompletableFuture.completedFuture(
                    List.of(
                            contextOverview(DEFAULT_CONTEXT),
                            contextOverview(ADMIN_CONTEXT)
                    )
            ));
            when(adminChannel.subscribeToContextUpdates()).thenReturn(contextUpdates);
            when(adminChannel.getContextOverview("foo")).thenReturn(CompletableFuture.completedFuture(
                    contextOverview("foo", Map.of("region", "eu-west"))
            ));

            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(
                    connectionManager,
                    tenant -> "foo".equals(tenant.tenantId())
            );
            RecordingTenantAwareComponent recorder = new RecordingTenantAwareComponent();
            testSubject.subscribe(recorder);

            // when
            testSubject.start()
                       .orTimeout(1, TimeUnit.SECONDS)
                       .join();

            // then
            assertThat(testSubject.tenants()).isEmpty();
            assertThat(recorder.tenants()).isEmpty();

            // when
            Runnable updateCallback = updateCallback();
            when(contextUpdates.nextIfAvailable()).thenReturn(contextCreated("foo"));
            updateCallback.run();

            // then
            assertThat(testSubject.tenants())
                    .extracting(TenantDescriptor::tenantId)
                    .containsExactly("foo");
            assertThat(recorder.tenants())
                    .extracting(TenantDescriptor::tenantId)
                    .containsExactly("foo");

            // when
            when(contextUpdates.nextIfAvailable()).thenReturn(contextDeleted("foo"));
            updateCallback.run();

            // then
            assertThat(testSubject.tenants()).isEmpty();
            assertThat(recorder.tenants()).isEmpty();
            verify(connectionManager).disconnect("foo");
        }

        private Runnable updateCallback() {
            ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
            verify(contextUpdates).onAvailable(callback.capture());
            return callback.getValue();
        }
    }

    @Nested
    class Subscriptions {

        @Test
        void cancellingASubscriptionDeregistersTenantsAddedAfterSubscribing() {
            // given a component subscribed before the tenant becomes known
            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
            RecordingTenantAwareComponent recorder = new RecordingTenantAwareComponent();
            Registration subscription = testSubject.subscribe(recorder);
            testSubject.addTenant(TenantDescriptor.tenantWithId("added-later"));
            assertThat(recorder.tenants())
                    .extracting(TenantDescriptor::tenantId)
                    .containsExactly("added-later");

            // when
            boolean cancelled = subscription.cancel();

            // then the registration made after subscribing is cancelled as well
            assertThat(cancelled).isTrue();
            assertThat(recorder.tenants()).isEmpty();
        }

        @Test
        void addingAKnownTenantAgainDoesNotRegisterItsComponentsTwice() {
            // given a subscribed component and a tenant that is already known
            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
            RecordingTenantAwareComponent recorder = new RecordingTenantAwareComponent();
            testSubject.subscribe(recorder);
            TenantDescriptor tenant = TenantDescriptor.tenantWithId("tenant-a");
            testSubject.addTenant(tenant);

            // when the same tenant is added again
            testSubject.addTenant(tenant);

            // then the component is registered for it only once
            assertThat(recorder.tenants()).containsExactly(tenant);
        }
    }

    @Nested
    class Shutdown {

        @Test
        void addingATenantAfterShutdownDoesNotRegisterIt() throws Exception {
            // given a subscribed component on a provider that has shut down
            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
            RecordingTenantAwareComponent recorder = new RecordingTenantAwareComponent();
            testSubject.subscribe(recorder);
            testSubject.shutdown().get(5, TimeUnit.SECONDS);

            // when a context update still in flight adds a tenant after shutdown
            testSubject.addTenant(TenantDescriptor.tenantWithId("added-after-shutdown"));

            // then the tenant is not registered, so its instance cannot escape the shutdown cleanup
            assertThat(recorder.tenants()).isEmpty();
        }
    }

    private static ContextOverview contextOverview(String contextName) {
        return contextOverview(contextName, Map.of());
    }

    private static ContextOverview contextOverview(String contextName,
                                                   Map<String, String> metadata) {
        return ContextOverview.newBuilder()
                              .setName(contextName)
                              .setReplicationGroup(
                                      ReplicationGroupOverview.newBuilder()
                                                              .setName("default-rg")
                                                              .build()
                              )
                              .putAllMetaData(metadata)
                              .build();
    }

    private static ContextUpdate contextCreated(String context) {
        return contextUpdate(context, ContextUpdateType.CREATED);
    }

    private static ContextUpdate contextDeleted(String context) {
        return contextUpdate(context, ContextUpdateType.DELETED);
    }

    private static ContextUpdate contextUpdate(String context,
                                               ContextUpdateType type) {
        return ContextUpdate.newBuilder()
                            .setContext(context)
                            .setType(type)
                            .build();
    }
}
