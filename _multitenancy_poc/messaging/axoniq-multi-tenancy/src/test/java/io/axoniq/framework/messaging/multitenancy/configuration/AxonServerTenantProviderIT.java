/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(60)
class AxonServerTenantProviderIT {

    private static final Logger logger = LoggerFactory.getLogger(AxonServerTenantProviderIT.class);
    private static final TenantDescriptor DEFAULT_TENANT = TenantDescriptor.tenantWithId("default");
    private static final AxonServerContainer AXON_SERVER_CONTAINER =
            new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:2025.2.0")
                    .withAxonServerHostname("localhost")
                    .withDevMode(true)
                    .withReuse(true);

    private static AxonServerConnectionManager connectionManager;

    @BeforeAll
    static void startAxonServer() {
        AXON_SERVER_CONTAINER.start();
        logger.info("Axon Server test container started on grpc port {}", AXON_SERVER_CONTAINER.getGrpcPort());

        AxonServerConfiguration axonServerConfiguration = new AxonServerConfiguration();
        axonServerConfiguration.setServers(
                AXON_SERVER_CONTAINER.getHost() + ":" + AXON_SERVER_CONTAINER.getGrpcPort()
        );

        connectionManager = AxonServerConnectionManager.builder()
                                                       .axonServerConfiguration(axonServerConfiguration)
                                                       .routingServers(axonServerConfiguration.getServers())
                                                       .build();
        connectionManager.start();
    }

    @AfterAll
    static void stopAxonServer() {
        if (connectionManager != null) {
            connectionManager.shutdown();
        }
        AXON_SERVER_CONTAINER.stop();
    }

    @Test
    void discoversDefaultTenantAndRegistersTenantAwareComponents() {
        AxonServerTenantProvider tenantProvider = new AxonServerTenantProvider(
                connectionManager,
                tenant -> "default".equals(tenant.tenantId())
        );
        RecordingTenantAwareComponent component = new RecordingTenantAwareComponent();

        tenantProvider.subscribe(component);
        tenantProvider.start().join();

        assertThat(tenantProvider.getTenants())
                .extracting(TenantDescriptor::tenantId)
                .containsExactly("default");
        assertThat(component.registeredTenants())
                .extracting(TenantDescriptor::tenantId)
                .containsExactly("default");
    }

    private static final class RecordingTenantAwareComponent implements MultiTenantAwareComponent {

        private final List<TenantDescriptor> registeredTenants = new CopyOnWriteArrayList<>();

        @Override
        public @NonNull Registration registerTenant(@NonNull TenantDescriptor tenantDescriptor) {
            registeredTenants.add(tenantDescriptor);
            return () -> registeredTenants.remove(tenantDescriptor);
        }

        @Override
        public @NonNull Registration registerAndStartTenant(@NonNull TenantDescriptor tenantDescriptor) {
            return registerTenant(tenantDescriptor);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("tenantCount", registeredTenants.size());
        }

        private List<TenantDescriptor> registeredTenants() {
            return List.copyOf(registeredTenants);
        }
    }
}
