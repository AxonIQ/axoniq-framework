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

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.axonserver.api.AxonServerTenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.axonserver.api.AxonServerTenantProvider;
import io.axoniq.framework.messaging.multitenancy.util.RecordingTenantAwareComponent;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DefaultAxonApplication;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor.tenantWithId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
@Timeout(60)
class AxonServerTenantProviderIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();

    private AxonServerTestInfrastructure.ContextManager contextManager;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();

        assertThat(contextManager.getContexts()).containsExactlyInAnyOrder(DEFAULT_CONTEXT, ADMIN_CONTEXT);
    }

    @AfterEach
    void tearDown() {
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void axonServerTenantProviderIsConfiguredViaEnhancer() {
        RecordingTenantAwareComponent tenantDescriptorRecorder = new RecordingTenantAwareComponent();

        AxonConfiguration application = new DefaultAxonApplication()
                .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                .componentRegistry(cr -> cr.registerComponent(TenantResolver.class,
                                                              c -> new MetadataBasedTenantResolver()))
                .start();

        AxonServerTenantProvider tenantProvider = (AxonServerTenantProvider) application.getComponent(TenantProvider.class);


        await().untilAsserted(
                () -> assertThat(tenantProvider.tenants())
                        .extracting(TenantDescriptor::tenantId)
                        .containsExactly(DEFAULT_CONTEXT)
        );

        tenantProvider.subscribe(tenantDescriptorRecorder);

        await().untilAsserted(
                () -> assertThat(tenantDescriptorRecorder.tenants())
                        .extracting(TenantDescriptor::tenantId)
                        .containsExactly(DEFAULT_CONTEXT)
        );
    }

    @Test
    void dynamicallyAddTenantViaNewServerContext() {
        RecordingTenantAwareComponent tenantDescriptorRecorder = new RecordingTenantAwareComponent();

        TenantConnectPredicate predicate = tenantDescriptor -> new AxonServerTenantConnectPredicate()
                .and(it -> !DEFAULT_CONTEXT.equals(it.tenantId()))
                .test(tenantDescriptor);

        AxonConfiguration application = new DefaultAxonApplication()
                .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                .componentRegistry(cr -> cr.registerComponent(TenantResolver.class,
                                                              c -> new MetadataBasedTenantResolver()))
                .componentRegistry(cr -> cr.registerComponent(TenantConnectPredicate.class, c -> predicate))
                .start();

        AxonServerTenantProvider tenantProvider = (AxonServerTenantProvider) application.getComponent(TenantProvider.class);

        // the default predicate must have been replaced
        assertThat(application.getComponent(TenantConnectPredicate.class)).rejects(tenantWithId(DEFAULT_CONTEXT));

        // empty because both existing contexts are filtered out
        await().untilAsserted(() -> assertThat(tenantProvider.tenants()).isEmpty());
        tenantProvider.subscribe(tenantDescriptorRecorder);

        // subscribe to updates did not work unless the resultStream is held
        // as property
        contextManager.createContext("foo");
        await().untilAsserted(() -> assertThat(tenantProvider.tenants()).isNotEmpty());
        await().untilAsserted(() -> assertThat(tenantDescriptorRecorder.tenants())
                .extracting(TenantDescriptor::tenantId)
                .containsExactly("foo")
        );

        // unsubscribe on remove of context
        contextManager.deleteContext("foo");
        await().untilAsserted(() -> assertThat(tenantProvider.tenants()).isEmpty());
        await().untilAsserted(() -> assertThat(tenantDescriptorRecorder.tenants()).isEmpty());
    }
}
