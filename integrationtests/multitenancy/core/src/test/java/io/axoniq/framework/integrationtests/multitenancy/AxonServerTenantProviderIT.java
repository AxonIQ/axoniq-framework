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
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.api.AxonServerTenantProvider;
import io.axoniq.framework.messaging.multitenancy.configuration.StaticTenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.util.RecordingTenantAwareComponent;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DefaultAxonApplication;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;

import java.util.Properties;
import java.util.Set;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor.tenantWithId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
@Timeout(60)
class AxonServerTenantProviderIT {

    private static final String DYNAMIC_CONTEXT = "tenant-provider-dynamic";
    private static final String STATIC_TENANT_A = "tenant-provider-static-a";
    private static final String STATIC_TENANT_B = "tenant-provider-static-b";
    private static final Set<String> OWN_CONTEXTS = Set.of(DYNAMIC_CONTEXT, STATIC_TENANT_A, STATIC_TENANT_B);

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    private AxonServerTestInfrastructure.ContextManager contextManager;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        contextManager = INFRASTRUCTURE.getContextManager();

        // Sanity check only: the container is shared build-wide, so other suites' own contexts may also be
        // present. This class's own contexts must not be, though -- their presence here would mean a previous
        // run of this class crashed before tearDown() could clean up.
        assertThat(contextManager.getContexts())
                .contains(DEFAULT_CONTEXT, ADMIN_CONTEXT)
                .doesNotContainAnyElementsOf(OWN_CONTEXTS);
    }

    @AfterEach
    void tearDown() {
        // Only ever delete contexts this class itself may have created, never the shared container's other state.
        contextManager.getContexts().stream().filter(OWN_CONTEXTS::contains).forEach(contextManager::deleteContext);
        INFRASTRUCTURE.stop();
    }

    @Test
    void axonServerTenantProviderIsConfiguredViaEnhancer() {
        RecordingTenantAwareComponent tenantDescriptorRecorder = new RecordingTenantAwareComponent();

        AxonConfiguration application = new DefaultAxonApplication()
                .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                .start();

        AxonServerTenantProvider tenantProvider =
                (AxonServerTenantProvider) application.getComponent(TenantProvider.class);


        // The default predicate connects every non-admin context, so only assert what it must include/exclude --
        // not the exhaustive set, since the shared container may carry other suites' contexts too.
        await().untilAsserted(
                () -> assertThat(tenantProvider.tenants())
                        .extracting(TenantDescriptor::tenantId)
                        .contains(DEFAULT_CONTEXT)
                        .doesNotContain(ADMIN_CONTEXT)
        );

        tenantProvider.subscribe(tenantDescriptorRecorder);

        await().untilAsserted(
                () -> assertThat(tenantDescriptorRecorder.tenants())
                        .extracting(TenantDescriptor::tenantId)
                        .contains(DEFAULT_CONTEXT)
        );
    }

    @Test
    void dynamicallyAddTenantViaNewServerContext() {
        RecordingTenantAwareComponent tenantDescriptorRecorder = new RecordingTenantAwareComponent();

        // Scoped to this test's own context name, so it is unaffected by whatever else is on the shared container.
        TenantConnectPredicate predicate = tenantDescriptor -> DYNAMIC_CONTEXT.equals(tenantDescriptor.tenantId());

        AxonConfiguration application = new DefaultAxonApplication()
                .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                .componentRegistry(cr -> cr.registerComponent(TenantConnectPredicate.class, c -> predicate))
                .start();

        AxonServerTenantProvider tenantProvider =
                (AxonServerTenantProvider) application.getComponent(TenantProvider.class);

        // the default predicate must have been replaced
        assertThat(application.getComponent(TenantConnectPredicate.class)).rejects(tenantWithId(DEFAULT_CONTEXT));

        // empty because the predicate only accepts DYNAMIC_CONTEXT, which doesn't exist yet
        await().untilAsserted(() -> assertThat(tenantProvider.tenants()).isEmpty());
        tenantProvider.subscribe(tenantDescriptorRecorder);

        // subscribe to updates did not work unless the resultStream is held
        // as property
        contextManager.createContext(DYNAMIC_CONTEXT);
        await().untilAsserted(() -> assertThat(tenantProvider.tenants()).isNotEmpty());
        await().untilAsserted(() -> assertThat(tenantDescriptorRecorder.tenants())
                .extracting(TenantDescriptor::tenantId)
                .containsExactly(DYNAMIC_CONTEXT)
        );

        // unsubscribe on remove of context
        contextManager.deleteContext(DYNAMIC_CONTEXT);
        await().untilAsserted(() -> assertThat(tenantProvider.tenants()).isEmpty());
        await().untilAsserted(() -> assertThat(tenantDescriptorRecorder.tenants()).isEmpty());
    }

    @Test
    void connectsOnlyTenantsConfiguredThroughStaticTenantProperty() {
        // given Axon Server contexts and a properties-backed static tenant predicate
        contextManager.createContext(STATIC_TENANT_A);
        contextManager.createContext(STATIC_TENANT_B);
        Properties properties = new Properties();
        properties.setProperty(StaticTenantConnectPredicate.TENANTS_PROPERTY, STATIC_TENANT_A);

        // when starting an application with the static tenant predicate
        AxonConfiguration application = new DefaultAxonApplication()
                .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                .componentRegistry(registry -> registry.registerComponent(
                        TenantConnectPredicate.class,
                        config -> StaticTenantConnectPredicate.from(
                                properties.getProperty(StaticTenantConnectPredicate.TENANTS_PROPERTY))))
                .start();
        try {
            AxonServerTenantProvider tenantProvider =
                    (AxonServerTenantProvider) application.getComponent(TenantProvider.class);

            // then only the configured context becomes a tenant
            await().untilAsserted(() -> assertThat(tenantProvider.tenants())
                    .extracting(TenantDescriptor::tenantId)
                    .containsExactly(STATIC_TENANT_A));
        } finally {
            application.shutdown();
        }
    }
}
