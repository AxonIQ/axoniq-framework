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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.Configuration;
import org.junit.jupiter.api.*;

import java.util.Map;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies {@link TenantComponentProviderSubscriber} subscribes every {@link TenantComponentProvider} to the
 * {@link TenantProvider}, keeps them in sync with the tenant lifecycle, and cancels their registrations again.
 */
class TenantComponentProviderSubscriberTest {

    private final StubTenantProvider tenantProvider = new StubTenantProvider();
    private final TenantComponentProvider<CourseRepository> componentProvider =
            TenantComponentProvider.withFactory(CourseRepository.class, CourseRepository::new);

    private TenantComponentProviderSubscriber testSubject;

    @BeforeEach
    void setUp() {
        tenantProvider.addTenant(TENANT_A);
        Configuration configuration = mock(Configuration.class);
        when(configuration.getComponent(TenantProvider.class)).thenReturn(tenantProvider);
        when(configuration.getComponents(TenantComponentProvider.class))
                .thenReturn(Map.of("componentProvider", componentProvider));
        testSubject = new TenantComponentProviderSubscriber(configuration);
    }

    @Test
    void subscribeProvidersSubscribesEveryProviderAndReplaysKnownTenants() {
        // when
        testSubject.subscribeProviders();

        // then
        assertThat(tenantProvider.subscribedComponents()).contains(componentProvider);
        assertThat(componentProvider.tenants()).containsExactly(TENANT_A);
    }

    @Test
    void propagatesTenantsAddedAtRuntimeToTheProvider() {
        // given
        testSubject.subscribeProviders();

        // when
        tenantProvider.addTenant(TENANT_B);

        // then
        assertThat(componentProvider.tenants()).containsExactlyInAnyOrder(TENANT_A, TENANT_B);
    }

    @Test
    void removingATenantDestroysItsComponentInstance() {
        // given
        testSubject.subscribeProviders();
        tenantProvider.addTenant(TENANT_B);
        CourseRepository repository = componentProvider.componentFor(TENANT_B);

        // when
        tenantProvider.removeTenant(TENANT_B);

        // then
        assertThat(componentProvider.tenants()).containsExactly(TENANT_A);
        assertThat(repository.closed).isTrue();
    }

    @Test
    void tenantProviderShutdownDestroysAllComponentInstancesWithoutCancelSubscriptionsBeingCalled() {
        // given
        testSubject.subscribeProviders();
        CourseRepository repository = componentProvider.componentFor(TENANT_A);

        // when the tenant provider shuts down on its own, deregistering all subscribed components
        tenantProvider.shutdown();

        // then
        assertThat(repository.closed).isTrue();
        assertThat(componentProvider.tenants()).isEmpty();
    }

    @Test
    void cancelSubscriptionsDestroysAllComponentInstances() {
        // given
        testSubject.subscribeProviders();
        tenantProvider.addTenant(TENANT_B);
        CourseRepository repositoryA = componentProvider.componentFor(TENANT_A);
        CourseRepository repositoryB = componentProvider.componentFor(TENANT_B);

        // when
        testSubject.cancelSubscriptions();

        // then
        assertThat(repositoryA.closed).isTrue();
        assertThat(repositoryB.closed).isTrue();
        assertThat(componentProvider.tenants()).isEmpty();
    }

    private static final class CourseRepository implements AutoCloseable {

        private final TenantDescriptor tenant;
        private boolean closed;

        private CourseRepository(TenantDescriptor tenant) {
            this.tenant = tenant;
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
