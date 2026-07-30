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

package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.common.Registration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class TenantComponentProviderTest {

    private final RecordingFactory factory = new RecordingFactory();
    private final TenantComponentProvider<TestComponent> testSubject =
            TenantComponentProvider.withFactory(TestComponent.class, factory);

    @Test
    @SuppressWarnings("DataFlowIssue")
    void withFactoryRejectsNullArguments() {
        // when / then
        assertThatThrownBy(() -> TenantComponentProvider.withFactory(null, factory))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> TenantComponentProvider.withFactory(TestComponent.class, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void reportsConfiguredComponentType() {
        // when
        Class<TestComponent> componentType = testSubject.componentType();

        // then
        assertThat(componentType).isEqualTo(TestComponent.class);
    }

    @Test
    void describesComponentTypeAndTenants() {
        // given
        testSubject.registerTenant(TENANT_A);
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        // when
        testSubject.describeTo(descriptor);

        // then
        assertThat(descriptor.<String>getProperty("componentType")).isEqualTo(TestComponent.class.getName());
        assertThat(descriptor.<List<TenantDescriptor>>getProperty("tenants")).containsExactly(TENANT_A);
    }

    @Nested
    class ComponentFor {

        @Test
        void createsComponentLazilyOnFirstAccessAndCachesIt() {
            // given
            testSubject.registerTenant(TENANT_A);

            // when
            TestComponent first = testSubject.componentFor(TENANT_A);
            TestComponent second = testSubject.componentFor(TENANT_A);

            // then
            assertThat(first).isSameAs(second);
            assertThat(factory.createCount(TENANT_A)).isEqualTo(1);
        }

        @Test
        void rejectsUnregisteredTenant() {
            // when / then
            assertThatThrownBy(() -> testSubject.componentFor(TENANT_A))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessageContaining(TENANT_A.tenantId())
                    .hasMessageContaining(TestComponent.class.getName());
            assertThat(factory.createCount(TENANT_A)).isZero();
        }

        @Test
        void createsSeparateInstancePerTenant() {
            // given
            testSubject.registerTenant(TENANT_A);
            testSubject.registerTenant(TENANT_B);

            // when
            TestComponent componentA = testSubject.componentFor(TENANT_A);
            TestComponent componentB = testSubject.componentFor(TENANT_B);

            // then
            assertThat(componentA).isNotSameAs(componentB);
            assertThat(componentA.tenant()).isEqualTo(TENANT_A);
            assertThat(componentB.tenant()).isEqualTo(TENANT_B);
        }
    }

    @Nested
    class TenantLifecycle {

        @Test
        void exposesRegisteredTenants() {
            // when
            testSubject.registerTenant(TENANT_A);
            testSubject.registerTenant(TENANT_B);

            // then
            assertThat(testSubject.tenants()).containsExactlyInAnyOrder(TENANT_A, TENANT_B);
        }

        @Test
        void unregisteringTenantRemovesTenantAndDestroysCreatedComponent() {
            // given
            Registration registration = testSubject.registerTenant(TENANT_A);
            TestComponent component = testSubject.componentFor(TENANT_A);

            // when
            registration.cancel();

            // then
            assertThat(testSubject.tenants()).doesNotContain(TENANT_A);
            assertThat(factory.destroyed()).containsExactly(component);
            // the tenant is unknown again, so a subsequent access is rejected
            assertThatThrownBy(() -> testSubject.componentFor(TENANT_A))
                    .isInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void reRegisteringATenantSupersedesThePreviousRegistration() {
            // given an instance created under the first registration
            Registration staleRegistration = testSubject.registerTenant(TENANT_A);
            TestComponent first = testSubject.componentFor(TENANT_A);

            // when the tenant is registered again
            testSubject.registerTenant(TENANT_A);
            TestComponent second = testSubject.componentFor(TENANT_A);

            // then the newer registration serves its own instance
            assertThat(second).isNotSameAs(first);

            // and the stale cancel destroys only the instance it created, leaving the newer registration untouched
            boolean cancelled = staleRegistration.cancel();
            assertThat(cancelled).isFalse();
            assertThat(testSubject.tenants()).contains(TENANT_A);
            assertThat(testSubject.componentFor(TENANT_A)).isSameAs(second);
            assertThat(factory.destroyed()).containsExactly(first);
        }

        @Test
        void cancellingARegistrationTwiceDoesNotAffectAReRegisteredTenant() {
            // given a stale registration whose tenant was re-registered after the first cancel
            Registration staleRegistration = testSubject.registerTenant(TENANT_A);
            testSubject.componentFor(TENANT_A);
            staleRegistration.cancel();
            testSubject.registerTenant(TENANT_A);
            TestComponent recreated = testSubject.componentFor(TENANT_A);

            // when
            boolean secondCancel = staleRegistration.cancel();

            // then the second cancel is a no-op, so the re-registered tenant keeps its live component
            assertThat(secondCancel).isFalse();
            assertThat(testSubject.tenants()).contains(TENANT_A);
            assertThat(testSubject.componentFor(TENANT_A)).isSameAs(recreated);
            assertThat(factory.destroyed()).hasSize(1);
        }

        // The superseded registration's instance is reachable through neither the provider nor its own cancellation
        // once that Registration is dropped, so it has to be destroyed there and then.
        @Test
        void reRegisteringATenantDestroysTheSupersededInstance() {
            // given an instance created under the first registration
            testSubject.registerTenant(TENANT_A);
            TestComponent superseded = testSubject.componentFor(TENANT_A);

            // when the tenant is registered again
            testSubject.registerTenant(TENANT_A);

            // then the superseded instance is destroyed, and the tenant serves a fresh one
            assertThat(factory.destroyed()).containsExactly(superseded);
            assertThat(testSubject.componentFor(TENANT_A)).isNotSameAs(superseded);
        }

        // Every tenant's first message may arrive on many threads at once, and each instance may own a resource, so
        // exactly one may be built and none may be destroyed.
        @Test
        void concurrentFirstAccessesShareTheOneInstanceTheFactoryBuilt() throws Exception {
            int threads = 8;
            testSubject.registerTenant(TENANT_A);
            CountDownLatch allReady = new CountDownLatch(threads);
            CountDownLatch startTogether = new CountDownLatch(1);

            try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
                List<Future<TestComponent>> accesses = IntStream.range(0, threads)
                                                               .mapToObj(index -> executor.submit(() -> {
                                                                   allReady.countDown();
                                                                   startTogether.await();
                                                                   return testSubject.componentFor(TENANT_A);
                                                               }))
                                                               .toList();
                assertThat(allReady.await(5, TimeUnit.SECONDS)).isTrue();
                startTogether.countDown();

                List<TestComponent> components = accesses.stream()
                                                         .map(TenantComponentProviderTest::instanceOf)
                                                         .toList();
                // Compared by identity: every TestComponent of one tenant is equal to every other, so equality would
                // hold even if each thread got its own instance.
                TestComponent sharedComponent = components.getFirst();
                assertThat(components).allSatisfy(component -> assertThat(component).isSameAs(sharedComponent));
            }
            assertThat(factory.createCount(TENANT_A)).isEqualTo(1);
            assertThat(factory.destroyed()).isEmpty();
        }

        @Test
        void unregisteringTenantWithoutCreatedComponentDestroysNothing() {
            // given
            Registration registration = testSubject.registerTenant(TENANT_A);

            // when
            registration.cancel();

            // then
            assertThat(factory.destroyed()).isEmpty();
        }

        @Test
        void destroysInsteadOfLeaksAnInstanceCreatedWhileItsTenantIsBeingUnregistered() throws Exception {
            // given a factory whose creation overlaps with the tenant being unregistered on another thread,
            // as happens when a tenant is removed while a message for it is being handled
            AtomicReference<TenantComponentProvider<TestComponent>> providerRef = new AtomicReference<>();
            AtomicReference<Registration> registration = new AtomicReference<>();
            AtomicReference<Thread> unregistration = new AtomicReference<>();
            List<TestComponent> destroyed = new CopyOnWriteArrayList<>();
            TenantComponentProvider<TestComponent> provider = TenantComponentProvider.withFactory(
                    TestComponent.class,
                    new TenantComponentFactory<>() {
                        @Override
                        public TestComponent create(TenantDescriptor tenant) {
                            Thread cancelling = new Thread(() -> registration.get().cancel());
                            unregistration.set(cancelling);
                            cancelling.start();
                            await().atMost(Duration.ofSeconds(5))
                                   .until(() -> !providerRef.get().tenants().contains(tenant));
                            return new TestComponent(tenant);
                        }

                        @Override
                        public void destroy(TenantDescriptor tenant, TestComponent component) {
                            destroyed.add(component);
                        }
                    }
            );
            providerRef.set(provider);
            registration.set(provider.registerTenant(TENANT_A));

            // when / then the freshly created instance is destroyed rather than cached past its tenant
            assertThatThrownBy(() -> provider.componentFor(TENANT_A))
                    .isInstanceOf(TenantNotResolvedException.class);
            unregistration.get().join(TimeUnit.SECONDS.toMillis(5));
            assertThat(unregistration.get().isAlive()).isFalse();
            assertThat(destroyed).hasSize(1);
            assertThat(provider.tenants()).isEmpty();
        }
    }

    private record TestComponent(TenantDescriptor tenant) {

    }

    private static TestComponent instanceOf(Future<TestComponent> access) {
        try {
            return access.get(5, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException("Concurrent instance access failed", failure);
        }
    }

    private static final class RecordingFactory implements TenantComponentFactory<TestComponent> {

        private final Map<TenantDescriptor, Integer> createCounts = new ConcurrentHashMap<>();
        private final List<TestComponent> destroyed = new CopyOnWriteArrayList<>();

        @Override
        public TestComponent create(TenantDescriptor tenant) {
            createCounts.merge(tenant, 1, Integer::sum);
            return new TestComponent(tenant);
        }

        @Override
        public void destroy(TenantDescriptor tenant, TestComponent component) {
            destroyed.add(component);
        }

        private int createCount(TenantDescriptor tenant) {
            return createCounts.getOrDefault(tenant, 0);
        }

        private List<TestComponent> destroyed() {
            return destroyed;
        }
    }
}
