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
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class TenantScopedCacheTest {

    // Each create returns a distinct value, and per-tenant creation counts are recorded, so caching and eviction
    // are observable through both instance identity and how often the factory ran.
    private final ConcurrentHashMap<TenantDescriptor, AtomicInteger> creations = new ConcurrentHashMap<>();
    private final Function<TenantDescriptor, String> countingFactory = tenant -> {
        int creation = creations.computeIfAbsent(tenant, ignored -> new AtomicInteger()).incrementAndGet();
        return tenant.tenantId() + "#" + creation;
    };
    private final TenantScopedCache<String> testSubject = new TenantScopedCache<>(countingFactory, "a cache");

    @Nested
    class Creation {

        @Test
        void appliesTheFactoryOncePerTenantAndCachesTheResult() {
            testSubject.registerTenant(TENANT_A);

            String first = testSubject.componentFor(TENANT_A);
            String second = testSubject.componentFor(TENANT_A);

            assertThat(second).isSameAs(first);
            assertThat(creations.get(TENANT_A)).hasValue(1);
        }

        @Test
        void buildsADistinctComponentPerTenant() {
            testSubject.registerTenant(TENANT_A);
            testSubject.registerTenant(TENANT_B);

            assertThat(testSubject.componentFor(TENANT_A)).isNotEqualTo(testSubject.componentFor(TENANT_B));
        }

        @Test
        void rejectsANullFactory() {
            assertThatThrownBy(() -> new TenantScopedCache<String>(null, "a cache"))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The component factory must not be null");
        }

        @Test
        void rejectsANullOwner() {
            assertThatThrownBy(() -> new TenantScopedCache<>(countingFactory, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The owner must not be null");
        }

        // Registering a tenant only records it. A deployment with many tenants must not pay for a component, and the
        // connection behind it, for a tenant no message has arrived for yet.
        @Test
        void registeringATenantBuildsNothingUntilItsComponentIsRequested() {
            testSubject.registerTenant(TENANT_A);
            testSubject.registerAndStartTenant(TENANT_B);

            assertThat(creations).isEmpty();

            testSubject.componentFor(TENANT_A);

            assertThat(creations).containsOnlyKeys(TENANT_A);
        }

        @Test
        void rejectsATenantThatWasNeverRegistered() {
            assertThatThrownBy(() -> testSubject.componentFor(TENANT_A))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessageContaining(TENANT_A.tenantId());
            assertThat(creations).isEmpty();
        }

        @Test
        void rejectsAFactoryThatReturnsNull() {
            TenantScopedCache<String> nullBuilding = new TenantScopedCache<>(tenant -> null, "a cache");
            nullBuilding.registerTenant(TENANT_A);

            assertThatThrownBy(() -> nullBuilding.componentFor(TENANT_A))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining(TENANT_A.tenantId());
        }

        // A tenant's component is built against its Axon Server connection, so creation can fail transiently. Nothing
        // may be cached in that case, or the tenant would keep serving the failure after the cause is gone.
        @Test
        void aFailedCreationCachesNothingAndIsRetriedOnTheNextAccess() {
            AtomicInteger attempts = new AtomicInteger();
            TenantScopedCache<String> failingOnce = new TenantScopedCache<>(tenant -> {
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("no connection");
                }
                return tenant.tenantId();
            }, "a cache");
            failingOnce.registerTenant(TENANT_A);

            assertThatThrownBy(() -> failingOnce.componentFor(TENANT_A)).isInstanceOf(IllegalStateException.class);

            assertThat(failingOnce.componentFor(TENANT_A)).isEqualTo(TENANT_A.tenantId());
            assertThat(attempts).hasValue(2);
        }

        // Every tenant's first message may arrive on many threads at once, and each component owns a connection, so
        // exactly one may be built.
        @Test
        void concurrentFirstAccessesShareTheOneComponentTheFactoryBuilt() throws Exception {
            int threads = 8;
            testSubject.registerTenant(TENANT_A);
            CountDownLatch allReady = new CountDownLatch(threads);
            CountDownLatch startTogether = new CountDownLatch(1);

            try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
                List<Future<String>> accesses = IntStream.range(0, threads)
                                                        .mapToObj(index -> executor.submit(() -> {
                                                            allReady.countDown();
                                                            startTogether.await();
                                                            return testSubject.componentFor(TENANT_A);
                                                        }))
                                                        .toList();
                assertThat(allReady.await(5, TimeUnit.SECONDS)).isTrue();
                startTogether.countDown();

                assertThat(accesses.stream().map(TenantScopedCacheTest::componentOf).distinct()).hasSize(1);
            }
            assertThat(creations.get(TENANT_A)).hasValue(1);
        }
    }

    @Nested
    class Eviction {

        @Test
        void cancellingATenantRegistrationEvictsItsCachedComponent() {
            Registration registration = testSubject.registerTenant(TENANT_A);
            String before = testSubject.componentFor(TENANT_A);

            assertThat(registration.cancel()).isTrue();

            testSubject.registerTenant(TENANT_A);
            assertThat(testSubject.componentFor(TENANT_A)).isNotEqualTo(before);
            assertThat(creations.get(TENANT_A)).hasValue(2);
        }

        @Test
        void registerAndStartTenantEvictsOnCancelAsWell() {
            Registration registration = testSubject.registerAndStartTenant(TENANT_A);
            testSubject.componentFor(TENANT_A);

            assertThat(registration.cancel()).isTrue();

            testSubject.registerAndStartTenant(TENANT_A);
            assertThat(testSubject.componentFor(TENANT_A)).isEqualTo(TENANT_A.tenantId() + "#2");
        }

        // Nothing may rebuild a component for a removed tenant. The registration that would evict it again is already
        // cancelled, so the component, and the connection behind it, would be held for a context that is gone.
        @Test
        void aRemovedTenantGetsNoComponentInsteadOfAFreshOne() {
            Registration registration = testSubject.registerTenant(TENANT_A);
            testSubject.componentFor(TENANT_A);
            registration.cancel();

            assertThatThrownBy(() -> testSubject.componentFor(TENANT_A))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessageContaining(TENANT_A.tenantId());
            assertThat(creations.get(TENANT_A)).hasValue(1);
        }

        @Test
        void cancellingWithoutACachedComponentStillReportsTheRegistrationWasCancelled() {
            Registration registration = testSubject.registerTenant(TENANT_A);

            assertThat(registration.cancel()).isTrue();

            assertThat(creations).isEmpty();
        }

        @Test
        void cancellingTwiceReportsNothingCancelledTheSecondTime() {
            Registration registration = testSubject.registerTenant(TENANT_A);
            registration.cancel();

            assertThat(registration.cancel()).isFalse();
        }

        // Re-registering supersedes, so the superseded registration's component is handed over there and then: its own
        // Registration may never be cancelled, leaving nothing else to reclaim it.
        private final List<String> evicted = new CopyOnWriteArrayList<>();
        private final TenantScopedCache<String> evictionRecording =
                new TenantScopedCache<>(countingFactory, (tenant, component) -> evicted.add(component), "a cache");

        @Test
        void reRegisteringATenantEvictsTheSupersededComponent() {
            evictionRecording.registerTenant(TENANT_A);
            String supersededComponent = evictionRecording.componentFor(TENANT_A);

            evictionRecording.registerTenant(TENANT_A);

            assertThat(evicted).containsExactly(supersededComponent);
            assertThat(evictionRecording.componentFor(TENANT_A)).isNotEqualTo(supersededComponent);
        }

        // Whoever owns the component's lifecycle gets it back on eviction, so it can be released.
        @Test
        void cancellingATenantRegistrationHandsTheComponentToTheEvictionCallback() {
            Registration registration = evictionRecording.registerTenant(TENANT_A);
            String component = evictionRecording.componentFor(TENANT_A);

            assertThat(registration.cancel()).isTrue();

            assertThat(evicted).containsExactly(component);
        }

        @Test
        void cancellingATenantRegistrationWithoutAComponentEvictsNothing() {
            Registration registration = evictionRecording.registerTenant(TENANT_A);

            assertThat(registration.cancel()).isTrue();

            assertThat(evicted).isEmpty();
        }

        // A component created while its tenant is being unregistered must not outlive that registration. Whichever of
        // the two threads removes it hands it over, and only one of them can, so it is released exactly once.
        @Test
        void evictsAComponentCreatedWhileItsTenantIsBeingUnregistered() throws Exception {
            AtomicReference<TenantScopedCache<String>> cacheRef = new AtomicReference<>();
            AtomicReference<Registration> registration = new AtomicReference<>();
            AtomicReference<Thread> unregistration = new AtomicReference<>();
            TenantScopedCache<String> cache = new TenantScopedCache<>(
                    tenant -> {
                        Thread cancelling = new Thread(() -> registration.get().cancel());
                        unregistration.set(cancelling);
                        cancelling.start();
                        // Waits for the registration to be gone, not for the whole cancellation, which cannot finish
                        // while this creation holds the cache entry being written.
                        await().atMost(Duration.ofSeconds(5))
                               .until(() -> !cacheRef.get().tenants().contains(tenant));
                        return tenant.tenantId() + "-created-late";
                    },
                    (tenant, component) -> evicted.add(component),
                    "a cache");
            cacheRef.set(cache);
            registration.set(cache.registerTenant(TENANT_A));

            assertThatThrownBy(() -> cache.componentFor(TENANT_A))
                    .isInstanceOf(TenantNotResolvedException.class);

            unregistration.get().join(TimeUnit.SECONDS.toMillis(5));
            assertThat(unregistration.get().isAlive()).isFalse();
            assertThat(evicted).containsExactly(TENANT_A.tenantId() + "-created-late");
            assertThat(cache.tenants()).isEmpty();
        }

        // Tenant removals reach a component through retained registrations, so a stale one can be cancelled after its
        // tenant was re-added. That cancellation must not take the newer registration's component with it.
        @Test
        void aStaleCancellationLeavesANewerRegistrationUntouched() {
            Registration stale = testSubject.registerTenant(TENANT_A);
            testSubject.registerTenant(TENANT_A);
            String current = testSubject.componentFor(TENANT_A);

            assertThat(stale.cancel()).isFalse();

            assertThat(testSubject.componentFor(TENANT_A)).isSameAs(current);
            assertThat(creations.get(TENANT_A)).hasValue(1);
        }
    }

    @Test
    void describesItsRegisteredTenants() {
        testSubject.registerTenant(TENANT_A);
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties()).containsEntry("tenants", List.of(TENANT_A));
    }

    private static String componentOf(Future<String> access) {
        try {
            return access.get(5, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException("Concurrent component access failed", failure);
        }
    }
}
