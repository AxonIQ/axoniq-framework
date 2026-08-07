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

import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingStreamingEventProcessor;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests that {@link MultiTenantStreamingProcessorRestarter} restarts running streaming event processors on tenant
 * changes, coalesces bursts, isolates a failing processor from the rest, and stops cleanly.
 *
 * @author Laura Devriendt
 */
class MultiTenantStreamingProcessorRestarterTest {

    private final StubTenantProvider tenantProvider = new StubTenantProvider();
    private final InMemoryEventStorageEngine tenantEngine = new InMemoryEventStorageEngine();
    private final MultiTenantEventStorageEngine routingEngine = newRoutingEngine();
    private final RecordingStreamingEventProcessor runningProcessor =
            new RecordingStreamingEventProcessor("running-processor", true);
    private final RecordingStreamingEventProcessor stoppedProcessor =
            new RecordingStreamingEventProcessor("stopped-processor", false);

    private AxonConfiguration configuration;
    private MultiTenantStreamingProcessorRestarter testSubject;

    @BeforeEach
    void setUp() {
        configuration = configurationWith(registry -> registry
                .registerComponent(StreamingEventProcessor.class, runningProcessor.name(), config -> runningProcessor)
                .registerComponent(StreamingEventProcessor.class, stoppedProcessor.name(), config -> stoppedProcessor));
        configuration.start();
        testSubject = new MultiTenantStreamingProcessorRestarter(configuration);
        testSubject.follow(routingEngine);
    }

    @AfterEach
    void tearDown() {
        testSubject.stop();
        configuration.shutdown();
    }

    private AxonConfiguration configurationWith(Consumer<ComponentRegistry> components) {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> {
                                      registry.disableEnhancerScanning()
                                              .registerComponent(TenantProvider.class, config -> tenantProvider)
                                              .registerComponent(
                                                  MultiTenantStreamingProcessorRestartConfiguration.class,
                                                  config -> MultiTenantStreamingProcessorRestartConfiguration.DEFAULT);
                                      components.accept(registry);
                                  })
                                  .build();
    }

    private static long restartCount(MultiTenantStreamingProcessorRestarter restarter) {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();
        restarter.describeTo(descriptor);
        return (long) descriptor.getDescribedProperties().get("restartCount");
    }

    @Nested
    class RestartTriggering {

        @Test
        void restartsRunningProcessorWhenTenantAddedAtRuntime() {
            testSubject.start();

            routingEngine.registerTenant(TENANT_A);

            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(runningProcessor.shutdownCount()).isGreaterThanOrEqualTo(1);
                assertThat(runningProcessor.startCount()).isGreaterThanOrEqualTo(1);
            });
            assertThat(runningProcessor.isRunning()).isTrue();
        }

        @Test
        void doesNotRestartAProcessorThatIsNotRunning() {
            testSubject.start();

            routingEngine.registerTenant(TENANT_A);

            // the running processor is restarted, proving the restart cycle ran and iterated all processors
            await().atMost(Duration.ofSeconds(2))
                   .untilAsserted(() -> assertThat(runningProcessor.startCount()).isGreaterThanOrEqualTo(1));
            // while the stopped processor was left untouched
            assertThat(stoppedProcessor.startCount()).isZero();
            assertThat(stoppedProcessor.shutdownCount()).isZero();
        }

        @Test
        void doesNotRestartWhenATenantChangesBeforeStart() {
            // The restarter follows the engine but was never started, so a tenant change finds no running restarter.
            routingEngine.registerTenant(TENANT_A);

            assertThat(runningProcessor.startCount()).isZero();
            assertThat(runningProcessor.shutdownCount()).isZero();
        }
    }

    @Nested
    class Coalescing {

        @Test
        void coalescesABurstOfTenantChangesIntoFarFewerRestartsThanChanges() throws InterruptedException {
            // The first restart blocks inside the processor shutdown until released, so a burst of changes arrives
            // while a cycle is already running and can only collapse into a single follow-up cycle.
            CountDownLatch firstRestartReached = new CountDownLatch(1);
            CountDownLatch releaseFirstRestart = new CountDownLatch(1);
            AtomicBoolean firstShutdown = new AtomicBoolean(true);
            RecordingStreamingEventProcessor gatedProcessor = new RecordingStreamingEventProcessor("gated", true) {
                @Override
                public CompletableFuture<Void> shutdown() {
                    if (firstShutdown.compareAndSet(true, false)) {
                        firstRestartReached.countDown();
                        awaitUninterruptibly(releaseFirstRestart);
                    }
                    return super.shutdown();
                }
            };
            AxonConfiguration gatedConfiguration = configurationWith(registry -> registry.registerComponent(
                    StreamingEventProcessor.class, gatedProcessor.name(), config -> gatedProcessor));
            gatedConfiguration.start();
            MultiTenantStreamingProcessorRestarter gatedSubject =
                    new MultiTenantStreamingProcessorRestarter(gatedConfiguration);
            gatedSubject.follow(routingEngine);
            gatedSubject.start();
            try {
                routingEngine.registerTenant(TENANT_A);
                assertThat(firstRestartReached.await(2, TimeUnit.SECONDS)).isTrue();

                int burst = 8;
                for (int index = 0; index < burst; index++) {
                    routingEngine.registerTenant(TenantDescriptor.tenantWithId("burst-tenant-" + index));
                }
                releaseFirstRestart.countDown();

                // The blocked cycle and the single coalesced follow-up each restart the processor once, so its second
                // start marks the burst as fully drained.
                await().atMost(Duration.ofSeconds(2))
                       .untilAsserted(() -> assertThat(gatedProcessor.startCount()).isGreaterThanOrEqualTo(2));
                // Two restarts in total, far fewer than the nine tenant changes that each requested one.
                assertThat(restartCount(gatedSubject)).isGreaterThanOrEqualTo(2).isLessThan(1 + burst);
            } finally {
                gatedSubject.stop();
                gatedConfiguration.shutdown();
            }
        }
    }

    @Nested
    class FailureIsolation {

        @Test
        void aFailingProcessorDoesNotSkipTheOthersInTheSameCycle() {
            // Both processors fail their restart. A single tenant change is a single cycle iterating both. Without
            // per-processor isolation the first failure would abort the cycle and the other would never be attempted,
            // so asserting both are attempted proves the isolation regardless of the order they are iterated in.
            AtomicInteger firstShutdowns = new AtomicInteger();
            AtomicInteger secondShutdowns = new AtomicInteger();
            StreamingEventProcessor firstFailing = failingProcessor("failing-one", firstShutdowns);
            StreamingEventProcessor secondFailing = failingProcessor("failing-two", secondShutdowns);
            AxonConfiguration mixedConfiguration = configurationWith(registry -> registry
                    .registerComponent(StreamingEventProcessor.class, firstFailing.name(), config -> firstFailing)
                    .registerComponent(StreamingEventProcessor.class, secondFailing.name(), config -> secondFailing));
            mixedConfiguration.start();
            MultiTenantStreamingProcessorRestarter mixedSubject =
                    new MultiTenantStreamingProcessorRestarter(mixedConfiguration);
            mixedSubject.follow(routingEngine);
            mixedSubject.start();
            try {
                routingEngine.registerTenant(TENANT_A);

                await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                    assertThat(firstShutdowns.get()).isGreaterThanOrEqualTo(1);
                    assertThat(secondShutdowns.get()).isGreaterThanOrEqualTo(1);
                });
            } finally {
                mixedSubject.stop();
                mixedConfiguration.shutdown();
            }
        }

        @Test
        void keepsProcessingLaterTenantChangesAfterAProcessorFailsToRestart() {
            AtomicInteger shutdownAttempts = new AtomicInteger();
            StreamingEventProcessor failingProcessor = failingProcessor("failing", shutdownAttempts);
            AxonConfiguration failingConfiguration = configurationWith(registry -> registry.registerComponent(
                    StreamingEventProcessor.class, failingProcessor.name(), config -> failingProcessor));
            failingConfiguration.start();
            MultiTenantStreamingProcessorRestarter failingSubject =
                    new MultiTenantStreamingProcessorRestarter(failingConfiguration);
            failingSubject.follow(routingEngine);
            failingSubject.start();
            try {
                routingEngine.registerTenant(TENANT_A);
                await().atMost(Duration.ofSeconds(2))
                       .untilAsserted(() -> assertThat(shutdownAttempts.get()).isGreaterThanOrEqualTo(1));

                // a later change is still processed, so a failing restart does not wedge the restarter
                routingEngine.registerTenant(TENANT_A);
                await().atMost(Duration.ofSeconds(2))
                       .untilAsserted(() -> assertThat(shutdownAttempts.get()).isGreaterThanOrEqualTo(2));
            } finally {
                failingSubject.stop();
                failingConfiguration.shutdown();
            }
        }
    }

    @Nested
    class FollowingTheRoutingEngine {

        // Its own engine, so the listener count belongs to this nest rather than being shared with the outer subject.
        private final MultiTenantEventStorageEngine followedEngine = newRoutingEngine();

        private AxonConfiguration engineConfiguration;
        private MultiTenantStreamingProcessorRestarter engineSubject;

        @BeforeEach
        void startWithARoutingEngine() {
            engineConfiguration = configurationWith(registry -> registry
                    .registerComponent(StreamingEventProcessor.class,
                                       runningProcessor.name(),
                                       config -> runningProcessor));
            engineConfiguration.start();
            engineSubject = new MultiTenantStreamingProcessorRestarter(engineConfiguration);
            engineSubject.follow(followedEngine);
            engineSubject.start();
        }

        @AfterEach
        void stopEngineSubject() {
            engineSubject.stop();
            engineConfiguration.shutdown();
        }

        @Test
        void doesNotRestartOnATenantChangeThatOnlyReachesTheTenantProvider() {
            // The provider announcing a tenant is not what decides the re-opened stream's tenants, so on its own it is
            // no reason to restart. Only the engine's own registration is.
            tenantProvider.addTenant(TENANT_A);

            await().during(Duration.ofMillis(200))
                   .atMost(Duration.ofSeconds(2))
                   .untilAsserted(() -> assertThat(restartCount(engineSubject)).isZero());
        }

        @Test
        void stopCancelsTheListenerItSubscribed() {
            // Asserted on the engine, since a stopped restarter ignores a restart request either way, so counting
            // restarts cannot tell a cancelled listener from a still-registered one.
            assertThat(subscribedListenerCount()).isOne();

            engineSubject.stop();

            assertThat(subscribedListenerCount()).isZero();
        }

        @Test
        void rejectsANullEngine() {
            MultiTenantStreamingProcessorRestarter notFollowing =
                    new MultiTenantStreamingProcessorRestarter(configuration);

            assertThatThrownBy(() -> notFollowing.follow(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The multi-tenant event storage engine must not be null");
        }

        @Test
        void rejectsASecondEngineAndKeepsFollowingTheFirst() {
            // A second engine would restart on tenants that decide nothing about the merged stream the first one spans.
            MultiTenantStreamingProcessorRestarter followingOne =
                    new MultiTenantStreamingProcessorRestarter(configuration);
            MultiTenantEventStorageEngine firstEngine = newRoutingEngine();
            MultiTenantEventStorageEngine secondEngine = newRoutingEngine();
            followingOne.follow(firstEngine);
            try {
                assertThatThrownBy(() -> followingOne.follow(secondEngine))
                        .isInstanceOf(AxonConfigurationException.class)
                        .hasMessageContaining("already follows");

                assertThat(listenerCountOf(secondEngine)).isZero();
                assertThat(listenerCountOf(firstEngine)).isOne();
            } finally {
                followingOne.stop();
            }
        }

        private int subscribedListenerCount() {
            return listenerCountOf(followedEngine);
        }
    }

    @Nested
    class Lifecycle {

        @Test
        void stopDuringAnInFlightRestartDoesNotBringTheProcessorBackUp() throws InterruptedException {
            CountDownLatch shutdownReached = new CountDownLatch(1);
            CompletableFuture<Void> shutdownGate = new CompletableFuture<>();
            RecordingStreamingEventProcessor gatedProcessor = new RecordingStreamingEventProcessor("gated", true) {
                @Override
                public CompletableFuture<Void> shutdown() {
                    shutdownReached.countDown();
                    return shutdownGate.thenCompose(ignored -> super.shutdown());
                }
            };
            AxonConfiguration gatedConfiguration = configurationWith(registry -> registry.registerComponent(
                    StreamingEventProcessor.class, gatedProcessor.name(), config -> gatedProcessor));
            gatedConfiguration.start();
            MultiTenantStreamingProcessorRestarter gatedSubject =
                    new MultiTenantStreamingProcessorRestarter(gatedConfiguration);
            gatedSubject.follow(routingEngine);
            gatedSubject.start();
            try {
                routingEngine.registerTenant(TENANT_A);
                assertThat(shutdownReached.await(2, TimeUnit.SECONDS)).isTrue();

                // stop() lands while the restart is blocked on the processor's shutdown
                gatedSubject.stop();
                shutdownGate.complete(null);

                // the shutdown completes, but the processor is not started again after stop()
                await().atMost(Duration.ofSeconds(2))
                       .untilAsserted(() -> assertThat(gatedProcessor.shutdownCount()).isEqualTo(1));
                assertThat(gatedProcessor.startCount()).isZero();
            } finally {
                shutdownGate.complete(null);
                gatedConfiguration.shutdown();
            }
        }
    }

    @Nested
    class Describing {

        @Test
        void describesWhetherItIsRunning() {
            testSubject.start();
            MockComponentDescriptor descriptor = new MockComponentDescriptor();

            testSubject.describeTo(descriptor);

            assertThat(descriptor.getDescribedProperties()).containsEntry("running", true);
        }

        @Test
        void describesTheEngineItFollows() {
            MultiTenantStreamingProcessorRestarter notFollowing =
                    new MultiTenantStreamingProcessorRestarter(configuration);
            MockComponentDescriptor beforeFollowing = new MockComponentDescriptor();
            MockComponentDescriptor afterFollowing = new MockComponentDescriptor();
            try {
                notFollowing.describeTo(beforeFollowing);
                notFollowing.follow(routingEngine);
                notFollowing.describeTo(afterFollowing);
            } finally {
                notFollowing.stop();
            }

            assertThat(beforeFollowing.getDescribedProperties())
                    .containsEntry("followingTenantChanges", false)
                    .doesNotContainKey("followedEngine");
            assertThat(afterFollowing.getDescribedProperties())
                    .containsEntry("followingTenantChanges", true)
                    .containsEntry("followedEngine", routingEngine);
        }

        @Test
        void appliesTheDefaultRestartTimeout() {
            MockComponentDescriptor descriptor = new MockComponentDescriptor();

            testSubject.describeTo(descriptor);

            assertThat(descriptor.getDescribedProperties())
                    .containsEntry("restartTimeout",
                                   MultiTenantStreamingProcessorRestartConfiguration.DEFAULT.restartTimeout());
        }

        @Test
        void appliesARegisteredRestartTimeoutOverride() {
            Duration override = Duration.ofSeconds(120);
            MultiTenantStreamingProcessorRestartConfiguration overrideConfiguration =
                    new MultiTenantStreamingProcessorRestartConfiguration(override);
            AxonConfiguration overriddenConfiguration =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry
                                               .disableEnhancerScanning()
                                               .registerComponent(
                                                       MultiTenantStreamingProcessorRestartConfiguration.class,
                                                       config -> overrideConfiguration))
                                       .build();
            MockComponentDescriptor descriptor = new MockComponentDescriptor();

            new MultiTenantStreamingProcessorRestarter(overriddenConfiguration).describeTo(descriptor);

            assertThat(descriptor.getDescribedProperties()).containsEntry("restartTimeout", override);
        }
    }

    private static StreamingEventProcessor failingProcessor(String name, AtomicInteger shutdownAttempts) {
        return new RecordingStreamingEventProcessor(name, true) {
            @Override
            public CompletableFuture<Void> shutdown() {
                shutdownAttempts.incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalStateException("cannot stop"));
            }
        };
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the restart gate");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the restart gate", interrupted);
        }
    }

    private MultiTenantEventStorageEngine newRoutingEngine() {
        return new MultiTenantEventStorageEngine(tenant -> tenantEngine,
                                                 tenant -> new InMemorySnapshotStore(),
                                                 new TenantRouter(new MetadataBasedTenantResolver(), tenantProvider));
    }

    private static int listenerCountOf(MultiTenantEventStorageEngine engine) {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();
        engine.describeTo(descriptor);
        return (int) descriptor.getDescribedProperties().get("tenantChangeListenerCount");
    }
}
