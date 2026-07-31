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

package io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.util.RecordingPersistentStreams;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.Registration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link MultiTenantPersistentStreamEventSource}.
 */
class MultiTenantPersistentStreamEventSourceTest {

    private static final String STREAM_NAME = "test-stream";
    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");
    private static final TenantDescriptor TENANT_B = TenantDescriptor.tenantWithId("tenant-b");

    private RecordingPersistentStreams streams;
    private StubTenantProvider tenantProvider;
    private RecordingSchedulerFactory schedulerFactory;
    private MultiTenantPersistentStreamEventSource testSubject;

    @BeforeEach
    void setUp() {
        streams = new RecordingPersistentStreams();
        tenantProvider = new StubTenantProvider();
        schedulerFactory = new RecordingSchedulerFactory();
        testSubject = new MultiTenantPersistentStreamEventSource(
                STREAM_NAME,
                new PersistentStreamProperties(STREAM_NAME, 1, "Seq", Collections.emptyList(), "0", null),
                schedulerFactory,
                100,
                configurationWith(streams.connectionManager()),
                tenantProvider
        );
    }

    @AfterEach
    void tearDown() {
        schedulerFactory.shutdownAll();
    }

    @Nested
    class Subscribing {

        @Test
        void opensAStreamForEveryTenantKnownWhenSubscribing() {
            // given
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);

            // when
            testSubject.subscribe(recordingConsumer());

            // then — one stream per tenant, each in that tenant's own Axon Server context
            assertThat(streams.openedContexts()).containsExactlyInAnyOrder("tenant-a", "tenant-b");
        }

        @Test
        void opensNoStreamBeforeSubscribing() {
            // given
            tenantProvider.addTenant(TENANT_A);

            // when / then — a known tenant alone does not open a stream, there is no consumer to feed
            assertThat(streams.openedContexts()).isEmpty();
        }

        @Test
        void opensNoStreamWhenThereAreNoTenants() {
            // when
            testSubject.subscribe(recordingConsumer());

            // then
            assertThat(streams.openedContexts()).isEmpty();
        }

        @Test
        void rejectsADifferentConsumerWhileSubscribed() {
            // given
            testSubject.subscribe(recordingConsumer());

            // when / then
            assertThatThrownBy(() -> testSubject.subscribe(recordingConsumer()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(STREAM_NAME)
                    .hasMessageContaining("there is already an active subscription");
        }

        @Test
        void acceptsTheSameConsumerAgainWithoutOpeningAnotherStream() {
            // given
            tenantProvider.addTenant(TENANT_A);
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer =
                    recordingConsumer();
            testSubject.subscribe(consumer);

            // when
            testSubject.subscribe(consumer);

            // then — still exactly one scheduler, so the tenant's stream was not opened a second time
            assertThat(schedulerFactory.createCount()).isEqualTo(1);
        }

        @Test
        void canBeSubscribedAgainAfterCancelling() {
            // given
            tenantProvider.addTenant(TENANT_A);
            Registration registration = testSubject.subscribe(recordingConsumer());
            registration.cancel();

            // when
            testSubject.subscribe(recordingConsumer());

            // then — the tenant's stream is open again
            assertThat(streams.hasOpenStream("tenant-a")).isTrue();
        }
    }

    @Nested
    class TenantLabelling {

        @Test
        void labelsEveryEventWithTheTenantOfItsStream() {
            // given
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);
            RecordingConsumer consumer = new RecordingConsumer();
            testSubject.subscribe(consumer);

            // when
            streams.publish("tenant-a", 0, "event-for-a");
            streams.publish("tenant-b", 0, "event-for-b");

            // then — each event carries the tenant of the stream it came from, and never the other one
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.tenants().size() == 2);
            assertThat(consumer.tenantsOf("event-for-a")).containsExactly(TENANT_A);
            assertThat(consumer.tenantsOf("event-for-b")).containsExactly(TENANT_B);
        }

        @Test
        void labelsEveryEventOfOneTenantWithThatSameTenant() {
            // given
            tenantProvider.addTenant(TENANT_A);
            RecordingConsumer consumer = new RecordingConsumer();
            testSubject.subscribe(consumer);

            // when
            streams.publish("tenant-a", 0, "first");
            streams.publish("tenant-a", 1, "second");

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.tenants().size() == 2);
            assertThat(consumer.tenants()).containsExactly(TENANT_A, TENANT_A);
        }
    }

    @Nested
    class RuntimeTenantChanges {

        @Test
        void joinsATenantAddedWhileSubscribedToTheRunningConsumer() {
            // given
            tenantProvider.addTenant(TENANT_A);
            RecordingConsumer consumer = new RecordingConsumer();
            testSubject.subscribe(consumer);

            // when
            tenantProvider.addTenant(TENANT_B);
            streams.publish("tenant-b", 0, "event-for-b");

            // then — the newly added tenant feeds the already running consumer, labelled as itself
            await().atMost(Duration.ofSeconds(5)).until(() -> !consumer.tenants().isEmpty());
            assertThat(consumer.tenantsOf("event-for-b")).containsExactly(TENANT_B);
        }

        @Test
        void leavesTheOtherTenantsRunningWhenATenantIsAdded() {
            // given
            tenantProvider.addTenant(TENANT_A);
            RecordingConsumer consumer = new RecordingConsumer();
            testSubject.subscribe(consumer);

            // when
            tenantProvider.addTenant(TENANT_B);
            streams.publish("tenant-a", 0, "still-flowing");

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !consumer.tenants().isEmpty());
            assertThat(streams.hasOpenStream("tenant-a")).isTrue();
            assertThat(consumer.tenantsOf("still-flowing")).containsExactly(TENANT_A);
        }

        @Test
        void closesOnlyTheStreamOfTheRemovedTenant() {
            // given
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);
            testSubject.subscribe(recordingConsumer());

            // when
            tenantProvider.removeTenant(TENANT_A);

            // then
            assertThat(streams.hasOpenStream("tenant-a")).isFalse();
            assertThat(streams.hasOpenStream("tenant-b")).isTrue();
        }

        @Test
        void releasesTheSchedulerOfTheRemovedTenant() {
            // given
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);
            testSubject.subscribe(recordingConsumer());

            // when
            tenantProvider.removeTenant(TENANT_A);

            // then — the removed tenant's threads are released, the remaining tenant keeps its own
            assertThat(schedulerFactory.isShutdown(STREAM_NAME + "@tenant-a")).isTrue();
            assertThat(schedulerFactory.isShutdown(STREAM_NAME + "@tenant-b")).isFalse();
        }

        @Test
        void opensAFreshStreamForAReAddedTenant() {
            // given
            tenantProvider.addTenant(TENANT_A);
            testSubject.subscribe(recordingConsumer());
            tenantProvider.removeTenant(TENANT_A);

            // when
            tenantProvider.addTenant(TENANT_A);

            // then — a re-added tenant is served by a newly built stream, not the one bound to the dropped connection
            assertThat(streams.hasOpenStream("tenant-a")).isTrue();
            assertThat(schedulerFactory.createCount()).isEqualTo(2);
        }
    }

    @Nested
    class Unsubscribing {

        @Test
        void closesEveryTenantStream() {
            // given
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);
            Registration registration = testSubject.subscribe(recordingConsumer());

            // when
            registration.cancel();

            // then
            assertThat(streams.hasOpenStream("tenant-a")).isFalse();
            assertThat(streams.hasOpenStream("tenant-b")).isFalse();
        }

        @Test
        void releasesEveryTenantScheduler() {
            // given
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);
            Registration registration = testSubject.subscribe(recordingConsumer());

            // when
            registration.cancel();

            // then
            assertThat(schedulerFactory.isShutdown(STREAM_NAME + "@tenant-a")).isTrue();
            assertThat(schedulerFactory.isShutdown(STREAM_NAME + "@tenant-b")).isTrue();
        }

        @Test
        void unsubscribesFromTheTenantProvider() {
            // given
            tenantProvider.addTenant(TENANT_A);
            Registration registration = testSubject.subscribe(recordingConsumer());

            // when
            registration.cancel();

            // then — no tenant change reaches this source any more
            assertThat(tenantProvider.subscribedComponents()).doesNotContain(testSubject);
        }

        @Test
        void opensNoStreamForATenantAddedAfterCancelling() {
            // given
            Registration registration = testSubject.subscribe(recordingConsumer());
            registration.cancel();

            // when
            tenantProvider.addTenant(TENANT_A);

            // then
            assertThat(streams.openedContexts()).isEmpty();
        }

        @Test
        void reportsWhetherItCancelledTheActiveSubscription() {
            // given
            Registration registration = testSubject.subscribe(recordingConsumer());

            // when / then — cancelling twice only reports success for the cancellation that took effect
            assertThat(registration.cancel()).isTrue();
            assertThat(registration.cancel()).isFalse();
        }
    }

    @Nested
    class ProviderShutdown {

        @Test
        void closesEveryTenantStreamWhenTheProviderShutsDown() {
            // given
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);
            testSubject.subscribe(recordingConsumer());

            // when
            tenantProvider.shutdown();

            // then
            assertThat(streams.hasOpenStream("tenant-a")).isFalse();
            assertThat(streams.hasOpenStream("tenant-b")).isFalse();
        }
    }

    @Nested
    class ConstructorValidation {

        @Test
        void rejectsAnEmptyName() {
            assertThatThrownBy(() -> sourceWith("", 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("name");
        }

        @Test
        void rejectsANonPositiveBatchSize() {
            assertThatThrownBy(() -> sourceWith(STREAM_NAME, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batch size");
        }

        @Test
        void rejectsANullSchedulerFactory() {
            assertThatThrownBy(() -> new MultiTenantPersistentStreamEventSource(
                    STREAM_NAME,
                    new PersistentStreamProperties(STREAM_NAME, 1, "Seq", Collections.emptyList(), "0", null),
                    null,
                    100,
                    configurationWith(streams.connectionManager()),
                    tenantProvider))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("scheduler factory");
        }

        @Test
        void rejectsANullTenantProvider() {
            assertThatThrownBy(() -> new MultiTenantPersistentStreamEventSource(
                    STREAM_NAME,
                    new PersistentStreamProperties(STREAM_NAME, 1, "Seq", Collections.emptyList(), "0", null),
                    schedulerFactory,
                    100,
                    configurationWith(streams.connectionManager()),
                    null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("tenant provider");
        }

        private MultiTenantPersistentStreamEventSource sourceWith(String name, int batchSize) {
            return new MultiTenantPersistentStreamEventSource(
                    name,
                    new PersistentStreamProperties(STREAM_NAME, 1, "Seq", Collections.emptyList(), "0", null),
                    schedulerFactory,
                    batchSize,
                    configurationWith(streams.connectionManager()),
                    tenantProvider
            );
        }
    }

    @Nested
    class DescribeTo {

        @Test
        void describesTheStreamNameSubscriptionStateAndTenants() {
            // given
            tenantProvider.addTenant(TENANT_A);
            testSubject.subscribe(recordingConsumer());
            MockComponentDescriptor descriptor = new MockComponentDescriptor();

            // when
            testSubject.describeTo(descriptor);

            // then
            assertThat((String) descriptor.getProperty("name")).isEqualTo(STREAM_NAME);
            assertThat((Boolean) descriptor.getProperty("subscribed")).isTrue();
            assertThat((List<TenantDescriptor>) descriptor.getProperty("tenants")).containsExactly(TENANT_A);
        }
    }

    private static BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> recordingConsumer() {
        return new RecordingConsumer();
    }

    private static Configuration configurationWith(AxonServerConnectionManager connectionManager) {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getComponent(AxonServerConnectionManager.class)).thenReturn(connectionManager);
        when(configuration.getComponent(AxonServerConfiguration.class)).thenReturn(new AxonServerConfiguration());
        when(configuration.getComponent(EventConverter.class))
                .thenReturn(new DelegatingEventConverter(new JacksonConverter()));
        when(configuration.getComponent(UnitOfWorkFactory.class)).thenReturn(UnitOfWorkTestUtils.SIMPLE_FACTORY);
        when(configuration.getOptionalComponent(any(Class.class))).thenReturn(Optional.empty());
        return configuration;
    }

    /**
     * Records the tenant each consumed event was labelled with, keyed by the event's name, so a test can assert which
     * tenant a specific published event arrived under.
     */
    private static final class RecordingConsumer
            implements BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> {

        private final List<TenantDescriptor> tenants = Collections.synchronizedList(new ArrayList<>());
        private final Map<String, List<TenantDescriptor>> tenantsByName = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<?> apply(List<? extends EventMessage> events, ProcessingContext context) {
            TenantDescriptor tenant = context.getResource(TenantDescriptor.RESOURCE_KEY);
            for (EventMessage event : events) {
                tenants.add(tenant);
                tenantsByName.computeIfAbsent(event.type().name(),
                                              ignored -> Collections.synchronizedList(new ArrayList<>()))
                             .add(tenant);
            }
            return CompletableFuture.completedFuture(null);
        }

        private List<TenantDescriptor> tenants() {
            return List.copyOf(tenants);
        }

        private List<TenantDescriptor> tenantsOf(String eventName) {
            return List.copyOf(tenantsByName.getOrDefault(eventName, List.of()));
        }
    }

    /**
     * Creates real single-threaded schedulers while recording them per requested pool name, so a test can assert that
     * the scheduler of a specific tenant was released.
     */
    private static final class RecordingSchedulerFactory implements Function<String, ScheduledExecutorService> {

        private final Map<String, ScheduledExecutorService> created = new ConcurrentHashMap<>();
        private final AtomicInteger createCount = new AtomicInteger();

        @Override
        public ScheduledExecutorService apply(String poolName) {
            createCount.incrementAndGet();
            ScheduledExecutorService scheduler =
                    PersistentStreamScheduledExecutorBuilder.defaultFactory().build(1, poolName);
            created.put(poolName, scheduler);
            return scheduler;
        }

        private int createCount() {
            return createCount.get();
        }

        private boolean isShutdown(String poolName) {
            ScheduledExecutorService scheduler = created.get(poolName);
            return scheduler != null && scheduler.isShutdown();
        }

        private void shutdownAll() {
            created.values().forEach(ScheduledExecutorService::shutdownNow);
        }
    }
}
