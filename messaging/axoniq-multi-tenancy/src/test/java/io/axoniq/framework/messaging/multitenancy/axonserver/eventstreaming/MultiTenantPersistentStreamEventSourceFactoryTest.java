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
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.util.RecordingPersistentStreams;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link MultiTenantPersistentStreamEventSourceFactory}.
 */
class MultiTenantPersistentStreamEventSourceFactoryTest {

    private static final String STREAM_NAME = "test-stream";
    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");

    private final MultiTenantPersistentStreamEventSourceFactory testSubject =
            new MultiTenantPersistentStreamEventSourceFactory();

    private RecordingPersistentStreams streams;
    private StubTenantProvider tenantProvider;
    private AxonServerConfiguration serverConfiguration;
    private RecordingSchedulerBuilder schedulerBuilder;
    private ScheduledExecutorService suppliedScheduler;

    @BeforeEach
    void setUp() {
        streams = new RecordingPersistentStreams();
        tenantProvider = new StubTenantProvider();
        serverConfiguration = new AxonServerConfiguration();
        schedulerBuilder = new RecordingSchedulerBuilder();
        suppliedScheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        schedulerBuilder.shutdownAll();
        suppliedScheduler.shutdownNow();
    }

    @Test
    void buildsASourceFanningOutAcrossTenants() {
        // given
        tenantProvider.addTenant(TENANT_A);

        // when
        SubscribableEventSource result = build(STREAM_NAME);

        // then — a source that opens the stream in the tenant's own context once subscribed
        result.subscribe(noOpConsumer());
        assertThat(streams.openedContexts()).containsExactly("tenant-a");
    }

    @Nested
    class ThreadCountResolution {

        @Test
        void takesTheThreadCountConfiguredUnderTheStreamsMapKey() {
            // given
            serverConfiguration.getPersistentStreams().put(STREAM_NAME, settingsWithThreadCount(4, null));
            tenantProvider.addTenant(TENANT_A);

            // when
            build(STREAM_NAME).subscribe(noOpConsumer());

            // then — the tenant's own pool is sized as configured for the stream, not shared across tenants
            assertThat(schedulerBuilder.threadCountFor(STREAM_NAME + "@tenant-a")).isEqualTo(4);
        }

        @Test
        void takesTheThreadCountOfTheSettingsWhoseExplicitNameMatches() {
            // given — the map key differs from the configured stream name
            serverConfiguration.getPersistentStreams()
                               .put("someBeanName", settingsWithThreadCount(6, STREAM_NAME));
            tenantProvider.addTenant(TENANT_A);

            // when
            build(STREAM_NAME).subscribe(noOpConsumer());

            // then
            assertThat(schedulerBuilder.threadCountFor(STREAM_NAME + "@tenant-a")).isEqualTo(6);
        }

        @Test
        void fallsBackToTheAutoPersistentStreamSettingsForAnUnconfiguredStream() {
            // given — the stream is in neither map, as an automatically created one is
            serverConfiguration.getAutoPersistentStreamsSettings().setThreadCount(3);
            tenantProvider.addTenant(TENANT_A);

            // when
            build("MyProcessor-stream").subscribe(noOpConsumer());

            // then
            assertThat(schedulerBuilder.threadCountFor("MyProcessor-stream@tenant-a")).isEqualTo(3);
        }
    }

    private static BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> noOpConsumer() {
        return (events, context) -> CompletableFuture.completedFuture(null);
    }

    private SubscribableEventSource build(String streamName) {
        return testSubject.build(streamName,
                                 new PersistentStreamProperties(streamName,
                                                                1,
                                                                "Seq",
                                                                Collections.emptyList(),
                                                                "0",
                                                                null),
                                 suppliedScheduler,
                                 100,
                                 configuration());
    }

    private static AxonServerConfiguration.PersistentStreamSettings settingsWithThreadCount(int threadCount,
                                                                                           String name) {
        AxonServerConfiguration.PersistentStreamSettings settings =
                new AxonServerConfiguration.PersistentStreamSettings();
        settings.setThreadCount(threadCount);
        if (name != null) {
            settings.setName(name);
        }
        return settings;
    }

    private Configuration configuration() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getComponent(AxonServerConnectionManager.class)).thenReturn(streams.connectionManager());
        when(configuration.getComponent(AxonServerConfiguration.class)).thenReturn(serverConfiguration);
        when(configuration.getComponent(EventConverter.class))
                .thenReturn(new DelegatingEventConverter(new JacksonConverter()));
        when(configuration.getComponent(UnitOfWorkFactory.class)).thenReturn(UnitOfWorkTestUtils.SIMPLE_FACTORY);
        when(configuration.getComponent(TenantProvider.class)).thenReturn(tenantProvider);
        when(configuration.getComponent(eq(PersistentStreamScheduledExecutorBuilder.class), any(Supplier.class)))
                .thenReturn(schedulerBuilder);
        when(configuration.getOptionalComponent(any(Class.class))).thenReturn(Optional.empty());
        return configuration;
    }

    /**
     * Records the thread count each scheduler was requested with, per stream name, so the per-tenant sizing is
     * observable.
     */
    private static final class RecordingSchedulerBuilder implements PersistentStreamScheduledExecutorBuilder {

        private record Request(String streamName, int threadCount, ScheduledExecutorService scheduler) {

        }

        private final List<Request> requests = new CopyOnWriteArrayList<>();

        @Override
        public ScheduledExecutorService apply(Integer threadCount, String streamName) {
            ScheduledExecutorService scheduler =
                    PersistentStreamScheduledExecutorBuilder.defaultFactory().build(threadCount, streamName);
            requests.add(new Request(streamName, threadCount, scheduler));
            return scheduler;
        }

        private int threadCountFor(String streamName) {
            return requests.stream()
                           .filter(request -> request.streamName().equals(streamName))
                           .map(Request::threadCount)
                           .findFirst()
                           .orElseThrow(() -> new AssertionError("No scheduler was built for [" + streamName + "]."));
        }

        private void shutdownAll() {
            requests.forEach(request -> request.scheduler().shutdownNow());
        }
    }
}
