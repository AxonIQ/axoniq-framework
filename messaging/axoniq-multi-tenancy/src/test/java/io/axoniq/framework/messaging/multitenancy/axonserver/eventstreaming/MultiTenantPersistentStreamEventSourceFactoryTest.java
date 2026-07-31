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
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.util.RecordingPersistentStreams;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.common.AxonConfigurationException;
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
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link MultiTenantPersistentStreamEventSourceFactory}.
 */
class MultiTenantPersistentStreamEventSourceFactoryTest {

    private static final String STREAM_NAME = "test-stream";
    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");
    private static final TenantDescriptor TENANT_B = TenantDescriptor.tenantWithId("tenant-b");

    private final MultiTenantPersistentStreamEventSourceFactory testSubject =
            new MultiTenantPersistentStreamEventSourceFactory();

    private RecordingPersistentStreams streams;
    private StubTenantProvider tenantProvider;
    private RecordingSchedulerFactory schedulerFactory;
    private ScheduledExecutorService suppliedScheduler;

    @BeforeEach
    void setUp() {
        streams = new RecordingPersistentStreams();
        tenantProvider = new StubTenantProvider();
        schedulerFactory = new RecordingSchedulerFactory();
        suppliedScheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        schedulerFactory.shutdownAll();
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
    class SchedulerFactoryUsage {

        @Test
        void takesAPoolPerTenantFromTheSuppliedFactory() {
            // given
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);

            // when
            build(STREAM_NAME).subscribe(noOpConsumer());

            // then — one pool per tenant, each named after the stream and the tenant it serves
            assertThat(schedulerFactory.requestedPoolNames())
                    .containsExactlyInAnyOrder(STREAM_NAME + "@tenant-a", STREAM_NAME + "@tenant-b");
        }

        @Test
        void takesNoPoolBeforeTheSourceIsSubscribed() {
            // given
            tenantProvider.addTenant(TENANT_A);

            // when — a source is built, but nothing consumes it yet
            build(STREAM_NAME);

            // then — no tenant stream exists, so no pool was created for one
            assertThat(schedulerFactory.requestedPoolNames()).isEmpty();
        }

        @Test
        void rejectsTheDirectSchedulerContract() {
            // given — a caller passing an already created scheduler rather than a factory
            tenantProvider.addTenant(TENANT_A);
            tenantProvider.addTenant(TENANT_B);

            // when / then — sharing one scheduler across every tenant would defeat the per-tenant isolation this
            // factory exists to provide, so the contract is refused outright rather than silently honored
            assertThatThrownBy(() -> testSubject.build(STREAM_NAME, properties(STREAM_NAME), suppliedScheduler, 100,
                                                       configuration()))
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("schedulerFactory-based build");
            assertThat(streams.openedContexts()).isEmpty();
        }
    }

    @Nested
    class DuplicateStreamNameWarning {

        private ListAppender logAppender;

        @BeforeEach
        void attachAppender() {
            logAppender = new ListAppender("MultiTenantDuplicateStreamNameWarningLog");
            logAppender.start();
            ((Logger) LogManager.getLogger(MultiTenantPersistentStreamEventSourceFactory.class))
                    .addAppender(logAppender);
        }

        @AfterEach
        void detachAppender() {
            ((Logger) LogManager.getLogger(MultiTenantPersistentStreamEventSourceFactory.class))
                    .removeAppender(logAppender);
        }

        @Test
        void noWarningOnFirstBuild() {
            // when
            build(STREAM_NAME);

            // then
            assertThat(logAppender.getEvents())
                    .noneMatch(event -> event.getLevel() == Level.WARN);
        }

        @Test
        void warnsWhenSameStreamNameUsedTwice() {
            // given
            build(STREAM_NAME);

            // when
            build(STREAM_NAME);

            // then
            assertThat(logAppender.getEvents())
                    .anyMatch((LogEvent event) -> event.getLevel() == Level.WARN
                            && event.getMessage().getFormattedMessage().contains(STREAM_NAME));
        }

        @Test
        void noWarningForDifferentStreamNames() {
            // when
            build(STREAM_NAME);
            build("otherStream");

            // then
            assertThat(logAppender.getEvents())
                    .noneMatch(event -> event.getLevel() == Level.WARN);
        }
    }

    private static BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> noOpConsumer() {
        return (events, context) -> CompletableFuture.completedFuture(null);
    }

    private SubscribableEventSource build(String streamName) {
        return testSubject.build(streamName, properties(streamName), schedulerFactory, 100, configuration());
    }

    private static PersistentStreamProperties properties(String streamName) {
        return new PersistentStreamProperties(streamName, 1, "Seq", Collections.emptyList(), "0", null);
    }

    private Configuration configuration() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getComponent(AxonServerConnectionManager.class)).thenReturn(streams.connectionManager());
        when(configuration.getComponent(AxonServerConfiguration.class)).thenReturn(new AxonServerConfiguration());
        when(configuration.getComponent(EventConverter.class))
                .thenReturn(new DelegatingEventConverter(new JacksonConverter()));
        when(configuration.getComponent(UnitOfWorkFactory.class)).thenReturn(UnitOfWorkTestUtils.SIMPLE_FACTORY);
        when(configuration.getComponent(TenantProvider.class)).thenReturn(tenantProvider);
        when(configuration.getOptionalComponent(any(Class.class))).thenReturn(Optional.empty());
        return configuration;
    }

    /**
     * Records the pool name each scheduler was requested for, so which pools a source takes is observable.
     */
    private static final class RecordingSchedulerFactory implements Function<String, ScheduledExecutorService> {

        private record Request(String poolName, ScheduledExecutorService scheduler) {

        }

        private final List<Request> requests = new CopyOnWriteArrayList<>();

        @Override
        public ScheduledExecutorService apply(String poolName) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            requests.add(new Request(poolName, scheduler));
            return scheduler;
        }

        private List<String> requestedPoolNames() {
            return requests.stream().map(Request::poolName).toList();
        }

        private void shutdownAll() {
            requests.forEach(request -> request.scheduler().shutdownNow());
        }
    }
}
