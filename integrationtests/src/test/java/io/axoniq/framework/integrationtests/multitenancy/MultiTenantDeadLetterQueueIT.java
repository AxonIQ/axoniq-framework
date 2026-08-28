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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.deadletter.Decisions;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import io.axoniq.framework.messaging.eventhandling.deadletter.jdbc.GenericDeadLetterTableFactory;
import io.axoniq.framework.messaging.eventhandling.deadletter.jdbc.JdbcSequencedDeadLetterQueue;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.configuration.TenantComponentProviderUtil;
import io.axoniq.framework.messaging.multitenancy.deadletter.TenantAwareSequencedDeadLetterQueueFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.io.*;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.sql.DataSource;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.axonframework.common.FutureUtils.joinAndUnwrap;

/**
 * Integration test for tenant-aware dead-letter queue operations against a multi-context Axon Server.
 *
 * @author Jan Galinski
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantDeadLetterQueueIT {

    private final AtomicInteger retriedLetters = new AtomicInteger();

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();
    private static final String TENANT_A = "dlq-tenant-a";
    private static final String TENANT_B = "dlq-tenant-b";
    private static final String PROCESSOR_NAME = "multi-tenant-dlq";
    private static final String COMPONENT_NAME = "failing-handler";

    private final List<String> handledTenants = new CopyOnWriteArrayList<>();
    private final Map<String, DataSource> tenantDataSources = new ConcurrentHashMap<>();
    private final List<String> factoryTenants = new CopyOnWriteArrayList<>();
    @TempDir
    Path databaseDirectory;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);
        application = buildApplication();
        await()
                .untilAsserted(() -> assertThat(application.getComponent(TenantProvider.class).tenants())
                        .extracting(TenantDescriptor::tenantId)
                        .contains(TENANT_A, TENANT_B));
        await().untilAsserted(() -> {
            StreamingEventProcessor processor = application.getComponents(StreamingEventProcessor.class)
                                                           .get(PROCESSOR_NAME);
            assertThat(processor).isNotNull();
            assertThat(processor.isRunning()).isTrue();
        });
        EventStorageEngine eventStorageEngine = application.getComponent(EventStorageEngine.class);
        assertThat(eventStorageEngine).isInstanceOf(MultiTenantEventStorageEngine.class);
        assertThat(((MultiTenantEventStorageEngine) eventStorageEngine).tenants())
                .extracting(TenantDescriptor::tenantId)
                .contains(TENANT_A, TENANT_B);
    }

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;

    @Test
    void routesEnqueueAndProcessingToTheTenantInTheProcessingContext() {
        // given
        publishEvent(TENANT_A, "event-a");
        publishEvent(TENANT_B, "event-b");

        // when
        await().untilAsserted(() -> {
            assertThat(firstStoredEvent(TENANT_A)).isNotNull();
            assertThat(firstStoredEvent(TENANT_B)).isNotNull();
        });
        await().untilAsserted(() ->
                                      assertThat(handledTenants).containsExactlyInAnyOrder(TENANT_A, TENANT_B)
        );
        await().untilAsserted(() ->
                                      assertThat(factoryTenants).containsExactlyInAnyOrder(TENANT_A, TENANT_B)
        );
        await().untilAsserted(() -> {
            assertThat(deadLetterQueue().size(contextFor(TENANT_A)).join()).isEqualTo(1L);
            assertThat(deadLetterQueue().size(contextFor(TENANT_B)).join()).isEqualTo(1L);
        });
        assertThat(deadLetterCount(TENANT_A)).isEqualTo(1L);
        assertThat(deadLetterCount(TENANT_B)).isEqualTo(1L);

        deadLetterQueue().process(letter -> true, letter -> {
            retriedLetters.incrementAndGet();
            return completedFuture(Decisions.evict());
        }, contextFor(TENANT_A)).join();

        // then
        assertThat(retriedLetters).hasValue(1);
        assertThat(deadLetterQueue().size(contextFor(TENANT_A)).join()).isZero();
        assertThat(deadLetterQueue().size(contextFor(TENANT_B)).join()).isEqualTo(1L);
        assertThat(deadLetterCount(TENANT_A)).isZero();
        assertThat(deadLetterCount(TENANT_B)).isEqualTo(1L);
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
            application = null;
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    private AxonConfiguration buildApplication() {
        var processor = EventProcessorModule
                .pooledStreaming(PROCESSOR_NAME)
                .eventHandlingComponents(components -> components.declarative(COMPONENT_NAME,
                                                                              configuration -> Fixture.failingComponent(
                                                                                      handledTenants)))
                // Enables DLQ support for this processor. The multi-tenancy enhancer decorates its queue factory.
                .customized((configuration, pooled) -> pooled.extend(DeadLetterQueueConfiguration.class,
                                                                     () -> new DeadLetterQueueConfiguration().enabled()));

        Consumer<ComponentRegistry> registerTenantAwareSequencedDeadLetterQueueFactory = registry -> registry.registerComponent(
                TenantAwareSequencedDeadLetterQueueFactory.class,
                configuration -> (tenant, processorName, queueConfiguration) -> {
                    // reuses the single-per-tenant-datasource configured above
                    TenantComponentProvider<DataSource> dataSourceProvider =
                            TenantComponentProviderUtil.find(
                                    queueConfiguration, DataSource.class
                            ).orElseThrow();
                    EventConverter eventConverter = queueConfiguration.getComponent(
                                                          EventConverter.class
                                                  );
                    GeneralConverter generalConverter = queueConfiguration.getComponent(
                                                          GeneralConverter.class
                                                  );
                    JdbcTransactionalExecutorProvider executorProvider =
                            new JdbcTransactionalExecutorProvider(
                                                                                                  dataSourceProvider.componentFor(tenant)
                            );
                    JdbcSequencedDeadLetterQueue<EventMessage> queue =
                            JdbcSequencedDeadLetterQueue.<EventMessage>builder()
                                                        .processingGroup(processorName)
                                                        .transactionalExecutorProvider(
                                                                ignored -> executorProvider.getTransactionalExecutor(
                                                                        null)
                                                        )
                                                        .eventConverter(eventConverter)
                                                        .genericConverter(generalConverter)
                                                        .build();
                    joinAndUnwrap(queue.createSchema(new GenericDeadLetterTableFactory(),
                                                     null));
                    factoryTenants.add(tenant.tenantId());
                    return queue;
                });

        return EventSourcingConfigurer.create()
                                      .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                      .componentRegistry(TenantFixture::connectOnlyCustomTenantsPredicate)

                                      // The application's tenant-scoped datasource provider owns the physical storage
                                      .componentRegistry(Fixture.registerTenantDataSourceProvider(databaseDirectory,
                                                                                                  tenantDataSources))
                                      .componentRegistry(registerTenantAwareSequencedDeadLetterQueueFactory)
                                      // The processor itself needs no tenant-specific configuration: DLQ operations
                                      // carry their tenant in the ProcessingContext and are routed by the registry.
                                      .messaging(messaging -> messaging.eventProcessing(
                                              processing -> processing.pooledStreaming(
                                                      pooled -> pooled.processor(processor))))
                                      .start();
    }

    private StreamEventsResponse firstStoredEvent(String tenantId) {
        AxonServerConnectionManager connectionManager = application.getComponent(AxonServerConnectionManager.class);
        try (ResultStream<StreamEventsResponse> stream = connectionManager.getConnection(tenantId)
                                                                          .dcbEventChannel()
                                                                          .stream(StreamEventsRequest.newBuilder()
                                                                                                     .setFromSequence(0)
                                                                                                     .build())) {
            return stream.nextIfAvailable(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrieving the event stored for tenant ["
                                                    + tenantId + "]", e);
        }
    }

    private void publishEvent(String tenantId, String id) {
        UnitOfWorkFactory unitOfWorkFactory = application.getComponent(UnitOfWorkFactory.class);
        var unitOfWork = unitOfWorkFactory.create();
        unitOfWork.runOnInvocation(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            EventAppender.forContext(context).append(new TenantDlqEvent(id));
        });
        unitOfWork.execute().orTimeout(15, TimeUnit.SECONDS).join();
    }

    private ProcessingContext contextFor(String tenantId) {
        TenantDescriptor tenant = TenantDescriptor.tenantWithId(tenantId);
        return new StubProcessingContext().withResource(TenantDescriptor.RESOURCE_KEY, tenant);
    }

    @SuppressWarnings("unchecked")
    private SequencedDeadLetterQueue<EventMessage> deadLetterQueue() {
        String queueName = "DeadLetterQueue[EventHandlingComponent[" + PROCESSOR_NAME + "][" + COMPONENT_NAME + "]]";
        return moduleConfiguration().getComponent(SequencedDeadLetterQueue.class, queueName);
    }

    private Configuration moduleConfiguration() {
        return application.getModuleConfiguration("EventProcessor[" + PROCESSOR_NAME + "]")
                          .or(() -> application.getModuleConfiguration(PROCESSOR_NAME))
                          .orElseThrow();
    }

    private long deadLetterCount(String tenantId) {
        DataSource dataSource = tenantDataSources.get(tenantId);
        assertThat(dataSource).as("datasource for tenant %s", tenantId).isNotNull();
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("SELECT COUNT(*) FROM DeadLetterEntry");
             ResultSet result = statement.executeQuery()) {
            result.next();
            return result.getLong(1);
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to count dead letters for tenant [" + tenantId + "]", exception);
        }
    }

    /**
     * Fixture wrapping application/test specific setup created by a user of the multi-tenancy module.
     */
    private static final class Fixture {

        /**
         * Creates a {@link SimpleEventHandlingComponent} that subscribes to {@link TenantDlqEvent} and always fails.
         *
         * @param handledTenants the list of tenant IDs that have handled an event, to be updated by the component
         * @return a {@link SimpleEventHandlingComponent} that subscribes to {@link TenantDlqEvent} and always fails
         */
        private static SimpleEventHandlingComponent failingComponent(List<String> handledTenants) {
            SimpleEventHandlingComponent component = SimpleEventHandlingComponent.create(COMPONENT_NAME,
                                                                                         SequentialPolicy.INSTANCE);
            component.subscribe(new QualifiedName("test", "TenantDlqEvent"), (event, context) -> {
                handledTenants.add(TenantDescriptor.fromContext(context).orElseThrow().tenantId());
                throw new IllegalStateException("Expected failure for event " + event.identifier());
            });
            return component;
        }

        /**
         * Uses {@link #createDataSource(Path, String)} to register a {@link TenantComponentProvider} for
         * {@link DataSource} in the given registry. The provider creates a tenant-specific {@link DataSource} for the
         * given database directory and tenant ID, and caches it in the provided map of tenant data sources.
         *
         * @param databaseDirectory the directory where the tenant's database file will be stored
         * @param tenantDataSources the map of tenant data sources to cache the created {@link DataSource} for each
         *                          tenant ID
         * @return a {@link Consumer} that registers the {@link TenantComponentProvider} for {@link DataSource} in the
         * given registry
         */
        static Consumer<ComponentRegistry> registerTenantDataSourceProvider(Path databaseDirectory,
                                                                            Map<String, DataSource> tenantDataSources) {
            return registry -> registry.registerComponent(
                    TenantComponentProvider.class,
                    configuration -> TenantComponentProvider.withFactory(
                            DataSource.class,
                            tenant -> tenantDataSources.computeIfAbsent(
                                    tenant.tenantId(), tenantId -> createDataSource(databaseDirectory, tenantId)
                            )
                    ));
        }

        /**
         * Creates a tenant-specific {@link DataSource} for the given database directory and tenant ID.
         *
         * @param databaseDirectory the directory where the tenant's database file will be stored
         * @param tenantId          the ID of the tenant for which the {@link DataSource} is created
         * @return the tenant-specific {@link DataSource} for the given database directory and tenant ID
         */
        private static DataSource createDataSource(Path databaseDirectory, String tenantId) {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:file:" + databaseDirectory.resolve(tenantId).toAbsolutePath().toString()
                                                                 .replace('\\', '/') + ";DB_CLOSE_ON_EXIT=FALSE");
            dataSource.setUser("sa");
            dataSource.setPassword("");
            return dataSource;
        }
    }

    @Event(namespace = "test", name = "TenantDlqEvent", version = "1.0.0")
    record TenantDlqEvent(String id) {

    }
}
