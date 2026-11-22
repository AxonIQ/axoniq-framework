/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.axoniq.framework.workflow.configuration.AbstractEventSourcedEntityRepositoryTestBase.TestContext;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexEntry;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexInitialization;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.jpa.EntityManagerExecutor;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.TokenEntry;
import org.hibernate.jpa.HibernatePersistenceConfiguration;
import org.hibernate.tool.schema.Action;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs a workflow module whose event processor handles every batch in a JPA transaction, with a
 * {@link JpaWorkflowIndex} and a JPA token store in the same database, through a connection pool smaller than the
 * number of segments that are busy at once.
 * <p>
 * Every busy segment holds a connection for its batch until the batch commits, the way a JPA transaction of a Spring
 * application does. A checkpoint that needed another connection for its index writes would wait for connections the
 * other segments hold while they wait for theirs, until its writes time out and the checkpoint fails. The index writes
 * within the batch's own transaction instead, so every checkpoint succeeds, and no segment is claimed again.
 *
 * @author Allard Buijze
 */
class JpaWorkflowIndexConnectionPoolTest {

    private static final String MODULE = "jpa-index-pool";
    private static final int SEGMENTS = 4;
    private static final int POOL_SIZE = 2;
    private static final int WORKFLOWS = 40;
    private static final WaitForStepDefinition AWAIT_PAID = new WaitForStepDefinition(
            new PrimitiveMetadata("awaitPaid", DefaultEventNameCustomizer.Builder.defaults()),
            EventConditions.fromQualifiedName(new QualifiedName("paid")),
            new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
            new Timing(Duration.ofMinutes(5))
    );

    private final HikariDataSource dataSource = dataSource();
    private final EntityManagerFactory entityManagerFactory =
            new HibernatePersistenceConfiguration("workflow-index-pool")
                    .managedClasses(List.of(TokenEntry.class,
                                            WorkflowIndexEntry.class,
                                            WorkflowIndexInitialization.class))
                    .property("hibernate.connection.datasource", dataSource)
                    .schemaToolingAction(Action.CREATE_DROP)
                    .createEntityManagerFactory();
    private final JpaWorkflowIndex reader = JpaWorkflowIndex.builder()
                                                            .entityManagerFactory(entityManagerFactory)
                                                            .build();
    private final AtomicInteger loads = new AtomicInteger();
    private final AtomicInteger failedWrites = new AtomicInteger();
    private final Set<String> runningBodies = ConcurrentHashMap.newKeySet();
    private AxonConfiguration configuration;

    private static HikariDataSource dataSource() {
        var config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:" + UUID.randomUUID());
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(POOL_SIZE);
        // Long enough for a segment to wait for the batch of another, short enough to fail a starved checkpoint well
        // within the time the processor waits for it.
        config.setConnectionTimeout(5_000);
        return new HikariDataSource(config);
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        entityManagerFactory.close();
        dataSource.close();
    }

    @Test
    void checkpointsOfMoreBusySegmentsThanPooledConnectionsSucceed() {
        // given
        configuration = start();

        // when - every segment receives workflows to start at once
        Set<String> started = new HashSet<>();
        IntStream.range(0, WORKFLOWS).forEach(i -> started.add(publish("start")));

        // then - every start reaches the index through the checkpoints of the segments
        await().atMost(30, TimeUnit.SECONDS).until(() -> runningBodies.containsAll(started));
        await().atMost(30, TimeUnit.SECONDS).until(() -> indexed().equals(started));
        // and - no checkpoint failed, as no segment was claimed more than once
        assertThat(failedWrites).hasValue(0);
        assertThat(loads).hasValue(SEGMENTS);
    }

    private AxonConfiguration start() {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                               runningBodies.add(ctx.workflowId());
                                               ctx.awaitEvent(AWAIT_PAID);
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(SEGMENTS)
                                                                   .unitOfWorkFactory(new TransactionalUnitOfWorkFactory(
                                                                           new EntityManagerPerTransaction(
                                                                                   entityManagerFactory
                                                                           ),
                                                                           pc.unitOfWorkFactory()
                                                                   )))
                                   .contextFactory(c -> TestContext::new);
        var transactions = new JpaTransactionalExecutorProvider(entityManagerFactory);
        var config = WorkflowConfigurer.create()
                .registerWorkflowIndex(cfg -> new CountingWorkflowIndex(JpaWorkflowIndex.builder()
                                                                                         .entityManagerFactory(
                                                                                                 entityManagerFactory
                                                                                         )
                                                                                         .build()))
                .componentRegistry(cr -> cr
                        .registerComponent(EventStorageEngine.class, cfg -> new InMemoryEventStorageEngine())
                        .registerComponent(TokenStore.class,
                                           cfg -> new JpaTokenStore(transactions,
                                                                    cfg.getComponent(GeneralConverter.class),
                                                                    JpaTokenStoreConfiguration.DEFAULT))
                        .registerModule(module)
                )
                .build();
        config.start();
        return config;
    }

    private String publish(String eventName) {
        var event = new GenericEventMessage(new MessageType(eventName), Map.of("orderId", "order-1"));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + eventName)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }

    private Set<String> indexed() {
        return reader.load(MODULE, Segment.ROOT_SEGMENT)
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join()
                     .orElse(Set.of())
                     .stream()
                     .map(WorkflowIndex.Entry::workflowId)
                     .collect(Collectors.toSet());
    }

    /**
     * Runs every unit of work of the event processor in a JPA transaction of its own, begun before the work runs, so
     * the work holds a connection from the start, and binds its {@link EntityManager} to the processing context, as
     * the transaction manager of a Spring application does.
     */
    static final class EntityManagerPerTransaction implements TransactionManager {

        private final EntityManagerFactory entityManagerFactory;

        EntityManagerPerTransaction(EntityManagerFactory entityManagerFactory) {
            this.entityManagerFactory = entityManagerFactory;
        }

        @Override
        public Transaction startTransaction() {
            throw new UnsupportedOperationException("Only runs the units of work of the event processor.");
        }

        @Override
        public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
            processingLifecycle.runOnPreInvocation(context -> {
                EntityManager entityManager = entityManagerFactory.createEntityManager();
                entityManager.getTransaction().begin();
                var executor = new EntityManagerExecutor(() -> entityManager);
                context.putResource(JpaTransactionalExecutorProvider.SUPPLIER_KEY, () -> executor);
                AtomicBoolean concluded = new AtomicBoolean();
                context.runOnCommit(committed -> {
                    if (concluded.compareAndSet(false, true)) {
                        try {
                            entityManager.getTransaction().commit();
                        } finally {
                            entityManager.close();
                        }
                    }
                });
                context.onError((failed, phase, error) -> {
                    if (concluded.compareAndSet(false, true)) {
                        try {
                            if (entityManager.getTransaction().isActive()) {
                                entityManager.getTransaction().rollback();
                            }
                        } finally {
                            entityManager.close();
                        }
                    }
                });
            });
        }

        @Override
        public boolean requiresSameThreadInvocations() {
            return true;
        }
    }

    /**
     * Counts the loads of the index, one for every claim of a segment, and the writes that failed.
     */
    private final class CountingWorkflowIndex implements WorkflowIndex {

        private final WorkflowIndex delegate;

        private CountingWorkflowIndex(WorkflowIndex delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Optional<Set<WorkflowIndex.Entry>>> load(String indexName, Segment segment) {
            loads.incrementAndGet();
            return delegate.load(indexName, segment);
        }

        @Override
        public CompletableFuture<Void> store(String indexName, IndexDelta delta) {
            return counted(delegate.store(indexName, delta));
        }

        @Override
        public Optional<CompletableFuture<Void>> storeWithin(String indexName,
                                                             IndexDelta delta,
                                                             ProcessingContext context) {
            return delegate.storeWithin(indexName, delta, context).map(this::counted);
        }

        @Override
        public CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds) {
            return delegate.initialize(indexName, runningWorkflowIds);
        }

        private CompletableFuture<Void> counted(CompletableFuture<Void> write) {
            return write.whenComplete((written, failure) -> {
                if (failure != null) {
                    failedWrites.incrementAndGet();
                }
            });
        }
    }
}
