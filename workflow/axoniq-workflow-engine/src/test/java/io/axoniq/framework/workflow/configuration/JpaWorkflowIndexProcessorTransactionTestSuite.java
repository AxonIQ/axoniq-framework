/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.configuration.AbstractEventSourcedEntityRepositoryTestBase.TestContext;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineHibernationPasses;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.EntityManagerTransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/**
 * Tests the {@link JpaWorkflowIndex} in a workflow processor whose units of work run in a JPA transaction that the
 * {@link JpaTokenStore} and the index both join, so the rows of the index and the token are committed together.
 * <p>
 * A subclass names the database. The event store stays in memory, as only the index and the token store are under
 * test.
 *
 * @author Stefan Dragisic
 */
abstract class JpaWorkflowIndexProcessorTransactionTestSuite {

    private static final String MODULE = "workflow-index-transaction";
    private static final int SEGMENT_COUNT = 4;
    private static final String WAIT_STEP = "awaitPaid";

    private final InMemoryEventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final List<AxonConfiguration> configurations = new ArrayList<>();
    private EntityManagerFactory entityManagerFactory;
    // Reads the rows in transactions of its own, the way a segment claim does.
    private JpaWorkflowIndex index;
    // The store of the configuration started last, as a spy that shows what a claim sourced.
    private @Nullable WorkflowStore workflowStore;

    /**
     * Returns the JDBC settings of the database under test, as properties of the persistence unit.
     *
     * @return the driver, the url, the user and the password of the database
     */
    abstract Map<String, Object> jdbcProperties();

    @BeforeEach
    void setUp() {
        // The schema is created with the factory and dropped with it, so every test starts with empty tables.
        entityManagerFactory = Persistence.createEntityManagerFactory("workflow-index", jdbcProperties());
        index = new JpaWorkflowIndex(new JpaTransactionalExecutorProvider(entityManagerFactory),
                                     new JacksonConverter(),
                                     JpaWorkflowIndexConfiguration.DEFAULT);
    }

    @AfterEach
    void tearDown() {
        configurations.forEach(AxonConfiguration::shutdown);
        entityManagerFactory.close();
    }

    /**
     * Waits for a {@code paid} event of the order the workflow was started for.
     */
    private static void awaitPaymentOfOwnOrder(TestContext ctx) {
        var orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
        ctx.awaitEvent(new WaitForStepDefinition(
                new PrimitiveMetadata(WAIT_STEP, DefaultEventNameCustomizer.Builder.defaults()),
                EventConditions.fromQualifiedName(new QualifiedName("paid"),
                                                  Associations.associate(payloadProperty("orderId"), "=", orderId)),
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(Duration.ofMinutes(5))
        ));
    }

    @Nested
    class RowsOfRunningWorkflows {

        @Test
        void areStoredWithTheTokenAndRestoreTheWorkflowsAfterARestart() {
            // given: waiting workflows on a node that stops
            var stopped = start();
            var workflowIds = IntStream.range(0, 8)
                                       .mapToObj(order -> startWaitingWorkflow(stopped, "order-" + order))
                                       .toList();
            await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(rows().runningWorkflowIds()).containsExactlyInAnyOrderElementsOf(workflowIds);
                assertThat(rows().waits()).extracting(WaitAssociation::workflowId)
                                          .containsExactlyInAnyOrderElementsOf(workflowIds);
            });
            stopped.shutdown();

            // when
            var restarted = start();

            // then: every workflow is in memory again, read from the rows and not from the lists
            await().atMost(20, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(workflowsInMemory(restarted))
                           .containsExactlyInAnyOrderElementsOf(workflowIds));
            verify(workflowStore, never()).loadRunningWorkflows(any());
            verify(workflowStore, never()).loadWaitAssociations(any());
        }

        @Test
        void areNotStoredByANodeThatLostTheClaimsOfItsSegments() {
            // given: a node whose rows are stored, as the row of its first workflow shows
            var configuration = start();
            var first = startWaitingWorkflow(configuration, "order-1");
            await().atMost(20, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(rows().runningWorkflowIds()).containsExactly(first));

            // when: another node owns every segment, and this node starts a second workflow
            giveEveryClaimToAnotherNode();
            var second = startWaitingWorkflow(configuration, "order-2");

            // then: the token cannot be stored, so the row of the second workflow is rolled back with it
            await().during(Duration.ofSeconds(3)).atMost(6, TimeUnit.SECONDS)
                   .until(() -> !rows().runningWorkflowIds().contains(second));
            assertThat(rows().runningWorkflowIds()).containsExactly(first);
        }

        @Test
        void areSelectedBySegmentInThisDatabase() {
            // given
            var workflowIds = IntStream.range(0, 200).mapToObj(i -> "order-" + i).toList();
            List<WorkflowIndexChange> changes = new ArrayList<>();
            workflowIds.forEach(workflowId -> changes.add(new WorkflowStarted(workflowId)));
            index.store(MODULE, new WorkflowIndexChanges(changes, null), null).orTimeout(20, TimeUnit.SECONDS).join();

            for (Segment segment : Segment.splitBalanced(Segment.ROOT_SEGMENT, SEGMENT_COUNT - 1)) {
                // when
                var rows = index.load(MODULE, segment, null).orTimeout(20, TimeUnit.SECONDS).join();

                // then
                assertThat(rows.runningWorkflowIds()).containsExactlyInAnyOrderElementsOf(
                        workflowIds.stream().filter(segment::matches).toList()
                );
            }
        }
    }

    @Nested
    class HibernatedWorkflow {

        @Test
        void staysOutOfMemoryAfterARestart() {
            // given: a hibernated workflow on a node that stops
            var stopped = start();
            var workflowId = startWaitingWorkflow(stopped, "order-1");
            await().atMost(10, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(50)).until(() -> {
                WorkflowEngineHibernationPasses.runPass(engine(stopped));
                return !workflowsInMemory(stopped).contains(workflowId);
            });
            stopped.shutdown();
            assertThat(rows().wakesAt()).containsKey(workflowId);

            // when
            var restarted = start();

            // then: the claim takes it from its row as hibernated, and does not load it
            await().atMost(20, TimeUnit.SECONDS)
                   .pollInterval(Duration.ofMillis(10))
                   .failFast("The claim loaded the workflow into memory.",
                             () -> workflowsInMemory(restarted).contains(workflowId))
                   .until(() -> WorkflowEngineHibernationPasses.isHibernated(engine(restarted), workflowId));
        }
    }

    /**
     * Starts a node whose workflow processor runs each unit of work in a JPA transaction of the database under test,
     * with a {@link JpaTokenStore} and a {@link JpaWorkflowIndex} that both join that transaction.
     */
    private AxonConfiguration start() {
        var transactions = new JpaTransactionalExecutorProvider(entityManagerFactory);
        var transactionManager = new EntityManagerTransactionManager(entityManagerPerThread());
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> JpaWorkflowIndexProcessorTransactionTestSuite
                                                   ::awaitPaymentOfOwnOrder)
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(SEGMENT_COUNT)
                                                                   .unitOfWorkFactory(
                                                                           new TransactionalUnitOfWorkFactory(
                                                                                   transactionManager,
                                                                                   pc.unitOfWorkFactory()
                                                                           )
                                                                   ))
                                   .contextFactory(c -> TestContext::new);
        module.withHibernation();
        var configurer = WorkflowConfigurer.create();
        configurer.registerWorkflowIndex(cfg -> new JpaWorkflowIndex(transactions,
                                                     cfg.getComponent(GeneralConverter.class),
                                                     JpaWorkflowIndexConfiguration.DEFAULT));
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                .registerComponent(TokenStore.class, cfg -> new JpaTokenStore(
                        transactions, cfg.getComponent(GeneralConverter.class), JpaTokenStoreConfiguration.DEFAULT
                ))
                .registerDecorator(WorkflowStore.class, 0, (cfg, name, delegate) -> workflowStore = spy(delegate))
                .registerModule(module));
        var configuration = configurer.build();
        configurations.add(configuration);
        configuration.start();
        return configuration;
    }

    /**
     * Provides one entity manager per thread, with an empty persistence context for every new transaction. The
     * {@link EntityManagerTransactionManager} begins and ends the transaction of a unit of work on it.
     */
    private EntityManagerProvider entityManagerPerThread() {
        ThreadLocal<EntityManager> entityManagers = ThreadLocal.withInitial(entityManagerFactory::createEntityManager);
        return () -> {
            var entityManager = entityManagers.get();
            if (!entityManager.getTransaction().isActive()) {
                entityManager.clear();
            }
            return entityManager;
        };
    }

    private void giveEveryClaimToAnotherNode() {
        inTransaction(entityManager -> entityManager
                .createQuery("update TokenEntry t set t.owner = :owner where t.processorName = :processorName")
                .setParameter("owner", "another-node")
                .setParameter("processorName", MODULE)
                .executeUpdate());
    }

    private void inTransaction(Consumer<EntityManager> work) {
        try (var entityManager = entityManagerFactory.createEntityManager()) {
            entityManager.getTransaction().begin();
            work.accept(entityManager);
            entityManager.getTransaction().commit();
        }
    }

    /**
     * Returns the rows of every segment, read the way a claim of each segment reads its own.
     */
    private WorkflowIndexRows rows() {
        var running = new HashSet<String>();
        var wakesAt = new HashMap<String, Instant>();
        var waits = new ArrayList<WaitAssociation>();
        for (Segment segment : Segment.splitBalanced(Segment.ROOT_SEGMENT, SEGMENT_COUNT - 1)) {
            var rows = index.load(MODULE, segment, null).orTimeout(10, TimeUnit.SECONDS).join();
            running.addAll(rows.runningWorkflowIds());
            wakesAt.putAll(rows.wakesAt());
            waits.addAll(rows.waits());
        }
        return new WorkflowIndexRows(running, wakesAt, waits, OptionalLong.empty());
    }

    /**
     * Starts a workflow for the given order and returns its id once its wait is durably started.
     */
    private String startWaitingWorkflow(AxonConfiguration configuration, String orderId) {
        // The default workflow id is the identifier of the start event.
        var workflowId = publish(configuration, "start", orderId);
        await().atMost(10, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, workflowId).containsStep(WAIT_STEP)
                       && durableState(configuration, workflowId).getStep(WAIT_STEP).status() == StepStatus.STARTED);
        return workflowId;
    }

    private static String publish(AxonConfiguration configuration, String eventName, String orderId) {
        var event = new GenericEventMessage(new MessageType(eventName), Map.of("orderId", orderId));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + eventName)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }

    private static List<String> workflowsInMemory(Configuration configuration) {
        return engine(configuration).workflowExecutions().stream().map(WorkflowExecution::workflowId).toList();
    }

    private static WorkflowEngine engine(Configuration configuration) {
        return findEngine(configuration).orElseThrow();
    }

    private static Optional<WorkflowEngine> findEngine(Configuration configuration) {
        return configuration.getComponents(WorkflowEngine.class)
                            .values()
                            .stream()
                            .findFirst()
                            .or(() -> configuration.getModuleConfigurations()
                                                   .stream()
                                                   .map(JpaWorkflowIndexProcessorTransactionTestSuite::findEngine)
                                                   .flatMap(Optional::stream)
                                                   .findFirst());
    }

    @SuppressWarnings("unchecked")
    private static EventSourcedWorkflowState durableState(AxonConfiguration configuration, String workflowId) {
        var repository = (Repository<String, EventSourcedWorkflowState>) configuration
                .getComponents(Repository.class)
                .values()
                .stream()
                .filter(candidate -> candidate.entityType().equals(EventSourcedWorkflowState.class))
                .findFirst()
                .orElseThrow();
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("read-" + workflowId)
                            .executeWithResult(ctx -> repository.loadOrCreate(workflowId, ctx)
                                                                .thenApply(managed -> managed.entity()))
                            .orTimeout(5, TimeUnit.SECONDS)
                            .join();
    }
}
