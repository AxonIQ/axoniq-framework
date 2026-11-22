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
import io.axoniq.framework.workflow.configuration.JpaWorkflowIndexConnectionPoolTest.EntityManagerPerTransaction;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexEntry;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexInitialization;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.TokenEntry;
import org.hibernate.jpa.HibernatePersistenceConfiguration;
import org.hibernate.tool.schema.Action;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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
 * Restarts a workflow module whose event processor handles every batch in a JPA transaction, with a
 * {@link JpaWorkflowIndex} and a JPA token store in the same database, so the index writes the changes of every
 * checkpoint within the transaction the processor stores its token in.
 * <p>
 * The oracles are which workflows a restarted configuration runs, and whether any claim fell back to loading the full
 * running-workflow list instead of the stored index plus the lifecycle events from the token it resumes from.
 *
 * @author Allard Buijze
 */
class JpaWorkflowIndexProcessorTransactionTest {

    private static final String MODULE = "jpa-index-transaction";
    private static final int SEGMENTS = 4;
    private static final int WORKFLOWS = 40;
    // Generous, as a segment whose checkpoint failed is claimed again only after a back-off that grows on repeats.
    private static final Duration AWAIT = Duration.ofSeconds(60);
    private static final WaitForStepDefinition AWAIT_PAID = new WaitForStepDefinition(
            new PrimitiveMetadata("awaitPaid", DefaultEventNameCustomizer.Builder.defaults()),
            EventConditions.fromQualifiedName(new QualifiedName("paid")),
            new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
            new Timing(Duration.ofMinutes(5))
    );

    private final HikariDataSource dataSource = dataSource();
    private final EntityManagerFactory entityManagerFactory =
            new HibernatePersistenceConfiguration("workflow-index-transaction")
                    .managedClasses(List.of(TokenEntry.class,
                                            WorkflowIndexEntry.class,
                                            WorkflowIndexInitialization.class))
                    .property("hibernate.connection.datasource", dataSource)
                    .schemaToolingAction(Action.CREATE_DROP)
                    .createEntityManagerFactory();
    private final EventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final JpaWorkflowIndex reader = JpaWorkflowIndex.builder()
                                                            .entityManagerFactory(entityManagerFactory)
                                                            .build();
    private final AtomicInteger fullLoads = new AtomicInteger();
    private final AtomicInteger writesOfTheirOwn = new AtomicInteger();
    private final AtomicBoolean failWrites = new AtomicBoolean();
    private final AtomicInteger tokenStoresToFail = new AtomicInteger();
    private final AtomicInteger failedTokenStores = new AtomicInteger();
    private final Set<String> runningBodies = ConcurrentHashMap.newKeySet();
    private final List<AxonConfiguration> configurations = new ArrayList<>();
    private long checkpointInterval = 0L;

    private static HikariDataSource dataSource() {
        var config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:" + UUID.randomUUID());
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    @AfterEach
    void tearDown() {
        List.copyOf(configurations).forEach(this::stop);
        entityManagerFactory.close();
        dataSource.close();
    }

    @Test
    void restoresTheRunningWorkflowsFromEntriesWrittenWithinTheProcessorsTransaction() {
        // given
        var first = start();
        var waiting = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, waiting);
        await().atMost(AWAIT).until(() -> indexed().equals(waiting));
        // every checkpoint was handed the processor's context, so none wrote in a transaction of its own
        assertThat(writesOfTheirOwn).hasValue(0);
        stop(first);
        fullLoads.set(0);
        runningBodies.clear();

        // when
        var restarted = start();

        // then
        awaitRunning(restarted, waiting);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(waiting);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void restoresEveryWorkflowWhenStoringTheTokenRolledBackTheWritesOfTheIndex() {
        // given
        var first = start();
        var indexed = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, indexed);
        // and - checkpoints whose index writes joined the processor's transaction, which then fails to store the token,
        // so the writes roll back with it, and the processor claims those segments again
        tokenStoresToFail.set(SEGMENTS);
        var rolledBack = startWorkflows(first, WORKFLOWS);
        var all = new HashSet<>(indexed);
        all.addAll(rolledBack);
        await().atMost(AWAIT).until(() -> failedTokenStores.get() > 0);

        // when - the claims restore every workflow, and later checkpoints write their starts again
        awaitRunning(first, all);
        await().atMost(AWAIT).until(() -> indexed().equals(all));
        stop(first);
        fullLoads.set(0);
        runningBodies.clear();
        var restarted = start();

        // then
        awaitRunning(restarted, all);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(all);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void releasesTheSegmentsWithoutAdvancingTheirTokensWhenTheReleaseCannotWriteTheIndex() {
        // given - only the first checkpoint of each segment is taken, so later starts are pending until the release
        checkpointInterval = TimeUnit.HOURS.toMillis(1);
        var first = start();
        var running = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, running);
        assertThat(indexed()).isNotEqualTo(running);
        var converter = first.getComponent(GeneralConverter.class);
        var tokensBeforeRelease = storedTokens(converter);

        // when - the index cannot be written while the segments are released
        failWrites.set(true);
        stop(first);

        // then - the claims are released all the same, without the tokens passing the starts the index lacks
        assertThat(owners()).hasSize(SEGMENTS).containsOnlyNulls();
        assertThat(storedTokens(converter)).isEqualTo(tokensBeforeRelease);

        // when
        failWrites.set(false);
        fullLoads.set(0);
        runningBodies.clear();
        var restarted = start();

        // then
        awaitRunning(restarted, running);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(running);
        assertThat(fullLoads).hasValue(0);
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
                                   .processorConfiguration(pc -> pc.initialSegmentCount(SEGMENTS).unitOfWorkFactory(
                                           new TransactionalUnitOfWorkFactory(
                                                   new EntityManagerPerTransaction(entityManagerFactory),
                                                   pc.unitOfWorkFactory()
                                           )
                                   ))
                                   .checkpointInterval(checkpointInterval)
                                   .contextFactory(c -> TestContext::new);
        var transactions = new JpaTransactionalExecutorProvider(entityManagerFactory);
        var configuration = WorkflowConfigurer.create()
                .registerWorkflowIndex(cfg -> new ObservedWorkflowIndex(JpaWorkflowIndex.builder()
                                                                                         .entityManagerFactory(
                                                                                                 entityManagerFactory
                                                                                         )
                                                                                         .build()))
                .componentRegistry(cr -> cr
                        .registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                        .registerComponent(TokenStore.class,
                                           cfg -> new FailingTokenStore(new JpaTokenStore(
                                                   transactions,
                                                   cfg.getComponent(GeneralConverter.class),
                                                   JpaTokenStoreConfiguration.DEFAULT
                                           )))
                        .registerDecorator(DecoratorDefinition.forType(WorkflowStore.class)
                                                              .with((cfg, name, delegate) -> new CountingWorkflowStore(
                                                                      delegate
                                                              )))
                        .registerModule(module)
                )
                .build();
        configurations.add(configuration);
        configuration.start();
        return configuration;
    }

    private void stop(AxonConfiguration configuration) {
        configurations.remove(configuration);
        configuration.shutdown();
    }

    private Set<String> startWorkflows(AxonConfiguration configuration, int count) {
        Set<String> ids = new HashSet<>();
        IntStream.range(0, count).forEach(index -> ids.add(publish(configuration)));
        return ids;
    }

    private static String publish(AxonConfiguration configuration) {
        var event = new GenericEventMessage(new MessageType("start"), Map.of("orderId", "order-1"));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-start")
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }

    private void awaitRunning(AxonConfiguration configuration, Set<String> workflowIds) {
        await().atMost(AWAIT)
               .until(() -> runningBodies.containsAll(workflowIds)
                       && engine(configuration).workflowExecutions().size() == workflowIds.size());
    }

    private static WorkflowEngine engine(AxonConfiguration configuration) {
        return configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[" + MODULE + "]");
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

    private List<@Nullable String> owners() {
        try (var entityManager = entityManagerFactory.createEntityManager()) {
            return entityManager.createQuery("select t from TokenEntry t where t.processorName = :processor",
                                             TokenEntry.class)
                                .setParameter("processor", MODULE)
                                .getResultStream()
                                .map(TokenEntry::getOwner)
                                .toList();
        }
    }

    /**
     * Reads the stored tokens by segment without claiming them.
     */
    private Map<Integer, @Nullable TrackingToken> storedTokens(GeneralConverter converter) {
        try (var entityManager = entityManagerFactory.createEntityManager()) {
            Map<Integer, @Nullable TrackingToken> tokens = new HashMap<>();
            entityManager.createQuery("select t from TokenEntry t where t.processorName = :processor",
                                      TokenEntry.class)
                         .setParameter("processor", MODULE)
                         .getResultStream()
                         .forEach(entry -> tokens.put(entry.getSegment().getSegmentId(), entry.getToken(converter)));
            return tokens;
        }
    }

    /**
     * Counts how often a claim loads the full running-workflow list, the fallback without a stored index.
     */
    private final class CountingWorkflowStore implements WorkflowStore {

        private final WorkflowStore delegate;

        private CountingWorkflowStore(WorkflowStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext) {
            fullLoads.incrementAndGet();
            return delegate.loadRunningWorkflows(processingContext);
        }

        @Override
        public CompletableFuture<WorkflowState> loadWorkflow(String workflowId, ProcessingContext processingContext) {
            return delegate.loadWorkflow(workflowId, processingContext);
        }

        @Override
        public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                       ProcessingContext processingContext) {
            return delegate.findWorkflow(workflowId, processingContext);
        }
    }

    /**
     * Counts the writes in a transaction of their own, which are the writes without a processing context, and fails
     * every write while {@link #failWrites} is set. A write within a processing context then fails within its
     * transaction, as a write the database rejects does, which makes that transaction roll back.
     */
    private final class ObservedWorkflowIndex implements WorkflowIndex {

        private final WorkflowIndex delegate;

        private ObservedWorkflowIndex(WorkflowIndex delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Optional<Set<WorkflowIndex.Entry>>> load(String indexName, Segment segment) {
            return delegate.load(indexName, segment);
        }

        @Override
        public CompletableFuture<Void> store(String indexName, IndexDelta delta) {
            writesOfTheirOwn.incrementAndGet();
            if (failWrites.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("database down"));
            }
            return delegate.store(indexName, delta);
        }

        @Override
        public Optional<CompletableFuture<Void>> storeWithin(String indexName,
                                                             IndexDelta delta,
                                                             ProcessingContext context) {
            if (failWrites.get()) {
                // An entry whose index name is longer than its column, which the database rejects.
                return delegate.storeWithin("i".repeat(256), new IndexDelta(Set.of("rejected"), Set.of()), context);
            }
            return delegate.storeWithin(indexName, delta, context);
        }

        @Override
        public CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds) {
            return delegate.initialize(indexName, runningWorkflowIds);
        }
    }

    /**
     * Fails the next {@link #tokenStoresToFail} stores of a token, after the batch's index writes joined its
     * transaction, so that transaction rolls back.
     */
    private final class FailingTokenStore implements TokenStore {

        private final TokenStore delegate;

        private FailingTokenStore(TokenStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName,
                                                                        int segmentCount,
                                                                        @Nullable TrackingToken initialToken,
                                                                        ProcessingContext context) {
            return delegate.initializeTokenSegments(processorName, segmentCount, initialToken, context);
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token,
                                                  String processorName,
                                                  int segmentId,
                                                  ProcessingContext context) {
            if (tokenStoresToFail.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
                failedTokenStores.incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalStateException("token store down"));
            }
            return delegate.storeToken(token, processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           int segmentId,
                                                           ProcessingContext context) {
            return delegate.fetchToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           Segment segment,
                                                           ProcessingContext context) {
            return delegate.fetchToken(processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> extendClaim(String processorName, int segmentId, ProcessingContext context) {
            return delegate.extendClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName, int segmentId, ProcessingContext context) {
            return delegate.releaseClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token,
                                                         String processorName,
                                                         Segment segment,
                                                         ProcessingContext context) {
            return delegate.initializeSegment(token, processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName, int segmentId, ProcessingContext context) {
            return delegate.deleteToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName,
                                                       int segmentId,
                                                       ProcessingContext context) {
            return delegate.fetchSegment(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchSegments(String processorName, ProcessingContext context) {
            return delegate.fetchSegments(processorName, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                       ProcessingContext context) {
            return delegate.fetchAvailableSegments(processorName, context);
        }

        @Override
        public CompletableFuture<String> retrieveStorageIdentifier(ProcessingContext context) {
            return delegate.retrieveStorageIdentifier(context);
        }
    }
}
