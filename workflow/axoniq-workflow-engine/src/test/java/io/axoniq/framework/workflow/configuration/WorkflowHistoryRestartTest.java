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
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Restarts the default {@link WorkflowConfigurer} wiring against an event store and a token store that both outlive
 * the first configuration, the way a PostgreSQL event store and a JDBC token store do, and checks what the history
 * holds after the restart for every pairing of history repository and history token store.
 *
 * @author Stefan Dragisic
 */
class WorkflowHistoryRestartTest {

    private static final String MODULE = "history-restart";
    private static final String HISTORY_PROCESSOR = "WorkflowHistoryProjector[" + MODULE + "]";

    private final EventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final TokenStore durableTokenStore = new InMemoryTokenStore();
    private final List<AxonConfiguration> configurations = new ArrayList<>();

    @AfterEach
    void tearDown() {
        configurations.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class DefaultInMemoryHistory {

        @Test
        void isRebuiltAfterRestartWithTheApplicationTokenStore() {
            // given
            var first = start(null, UnaryOperator.identity());
            var workflowId = completeWorkflow(first);
            first.shutdown();

            // when
            var restarted = start(null, UnaryOperator.identity());

            // then
            await().atMost(5, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableStatus(restarted, workflowId) == WorkflowStatus.COMPLETED);
            await().atMost(5, TimeUnit.SECONDS)
                   .until(() -> historyStatus(restarted, workflowId) == WorkflowStatus.COMPLETED);
        }

        @Test
        void isRebuiltAfterRestartWithAnExplicitInMemoryTokenStore() {
            // given
            var first = start(null, pc -> pc.tokenStore(new InMemoryTokenStore()));
            var workflowId = completeWorkflow(first);
            first.shutdown();

            // when
            var restarted = start(null, pc -> pc.tokenStore(new InMemoryTokenStore()));

            // then
            await().atMost(5, TimeUnit.SECONDS)
                   .until(() -> historyStatus(restarted, workflowId) == WorkflowStatus.COMPLETED);
        }

        @Test
        void missesThePreviousRunAfterRestartWhenTheCallerSetsADurableTokenStore() {
            // given
            var first = start(null, pc -> pc.tokenStore(durableTokenStore));
            var workflowId = completeWorkflow(first);
            first.shutdown();

            // when
            // The caller's own token store wins, so the history processor resumes past the previous run's events.
            var restarted = start(null, pc -> pc.tokenStore(durableTokenStore));
            awaitHistoryCaughtUp(restarted);

            // then
            assertThat(history(restarted, workflowId)).isEmpty();
        }
    }

    @Nested
    class DurableHistory {

        private final DurableHistoryRepository durableHistory = new DurableHistoryRepository();

        @Test
        void keepsItsEntryWithoutReplayAfterRestartWithTheApplicationTokenStore() {
            // given
            var first = start(durableHistory, UnaryOperator.identity());
            var workflowId = completeWorkflow(first);
            first.shutdown();
            var savesBeforeRestart = durableHistory.savesOf(workflowId);

            // when
            var restarted = start(durableHistory, UnaryOperator.identity());
            awaitHistoryCaughtUp(restarted);

            // then
            assertThat(historyStatus(restarted, workflowId)).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(durableHistory.savesOf(workflowId)).isEqualTo(savesBeforeRestart);
        }

        @Test
        void keepsItsEntryWithoutReplayAfterRestartWithAnExplicitDurableTokenStore() {
            // given
            var first = start(durableHistory, pc -> pc.tokenStore(durableTokenStore));
            var workflowId = completeWorkflow(first);
            first.shutdown();
            var savesBeforeRestart = durableHistory.savesOf(workflowId);

            // when
            var restarted = start(durableHistory, pc -> pc.tokenStore(durableTokenStore));
            awaitHistoryCaughtUp(restarted);

            // then
            assertThat(historyStatus(restarted, workflowId)).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(durableHistory.savesOf(workflowId)).isEqualTo(savesBeforeRestart);
        }

        @Test
        void keepsItsEntryAfterReplayWithAnExplicitInMemoryTokenStore() {
            // given
            var first = start(durableHistory, pc -> pc.tokenStore(new InMemoryTokenStore()));
            var workflowId = completeWorkflow(first);
            first.shutdown();
            var savesBeforeRestart = durableHistory.savesOf(workflowId);

            // when
            var restarted = start(durableHistory, pc -> pc.tokenStore(new InMemoryTokenStore()));
            awaitHistoryCaughtUp(restarted);

            // then
            assertThat(durableHistory.savesOf(workflowId)).isGreaterThan(savesBeforeRestart);
            assertThat(historyStatus(restarted, workflowId)).isEqualTo(WorkflowStatus.COMPLETED);
        }
    }

    /**
     * Starts a configuration on the shared event store and token store, using the default history repository when
     * {@code historyRepository} is {@code null}.
     */
    private AxonConfiguration start(@Nullable MutableWorkflowHistoryRepository historyRepository,
                                    UnaryOperator<PooledStreamingEventProcessorConfiguration> historyCustomizer) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(1))
                                   .historyProcessorConfiguration(
                                           pc -> historyCustomizer.apply(pc.initialSegmentCount(1))
                                   )
                                   .contextFactory(c -> TestContext::new);
        if (historyRepository != null) {
            module.withHistory(c -> historyRepository);
        }
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                .registerComponent(TokenStore.class, cfg -> durableTokenStore)
                .registerModule(module));
        var configuration = configurer.build();
        configurations.add(configuration);
        configuration.start();
        return configuration;
    }

    /**
     * Starts one workflow and waits until it is completed, projected into history, and passed by every history token
     * the shared token store holds, so a restart has nothing left to project.
     */
    private String completeWorkflow(AxonConfiguration configuration) {
        var event = new GenericEventMessage(new MessageType("start"), Map.of("orderId", "order-1"));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-start")
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        // The default workflow id is the identifier of the start event.
        var workflowId = event.identifier();
        await().atMost(5, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableStatus(configuration, workflowId) == WorkflowStatus.COMPLETED);
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> historyStatus(configuration, workflowId) == WorkflowStatus.COMPLETED);
        TrackingToken head = storageEngine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
        await().atMost(5, TimeUnit.SECONDS).until(() -> durableHistoryTokenCovers(head));
        return workflowId;
    }

    private boolean durableHistoryTokenCovers(TrackingToken position) {
        var segments = durableTokenStore.fetchSegments(HISTORY_PROCESSOR, null)
                                        .orTimeout(5, TimeUnit.SECONDS)
                                        .join();
        // No segments means the history processor runs on another token store, which leaves nothing to wait for.
        return segments.stream().allMatch(segment -> {
            var stored = durableTokenStore.fetchToken(HISTORY_PROCESSOR, segment.getSegmentId(), null)
                                          .orTimeout(5, TimeUnit.SECONDS)
                                          .join();
            return stored != null && stored.covers(position);
        });
    }

    /**
     * Waits until every segment of the restarted history processor holds a token at the head of the event store. A
     * processor that resumes from a stored token is there at once, while a processor that replays only gets there
     * once it handled every event again.
     */
    private void awaitHistoryCaughtUp(AxonConfiguration configuration) {
        var processor = configuration.getComponents(StreamingEventProcessor.class).get(HISTORY_PROCESSOR);
        TrackingToken head = storageEngine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> !processor.processingStatus().isEmpty()
                       && processor.processingStatus().values().stream().allMatch(status -> {
                   var position = status.getTrackingToken();
                   return position != null && position.covers(head);
               }));
    }

    private static Optional<WorkflowHistory> history(AxonConfiguration configuration, String workflowId) {
        return configuration.getComponents(MutableWorkflowHistoryRepository.class)
                            .get("MutableWorkflowHistoryRepository[" + MODULE + "]")
                            .findById(workflowId)
                            .orTimeout(5, TimeUnit.SECONDS)
                            .join();
    }

    @Nullable
    private static WorkflowStatus historyStatus(AxonConfiguration configuration, String workflowId) {
        return history(configuration, workflowId).map(h -> h.state().workflowStatus()).orElse(null);
    }

    @SuppressWarnings("unchecked")
    private static WorkflowStatus durableStatus(AxonConfiguration configuration, String workflowId) {
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
                            .join()
                            .workflowStatus();
    }

    /**
     * Stands in for a durable history repository, such as a database-backed one: it outlives the configuration that
     * filled it, and it is not an {@link InMemoryWorkflowHistoryRepository}. Records every saved workflow id, so a test
     * can tell whether the history processor replayed.
     */
    private static final class DurableHistoryRepository implements MutableWorkflowHistoryRepository {

        private final InMemoryWorkflowHistoryRepository storage = new InMemoryWorkflowHistoryRepository();
        private final List<String> savedWorkflowIds = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<List<WorkflowHistory>> findAll(WorkflowStateQuery query) {
            return storage.findAll(query);
        }

        @Override
        public CompletableFuture<Optional<WorkflowHistory>> findById(String workflowId) {
            return storage.findById(workflowId);
        }

        @Override
        public void save(WorkflowHistory workflowHistory) {
            savedWorkflowIds.add(workflowHistory.workflowId());
            storage.save(workflowHistory);
        }

        @Override
        public void clear() {
            storage.clear();
        }

        private long savesOf(String workflowId) {
            return savedWorkflowIds.stream().filter(workflowId::equals).count();
        }
    }
}
