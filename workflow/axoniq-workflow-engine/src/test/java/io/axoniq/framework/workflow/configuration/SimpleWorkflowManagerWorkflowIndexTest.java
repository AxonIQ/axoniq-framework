/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.SegmentKeyRange;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowManager;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociations;
import io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges.SegmentPosition;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how a {@link SimpleWorkflowManager} with a {@link WorkflowIndex} finds which candidates of a query for live
 * workflows run: from their rows and the events after the lowest stored position of the module, or by sourcing every
 * running workflow while a part of the module has no position.
 *
 * @author Stefan Dragisic
 */
class SimpleWorkflowManagerWorkflowIndexTest {

    private static final String MODULE = "orders";
    private static final List<String> CANDIDATES = List.of("order-1", "order-2", "order-3", "order-4");
    private static final Segment[] HALVES = Segment.ROOT_SEGMENT.split();

    private final InMemoryEventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final EventStore eventStore = new StorageEngineBackedEventStore(storageEngine,
                                                                            new SimpleEventBus(),
                                                                            new WorkflowEventTagResolver());
    private final UnitOfWorkFactory unitOfWorkFactory =
            new SimpleUnitOfWorkFactory(new EventStoreApplicationContext(eventStore));
    private final InMemoryWorkflowIndex index = new InMemoryWorkflowIndex();
    private final RecordingWorkflowStore workflowStore = new RecordingWorkflowStore();
    private SimpleWorkflowManager testSubject;
    private long positionOfTheRows;

    @BeforeEach
    void setUp() {
        // order-1 and order-2 started before the stored position, order-2 ended and order-3 started after it
        publish(lifecycle("order-1", WorkflowStatus.STARTED), lifecycle("order-2", WorkflowStatus.STARTED));
        positionOfTheRows = storageEngine.latestToken()
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join()
                                         .position()
                                         .orElseThrow();
        publish(lifecycle("order-2", WorkflowStatus.COMPLETED), lifecycle("order-3", WorkflowStatus.STARTED));
        index.initialize(MODULE, Set.of(), List.of()).orTimeout(5, TimeUnit.SECONDS).join();
        index.store(MODULE,
                    new WorkflowIndexChanges(List.of(new WorkflowStarted("order-1"), new WorkflowStarted("order-2"))),
                    null)
             .orTimeout(5, TimeUnit.SECONDS)
             .join();
        workflowStore.running.addAll(List.of("order-1", "order-3"));
        testSubject = new SimpleWorkflowManager(new InMemoryWorkflowHistoryRepository(),
                                                new InMemoryWorkflowExecutionRepository(),
                                                new WorkflowCancellationService(),
                                                workflowStore,
                                                unitOfWorkFactory,
                                                Runnable::run);
        testSubject.enableIndex(index, MODULE);
    }

    @Test
    void readsWhichCandidatesRunFromTheRowsAndTheEventsAfterTheLowestPosition() {
        // given
        storePosition(HALVES[0], positionOfTheRows + 10);
        storePosition(HALVES[1], positionOfTheRows);

        // when
        var running = runningCandidates();

        // then
        assertThat(running).containsExactlyInAnyOrder("order-1", "order-3");
        assertThat(workflowStore.runningWorkflowLoads).hasValue(0);
    }

    @Test
    void sourcesTheRunningWorkflowsWhileAPartOfTheModuleHasNoPosition() {
        // given
        storePosition(HALVES[0], positionOfTheRows);

        // when
        var running = runningCandidates();

        // then
        assertThat(running).containsExactlyInAnyOrder("order-1", "order-3");
        assertThat(workflowStore.runningWorkflowLoads).hasPositiveValue();
    }

    @Test
    void sourcesTheRunningWorkflowsAfterASplitUntilBothHalvesStoredAPosition() {
        // given
        storePosition(Segment.ROOT_SEGMENT, positionOfTheRows);

        // when: one half of the split segment stores a position, then the other
        storePosition(HALVES[0], positionOfTheRows);
        var whileOneHalfHasNoPosition = runningCandidates();
        var loadsWhileOneHalfHasNoPosition = workflowStore.runningWorkflowLoads.get();
        storePosition(HALVES[1], positionOfTheRows);
        var onceBothHalvesHaveAPosition = runningCandidates();

        // then
        assertThat(whileOneHalfHasNoPosition).containsExactlyInAnyOrder("order-1", "order-3");
        assertThat(onceBothHalvesHaveAPosition).containsExactlyInAnyOrder("order-1", "order-3");
        assertThat(loadsWhileOneHalfHasNoPosition).isPositive();
        assertThat(workflowStore.runningWorkflowLoads).hasValue(loadsWhileOneHalfHasNoPosition);
    }

    @Test
    void readsFromThePositionOfTheMergedSegmentAfterAMerge() {
        // given
        storePosition(HALVES[0], positionOfTheRows + 10);
        storePosition(HALVES[1], positionOfTheRows + 10);

        // when
        storePosition(Segment.ROOT_SEGMENT, positionOfTheRows);
        var running = runningCandidates();

        // then: the higher positions of the halves are gone, so the end of order-2 is read
        assertThat(running).containsExactlyInAnyOrder("order-1", "order-3");
        assertThat(workflowStore.runningWorkflowLoads).hasValue(0);
    }

    /**
     * Requests the cancellation of each candidate, and returns the candidates the manager sourced as running.
     */
    private Set<String> runningCandidates() {
        workflowStore.sourced.clear();
        CANDIDATES.forEach(workflowId -> testSubject.findMany(WorkflowStateQuery.byWorkflowId(workflowId))
                                                    .requestWorkflowCancellation(null)
                                                    .orTimeout(5, TimeUnit.SECONDS)
                                                    .join());
        return Set.copyOf(workflowStore.sourced);
    }

    private void storePosition(Segment segment, long position) {
        index.store(MODULE,
                    new WorkflowIndexChanges(List.of(), new SegmentPosition(SegmentKeyRange.of(segment), position)),
                    null)
             .orTimeout(5, TimeUnit.SECONDS)
             .join();
    }

    private void publish(EventMessage... events) {
        unitOfWorkFactory.create()
                         .executeWithResult(context -> eventStore.publish(context, List.of(events)))
                         .orTimeout(5, TimeUnit.SECONDS)
                         .join();
    }

    private static EventMessage lifecycle(String workflowId, WorkflowStatus status) {
        return new GenericEventMessage(new MessageType(new QualifiedName("WorkflowEvent")),
                                       Map.of(),
                                       MetadataUtils.create(workflowId, status));
    }

    /**
     * A store that records which workflows the manager sources and how often it sources the running workflows.
     */
    private static final class RecordingWorkflowStore implements WorkflowStore {

        private final Set<String> running = ConcurrentHashMap.newKeySet();
        private final Set<String> sourced = ConcurrentHashMap.newKeySet();
        private final AtomicInteger runningWorkflowLoads = new AtomicInteger();

        @Override
        public CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext) {
            runningWorkflowLoads.incrementAndGet();
            var workflowIds = Set.copyOf(running);
            return CompletableFuture.completedFuture(new RunningWorkflows() {
                @Override
                public Set<String> workflowIds() {
                    return workflowIds;
                }

                @Override
                public boolean contains(String workflowId) {
                    return workflowIds.contains(workflowId);
                }
            });
        }

        @Override
        public CompletableFuture<WaitAssociations> loadWaitAssociations(ProcessingContext processingContext) {
            throw new UnsupportedOperationException("not used by the manager");
        }

        @Override
        public CompletableFuture<WorkflowState> loadWorkflow(String workflowId, ProcessingContext processingContext) {
            throw new UnsupportedOperationException("not used by the manager");
        }

        @Override
        public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                       ProcessingContext processingContext) {
            sourced.add(workflowId);
            return CompletableFuture.completedFuture(
                    Optional.of(new EventSourcedWorkflowState(workflowId, VersionedType.of("order", "1.0")))
            );
        }
    }

    private record EventStoreApplicationContext(EventStore eventStore) implements ApplicationContext {

        @Override
        public <C> C component(Class<C> type, String name) {
            if (type.isInstance(eventStore)) {
                //noinspection unchecked
                return (C) eventStore;
            }
            throw new IllegalArgumentException("No component of type " + type);
        }
    }
}
