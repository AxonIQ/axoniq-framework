/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests where a {@link WorkflowEngine} keeps a hibernated workflow when the claim of its segment changes while the
 * workflow is restored or woken: a restore by a claim of an overlapping segment takes the workflow over, and a wake
 * that fails once the claim of the hibernation ended leaves the workflow out of this node. Also tests what becomes of
 * the cancellation requests that wait for a woken workflow whose start fails, and that hibernation is enabled only once,
 * before the engine starts.
 * <p>
 * The executions are the mocks of {@link WorkflowExecutionFixture}, as in the other restore tests of the engine; the
 * live workflows, the hibernation support and the claims are real.
 *
 * @author Allard Buijze
 */
class WorkflowEngineHibernatedWorkflowClaimTest {

    private static final Segment WHOLE = new Segment(0, 0);
    private static final Segment SECOND_HALF = new Segment(1, 1);
    private static final String WORKFLOW_ID = IntStream.range(0, 512)
                                                       .mapToObj(i -> "sharded-" + i)
                                                       .filter(id -> WorkflowSegmentOwnership.ownedBy(SECOND_HALF, id))
                                                       .findFirst()
                                                       .orElseThrow();
    private static final String WORKFLOW_NAME = "HibernatingWorkflow";
    private static final Instant DEADLINE = Instant.now().plus(Duration.ofHours(1));

    private final LiveWorkflows liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
    private final ControllableWorkflowScheduler scheduler = new ControllableWorkflowScheduler();
    private final List<String> bodyStarts = new CopyOnWriteArrayList<>();
    private final EventSourcedRunningWorkflows runningWorkflows = new EventSourcedRunningWorkflows();
    private final CompletableFuture<WorkflowState> sourced = new CompletableFuture<>();
    private final WorkflowStore workflowStore = mock(WorkflowStore.class);
    private final AtomicInteger loads = new AtomicInteger();
    private final ExecutorService wakeThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final WorkflowCancellationService cancellationService = new WorkflowCancellationService(liveWorkflows);
    private volatile Runnable onEvent = () -> {
    };
    // Runs as the body of an execution starts.
    private volatile Runnable onStart = () -> {
    };
    // Runs right after a woken execution took the place of its hibernation, before the engine hands it what woke it.
    private volatile Runnable onWoken = () -> {
    };
    // Runs the wakes for a deadline or a cancellation; on the waking thread, unless a test runs them on wake threads.
    private volatile Executor wakeExecutor = Runnable::run;
    // Counts each task of the wake executor from its submission until it ended.
    private final AtomicInteger wakeTasksInFlight = new AtomicInteger();
    // The cancellation of the execution created last.
    private final AtomicReference<WorkflowCancellation> lastCancellation = new AtomicReference<>();
    private WorkflowEngine engine;

    private static UnitOfWorkFactory unitOfWorkFactory() {
        return new SimpleUnitOfWorkFactory(new ApplicationContext() {
            @Override
            public <C> C component(Class<C> type, String name) {
                throw new ComponentNotFoundException(type, name);
            }
        });
    }

    @BeforeEach
    void setUp() {
        var configurationRegistry = new SimpleWorkflowConfigurationRegistry();
        engine = new WorkflowEngine(configurationRegistry,
                                    liveWorkflows,
                                    workflowStore,
                                    new InMemoryWorkflowIndex(),
                                    "WorkflowEngine",
                                    unitOfWorkFactory());
        engine.setCheckpointingSupport(new WorkflowEngineCheckpointingSupport(engine, 0L));
        engine.enableHibernation(scheduler, Clock.systemUTC(), Duration.ofMinutes(1), Duration.ofSeconds(10),
                                 task -> {
                                     wakeTasksInFlight.incrementAndGet();
                                     wakeExecutor.execute(() -> {
                                         try {
                                             task.run();
                                         } finally {
                                             wakeTasksInFlight.decrementAndGet();
                                         }
                                     });
                                 });
        engine.start(null);
        registerDefinition(configurationRegistry);

        var state = mock(WorkflowState.class);
        when(state.workflowDefinitionId()).thenReturn(VersionedType.of(new QualifiedName(WORKFLOW_NAME), "1.0.0"));
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(state.payload()).thenReturn(Map.of("id", WORKFLOW_ID));
        runningWorkflows.evolve(MetadataUtils.create(WORKFLOW_ID, WorkflowStatus.STARTED));
        when(workflowStore.loadRunningWorkflows(any()))
                .thenReturn(CompletableFuture.completedFuture(runningWorkflows));
        when(workflowStore.loadWorkflow(eq(WORKFLOW_ID), any())).thenAnswer(invocation -> {
            loads.incrementAndGet();
            return sourced;
        });
        sourced.complete(state);
    }

    @AfterEach
    void tearDown() {
        engine.shutdown();
        wakeThreads.shutdownNow();
    }

    @Test
    void aRestoreTakesTheWorkflowOverFromItsHibernationUnderAClaimOfAnOverlappingSegment() throws Exception {
        // given - a claim of the whole segment space that restores the workflow, while a claim of one half of it,
        // which stays current during a merge, holds it hibernated
        var halfClaim = engine.segmentClaims.startClaim(SECOND_HALF, new StubProcessingContext());
        var adopted = new CompletableFuture<Void>();
        when(workflowStore.loadWorkflow(eq(WORKFLOW_ID), any())).thenAnswer(invocation -> {
            // the restore of the whole segment dropped the hibernations it found before it sources the workflow
            adoptTheWorkflow(halfClaim);
            adopted.complete(null);
            return sourced;
        });

        // when
        engine.restoreWorkflowsFor(WHOLE, null, new StubProcessingContext(), new StubProcessingContext())
              .get(10, TimeUnit.SECONDS);

        // then - it runs in memory under the restoring claim, and its hibernation is gone with its deadline
        assertThat(adopted).isDone();
        assertThat(engine.hibernatedWorkflowIds()).isEmpty();
        assertThat(engine.workflowExecutions()).extracting(WorkflowExecution::workflowId)
                                               .containsExactly(WORKFLOW_ID);
        assertThat(bodyStarts).containsExactly(WORKFLOW_ID);
        assertThat(scheduler.pendingDeadlines()).doesNotContain(DEADLINE);
    }

    @Test
    void aRestoreThatOutlivesItsClaimLeavesTheHibernationOfAnOverlappingClaimInPlace() throws Exception {
        // given - a claim of the whole segment space that ends while it sources the workflow, which a claim of one half
        // of it holds hibernated
        var halfClaim = engine.segmentClaims.startClaim(SECOND_HALF, new StubProcessingContext());
        when(workflowStore.loadWorkflow(eq(WORKFLOW_ID), any())).thenAnswer(invocation -> {
            adoptTheWorkflow(halfClaim);
            engine.segmentClaims.release(WHOLE);
            return sourced;
        });

        // when
        engine.restoreWorkflowsFor(WHOLE, null, new StubProcessingContext(), new StubProcessingContext())
              .get(10, TimeUnit.SECONDS);

        // then - it stays hibernated under the claim of the half
        assertThat(engine.hibernatedWorkflowIds()).containsExactly(WORKFLOW_ID);
        assertThat(engine.workflowExecutions()).isEmpty();
        assertThat(bodyStarts).isEmpty();
        assertThat(scheduler.pendingDeadlines()).contains(DEADLINE);
    }

    @Test
    void aWakeThatFailsOnceItsClaimEndedLeavesTheWorkflowNeitherInMemoryNorHibernated() {
        // given - a hibernated workflow, whose woken execution fails to take the waking event once the claim of its
        // segment ended
        var claim = engine.segmentClaims.startClaim(WHOLE, new StubProcessingContext());
        adoptTheWorkflow(claim);
        onEvent = () -> {
            engine.segmentClaims.release(WHOLE);
            throw new IllegalStateException("unreadable event");
        };
        var event = new GenericEventMessage(new MessageType("paid"), Map.of("id", WORKFLOW_ID));

        // when
        assertThatThrownBy(() -> liveWorkflows.deliver(WORKFLOW_ID, event, new StubProcessingContext()))
                .isInstanceOf(IllegalStateException.class);

        // then - the ended claim no longer finds it, so it must not stay hibernated here
        assertThat(engine.hibernatedWorkflowIds()).isEmpty();
        assertThat(engine.workflowExecutions()).isEmpty();
        assertThat(scheduler.pendingDeadlines()).doesNotContain(DEADLINE);
    }

    @Nested
    class FailedStartOfAWokenWorkflow {

        private static final String START_FAILURE = "start failed";

        @BeforeEach
        void setUp() {
            // given - a hibernated workflow of a claim that caught up, whose woken execution fails to start
            var claim = engine.segmentClaims.startClaim(WHOLE, new StubProcessingContext());
            adoptTheWorkflow(claim);
            onStart = () -> {
                throw new IllegalStateException(START_FAILURE);
            };
        }

        @Test
        void failsTheRequestQueuedOnTheWokenExecutionWithTheStartFailure() {
            // given - the wakes run on wake threads
            wakeExecutor = wakeThreads;

            // when - a step cancellation wakes the workflow, and is queued on the woken execution, which fails to start
            var cancelled = cancellationService.requestStepCancellation(WORKFLOW_ID, "step", null);

            // then
            assertThat(cancelled).failsWithin(Duration.ofSeconds(5))
                                 .withThrowableThat()
                                 .havingRootCause()
                                 .isInstanceOf(IllegalStateException.class)
                                 .withMessage(START_FAILURE);
            // the request fails before the failing wake swapped the workflow back
            await().atMost(Duration.ofSeconds(5))
                   .untilAsserted(() -> assertThat(engine.hibernatedWorkflowIds()).containsExactly(WORKFLOW_ID));
            assertThat(bodyStarts).isEmpty();
        }

        @Test
        void wakesTheWorkflowOnceForARequestWhileItsStartKeepsFailing() {
            // given - the wakes run on wake threads
            wakeExecutor = wakeThreads;

            // when
            var cancelled = cancellationService.requestWorkflowCancellation(WORKFLOW_ID, null);

            // then - the request fails, instead of waking the workflow again and again
            assertThat(cancelled).failsWithin(Duration.ofSeconds(5));
            await().atMost(Duration.ofSeconds(5)).until(() -> wakeTasksInFlight.get() == 0);
            assertThat(loads).hasValue(1);
        }

        @Test
        void abortsAWorkflowCancellationThatReachedTheWokenExecutionAsItFailedToStart() {
            // given - a workflow cancellation is pending on the woken execution as its start fails, as one its body
            // took before the start failed
            var pending = new AtomicReference<CompletableFuture<Void>>();
            onStart = () -> {
                pending.set(lastCancellation.get().requestWorkflowCancellation(null));
                throw new IllegalStateException(START_FAILURE);
            };
            var event = new GenericEventMessage(new MessageType("paid"), Map.of("id", WORKFLOW_ID));

            // when - an event wakes the workflow, whose start fails
            assertThatThrownBy(() -> liveWorkflows.deliver(WORKFLOW_ID, event, new StubProcessingContext()))
                    .isInstanceOf(RuntimeException.class);

            // then
            assertThat(pending.get()).isCompletedExceptionally();
            assertThatThrownBy(() -> pending.get().join())
                    .isInstanceOf(CancellationException.class)
                    .hasMessage("Workflow execution failed to start before cancellation was performed")
                    .rootCause()
                    .hasMessage(START_FAILURE);
            assertThat(engine.hibernatedWorkflowIds()).containsExactly(WORKFLOW_ID);
        }
    }

    @Nested
    class WokenWorkflowThatLeavesMemoryBeforeItTakesTheRequest {

        @Test
        void makesTheRequestAgainInsteadOfLeavingItUnanswered() {
            // given - a hibernated workflow of a claim that caught up, whose segment is released right after a
            // cancellation woke it, before the request is queued on the woken execution
            var claim = engine.segmentClaims.startClaim(WHOLE, new StubProcessingContext());
            adoptTheWorkflow(claim);
            wakeExecutor = wakeThreads;
            onWoken = () -> engine.releaseWorkflowsFor(WHOLE);

            // when
            var cancelled = cancellationService.requestWorkflowCancellation(WORKFLOW_ID, null);

            // then - it is made again, and finds the workflow gone from this node
            assertThat(cancelled).failsWithin(Duration.ofSeconds(5))
                                 .withThrowableThat()
                                 .havingRootCause()
                                 .isInstanceOf(NoSuchElementException.class);
            assertThat(engine.workflowExecutions()).isEmpty();
            assertThat(engine.hibernatedWorkflowIds()).isEmpty();
        }
    }

    @Nested
    class EnablingHibernation {

        @Test
        void failsWhenHibernationIsEnabledAlready() {
            // when / then - the engine of this test enabled hibernation already
            assertThatThrownBy(() -> engine.enableHibernation(scheduler, Clock.systemUTC(), Duration.ofMinutes(1),
                                                              Duration.ofSeconds(10), Runnable::run))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(engine.isHibernationEnabled()).isTrue();
        }

        @Test
        void failsOnceTheEngineStarted() {
            // given - an engine without hibernation that started
            var started = new WorkflowEngine(new SimpleWorkflowConfigurationRegistry(),
                                             new LiveWorkflows(new InMemoryWorkflowExecutionRepository()),
                                             workflowStore,
                                             new InMemoryWorkflowIndex(),
                                             "StartedWorkflowEngine",
                                             unitOfWorkFactory());
            started.setCheckpointingSupport(new WorkflowEngineCheckpointingSupport(started, 0L));
            started.start(null);

            // when / then
            assertThatThrownBy(() -> started.enableHibernation(scheduler, Clock.systemUTC(), Duration.ofMinutes(1),
                                                               Duration.ofSeconds(10), Runnable::run))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(started.isHibernationEnabled()).isFalse();
            started.shutdown();
        }
    }

    /**
     * Leaves the workflow out of memory as a hibernated workflow of the given claim, as a claim that accepted its hint
     * does.
     */
    private void adoptTheWorkflow(SegmentClaim claim) {
        engine.hibernation.adopt(claim, Map.of(WORKFLOW_ID, new HibernationHint(DEADLINE, List.of())));
        assertThat(engine.hibernatedWorkflowIds()).contains(WORKFLOW_ID);
    }

    /**
     * Registers the definition of the workflow, whose executions record their body starts after they ran the current
     * {@link #onStart} action, run the current {@link #onEvent} action for every event, and keep a workflow
     * cancellation pending until it is aborted.
     */
    @SuppressWarnings("unchecked")
    private void registerDefinition(SimpleWorkflowConfigurationRegistry configurationRegistry) {
        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        when(configuration.workflowName()).thenReturn(WORKFLOW_NAME);
        when(configuration.workflowVersion()).thenReturn("1.0.0");
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        doAnswer(contextInvocation -> {
            var workflowId = contextInvocation.<String>getArgument(1);
            var execution = WorkflowExecutionFixture.mockExecution(
                    workflowId, sourced.orTimeout(10, TimeUnit.SECONDS).join(), false
            );
            doAnswer(startInvocation -> {
                onStart.run();
                bodyStarts.add(workflowId);
                return CompletableFuture.completedFuture(null);
            }).when(execution).execute(any());
            var cancellation = mock(WorkflowCancellation.Request.class);
            lastCancellation.set(cancellation);
            when(((WorkflowCancellationProvider) execution).workflowCancellation()).thenReturn(cancellation);
            var pending = new CompletableFuture<Void>();
            when(cancellation.requestWorkflowCancellation(any())).thenReturn(pending);
            doAnswer(abortInvocation -> pending.completeExceptionally(abortInvocation.getArgument(0)))
                    .when(cancellation).abortPendingWorkflowCancellation(any());
            doAnswer(eventInvocation -> {
                onEvent.run();
                return null;
            }).when(execution).onEvent(any(), any());
            // The registry hands a woken execution its safe point listener right after the wake swap.
            doAnswer(listenerInvocation -> {
                onWoken.run();
                return null;
            }).when(execution).registerCheckpointWorkStateListener(any());
            var workflowContext = mock(WorkflowContext.class);
            when(executionFactory.create(workflowContext)).thenReturn(execution);
            return workflowContext;
        }).when(contextFactory).createContext(anyMap(), any(), any(), eq(configuration));
        configurationRegistry.register(new QualifiedName(WORKFLOW_NAME + "Started"), configuration);
    }
}
