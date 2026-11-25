/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link DefaultWorkflowCancellation}
 *
 * @author Simon Zambrovski
 */
class DefaultWorkflowCancellationTest {

    private final List<ExecutorService> executorServices = new ArrayList<>();

    private static CompletableFuture<Void> requestWhenBothCallersAreReady(
            DefaultWorkflowCancellation cancellation,
            CountDownLatch callersReady,
            CyclicBarrier startTogether
    ) throws Exception {
        callersReady.countDown();
        startTogether.await(5, TimeUnit.SECONDS);
        return cancellation.requestWorkflowCancellation(null);
    }

    @AfterEach
    void shutDownExecutors() {
        executorServices.forEach(ExecutorService::shutdownNow);
    }

    @Test
    void requestWorkflowCancellationConcurrentCallersShareOnePendingRequest() throws Exception {
        var cancellation = requestForRealExecution();
        var callersReady = new CountDownLatch(2);
        var startTogether = new CyclicBarrier(2);

        try (var callers = Executors.newFixedThreadPool(2)) {
            var first = callers.submit(() -> requestWhenBothCallersAreReady(cancellation, callersReady, startTogether));
            var second = callers.submit(() -> requestWhenBothCallersAreReady(cancellation,
                                                                             callersReady,
                                                                             startTogether));

            assertThat(callersReady.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(first.get(5, TimeUnit.SECONDS)).isSameAs(second.get(5, TimeUnit.SECONDS));
        }

        assertThat(cancellation.consumeWorkflowCancellation()).isNotNull();
        assertThat(cancellation.consumeWorkflowCancellation()).isNull();
    }

    @Test
    void leavingMemoryWithPendingCancellationCompletesTheRequestExceptionally() {
        var execution = realExecution();
        var cancellation = (DefaultWorkflowCancellation) execution.workflowCancellation();
        var liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        liveWorkflows.enter("workflow-id", false, () -> execution);
        liveWorkflows.markCancellable(execution);
        var service = new WorkflowCancellationService(liveWorkflows);
        var request = service.requestWorkflowCancellation("workflow-id", null);

        liveWorkflows.leave(execution);

        assertThat(request).isCompletedExceptionally();
        assertThatThrownBy(request::join)
                .isInstanceOf(CancellationException.class)
                .satisfies(error -> assertThat(error.getCause() == null ? error : error.getCause())
                        .isInstanceOf(CancellationException.class)
                        .hasMessage("Workflow execution completed before cancellation was performed"));
        assertThat(cancellation.hasPendingWorkflowCancellation()).isFalse();
    }

    @Test
    void aFailedStartOfAWokenExecutionAbortsAWorkflowCancellationItsBodyMakesMeanwhile() {
        // given - a woken execution whose body started
        var execution = realExecution();
        var cancellation = (DefaultWorkflowCancellation) execution.workflowCancellation();
        var liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        var hibernated = hibernationOf(liveWorkflows, "workflow-id");
        assertThat(liveWorkflows.adopt(hibernated)).isTrue();
        assertThat(liveWorkflows.wake(hibernated, execution)).isTrue();
        liveWorkflows.markCancellable(execution);
        // and a workflow cancellation queued as its task, during which the start fails: after the failed start marked
        // the execution left, and before the request is pending, so only the check of the request itself can abort it
        var request = liveWorkflows.request("workflow-id", requested -> {
            try (var ignored = liveWorkflows.lock("workflow-id")) {
                liveWorkflows.failStart(execution, new IllegalStateException("The start failed."));
            }
            return requested.requestWorkflowCancellation(null);
        });

        // when - the body takes the task
        var task = execution.getNextTask();
        assertThat(task).isNotNull();
        task.accept(execution);

        // then - the request fails, instead of waiting for an execution that never performs it
        assertThat(request).isCompletedExceptionally();
        assertThatThrownBy(request::join)
                .isInstanceOf(CancellationException.class)
                .hasMessage("Workflow execution failed to start before cancellation was performed")
                .cause()
                .hasMessage("The start failed.");
        assertThat(cancellation.hasPendingWorkflowCancellation()).isFalse();
    }

    @Test
    void aRequestTheBodyOfAWokenExecutionTakesAsTheExecutionLeavesMemoryIsMadeAgain() {
        // given - a woken execution whose body started
        var execution = realExecution();
        var cancellation = execution.workflowCancellation();
        var liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        var hibernated = hibernationOf(liveWorkflows, "workflow-id");
        assertThat(liveWorkflows.adopt(hibernated)).isTrue();
        assertThat(liveWorkflows.wake(hibernated, execution)).isTrue();
        liveWorkflows.markCancellable(execution);
        // and a step cancellation queued as its task
        var service = new WorkflowCancellationService(liveWorkflows);
        var request = service.requestStepCancellation("workflow-id", "step", null);
        var task = execution.getNextTask();
        assertThat(task).isNotNull();
        // and a pending workflow cancellation, whose abort lets the body take the queued request while the execution
        // leaves memory: after the leave marked the execution left, and before it answers the requests queued on it
        cancellation.requestWorkflowCancellation(null).whenComplete((ignored, aborted) -> task.accept(execution));

        // when
        liveWorkflows.leave(execution);

        // then - the body finds the execution left, so it does not make the request against it, but makes it again,
        // which finds the workflow gone from this node
        assertThat(request).failsWithin(Duration.ofSeconds(5))
                           .withThrowableThat()
                           .havingRootCause()
                           .isInstanceOf(NoSuchElementException.class);
    }

    /**
     * Returns a hibernation of the given workflow under a claim of the whole segment space, without open waits.
     */
    private static LiveWorkflows.Hibernated hibernationOf(LiveWorkflows liveWorkflows, String workflowId) {
        var segmentClaims = new SegmentClaims();
        segmentClaims.initialize(null);
        var claim = segmentClaims.startClaim(Segment.ROOT_SEGMENT, new StubProcessingContext());
        var support = new WorkflowEngineHibernationSupport(new ControllableWorkflowScheduler(),
                                                           Clock.systemUTC(),
                                                           liveWorkflows,
                                                           new RunningWorkflowsProjection(WorkflowIndex.none(),
                                                                                          "orders"),
                                                           (toWake, beforeStart) -> false,
                                                           Duration.ofMinutes(1),
                                                           Runnable::run);
        var hint = new HibernationHint(Instant.now().plus(Duration.ofHours(1)), List.of());
        return support.hibernationOf(claim, workflowId, hint, CompletableFuture.completedFuture(null));
    }

    private DefaultWorkflowCancellation requestForRealExecution() {
        return (DefaultWorkflowCancellation) realExecution().workflowCancellation();
    }

    private SimpleWorkflowExecution realExecution() {
        var processingContext = mock(ProcessingContext.class);
        when(processingContext.component(UnitOfWorkFactory.class)).thenReturn(new SimpleUnitOfWorkFactory(
                processingContext));
        when(processingContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(processingContext.component(EventStore.class)).thenReturn(mock(EventStore.class));
        when(processingContext.component(WorkflowScheduler.class)).thenReturn(new ControllableWorkflowScheduler());
        when(processingContext.component(ExecuteStepActionResolver.class)).thenReturn(new DefaultExecuteStepActionResolver());

        var executor = Executors.newSingleThreadExecutor();
        executorServices.add(executor);
        when(processingContext.component(eq(ExecutorService.class), any())).thenReturn(executor);
        return new SimpleWorkflowExecution(
                "workflow-id", Map.of(), processingContext, new TestWorkflowConfiguration(), mock(WorkflowContext.class)
        );
    }

    private static final class TestWorkflowConfiguration implements WorkflowConfiguration<WorkflowContext> {

        @Override
        public Class<WorkflowContext> getWorkflowContextType() {
            return WorkflowContext.class;
        }

        @Override
        public WorkflowDefinition<WorkflowContext> workflowDefinition() {
            throw new UnsupportedOperationException("The workflow definition is not used by this test");
        }

        @Override
        public WorkflowContextFactory<WorkflowContext> workflowContextFactory() {
            throw new UnsupportedOperationException("The workflow context factory is not used by this test");
        }

        @Override
        public WorkflowExecutionFactory workflowExecutionFactory() {
            throw new UnsupportedOperationException("The workflow execution factory is not used by this test");
        }

        @Override
        public WorkflowIdProvider workflowIdProvider() {
            throw new UnsupportedOperationException("The workflow id provider is not used by this test");
        }

        @Override
        public String workflowName() {
            return "test-workflow";
        }

        @Override
        public DefaultEventNameCustomizer eventNameCustomizer() {
            return defaults();
        }
    }
}
