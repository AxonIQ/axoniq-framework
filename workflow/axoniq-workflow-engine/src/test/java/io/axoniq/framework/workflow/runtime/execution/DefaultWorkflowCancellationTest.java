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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link DefaultWorkflowCancellation}
 * @author Simon Zambrovski
 */
class DefaultWorkflowCancellationTest {

    private final List<ExecutorService> executorServices = new ArrayList<>();

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
            var second = callers.submit(() -> requestWhenBothCallersAreReady(cancellation, callersReady, startTogether));

            assertThat(callersReady.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(first.get(5, TimeUnit.SECONDS)).isSameAs(second.get(5, TimeUnit.SECONDS));
        }

        assertThat(cancellation.consumeWorkflowCancellation()).isNotNull();
        assertThat(cancellation.consumeWorkflowCancellation()).isNull();
    }

    @Test
    void unregisteringWorkflowWithPendingCancellationCompletesTheRequestExceptionally() {
        var cancellation = requestForRealExecution();
        var service = new WorkflowCancellationService();
        service.register("workflow-id", cancellation);
        var request = service.requestWorkflowCancellation("workflow-id", null);

        service.unregister("workflow-id");

        assertThat(request).isCompletedExceptionally();
        assertThatThrownBy(request::join)
                .isInstanceOf(java.util.concurrent.CancellationException.class)
                .satisfies(error -> assertThat(error.getCause() == null ? error : error.getCause())
                        .isInstanceOf(java.util.concurrent.CancellationException.class)
                        .hasMessage("Workflow execution completed before cancellation was performed"));
        assertThat(cancellation.hasPendingWorkflowCancellation()).isFalse();
    }

    private static CompletableFuture<Void> requestWhenBothCallersAreReady(
            DefaultWorkflowCancellation cancellation,
            CountDownLatch callersReady,
            CyclicBarrier startTogether
    ) throws Exception {
        callersReady.countDown();
        startTogether.await(5, TimeUnit.SECONDS);
        return cancellation.requestWorkflowCancellation(null);
    }

    private DefaultWorkflowCancellation requestForRealExecution() {
        var processingContext = mock(ProcessingContext.class);
        when(processingContext.component(UnitOfWorkFactory.class)).thenReturn(new SimpleUnitOfWorkFactory(processingContext));
        when(processingContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(processingContext.component(EventStore.class)).thenReturn(mock(EventStore.class));
        when(processingContext.component(WorkflowScheduler.class)).thenReturn(new ControllableWorkflowScheduler());
        when(processingContext.component(ExecuteStepActionResolver.class)).thenReturn(new DefaultExecuteStepActionResolver());

        var executor = Executors.newSingleThreadExecutor();
        executorServices.add(executor);
        when(processingContext.component(eq(ExecutorService.class), any())).thenReturn(executor);
        var execution = new SimpleWorkflowExecution(
                "workflow-id", Map.of(), processingContext, new TestWorkflowConfiguration(), mock(WorkflowContext.class)
        );
        return (DefaultWorkflowCancellation) execution.workflowCancellation();
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
