/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. You may not use this file except in compliance
 * with the License.
 *
 * You may obtain a copy of the License at:
 * https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 * https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SimpleWorkflowExecution}.
 *
 * @author Simon Zambrovski
 */
class SimpleWorkflowExecutionTest {

    private final List<ExecutorService> executorServices = new ArrayList<>();

    @AfterEach
    void shutDownExecutors() {
        executorServices.forEach(ExecutorService::shutdownNow);
    }

    @Test
    void hasTasksReflectsWhetherTheTaskQueueContainsTasks() {
        var execution = execution();

        assertThat(execution.hasTasks()).isFalse();

        execution.appendTask(ignored -> {
        });

        assertThat(execution.hasTasks()).isTrue();
    }

    @Test
    void workflowCancellationExposesPendingRequestToWorkflowDriver() {
        var execution = execution();
        var cancellation = (WorkflowCancellation.Request) execution.workflowCancellation();
        var cause = new IllegalStateException("operator requested cancellation");

        var completion = cancellation.requestWorkflowCancellation(cause);

        assertThat(completion).isNotCompleted();
        assertThat(cancellation.hasPendingWorkflowCancellation()).isTrue();
        var pendingRequest = cancellation.consumeWorkflowCancellation();
        assertThat(pendingRequest).isNotNull();
        assertThat(pendingRequest.cause()).isInstanceOf(WorkflowCancelledException.class).hasCause(cause);
        assertThat(cancellation.hasPendingWorkflowCancellation()).isFalse();

        pendingRequest.callback().complete(null);

        assertThat(completion).isCompleted();
    }

    @Test
    void interruptWorkflowDriverInterruptsAssignedWorkflowDriver() throws Exception {
        var execution = execution();
        var driverReady = new CountDownLatch(1);
        var interrupted = new CompletableFuture<Boolean>();
        var driver = Thread.ofPlatform().start(() -> {
            driverReady.countDown();
            try {
                new CountDownLatch(1).await();
                interrupted.complete(false);
            } catch (InterruptedException e) {
                interrupted.complete(true);
            }
        });
        setWorkflowDriver(execution, driver);

        assertThat(driverReady.await(5, TimeUnit.SECONDS)).isTrue();

        execution.interruptWorkflowDriver();

        assertThat(interrupted.get(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void stopForShutdownQueuesAnInterruptForWorkflowDriver() throws Exception {
        var execution = execution();

        execution.stopForShutdown();

        var task = execution.getNextTask();
        assertThat(task).isNotNull();
        try (var worker = Executors.newSingleThreadExecutor()) {
            var interrupted = worker.submit(() -> {
                task.accept(execution);
                return Thread.interrupted();
            });

            assertThat(interrupted.get(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void futureResolutionTimeoutStopsRuntimeWithoutTerminatingWorkflow() throws Exception {
        var timeout = new FutureResolutionTimeoutException(new TimeoutException("publication timed out"));
        var eventSink = mock(EventSink.class);
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.failedFuture(timeout));
        var execution = execution(eventSink, new DirectExecutorService());
        var terminationHandlerCalled = new CountDownLatch(1);

        assertThatThrownBy(() -> execution.execute(ignored -> terminationHandlerCalled.countDown()))
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .satisfies(exception -> assertThat(exception.getCause()).isSameAs(timeout));

        verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
        assertThat(terminationHandlerCalled.await(200, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(execution.isRunning()).isTrue();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.NONE);
    }

    @Test
    void futureResolutionTimeoutDuringCompletionLeavesWorkflowStarted() throws Exception {
        var timeout = new FutureResolutionTimeoutException(new TimeoutException("completion publication timed out"));
        var eventSink = failedPublicationSink(timeout);
        var execution = execution(eventSink, new DirectExecutorService());
        markStarted(execution);
        var terminationHandlerCalled = new CountDownLatch(1);

        assertThatThrownBy(() -> execution.execute(ignored -> terminationHandlerCalled.countDown()))
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .satisfies(exception -> assertThat(exception.getCause()).isSameAs(timeout));

        verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
        assertThat(terminationHandlerCalled.await(200, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(execution.isRunning()).isTrue();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void futureResolutionTimeoutDuringCancellationCompletesRequesterExceptionally() {
        var timeout = new FutureResolutionTimeoutException(new TimeoutException("cancellation publication timed out"));
        var execution = execution(failedPublicationSink(timeout), new DirectExecutorService());
        markStarted(execution);
        var cancellation = (WorkflowCancellation.Request) execution.workflowCancellation();
        var request = cancellation.requestWorkflowCancellation(null);

        assertThatThrownBy(() -> execution.execute(ignored -> { }))
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .satisfies(exception -> assertThat(exception.getCause()).isSameAs(timeout));

        assertThat(request).isCompletedExceptionally();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void futureResolutionTimeoutWhilePublishingFailureLeavesWorkflowNonTerminal() {
        var timeout = new FutureResolutionTimeoutException(new TimeoutException("failure publication timed out"));
        var execution = execution(
                failedPublicationSink(timeout),
                new DirectExecutorService(),
                ignored -> {
                    throw new WorkflowFailedException("business failure");
                }
        );
        markStarted(execution);

        assertThatThrownBy(() -> execution.execute(ignored -> { }))
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .satisfies(exception -> assertThat(exception.getCause()).isSameAs(timeout));

        assertThat(execution.isRunning()).isTrue();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
    }

    private static void setWorkflowDriver(SimpleWorkflowExecution execution, Thread driver)
            throws ReflectiveOperationException {
        Field field = SimpleWorkflowExecution.class.getDeclaredField("workflowThread");
        field.setAccessible(true);
        field.set(execution, driver);
    }

    private SimpleWorkflowExecution execution() {
        return execution(new NoOpEventSink());
    }

    private SimpleWorkflowExecution execution(EventSink eventSink) {
        var executor = Executors.newSingleThreadExecutor();
        executorServices.add(executor);
        return execution(eventSink, executor);
    }

    private SimpleWorkflowExecution execution(EventSink eventSink, ExecutorService executor) {
        return execution(eventSink, executor, ignored -> { });
    }

    private SimpleWorkflowExecution execution(EventSink eventSink,
                                              ExecutorService executor,
                                              WorkflowDefinition<WorkflowContext> workflowDefinition) {
        var processingContext = mock(ProcessingContext.class);
        when(processingContext.resources()).thenReturn(Map.of());
        when(processingContext.component(UnitOfWorkFactory.class)).thenReturn(new SimpleUnitOfWorkFactory(processingContext));
        when(processingContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(processingContext.component(EventSink.class)).thenReturn(eventSink);
        when(processingContext.component(EventConverter.class)).thenReturn(TestEventConverter.INSTANCE);
        when(processingContext.component(WorkflowScheduler.class)).thenReturn(new ControllableWorkflowScheduler());
        when(processingContext.component(ExecuteStepActionResolver.class)).thenReturn(new DefaultExecuteStepActionResolver());

        when(processingContext.component(eq(ExecutorService.class), any())).thenReturn(executor);
        when(processingContext.component(eq(java.util.concurrent.Executor.class), any())).thenReturn(executor);
        var workflowContext = mock(WorkflowContext.class);
        when(workflowContext.workflowId()).thenReturn("workflow-id");
        when(workflowContext.workflowVersion()).thenReturn("0.0.1");
        when(workflowContext.workflowPayload()).thenReturn(Map.of());
        when(workflowContext.processingContext()).thenReturn(processingContext);
        return new SimpleWorkflowExecution(
                "workflow-id", Map.of(), processingContext, new TestWorkflowConfiguration(workflowDefinition), workflowContext
        );
    }

    private static EventSink failedPublicationSink(FutureResolutionTimeoutException timeout) {
        var eventSink = mock(EventSink.class);
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.failedFuture(timeout));
        return eventSink;
    }

    private static void markStarted(SimpleWorkflowExecution execution) {
        var definition = execution.state().workflowDefinitionId();
        execution.onEvent(
                new GenericEventMessage(
                        new MessageType(new QualifiedName("test-workflow"), definition.version()),
                        Map.of(),
                        MetadataUtils.create(execution.workflowId(), WorkflowStatus.STARTED, definition)
                ),
                execution.processingContext()
        );
    }

    private static final class DirectExecutorService extends AbstractExecutorService {

        @Override
        public void shutdown() {
            // No-op
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    private static final class TestWorkflowConfiguration implements WorkflowConfiguration<WorkflowContext> {

        private final WorkflowDefinition<WorkflowContext> workflowDefinition;

        private TestWorkflowConfiguration() {
            this(ignored -> { });
        }

        private TestWorkflowConfiguration(WorkflowDefinition<WorkflowContext> workflowDefinition) {
            this.workflowDefinition = workflowDefinition;
        }

        @Override
        public Class<WorkflowContext> getWorkflowContextType() {
            return WorkflowContext.class;
        }

        @Override
        public WorkflowDefinition<WorkflowContext> workflowDefinition() {
            return workflowDefinition;
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

    private static final class NoOpEventSink implements EventSink {

        @Override
        public CompletableFuture<Void> publish(ProcessingContext context, List<? extends EventMessage> events) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // No-op
        }
    }
}
