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

import io.axoniq.framework.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
    void anEventPublishedByAnotherInstanceFeedsWaitConditionsButDoesNotEvolveState() {
        // given: a started execution, and a business event another workflow published through the publish primitive
        // (it carries that publisher's step metadata: workflowId, stepName, stepType=COMPLETED, stepPrimitive=PUBLISH)
        var execution = execution();
        markStarted(execution);
        var foreignPublish = new GenericEventMessage(
                new MessageType("io.acme.OrderApproved"),
                Map.of("orderId", "o-1"),
                MetadataUtils.create("another-workflow", "notifyApproved", StepStatus.COMPLETED)
                             .and(MetadataUtils.METADATA_KEY_STEP_PRIMITIVE, MetadataUtils.STEP_PRIMITIVE_PUBLISH)
        ).withConverter(TestEventConverter.INSTANCE);

        // when
        execution.onEvent(foreignPublish, execution.processingContext());

        // then: the wait conditions are offered the event (a task is queued), the publisher's step is not this state's
        assertThat(execution.hasTasks()).isTrue();
        assertThat(execution.state().containsStep("notifyApproved")).isFalse();
    }

    @Test
    void anEventThisInstancePublishedEvolvesItsOwnState() {
        // given
        var execution = execution();
        markStarted(execution);
        var ownPublish = new GenericEventMessage(
                new MessageType("io.acme.OrderApproved"),
                Map.of("orderId", "o-1"),
                MetadataUtils.create(execution.workflowId(), "notifyApproved", StepStatus.COMPLETED)
                             .and(MetadataUtils.METADATA_KEY_STEP_PRIMITIVE, MetadataUtils.STEP_PRIMITIVE_PUBLISH)
        ).withConverter(TestEventConverter.INSTANCE);

        // when
        execution.onEvent(ownPublish, execution.processingContext());

        // then
        assertThat(execution.state().containsStep("notifyApproved")).isTrue();
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
    void futureResolutionTimeoutStopsAndCleansUpRuntimeWithoutTerminatingWorkflow() throws Exception {
        var timeout = new FutureResolutionTimeoutException(new TimeoutException("publication timed out"));
        var eventSink = failingEventStore(timeout);
        var execution = execution(eventSink, new DirectExecutorService());
        var terminationHandlerCalled = new CountDownLatch(1);
        var checkpointLatchReleased = new CountDownLatch(1);
        execution.addCheckpointLatch(checkpointLatchReleased::countDown);

        assertThatThrownBy(() -> execution.execute(ignored -> terminationHandlerCalled.countDown()).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(FutureResolutionTimeoutException.class);

        verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
        assertThat(terminationHandlerCalled.await(200, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(checkpointLatchReleased.await(200, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(execution.isRunning()).isFalse();
        assertThat(execution.hasTasks()).isFalse();
        assertThat(execution.hasUnsafeCheckpointWork()).isFalse();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.NONE);

        markStarted(execution);

        assertThat(execution.hasTasks()).isFalse();
        assertThat(execution.hasUnsafeCheckpointWork()).isFalse();
    }

    @Test
    void futureResolutionTimeoutDuringCompletionLeavesWorkflowStarted() throws Exception {
        var timeout = new FutureResolutionTimeoutException(new TimeoutException("completion publication timed out"));
        var eventSink = failingEventStore(timeout);
        var execution = execution(eventSink, new DirectExecutorService());
        markStarted(execution);
        var terminationHandlerCalled = new CountDownLatch(1);

        assertThatThrownBy(() -> execution.execute(ignored -> terminationHandlerCalled.countDown()).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(FutureResolutionTimeoutException.class);

        verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
        assertThat(terminationHandlerCalled.await(200, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(execution.isRunning()).isFalse();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void futureResolutionTimeoutDuringCancellationCompletesRequesterExceptionally() {
        var timeout = new FutureResolutionTimeoutException(new TimeoutException("cancellation publication timed out"));
        var execution = execution(failingEventStore(timeout), new DirectExecutorService());
        markStarted(execution);
        var cancellation = (WorkflowCancellation.Request) execution.workflowCancellation();
        var request = cancellation.requestWorkflowCancellation(null);

        assertThatThrownBy(() -> execution.execute(ignored -> {
        }).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(FutureResolutionTimeoutException.class);

        assertThat(request).isCompletedExceptionally();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void checkedPublicationFailureDuringCancellationCompletesRequesterExceptionally() {
        var failure = new IOException("cancellation publication failed");
        var execution = execution(
                failingEventStore(failure),
                new DirectExecutorService()
        );
        markStarted(execution);
        var cancellation = (WorkflowCancellation.Request) execution.workflowCancellation();
        var request = cancellation.requestWorkflowCancellation(null);

        execution.execute(ignored -> {
        }).join();

        assertThat(request).isCompletedExceptionally();
        assertThatThrownBy(request::join)
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .satisfies(exception -> assertThat(exception.getCause()).isSameAs(failure));
    }

    @Test
    void futureResolutionTimeoutWhilePublishingFailureLeavesWorkflowNonTerminal() {
        var timeout = new FutureResolutionTimeoutException(new TimeoutException("failure publication timed out"));
        var execution = execution(
                failingEventStore(timeout),
                new DirectExecutorService(),
                ignored -> {
                    throw new WorkflowFailedException("business failure");
                }
        );
        markStarted(execution);

        assertThatThrownBy(() -> execution.execute(ignored -> {
        }).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(FutureResolutionTimeoutException.class);

        assertThat(execution.isRunning()).isFalse();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void concurrentWorkflowEventAppendsReachTheEventStoreOneAtATime() throws Exception {
        var eventStore = eventStore();
        var firstPublicationStarted = new CountDownLatch(1);
        var secondPublicationStarted = new CountDownLatch(1);
        var publications = new CopyOnWriteArrayList<CompletableFuture<Void>>();
        var inFlight = new AtomicInteger();
        var maximumInFlight = new AtomicInteger();
        when(eventStore.publish(any(ProcessingContext.class), any(EventMessage.class))).thenAnswer(invocation -> {
            var publication = new CompletableFuture<Void>();
            publications.add(publication);
            maximumInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            firstPublicationStarted.countDown();
            if (publications.size() == 2) {
                secondPublicationStarted.countDown();
            }
            return publication;
        });
        var execution = execution(eventStore, new DirectExecutorService());
        var first = CompletableFuture.supplyAsync(() -> execution.appendWorkflowEvent(event("first"),
                                                                                      execution.processingContext()));

        assertThat(firstPublicationStarted.await(5, TimeUnit.SECONDS)).isTrue();
        var second = CompletableFuture.supplyAsync(() -> execution.appendWorkflowEvent(event("second"),
                                                                                       execution.processingContext()));

        assertThat(second.get(5, TimeUnit.SECONDS)).isNotCompleted();
        assertThat(publications).hasSize(1);
        inFlight.decrementAndGet();
        publications.getFirst().complete(null);

        assertThat(secondPublicationStarted.await(5, TimeUnit.SECONDS)).isTrue();
        inFlight.decrementAndGet();
        publications.get(1).complete(null);

        first.get(5, TimeUnit.SECONDS).join();
        second.get(5, TimeUnit.SECONDS).join();
        assertThat(maximumInFlight).hasValue(1);
    }

    private static void setWorkflowDriver(SimpleWorkflowExecution execution, Thread driver)
            throws ReflectiveOperationException {
        Field field = SimpleWorkflowExecution.class.getDeclaredField("workflowThread");
        field.setAccessible(true);
        field.set(execution, driver);
    }

    private SimpleWorkflowExecution execution() {
        return execution(eventStore());
    }

    private SimpleWorkflowExecution execution(EventStore eventStore) {
        var executor = Executors.newSingleThreadExecutor();
        executorServices.add(executor);
        return execution(eventStore, executor);
    }

    private SimpleWorkflowExecution execution(EventStore eventStore, ExecutorService executor) {
        return execution(eventStore, executor, ignored -> {
        });
    }

    private SimpleWorkflowExecution execution(EventStore eventStore,
                                              ExecutorService executor,
                                              WorkflowDefinition<WorkflowContext> workflowDefinition) {
        var processingContext = mock(ProcessingContext.class);
        when(processingContext.resources()).thenReturn(Map.of());
        when(processingContext.component(UnitOfWorkFactory.class)).thenReturn(new SimpleUnitOfWorkFactory(
                processingContext));
        when(processingContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(processingContext.component(EventStore.class)).thenReturn(eventStore);
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
                "workflow-id",
                Map.of(),
                processingContext,
                new TestWorkflowConfiguration(workflowDefinition),
                workflowContext
        );
    }

    private static EventMessage event(String name) {
        return new GenericEventMessage(new MessageType(name), Map.of()).withConverter(TestEventConverter.INSTANCE);
    }

    private static EventStore failingEventStore(FutureResolutionTimeoutException timeout) {
        return failingEventStore((Throwable) timeout);
    }

    private static EventStore failingEventStore(Throwable failure) {
        var eventStore = eventStore();
        when(eventStore.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.failedFuture(failure));
        return eventStore;
    }

    /**
     * Returns an event store that accepts every append. Workflow events append under a condition, which only an event
     * store transaction carries, so a plain event sink is refused before a single event is published.
     */
    private static EventStore eventStore() {
        var eventStore = mock(EventStore.class);
        when(eventStore.transaction(any(ProcessingContext.class))).thenReturn(mock(EventStoreTransaction.class));
        when(eventStore.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        return eventStore;
    }

    private static void markStarted(SimpleWorkflowExecution execution) {
        var definition = execution.state().workflowDefinitionId();
        execution.onEvent(
                new GenericEventMessage(
                        new MessageType(new QualifiedName("test-workflow"), definition.version()),
                        Map.of(),
                        MetadataUtils.create(execution.workflowId(), WorkflowStatus.STARTED, definition)
                ).withConverter(TestEventConverter.INSTANCE),
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
            this(ignored -> {
            });
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
}
