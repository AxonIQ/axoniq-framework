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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;

import java.lang.reflect.Field;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
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
    void requestWorkflowCancellation_concurrentCallersShareOnePendingRequest() throws Exception {
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
    void requestWorkflowCancellation_returnsFirstRequestFuture_whenDriverConsumesRequestAfterSecondCallersFailedCas()
            throws Exception {
        var cancellation = requestForRealExecution();
        var pendingRequest = pendingRequestReference();
        replacePendingRequest(cancellation, pendingRequest);

        var storedRequest = new AtomicReference<WorkflowCancellation.PendingRequest>();
        var secondCallerThread = new AtomicReference<Thread>();
        var compareAndSetCalls = new AtomicInteger();
        var secondCallerFailedCas = new CountDownLatch(1);
        var allowSecondCallerToReadPendingRequest = new CountDownLatch(1);
        doAnswer(invocation -> {
            var request = invocation.getArgument(1, WorkflowCancellation.PendingRequest.class);
            if (compareAndSetCalls.incrementAndGet() == 1) {
                storedRequest.set(request);
                return true;
            }
            secondCallerFailedCas.countDown();
            return false;
        }).when(pendingRequest).compareAndSet(isNull(), any());
        doAnswer(invocation -> storedRequest.getAndSet(null)).when(pendingRequest).getAndSet(isNull());
        doAnswer(invocation -> {
            if (Thread.currentThread() == secondCallerThread.get()) {
                assertThat(allowSecondCallerToReadPendingRequest.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return storedRequest.get();
        }).when(pendingRequest).get();

        var firstRequest = cancellation.requestWorkflowCancellation(null);
        try (var callers = Executors.newSingleThreadExecutor()) {
            var secondRequest = callers.submit(() -> {
                secondCallerThread.set(Thread.currentThread());
                return cancellation.requestWorkflowCancellation(null);
            });

            assertThat(secondCallerFailedCas.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(cancellation.consumeWorkflowCancellation()).isNotNull();
            allowSecondCallerToReadPendingRequest.countDown();

            assertThat(secondRequest.get(5, TimeUnit.SECONDS)).isSameAs(firstRequest);
        }
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<WorkflowCancellation.PendingRequest> pendingRequestReference() {
        return mock(AtomicReference.class);
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

    private static void replacePendingRequest(DefaultWorkflowCancellation cancellation,
                                              AtomicReference<WorkflowCancellation.PendingRequest> pendingRequest)
            throws ReflectiveOperationException {
        Field field = DefaultWorkflowCancellation.class.getDeclaredField("pendingWorkflowCancellation");
        field.setAccessible(true);
        field.set(cancellation, pendingRequest);
    }

    private DefaultWorkflowCancellation requestForRealExecution() {
        var processingContext = mock(ProcessingContext.class);
        when(processingContext.component(UnitOfWorkFactory.class)).thenReturn(new SimpleUnitOfWorkFactory(processingContext));
        when(processingContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(processingContext.component(EventSink.class)).thenReturn(new NoOpEventSink());
        when(processingContext.component(WorkflowScheduler.class)).thenReturn(new DefaultWorkflowScheduler(Clock.systemUTC()));
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
