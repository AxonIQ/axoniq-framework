/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests how the hibernation support records the hibernated workflows of a segment and adopts them on a claim: a wake
 * of an adopted workflow before its segment started, a claim that is released before it completes, and engines of 2
 * workflow modules that share segment ids.
 *
 * @author Stefan Dragisic
 */
class WorkflowEngineHibernationSupportRecordTest {

    private static final Segment SEGMENT = Segment.ROOT_SEGMENT;
    private static final HibernatedWorkflows.HibernatedWorkflow WAITING_FOR_PAYMENT =
            new HibernatedWorkflows.HibernatedWorkflow(Set.of("awaitPayment"),
                                                       Instant.now().plus(Duration.ofHours(1)).toString());

    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
    private final WorkflowScheduler scheduler = new DefaultWorkflowScheduler(Clock.systemUTC(), timer);
    private final InMemoryWorkflowExecutionRepository repository = new InMemoryWorkflowExecutionRepository();
    private final WorkflowCancellationService cancellationService = new WorkflowCancellationService();
    private final RecordingWorkflowStore store = new RecordingWorkflowStore();
    private WorkflowEngineHibernationSupport support;

    @BeforeEach
    void setUp() {
        support = support("orders");
        support.start();
    }

    @AfterEach
    void tearDown() {
        support.stop();
        timer.shutdownNow();
    }

    @Nested
    class AdoptedWorkflow {

        @Test
        void completesACancellationRequestedBeforeAnEventWokeItAheadOfItsSegmentStart() {
            // given: a claim adopted the workflow, but has not started its segment, so the workflow may not wake yet
            var claim = support.claiming(SEGMENT);
            assertThat(support.adopt("order-1", SEGMENT, WAITING_FOR_PAYMENT, claim)).isTrue();
            var cancellation = cancellationService.requestWorkflowCancellation("order-1", null);
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1)).until(() -> !cancellation.isDone());

            // when: an event of the segment's catch-up wakes it
            var payment = new GenericEventMessage(new MessageType(new QualifiedName("PaymentReceived")), Map.of());
            support.deliver("order-1", payment, new StubProcessingContext());

            // then: the held cancellation runs against the woken execution instead of waiting forever
            assertThat(cancellation).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Nested
    class ReleasedClaim {

        @Test
        void keepsTheRecordOfTheLastCompletedClaim() {
            // given: a completed claim recorded the workflow on release
            var completed = support.claiming(SEGMENT);
            support.adopt("order-1", SEGMENT, WAITING_FOR_PAYMENT, completed);
            support.claimed(SEGMENT, completed);
            support.release(SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();

            // when: the next claim times out and is released before it completes
            support.claiming(SEGMENT);
            support.release(SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();

            // then: its empty view of the segment did not replace the record
            assertThat(store.writes).hasValue(1);
            assertThat(recordOf(support).workflows()).containsOnlyKeys("order-1");
        }

        @Test
        void adoptsNothingOnceTheSegmentIsReleased() {
            // given: a claim that is still loading when its segment is released
            var claim = support.claiming(SEGMENT);
            support.release(SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();

            // when: its loading finishes and it adopts
            var adopted = support.adopt("order-1", SEGMENT, WAITING_FOR_PAYMENT, claim);

            // then: the workflow stays with the segment's next claim
            assertThat(adopted).isFalse();
            assertThat(support.isHibernated("order-1")).isFalse();
        }
    }

    @Nested
    class EnginesOfTwoModules {

        @Test
        void recordTheirWorkflowsApartForTheSameSegment() {
            // given: 2 engines that hold segment 0 of their own processor, on 1 store
            var billing = support("billing");
            billing.start();
            try {
                recordOnRelease(support, "order-1");
                recordOnRelease(billing, "invoice-1");

                // when
                var orders = recordOf(support);
                var invoices = recordOf(billing);

                // then
                assertThat(orders.workflows()).containsOnlyKeys("order-1");
                assertThat(invoices.workflows()).containsOnlyKeys("invoice-1");
            } finally {
                billing.stop();
            }
        }
    }

    private WorkflowEngineHibernationSupport support(String name) {
        return new WorkflowEngineHibernationSupport(
                scheduler, Clock.systemUTC(), repository, cancellationService, new UnsafeCheckpointWorkIndex(),
                unitOfWorkFactory(), () -> Duration.ofSeconds(5), this::load, store, name, Duration.ofSeconds(30),
                command -> Thread.ofVirtual().start(command)
        );
    }

    private static void recordOnRelease(WorkflowEngineHibernationSupport support, String workflowId) {
        var claim = support.claiming(SEGMENT);
        support.adopt(workflowId, SEGMENT, WAITING_FOR_PAYMENT, claim);
        support.claimed(SEGMENT, claim);
        support.release(SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static HibernatedWorkflows recordOf(WorkflowEngineHibernationSupport support) {
        return support.loadRecordedWorkflows(SEGMENT, new StubProcessingContext())
                      .orTimeout(5, TimeUnit.SECONDS)
                      .join();
    }

    /**
     * Loads a woken workflow the way the engine does: the execution enters the repository, its own cancellation
     * replaces the stand-in, and the given action runs on it. Its tasks run at once.
     */
    private void load(String workflowId,
                      ProcessingContext executionContext,
                      @Nullable Segment segment,
                      Consumer<WorkflowExecution> beforeStart) {
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn(workflowId);
        doAnswer(invocation -> {
            Consumer<WorkflowExecution> task = invocation.getArgument(0);
            task.accept(execution);
            return null;
        }).when(execution).appendTask(any());
        repository.save(workflowId, () -> execution);
        cancellationService.register(workflowId, new CompletingCancellation());
        beforeStart.accept(execution);
    }

    private static SimpleUnitOfWorkFactory unitOfWorkFactory() {
        return new SimpleUnitOfWorkFactory(new ApplicationContext() {
            @Override
            public <C> C component(Class<C> type, String name) {
                throw new ComponentNotFoundException(type, name);
            }
        });
    }

    /**
     * Keeps the recorded hibernated workflows in memory and counts the writes. Loading workflow state is not needed by
     * these tests.
     */
    private static final class RecordingWorkflowStore implements WorkflowStore {

        private final Map<String, HibernatedWorkflows> records = new ConcurrentHashMap<>();
        private final AtomicInteger writes = new AtomicInteger();

        @Override
        public CompletableFuture<HibernatedWorkflows> loadHibernatedWorkflows(String identifier,
                                                                               ProcessingContext processingContext) {
            return CompletableFuture.completedFuture(records.getOrDefault(identifier, HibernatedWorkflows.NONE));
        }

        @Override
        public CompletableFuture<Void> storeHibernatedWorkflows(String identifier,
                                                                HibernatedWorkflows workflows,
                                                                ProcessingContext processingContext) {
            writes.incrementAndGet();
            records.put(identifier, workflows);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<WaitAssociations> loadWaitAssociations(ProcessingContext processingContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<WorkflowState> loadWorkflow(String workflowId, ProcessingContext processingContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                       ProcessingContext processingContext) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The cancellation of a loaded execution, which answers every request at once.
     */
    private static final class CompletingCancellation implements WorkflowCancellation {

        @Override
        public CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return CompletableFuture.completedFuture(0);
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
