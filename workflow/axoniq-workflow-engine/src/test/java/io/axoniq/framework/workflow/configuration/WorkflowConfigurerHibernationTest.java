/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.ExecuteStepDefinition;
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.api.retry.BackoffStrategy;
import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.runtime.execution.AbstractWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineHibernationPasses;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Tests, on a real configuration with an in-memory event store, that a workflow waiting only for an event or a deadline
 * leaves memory on a hibernation pass, and that the awaited event, its deadline, a cancellation or a segment claim
 * restores it from the event store.
 *
 * @author Stefan Dragisic
 */
class WorkflowConfigurerHibernationTest {

    static final String MODULE = "hibernation";
    static final int SEGMENT_COUNT = 2;
    static final String WAIT_STEP = "awaitPaid";
    static final String SLEEP_STEP = "cooldown";
    private static final String EXECUTE_STEP = "charge";
    private static final String SHIP_STEP = "awaitShipped";

    private final InMemoryTokenStore tokenStore = new InMemoryTokenStore();
    private final ControlledStorageEngine storageEngine =
            new ControlledStorageEngine(new InMemoryEventStorageEngine());
    private final List<AxonConfiguration> configurations = new ArrayList<>();
    // The hibernation interval of the next configuration, in milliseconds; 0 keeps the default.
    private long hibernationInterval;
    private final CountDownLatch chargeDone = new CountDownLatch(1);
    private final AtomicInteger flakyChargeAttempts = new AtomicInteger();

    @AfterEach
    void tearDown() {
        chargeDone.countDown();
        storageEngine.releaseHeldStart();
        configurations.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class SafeWorkflow {

        @Test
        void isRestoredAndCompletedByTheAwaitedEvent() {
            // given: a hibernated workflow, which left memory without writing an event
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);
            assertThat(durableState(configuration, workflowId).workflowStatus()).isEqualTo(WorkflowStatus.STARTED);

            // when
            publish(configuration, "paid", "order-1", "");

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void isRestoredAndCompletedAtItsDeadline() {
            // given: a sleep of 2 seconds
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "sleep", "order-1", SLEEP_STEP);
            awaitHibernated(configuration, workflowId);

            // when / then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            assertThat(durableState(configuration, workflowId).getStep(SLEEP_STEP).status())
                    .isEqualTo(StepStatus.TIMED_OUT);
        }

        @Test
        void isRestoredAndHasItsWaitCancelledByAStepCancellation() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when
            var cancelled = cancellationService(configuration).requestStepCancellation(workflowId, WAIT_STEP, null)
                                                              .orTimeout(10, TimeUnit.SECONDS)
                                                              .join();

            // then
            assertThat(cancelled).isTrue();
            await().atMost(10, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableState(configuration, workflowId).getStep(WAIT_STEP).status()
                           == StepStatus.CANCELLED);
        }

        @Test
        void isRestoredAndCancelledThroughTheWorkflowManagerByAQueryForManyInstances() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when
            find(configuration, WorkflowManager.class).orElseThrow()
                                                      .findMany(WorkflowStateQuery.byWorkflowId(workflowId))
                                                      .requestWorkflowCancellation(null)
                                                      .orTimeout(10, TimeUnit.SECONDS)
                                                      .join();

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.CANCELLED);
        }

        @Test
        void isWokenAgainWhenTheWakeAtItsDeadlineFails() {
            // given: a sleep of 2 seconds, an event store that cannot be read when the deadline expires, and a
            // hibernation interval of half a second, after which a failed wake is tried again
            hibernationInterval = 500;
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "sleep", "order-1", SLEEP_STEP);
            awaitHibernated(configuration, workflowId);
            storageEngine.failSourcing(true);

            // when: the wake at the deadline fails, and the event store recovers afterwards
            await().atMost(5, TimeUnit.SECONDS).until(() -> storageEngine.failedSourcings() > 0);
            storageEngine.failSourcing(false);

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void receivesEveryAwaitedEventWhileHibernationPassesRun() throws InterruptedException {
            // given: workflows waiting for their order to be paid, and passes running without a pause
            var configuration = start(true);
            var orderIds = IntStream.range(0, 20).mapToObj(i -> "order-" + i).toList();
            var workflowIds = orderIds.stream()
                                      .map(orderId -> startWorkflow(configuration, "wait", orderId, WAIT_STEP))
                                      .toList();
            var engine = engine(configuration);
            var passesRunning = new AtomicBoolean(true);
            var passes = Thread.ofVirtual().start(() -> {
                while (passesRunning.get()) {
                    WorkflowEngineHibernationPasses.runPass(engine);
                    LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
                }
            });

            try {
                // when: each payment can arrive while its workflow is being ejected from memory
                orderIds.forEach(orderId -> publish(configuration, "paid", orderId, ""));

                // then
                workflowIds.forEach(workflowId -> awaitWorkflowStatus(configuration,
                                                                      workflowId,
                                                                      WorkflowStatus.COMPLETED));
            } finally {
                passesRunning.set(false);
                passes.join(Duration.ofSeconds(5));
            }
        }

        @Test
        void leavesMemoryWhileACombinatorWaitsOnlyForAnEventAndADeadline() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "allMatchWaitAndSleep", "order-1", SLEEP_STEP);

            // when / then
            awaitHibernated(configuration, workflowId);
        }

        @Test
        void isRestoredByASegmentClaimAndCompletedByTheAwaitedEvent() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when: every segment is released and claimed again, which restores the workflow from the event store
            releaseAllSegments(configuration);
            publish(configuration, "paid", "order-1", "");

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }
    }

    @Nested
    class UnsafeWorkflow {

        @Test
        void staysInMemoryWhileACombinatorWaitsForARunningExecuteStep() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "anyMatchWaitAndExecute", "order-1", EXECUTE_STEP);

            // when
            awaitStaysInMemory(configuration, workflowId);
            chargeDone.countDown();

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void staysInMemoryWhileAnExecuteStepWaitsToRetry() {
            // given: an execute step whose first attempt fails, and which retries after 2 seconds
            var configuration = start(true);
            var workflowId = publish(configuration, "start", "order-1", "retry");
            await().atMost(5, TimeUnit.SECONDS).until(() -> flakyChargeAttempts.get() == 1);

            // when
            awaitStaysInMemory(configuration, workflowId);

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void staysInMemoryWhileAWaitPrimitiveWaitsForItsOwnStart() throws InterruptedException {
            // given: the workflow waits for its order's payment, and the start of its next wait is not stored yet
            var configuration = start(true);
            storageEngine.holdStartOf(SHIP_STEP);
            var workflowId = startWorkflow(configuration, "waitThenWait", "order-1", WAIT_STEP);
            assertThat(storageEngine.awaitHeldStart()).isTrue();

            // when: passes run while the second wait primitive waits for its STARTED event
            awaitStaysInMemory(configuration, workflowId);
            storageEngine.releaseHeldStart();

            // then
            awaitStepStarted(configuration, workflowId, SHIP_STEP);
            publish(configuration, "paid", "order-1", "");
            publish(configuration, "shipped", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void staysInMemoryWhileItsSegmentIsBeingReleased() {
            // given: the release of every segment has started, but has not stopped the workflow's execution yet
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            var engine = engine(configuration);
            IntStream.range(0, SEGMENT_COUNT).forEach(segmentId -> WorkflowEngineHibernationPasses.startReleaseOf(
                    engine, new Segment(segmentId, SEGMENT_COUNT - 1)
            ));

            // when / then: it is not parked on a node that no longer holds its segment
            awaitStaysInMemory(configuration, workflowId);
            assertThat(WorkflowEngineHibernationPasses.isHibernated(engine, workflowId)).isFalse();
        }

        @Test
        void staysInMemoryWhenHibernationIsDisabled() {
            // given
            var configuration = start(false);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);

            // when / then
            awaitStaysInMemory(configuration, workflowId);
        }
    }

    @Nested
    class HibernatedWorkflowOfAResizedSegment {

        @Test
        void isCompletedByItsEventAndListedNowhereAfterItsSegmentSplits() {
            // given
            var configuration = start(true, true);
            var workflowIds = startHibernatedWorkflows(configuration);

            // when: segment 0 splits into segments 0 and 2, and every workflow then receives its payment
            var processor = engineProcessor(configuration);
            assertThat(processor.splitSegment(0).orTimeout(5, TimeUnit.SECONDS).join()).isTrue();
            awaitSegments(processor, Set.of(0, 1, 2));
            workflowIds.forEach(workflowId -> publish(configuration, "paid", workflowId, ""));

            // then: none is lost, and none stays listed as hibernated under the segment it had before
            awaitCompletedAndNotHibernated(configuration, workflowIds);
        }

        @Test
        void isCompletedByItsEventAndListedNowhereAfterItsSegmentMerges() {
            // given
            var configuration = start(true, true);
            var workflowIds = startHibernatedWorkflows(configuration);

            // when: segments 0 and 1 merge into segment 0, and every workflow then receives its payment
            var processor = engineProcessor(configuration);
            assertThat(processor.mergeSegment(0).orTimeout(5, TimeUnit.SECONDS).join()).isTrue();
            awaitSegments(processor, Set.of(0));
            workflowIds.forEach(workflowId -> publish(configuration, "paid", workflowId, ""));

            // then: none is lost, and none stays listed as hibernated under the segment it had before
            awaitCompletedAndNotHibernated(configuration, workflowIds);
        }

        /**
         * Starts 8 workflows whose id is their order id, so they spread over the segments the same way in every run,
         * and waits until each one hibernated.
         */
        private List<String> startHibernatedWorkflows(AxonConfiguration configuration) {
            var workflowIds = IntStream.rangeClosed(1, 8).mapToObj(i -> "order-" + i).toList();
            workflowIds.forEach(workflowId -> {
                publish(configuration, "start", workflowId, "wait");
                awaitStepStarted(configuration, workflowId, WAIT_STEP);
            });
            workflowIds.forEach(workflowId -> awaitHibernated(configuration, workflowId));
            return workflowIds;
        }

        private static void awaitSegments(PooledStreamingEventProcessor processor, Set<Integer> segmentIds) {
            await().atMost(10, TimeUnit.SECONDS)
                   .until(() -> processor.processingStatus().keySet().equals(segmentIds));
        }

        private void awaitCompletedAndNotHibernated(AxonConfiguration configuration, List<String> workflowIds) {
            workflowIds.forEach(workflowId -> awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED));
            var engine = engine(configuration);
            assertThat(workflowIds).noneMatch(workflowId -> WorkflowEngineHibernationPasses.isHibernated(
                    engine, workflowId
            ));
        }
    }

    @Nested
    class StartOfAHibernatedWorkflow {

        @Test
        void wakesItWhenItAwaitsThatEventInsteadOfStartingADuplicate() {
            // given: the workflow id is the order id, and the workflow waits for the next start event of its order
            var configuration = start(true, true);
            publish(configuration, "start", "order-1", "waitNextStart");
            awaitStepStarted(configuration, "order-1", WAIT_STEP);
            awaitHibernated(configuration, "order-1");

            // when
            publish(configuration, "start", "order-1", "waitNextStart");

            // then
            awaitWorkflowStatus(configuration, "order-1", WorkflowStatus.COMPLETED);
        }

        @Test
        void isIgnoredSoTheWorkflowStaysCancellable() {
            // given: the workflow id is the order id, so a second start event of the order is a duplicate
            var configuration = start(true, true);
            publish(configuration, "start", "order-1", "wait");
            awaitStepStarted(configuration, "order-1", WAIT_STEP);
            awaitHibernated(configuration, "order-1");

            // when
            publish(configuration, "start", "order-1", "wait");

            // then: no duplicate execution appears, and a cancellation still reaches the hibernated workflow
            await().during(Duration.ofSeconds(1))
                   .atMost(3, TimeUnit.SECONDS)
                   .until(() -> !inMemory(configuration, "order-1"));
            cancellationService(configuration).requestWorkflowCancellation("order-1", null)
                                              .orTimeout(10, TimeUnit.SECONDS)
                                              .join();
            awaitWorkflowStatus(configuration, "order-1", WorkflowStatus.CANCELLED);
        }
    }

    /**
     * Runs the steps the start event asks for: a wait for the order's {@code paid} event or its next {@code start}
     * event, a sleep, an execute step that runs until the test lets it finish or that retries once, or a combinator of
     * them.
     */
    private void body(HibernationContext ctx) {
        var orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
        switch (String.valueOf(ctx.workflowPayload().get("kind"))) {
            case "sleep" -> ctx.waitForEvent(sleep(Duration.ofSeconds(2))).await();
            case "retry" -> ctx.awaitExecute(flakyCharge());
            case "waitNextStart" -> ctx.awaitEvent(waitFor(WAIT_STEP,
                                                           EventConditions.fromQualifiedName(
                                                                   new QualifiedName("start"),
                                                                   Associations.associate(payloadProperty("orderId"),
                                                                                          "=",
                                                                                          orderId)
                                                           ),
                                                           Duration.ofMinutes(5)));
            case "allMatchWaitAndSleep" -> ctx.allMatch(result -> true,
                                                        ctx.waitForEvent(paidOrder(orderId)),
                                                        ctx.waitForEvent(sleep(Duration.ofMinutes(5))))
                                              .await();
            case "waitThenWait" -> {
                var paid = ctx.waitForEvent(paidOrder(orderId));
                var shipped = ctx.waitForEvent(waitFor(SHIP_STEP,
                                                       EventConditions.fromQualifiedName(
                                                               new QualifiedName("shipped"),
                                                               Associations.associate(payloadProperty("orderId"),
                                                                                      "=",
                                                                                      orderId)
                                                       ),
                                                       Duration.ofMinutes(5)));
                ctx.allMatch(result -> true, paid, shipped).await();
            }
            case "anyMatchWaitAndExecute" -> ctx.anyMatch(result -> true,
                                                          ctx.waitForEvent(paidOrder(orderId)),
                                                          ctx.execute(charge()))
                                                .await();
            default -> ctx.awaitEvent(paidOrder(orderId));
        }
    }

    private static WaitForStepDefinition paidOrder(String orderId) {
        return waitFor(WAIT_STEP,
                       EventConditions.fromQualifiedName(
                               new QualifiedName("paid"),
                               Associations.associate(payloadProperty("orderId"), "=", orderId)
                       ),
                       Duration.ofMinutes(5));
    }

    private static WaitForStepDefinition sleep(Duration duration) {
        return waitFor(SLEEP_STEP, EventConditions.never(), duration);
    }

    private ExecuteStepDefinition charge() {
        return new ExecuteStepDefinition(
                new PrimitiveMetadata(EXECUTE_STEP, DefaultEventNameCustomizer.Builder.defaults()),
                Map.of(),
                (context, payload) -> {
                    try {
                        chargeDone.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return Map.of();
                },
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(Duration.ofMinutes(1)),
                RetryPolicy.maxRetries(0)
        );
    }

    /**
     * An execute step whose first attempt fails, and which retries once after 2 seconds.
     */
    private ExecuteStepDefinition flakyCharge() {
        return new ExecuteStepDefinition(
                new PrimitiveMetadata(EXECUTE_STEP, DefaultEventNameCustomizer.Builder.defaults()),
                Map.of(),
                (context, payload) -> {
                    if (flakyChargeAttempts.incrementAndGet() == 1) {
                        throw new IllegalStateException("The first charge attempt fails.");
                    }
                    return Map.of();
                },
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(Duration.ofMinutes(1)),
                RetryPolicy.maxRetries(1).withBackoff(BackoffStrategy.fixed(Duration.ofSeconds(2)))
        );
    }

    private static WaitForStepDefinition waitFor(String stepName, EventCondition condition, Duration timeout) {
        return new WaitForStepDefinition(
                new PrimitiveMetadata(stepName, DefaultEventNameCustomizer.Builder.defaults()),
                condition,
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(timeout)
        );
    }

    AxonConfiguration start(boolean hibernation) {
        return start(hibernation, false);
    }

    /**
     * Starts a configuration. The workflow id is the identifier of the start event, or its order id when
     * {@code idFromOrder} is set.
     */
    private AxonConfiguration start(boolean hibernation, boolean idFromOrder) {
        var module = WorkflowModule.configure(MODULE, HibernationContext.class)
                                   .definition(d -> {
                                       var definition = d.declarative(c -> this::body)
                                                         .workflowName(MODULE)
                                                         .on(c -> EventConditions.fromQualifiedName(
                                                                 new QualifiedName("start")
                                                         ));
                                       return idFromOrder
                                               ? definition.customized((c, wc) -> wc.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("orderId")))
                                               : definition.notCustomized();
                                   })
                                   // Claims a released segment again after half a second, instead of 5 seconds.
                                   .processorConfiguration(pc -> pc.initialSegmentCount(SEGMENT_COUNT)
                                                                   .tokenClaimInterval(500))
                                   .contextFactory(c -> HibernationContext::new);
        if (hibernation) {
            // left out otherwise, so the disabled case runs on the default
            module.withHibernation();
        }
        if (hibernationInterval > 0) {
            module.hibernationInterval(hibernationInterval);
        }
        var configurer = WorkflowConfigurer.create();
        configure(configurer);
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                .registerComponent(TokenStore.class, cfg -> tokenStore)
                .registerModule(module));
        var configuration = configurer.build();
        configurations.add(configuration);
        configuration.start();
        return configuration;
    }

    /**
     * Applies the settings a subclass runs these tests with. Does nothing here.
     *
     * @param configurer the configurer of the configuration that is about to be built
     */
    void configure(WorkflowConfigurer configurer) {
    }

    /**
     * Starts a workflow of the given kind and returns its id once the given step is durably started.
     */
    String startWorkflow(AxonConfiguration configuration, String kind, String orderId, String stepName) {
        // The default workflow id is the identifier of the start event.
        var workflowId = publish(configuration, "start", orderId, kind);
        awaitStepStarted(configuration, workflowId, stepName);
        return workflowId;
    }

    private void awaitStepStarted(AxonConfiguration configuration, String workflowId, String stepName) {
        await().atMost(5, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, workflowId).getStep(stepName).status() == StepStatus.STARTED);
    }

    /**
     * Runs hibernation passes until the workflow left memory. A pass that finds the workflow before it is safe, for
     * example before it received its own STARTED event, is dropped, so a later pass hibernates it.
     */
    void awaitHibernated(AxonConfiguration configuration, String workflowId) {
        var engine = engine(configuration);
        await().atMost(5, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(50)).until(() -> {
            WorkflowEngineHibernationPasses.runPass(engine);
            return !inMemory(configuration, workflowId);
        });
    }

    /**
     * Runs hibernation passes for a second, and fails as soon as the workflow left memory.
     */
    private static void awaitStaysInMemory(AxonConfiguration configuration, String workflowId) {
        var engine = engine(configuration);
        await().during(Duration.ofSeconds(1)).atMost(3, TimeUnit.SECONDS).until(() -> {
            WorkflowEngineHibernationPasses.runPass(engine);
            return inMemory(configuration, workflowId);
        });
    }

    /**
     * Releases every segment of the engine's processor, which claims them again after half a second.
     */
    static void releaseAllSegments(AxonConfiguration configuration) {
        var processor = engineProcessor(configuration);
        IntStream.range(0, SEGMENT_COUNT)
                 .mapToObj(segment -> processor.releaseSegment(segment, 500, TimeUnit.MILLISECONDS))
                 .forEach(release -> release.orTimeout(5, TimeUnit.SECONDS).join());
    }

    /**
     * Waits until the claim of the workflow's segment adopted it as hibernated, and fails as soon as the workflow is in
     * memory, which a claim that loads it would show.
     */
    static void awaitAdopted(AxonConfiguration configuration, String workflowId) {
        await().atMost(5, TimeUnit.SECONDS)
               .pollInterval(Duration.ofMillis(10))
               .failFast("The claim loaded the workflow into memory.", () -> inMemory(configuration, workflowId))
               .until(() -> WorkflowEngineHibernationPasses.isHibernated(engine(configuration), workflowId));
    }

    void awaitWorkflowStatus(AxonConfiguration configuration, String workflowId, WorkflowStatus status) {
        await().atMost(10, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, workflowId).workflowStatus() == status);
    }

    static boolean inMemory(Configuration configuration, String workflowId) {
        return engine(configuration).workflowExecutions()
                                    .stream()
                                    .map(WorkflowExecution::workflowId)
                                    .anyMatch(workflowId::equals);
    }

    static String publish(AxonConfiguration configuration, String eventName, String orderId, String kind) {
        var event = new GenericEventMessage(new MessageType(eventName), Map.of("orderId", orderId, "kind", kind));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + eventName)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }

    private static WorkflowEngine engine(Configuration configuration) {
        return find(configuration, WorkflowEngine.class).orElseThrow();
    }

    static WorkflowCancellationService cancellationService(Configuration configuration) {
        return find(configuration, WorkflowCancellationService.class).orElseThrow();
    }

    private static PooledStreamingEventProcessor engineProcessor(Configuration configuration) {
        return find(configuration, EventProcessor.class).filter(PooledStreamingEventProcessor.class::isInstance)
                                                        .map(PooledStreamingEventProcessor.class::cast)
                                                        .orElseThrow();
    }

    /**
     * Finds the only component of the given type, registered in the workflow module or one of its nested modules.
     */
    private static <T> Optional<T> find(Configuration configuration, Class<T> type) {
        return configuration.getComponents(type)
                            .values()
                            .stream()
                            .filter(component -> !(component instanceof PooledStreamingEventProcessor processor)
                                    || MODULE.equals(processor.name()))
                            .findFirst()
                            .or(() -> configuration.getModuleConfigurations()
                                                   .stream()
                                                   .map(module -> find(module, type))
                                                   .flatMap(Optional::stream)
                                                   .findFirst());
    }

    @SuppressWarnings("unchecked")
    static EventSourcedWorkflowState durableState(AxonConfiguration configuration, String workflowId) {
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
                            .join();
    }

    /**
     * Delegates to an in-memory storage engine. It fails every sourcing while told to, the way an unreachable event
     * store would, while streaming keeps working, so the processor keeps handling events. It also holds the first
     * append of a given step's events until released, so the workflow waits for that step's STARTED event.
     */
    private static final class ControlledStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final AtomicInteger failedSourcings = new AtomicInteger();
        private final CountDownLatch startHeld = new CountDownLatch(1);
        private final CompletableFuture<Void> heldStartReleased = new CompletableFuture<>();
        private volatile boolean failSourcing;
        private volatile @Nullable String holdStartOf;

        private ControlledStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private void failSourcing(boolean fail) {
            this.failSourcing = fail;
        }

        private int failedSourcings() {
            return failedSourcings.get();
        }

        private void holdStartOf(String stepName) {
            this.holdStartOf = stepName;
        }

        private boolean awaitHeldStart() throws InterruptedException {
            return startHeld.await(5, TimeUnit.SECONDS);
        }

        private void releaseHeldStart() {
            heldStartReleased.complete(null);
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            var stepName = holdStartOf;
            if (stepName != null && events.stream().anyMatch(
                    tagged -> stepName.equals(MetadataUtils.getStepName(tagged.event().metadata())))) {
                holdStartOf = null;
                startHeld.countDown();
                return heldStartReleased.thenCompose(
                        released -> delegate.appendEvents(condition, processingContext, events)
                );
            }
            return delegate.appendEvents(condition, processingContext, events);
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition, ProcessingContext processingContext) {
            if (failSourcing) {
                failedSourcings.incrementAndGet();
                return MessageStream.failed(new IllegalStateException("The event store is unreachable."));
            }
            return delegate.source(condition, processingContext);
        }

        @Override
        public MessageStream<EventMessage> stream(StreamingCondition condition) {
            return delegate.stream(condition);
        }

        @Override
        public CompletableFuture<TrackingToken> firstToken() {
            return delegate.firstToken();
        }

        @Override
        public CompletableFuture<TrackingToken> latestToken() {
            return delegate.latestToken();
        }

        @Override
        public CompletableFuture<TrackingToken> tokenAt(Instant at) {
            return delegate.tokenAt(at);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeWrapperOf(delegate);
        }
    }

    /**
     * Workflow context of the test workflows.
     */
    static class HibernationContext extends AbstractWorkflowContext {

        HibernationContext(Map<String, @Nullable Object> payload,
                           String workflowId,
                           ProcessingContext processingContext,
                           WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
