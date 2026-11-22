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
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.api.retry.BackoffStrategy;
import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.runtime.execution.AbstractWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.DefaultWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineHibernation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
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

import java.lang.ref.WeakReference;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Tests, on a real configuration with an in-memory event store, that a workflow waiting only for an event or a deadline
 * leaves memory once it made no progress for the idle delay, and that the awaited event, its own event, its deadline
 * or a cancellation restores it from the event store, while a claim of its segment restores it like every other
 * running workflow.
 *
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
class WorkflowConfigurerHibernationTest {

    private static final String MODULE = "hibernation";
    private static final int SEGMENT_COUNT = 2;
    private static final String WAIT_STEP = "awaitPaid";
    private static final String SLEEP_STEP = "cooldown";
    private static final String EXECUTE_STEP = "charge";
    private static final String SHIP_STEP = "awaitShipped";

    private final InMemoryTokenStore tokenStore = new InMemoryTokenStore();
    private final ControlledStorageEngine storageEngine =
            new ControlledStorageEngine(new InMemoryEventStorageEngine());
    private final List<AxonConfiguration> configurations = new ArrayList<>();
    private final CountDownLatch chargeDone = new CountDownLatch(1);
    private final AtomicInteger flakyChargeAttempts = new AtomicInteger();
    private final AtomicInteger caughtFailedWaits = new AtomicInteger();
    private final AtomicInteger catchingLoopExits = new AtomicInteger();
    private final AtomicInteger caughtWaitExceptions = new AtomicInteger();
    private final AtomicInteger gatedRuns = new AtomicInteger();
    private final CountDownLatch gateReached = new CountDownLatch(1);
    private final CountDownLatch gate = new CountDownLatch(1);
    private final List<Thread> drivers = new CopyOnWriteArrayList<>();
    // The driver thread of every execution that ran the body, in the order they started it.
    private final List<Thread> bodyDrivers = new CopyOnWriteArrayList<>();
    // The settings of the next configuration.
    private Duration idleDelay = Duration.ofMillis(200);
    private Duration wakeTimeout = Duration.ofSeconds(10);
    private @Nullable InMemoryWorkflowIndex workflowIndex;
    private @Nullable TwinFiringScheduler scheduler;
    private @Nullable AdjustableClock clock;
    // The tasks the timer of the next configuration ran, like hibernation passes, when that configuration has a clock.
    private final AtomicInteger firedTimers = new AtomicInteger();
    private @Nullable DefaultWorkflowScheduler countingScheduler;

    @AfterEach
    void tearDown() {
        chargeDone.countDown();
        gate.countDown();
        storageEngine.releaseHeldStart();
        configurations.forEach(AxonConfiguration::shutdown);
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (countingScheduler != null) {
            countingScheduler.shutdown();
        }
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

            // when / then: its re-entry finds the deadline passed and times the step out
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            assertThat(durableState(configuration, workflowId).getStep(SLEEP_STEP).status())
                    .isEqualTo(StepStatus.TIMED_OUT);
        }

        @Test
        void isWokenAgainWhenTheWakeAtItsDeadlineFails() {
            // given: a sleep of 2 seconds, and an event store that cannot be read when the deadline expires
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "sleep", "order-1", SLEEP_STEP);
            awaitHibernated(configuration, workflowId);
            storageEngine.failSourcing(true);

            // when: the wake at the deadline fails, and the event store recovers afterwards
            await().atMost(5, TimeUnit.SECONDS).until(() -> storageEngine.failedSourcings() > 0);
            storageEngine.failSourcing(false);

            // then: the wake is tried again after the idle delay
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void isForgottenWhenItsOwnEventShowsItEndedElsewhere() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when: another writer records the end of the workflow
            appendDirectly(new GenericEventMessage(
                    new MessageType(MODULE + "Cancelled"),
                    Map.of(),
                    MetadataUtils.create(workflowId,
                                         WorkflowStatus.CANCELLED,
                                         durableState(configuration, workflowId).workflowDefinitionId())
            ));

            // then: its own event woke it, found it terminal, and left nothing behind
            await().atMost(5, TimeUnit.SECONDS).until(
                    () -> !WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)
            );
            assertThat(inMemory(configuration, workflowId)).isFalse();
            assertThat(durableState(configuration, workflowId).workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);
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
            manager(configuration).findMany(WorkflowStateQuery.byWorkflowId(workflowId))
                                  .requestWorkflowCancellation(null)
                                  .orTimeout(10, TimeUnit.SECONDS)
                                  .join();

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.CANCELLED);
        }

        @Test
        void receivesEveryAwaitedEventWhileWorkflowsHibernate() {
            // given: workflows waiting for their order to be paid, which keep hibernating
            idleDelay = Duration.ofMillis(20);
            var configuration = start(true);
            var orderIds = IntStream.range(0, 20).mapToObj(i -> "order-" + i).toList();
            var workflowIds = orderIds.stream()
                                      .map(orderId -> startWorkflow(configuration, "wait", orderId, WAIT_STEP))
                                      .toList();

            // when: each payment can arrive while its workflow is being ejected from memory
            orderIds.forEach(orderId -> publish(configuration, "paid", orderId, ""));

            // then
            workflowIds.forEach(workflowId -> awaitWorkflowStatus(configuration,
                                                                  workflowId,
                                                                  WorkflowStatus.COMPLETED));
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
    class StoppedExecution {

        @Test
        void failsAStepCancellationItWasRecordingWithAnUnknownOutcome() throws InterruptedException {
            // given: a workflow that stays in memory, whose execution waits for the cancellation of its wait to be
            // recorded, which the event store holds
            var configuration = start(false);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            storageEngine.holdStartOf(WAIT_STEP);
            var cancelled = cancellationService(configuration).requestStepCancellation(workflowId, WAIT_STEP, null);
            assertThat(storageEngine.awaitHeldStart()).isTrue();

            // when
            engine(configuration).shutdown();
            storageEngine.releaseHeldStart();

            // then: the caller learns the outcome is unknown, as the held append may still record the cancellation,
            // instead of receiving the signal that unwinds the body
            assertThat(cancelled).failsWithin(Duration.ofSeconds(10))
                                 .withThrowableOfType(ExecutionException.class)
                                 .havingCause()
                                 .isExactlyInstanceOf(IllegalStateException.class)
                                 .withMessageContaining(WAIT_STEP);
        }

        @Test
        void appendsNoEventWhenItsBodyHandlesTheInterruptedWaitOnHibernation() {
            // given: a body that handles a failed wait and ends
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "catchingWait", "order-1", WAIT_STEP);

            // when
            awaitHibernated(configuration, workflowId);

            // then: the execution that left memory recorded nothing before its driver exited, so the workflow still
            // waits for the payment
            awaitFirstDriverExited();
            assertThat(WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)).isTrue();
            assertThat(durableState(configuration, workflowId).workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
            // and the awaited event completes it
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.COMPLETED);
        }

        @Test
        void appendsNoEventWhenItsBodyHandlesTheInterruptedWaitOnASegmentRelease() {
            // given: a body that handles a failed wait and ends, without hibernation
            var configuration = start(false);
            var workflowId = startWorkflow(configuration, "catchingWait", "order-1", WAIT_STEP);

            // when: its segment is released, which stops its execution, and claimed again, which restores it
            releaseAllSegments(configuration);
            await().atMost(5, TimeUnit.SECONDS).until(() -> inMemory(configuration, workflowId));

            // then: the stopped execution recorded nothing before its driver exited, so the workflow still waits for
            // the payment
            awaitFirstDriverExited();
            assertThat(durableState(configuration, workflowId).workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
            // and the awaited event completes the restored execution
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.COMPLETED);
        }

        @Test
        void unwindsABodyThatWaitsAgainAfterEveryFailedWait() {
            // given: a body that catches every failed wait and waits again, with an idle delay that leaves time to
            // find it in memory
            idleDelay = Duration.ofSeconds(1);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "catchingWaitLoop", "order-1", WAIT_STEP);
            var execution = execution(configuration, workflowId);

            // when
            awaitHibernated(configuration, workflowId);

            // then: the driver of the execution that left memory exits instead of waiting again in a loop, and stops
            // its runtime, which releases its checkpoint latches
            await().atMost(5, TimeUnit.SECONDS).until(() -> catchingLoopExits.get() == 1 && !execution.isRunning());
            assertThat(caughtFailedWaits.get()).isZero();
            // and the awaited event completes the workflow
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void unwindsABodyThatCatchesEveryExceptionOfAWaitAndWaitsAgainOnHibernation() {
            // given: a body that catches every exception of its wait and waits again, with an idle delay that leaves
            // time to find it in memory
            idleDelay = Duration.ofSeconds(1);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "exceptionCatchingWaitLoop", "order-1", WAIT_STEP);
            var execution = execution(configuration, workflowId);

            // when
            awaitHibernated(configuration, workflowId);

            // then: the body of the execution that left memory never caught the stop, so it unwound instead of waiting
            // again in a loop, and its driver exited
            await().atMost(5, TimeUnit.SECONDS).until(() -> catchingLoopExits.get() == 1 && !execution.isRunning());
            assertThat(caughtWaitExceptions.get()).isZero();
            // and it recorded nothing, so the workflow still waits for the payment
            assertThat(durableState(configuration, workflowId).workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.STARTED);
            // and the awaited event completes the workflow in the execution that restores it
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.COMPLETED);
            assertThat(caughtWaitExceptions.get()).isZero();
        }

        @Test
        void unwindsABodyThatCatchesEveryExceptionOfAWaitAndWaitsAgainOnASegmentRelease() {
            // given: a body that catches every exception of its wait and waits again, without hibernation
            var configuration = start(false);
            var workflowId = startWorkflow(configuration, "exceptionCatchingWaitLoop", "order-1", WAIT_STEP);
            var execution = execution(configuration, workflowId);

            // when: its segment is released, which stops its execution, and claimed again, which restores it
            releaseAllSegments(configuration);

            // then: the body of the stopped execution never caught the stop, so it unwound instead of waiting again in
            // a loop, and its driver exited
            await().atMost(5, TimeUnit.SECONDS).until(() -> catchingLoopExits.get() == 1 && !execution.isRunning());
            assertThat(caughtWaitExceptions.get()).isZero();
            await().atMost(5, TimeUnit.SECONDS).until(() -> inMemory(configuration, workflowId));
            // and it recorded nothing, so the workflow still waits for the payment
            assertThat(durableState(configuration, workflowId).workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.STARTED);
            // and the awaited event completes the workflow in the restored execution
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.COMPLETED);
            assertThat(caughtWaitExceptions.get()).isZero();
        }

        @Test
        void doesNotTimeOutAWaitWhoseTimerFiresWhileTheWorkflowHibernates() {
            // given: the timer of the wait fires while the workflow hibernates
            var timers = new TwinFiringScheduler();
            scheduler = timers;
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            timers.fireTwins();

            // when
            awaitHibernated(configuration, workflowId);

            // then: the execution that left memory did not time the wait out before its driver exited
            awaitFirstDriverExited();
            assertThat(WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)).isTrue();
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.STARTED);
            // and the awaited event completes it
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.COMPLETED);
        }
    }

    @Nested
    class MemoryOfAHibernatedWorkflow {

        @Test
        void keepsNeitherTheStartedExecutionNorItsDriverThread() {
            // given: a workflow waiting for its payment, with an idle delay that leaves time to find it in memory
            idleDelay = Duration.ofSeconds(1);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "recordedWaitThenWait", "order-1", WAIT_STEP);
            var execution = new WeakReference<>(execution(configuration, workflowId));
            var driver = drivers.getLast();

            // when
            awaitHibernated(configuration, workflowId);

            // then
            awaitReleased(execution, driver);
        }

        @Test
        void keepsNeitherTheExecutionRestoredByASegmentClaimNorItsDriverThread() {
            // given: a workflow restored by a claim of its segment, which runs it on the execution context of the claim
            idleDelay = Duration.ofSeconds(1);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "recordedWaitThenWait", "order-1", WAIT_STEP);
            releaseAllSegments(configuration);
            await().atMost(5, TimeUnit.SECONDS).until(() -> drivers.size() == 2 && inMemory(configuration, workflowId));
            var execution = new WeakReference<>(execution(configuration, workflowId));
            var driver = drivers.getLast();

            // when
            awaitHibernated(configuration, workflowId);

            // then
            awaitReleased(execution, driver);
        }

        @Test
        void keepsNeitherTheWokenExecutionNorItsDriverThreadOnceItHibernatesAgain() {
            // given: a hibernated workflow that the payment woke, and that waits for the shipment afterwards
            idleDelay = Duration.ofSeconds(1);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "recordedWaitThenWait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);
            publish(configuration, "paid", "order-1", "");
            await().atMost(5, TimeUnit.SECONDS).until(() -> drivers.size() == 2 && inMemory(configuration, workflowId));
            var execution = new WeakReference<>(execution(configuration, workflowId));
            var driver = drivers.getLast();

            // when
            awaitHibernated(configuration, workflowId);

            // then
            awaitReleased(execution, driver);
        }

        /**
         * Waits until the given driver thread ended and nothing holds on to the given execution anymore.
         */
        private static void awaitReleased(WeakReference<WorkflowExecution> execution, Thread driver) {
            await().atMost(5, TimeUnit.SECONDS).until(() -> !driver.isAlive());
            await().atMost(5, TimeUnit.SECONDS)
                   .pollInterval(Duration.ofMillis(100))
                   .until(() -> {
                       System.gc();
                       return execution.get() == null;
                   });
        }
    }

    @Nested
    class CancellationOfAHibernatedWorkflow {

        @Test
        void isAnsweredWhenTheWokenExecutionIsStoppedBeforeItRanTheRequest() throws InterruptedException {
            // given: a hibernated workflow, whose restored body waits for a gate before it waits again
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "gatedWait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when: a step cancellation wakes it, and the engine stops before the woken body took the request
            var cancelled = cancellationService(configuration).requestStepCancellation(workflowId, WAIT_STEP, null);
            assertThat(gateReached.await(5, TimeUnit.SECONDS)).isTrue();
            engine(configuration).shutdown();
            gate.countDown();

            // then: the request is made again, and fails, as the workflow no longer runs on this node
            assertThat(cancelled).failsWithin(Duration.ofSeconds(10))
                                 .withThrowableThat()
                                 .havingRootCause()
                                 .isInstanceOf(NoSuchElementException.class);
        }

        @Test
        void reachesTheHibernatedWorkflowThroughTheRegistrationOfAnExecutionThatLeftMemory() {
            // given: a hibernated workflow, with the cancellation of the execution that left memory registered again,
            // as a wake leaves it behind that fails after it registered the cancellation of the execution it woke
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            var execution = execution(configuration, workflowId);
            awaitHibernated(configuration, workflowId);
            WorkflowEngineHibernation.registerCancellationOf(engine(configuration), execution);

            // when
            var cancelled = cancellationService(configuration).requestWorkflowCancellation(workflowId, null);

            // then: the request goes to the hibernated workflow, instead of back to that registration
            assertThat(cancelled).succeedsWithin(Duration.ofSeconds(10));
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.CANCELLED);
        }
    }

    @Nested
    class Routing {

        @Test
        void wakesOnlyTheHibernatedWorkflowsAnEventMayMatch() {
            // given: a second of idle delay, so a workflow woken by mistake stays in memory for a while
            idleDelay = Duration.ofSeconds(1);
            var configuration = start(true);
            var orderIds = IntStream.range(0, 10).mapToObj(i -> "order-" + i).toList();
            var workflowIds = orderIds.stream()
                                      .map(orderId -> startWorkflow(configuration, "wait", orderId, WAIT_STEP))
                                      .toList();
            workflowIds.forEach(workflowId -> awaitHibernated(configuration, workflowId));

            // when
            publish(configuration, "paid", "order-7", "");

            // then: only the workflow of that order was restored
            awaitWorkflowStatus(configuration, workflowIds.get(7), WorkflowStatus.COMPLETED);
            var others = new ArrayList<>(workflowIds);
            others.remove(7);
            assertThat(others).allMatch(workflowId -> WorkflowEngineHibernation.isHibernated(engine(configuration),
                                                                                             workflowId))
                              .noneMatch(workflowId -> inMemory(configuration, workflowId));
        }

        @Test
        void wakesAWorkflowWaitingOnAPredicateForEveryEventOfItsName() {
            // given: a second of idle delay, so the woken workflow stays in memory for a while
            idleDelay = Duration.ofSeconds(1);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "waitOnPredicate", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when: a payment of another order arrives
            publish(configuration, "paid", "order-2", "");

            // then: the workflow is restored, as its wait cannot tell from its name only, and keeps waiting
            await().atMost(5, TimeUnit.SECONDS).until(() -> inMemory(configuration, workflowId));
            assertThat(durableState(configuration, workflowId).getStep(WAIT_STEP).status())
                    .isEqualTo(StepStatus.STARTED);
            // and it hibernates again, and completes with the payment of its own order
            awaitHibernated(configuration, workflowId);
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void wakesAWorkflowThatAwaitsTheNextStartOfItsIdInsteadOfStartingADuplicate() {
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
        void ignoresTheStartOfAHibernatedIdSoTheWorkflowStaysCancellable() {
            // given: the workflow id is the order id, so a second start event of the order is a duplicate
            var configuration = start(true, true);
            publish(configuration, "start", "order-1", "wait");
            awaitStepStarted(configuration, "order-1", WAIT_STEP);
            awaitHibernated(configuration, "order-1");

            // when
            publish(configuration, "start", "order-1", "wait");

            // then: once the processor handled that start, no duplicate execution appeared, and a cancellation still
            // reaches the hibernated workflow
            awaitEverySegmentHandledTheStoredEvents(configuration);
            assertThat(inMemory(configuration, "order-1")).isFalse();
            cancellationService(configuration).requestWorkflowCancellation("order-1", null)
                                              .orTimeout(10, TimeUnit.SECONDS)
                                              .join();
            awaitWorkflowStatus(configuration, "order-1", WorkflowStatus.CANCELLED);
        }

        @Test
        void deliversAnAwaitedEventToAWorkflowThatWakesWhileTheEventIsRouted() {
            // given: a hibernated workflow on the only segment, which wakes for another reason right after the routing
            // of an event listed the executions in memory
            var repository = new ListingInterceptingRepository();
            var configuration = start(true, false, module -> module
                    .processorConfiguration(pc -> pc.initialSegmentCount(1).tokenClaimInterval(500))
                    .executionRepository(c -> repository));
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);
            repository.afterNextSegmentListing(() -> wakeWithoutAnEvent(configuration, workflowId));

            // when
            publish(configuration, "paid", "order-1", "");

            // then: the payment, which is no event of the workflow and is never sourced, reached the woken workflow
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void deliversAnAwaitedEventToAWorkflowThatHibernatesAndWakesWhileTheEventIsRouted() {
            // given: a workflow in memory on the only segment, which only hibernates when asked to, and which the
            // routing of an event, after it matched the waits of the hibernated workflows a first time, sees hibernate
            // before it lists the executions in memory and wake for another reason before it matches them again
            idleDelay = Duration.ofMinutes(1);
            var repository = new ListingInterceptingRepository();
            var configuration = start(true, false, module -> module
                    .processorConfiguration(pc -> pc.initialSegmentCount(1).tokenClaimInterval(500))
                    .executionRepository(c -> repository));
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            repository.aroundNextSegmentListing(() -> hibernateNow(configuration, workflowId),
                                                () -> wakeWithoutAnEvent(configuration, workflowId));

            // when
            publish(configuration, "paid", "order-1", "");

            // then: the payment, which is no event of the workflow and is never sourced, reached the woken workflow
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }
    }

    @Nested
    class FailedWakeByAnEvent {

        @Test
        void failsTheEventOnEveryFailedWakeUntilTheWakeSucceeds() {
            // given: a hibernated workflow, and an event store that cannot be read
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);
            var paid = paidEvent("order-1");
            storageEngine.failSourcing(true);

            // when / then: every failed wake fails the event, so the processor delivers it again, and the workflow
            // stays hibernated
            for (int attempt = 0; attempt < 5; attempt++) {
                assertThat(handle(configuration, workflowId, paid)).isCompletedExceptionally();
            }
            assertThat(WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)).isTrue();

            // and once the event store can be read, the same event wakes and completes it
            storageEngine.failSourcing(false);
            assertThat(handle(configuration, workflowId, paid)).isNotCompletedExceptionally();
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void failsEveryEventWhoseWakeFailsOnceAcrossRepeatedFailuresThatAClaimRecoveredFrom() {
            // given: a hibernated workflow that every payment wakes, as it waits on a predicate
            idleDelay = Duration.ofMillis(500);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "waitOnPredicate", "order-1", WAIT_STEP);

            // when: two payments of other orders fail to wake it; each failure fails the event, so the processor
            // releases the segment, and its next claim restores the workflow, which receives the event and hibernates
            // again
            for (var orderId : List.of("order-2", "order-3")) {
                awaitHibernated(configuration, workflowId);
                var failures = storageEngine.failedSourcings();
                storageEngine.failNextSourcings(1);
                publish(configuration, "paid", orderId, "");
                await().atMost(10, TimeUnit.SECONDS).until(() -> storageEngine.failedSourcings() > failures);
                await().atMost(20, TimeUnit.SECONDS).until(() -> inMemory(configuration, workflowId));
            }
            // and the payment of its own order fails to wake it as well
            awaitHibernated(configuration, workflowId);
            storageEngine.failNextSourcings(1);
            publish(configuration, "paid", "order-1", "");

            // then: that payment is failed too, not skipped, so the claim that restores the workflow delivers it
            await().atMost(60, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableState(configuration, workflowId).workflowStatus() == WorkflowStatus.COMPLETED);
        }

        @Test
        void wakesEveryHibernatedWorkflowAnEventMayMatchWhenTheWakeOfOneFails() {
            // given: hibernated workflows of one segment that every payment wakes, as they wait on a predicate
            var configuration = start(true, true);
            var orderIds = List.of("order-1", "order-2", "order-3", "order-4");
            orderIds.forEach(orderId -> {
                publish(configuration, "start", orderId, "waitOnPredicate");
                awaitStepStarted(configuration, orderId, WAIT_STEP);
            });
            orderIds.forEach(orderId -> awaitHibernated(configuration, orderId));
            // with four workflows over two segments, at least two share one
            var sameSegment = orderIds.stream()
                                      .collect(Collectors.groupingBy(WorkflowConfigurerHibernationTest::segmentOf))
                                      .values()
                                      .stream()
                                      .max(Comparator.comparingInt(List::size))
                                      .orElseThrow();
            storageEngine.failSourcing(true);
            var failures = storageEngine.failedSourcings();

            // when: a payment arrives at that segment, while no wake can load its workflow
            var handled = handle(configuration, sameSegment.getFirst(), paidEvent("order-9"));

            // then: the event fails, after a wake of every workflow of that segment was tried
            assertThat(handled).isCompletedExceptionally();
            assertThat(storageEngine.failedSourcings() - failures).isEqualTo(sameSegment.size());
        }

        @Test
        void leavesAWorkflowOnlyHibernatedWhenItsWakeFailsAfterItWasLoaded() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);
            var definitionId = durableState(configuration, workflowId).workflowDefinitionId();
            // an event of the workflow that its state cannot apply: its retry info is not one
            var unreadable = new GenericEventMessage(
                    new MessageType(MODULE + "Retrying"),
                    "not retry info",
                    MetadataUtils.withWorkflowDefinitionId(
                            MetadataUtils.create(workflowId, "charge", StepStatus.RETRYING), definitionId
                    )
            );

            // when: the wake loads the workflow, and fails to hand it the event
            var handled = handle(configuration, workflowId, unreadable);

            // then: the workflow is hibernated only, not in memory as well
            assertThat(handled).isCompletedExceptionally();
            assertThat(WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)).isTrue();
            assertThat(inMemory(configuration, workflowId)).isFalse();
            // and the awaited event still completes it
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void failsWhenTheWakeTakesLongerThanTheWakeTimeout() {
            // given
            wakeTimeout = Duration.ofMillis(100);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);
            storageEngine.hangSourcing(true);

            // when
            var handled = handle(configuration, workflowId, paidEvent("order-1"));

            // then
            assertThat(handled).isCompletedExceptionally();
            assertThat(WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)).isTrue();
        }

        /**
         * Hands the event to the engine as the segment owning the workflow handles it, without storing it, so the
         * processor never delivers it and only the outcome of this delivery counts. The processor did not hand it
         * over, so the engine does not ask the processor to release the segment when the event fails either.
         */
        private CompletableFuture<?> handle(AxonConfiguration configuration, String workflowId, EventMessage event) {
            engine(configuration).setSegmentReleaser(segment -> {
            });
            var segment = segmentOf(workflowId);
            return configuration.getComponent(UnitOfWorkFactory.class)
                                .create("handle-" + event.identifier())
                                .executeWithResult(context -> {
                                    context.putResource(Segment.RESOURCE_KEY, segment);
                                    engine(configuration).handle(event, context);
                                    return CompletableFuture.completedFuture(null);
                                })
                                // Waits for the outcome, which the caller inspects.
                                .handle((result, failure) -> failure)
                                .thenApply(failure -> failure == null
                                        ? CompletableFuture.completedFuture(null)
                                        : CompletableFuture.failedFuture(failure))
                                .orTimeout(10, TimeUnit.SECONDS)
                                .join();
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
        void staysInMemoryUntilItMadeNoProgressForTheIdleDelay() {
            // given: a clock that only moves when the test moves it
            var time = new AdjustableClock();
            clock = time;
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            // and: every event of the workflow handled, the last of which was its last progress
            awaitEverySegmentHandledTheStoredEvents(configuration);

            // when: all but a millisecond of the idle delay passed since then
            time.advance(idleDelay.minusMillis(1));

            // then: it stays in memory, also once a hibernation pass ran entirely after that
            awaitTimersFired(2);
            assertThat(inMemory(configuration, workflowId)).isTrue();
            assertThat(WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)).isFalse();

            // when: the idle delay passed
            time.advance(Duration.ofMillis(1));

            // then
            awaitHibernated(configuration, workflowId);
        }

        @Test
        void staysInMemoryWhenHibernationIsDisabled() {
            // given
            var configuration = start(false);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);

            // when / then: without hibernation, nothing asks the workflow to leave memory
            assertThat(WorkflowEngineHibernation.isEnabled(engine(configuration))).isFalse();
            assertThat(inMemory(configuration, workflowId)).isTrue();
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }
    }

    @Nested
    class Combinator {

        @Test
        void hibernatesWhileWaitingOnlyForAnEventAndADeadlineAndRecordsNoStepOfItsOwn() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "anyMatchWaitAndSleep", "order-1", SLEEP_STEP);

            // when
            awaitHibernated(configuration, workflowId);
            publish(configuration, "paid", "order-1", "");

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            assertThat(durableState(configuration, workflowId).workflowStepNames())
                    .containsExactlyInAnyOrder(WAIT_STEP, SLEEP_STEP);
        }
    }

    @Nested
    class Manager {

        @Test
        void findsAHibernatedWorkflowByIdAndAmongTheRunningOnes() {
            // given: no history, so only the engine knows the workflow is live
            var configuration = start(true, false, module -> module.withoutHistory());
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);
            var manager = manager(configuration);

            // when
            var found = manager.findOne(WorkflowStateQuery.byWorkflowId(workflowId))
                               .singleState()
                               .orTimeout(5, TimeUnit.SECONDS)
                               .join();
            var running = manager.findMany(WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED))
                                 .size()
                                 .orTimeout(5, TimeUnit.SECONDS)
                                 .join();

            // then
            assertThat(found).extracting(WorkflowState::workflowStatus).isEqualTo(WorkflowStatus.STARTED);
            assertThat(running).isEqualTo(1);
            assertThat(inMemory(configuration, workflowId)).isFalse();
        }

        @Test
        void sourcesOnlyTheWorkflowAQueryByIdNamesAmongManyHibernatedOnes() {
            // given
            var configuration = start(true);
            var workflowIds = startHibernatedWorkflows(configuration, 10);
            storageEngine.resetSourcings();

            // when
            var found = manager(configuration).findOne(WorkflowStateQuery.byWorkflowId(workflowIds.get(3)))
                                              .singleState()
                                              .orTimeout(5, TimeUnit.SECONDS)
                                              .join();

            // then
            assertThat(found).extracting(WorkflowState::workflowId).isEqualTo(workflowIds.get(3));
            assertThat(storageEngine.sourcings()).isEqualTo(1);
        }

        @Test
        void sourcesOnlyTheWorkflowAQueryByIdNamesAmongManyHibernatedOnesWithoutHistory() {
            // given
            var configuration = start(true, false, module -> module.withoutHistory());
            var workflowIds = startHibernatedWorkflows(configuration, 10);
            storageEngine.resetSourcings();

            // when
            var found = manager(configuration).findOne(WorkflowStateQuery.byWorkflowId(workflowIds.get(3)))
                                              .singleState()
                                              .orTimeout(5, TimeUnit.SECONDS)
                                              .join();

            // then
            assertThat(found).extracting(WorkflowState::workflowId).isEqualTo(workflowIds.get(3));
            assertThat(storageEngine.sourcings()).isEqualTo(1);
        }

        @Test
        void sourcesOnlyTheHibernatedWorkflowACancellationByIdNames() {
            // given
            var configuration = start(true);
            var workflowIds = startHibernatedWorkflows(configuration, 10);
            storageEngine.resetSourcings();

            // when
            manager(configuration).findMany(WorkflowStateQuery.byWorkflowId(workflowIds.get(3)))
                                  .requestWorkflowCancellation(null)
                                  .orTimeout(10, TimeUnit.SECONDS)
                                  .join();

            // then: the query sourced that workflow once, and its wake once more
            assertThat(storageEngine.sourcings()).isLessThanOrEqualTo(2);
            awaitWorkflowStatus(configuration, workflowIds.get(3), WorkflowStatus.CANCELLED);
        }

        private List<String> startHibernatedWorkflows(AxonConfiguration configuration, int count) {
            var workflowIds = IntStream.range(0, count)
                                       .mapToObj(i -> startWorkflow(configuration, "wait", "order-" + i, WAIT_STEP))
                                       .toList();
            workflowIds.forEach(workflowId -> awaitHibernated(configuration, workflowId));
            return workflowIds;
        }

        @Test
        void cancelsTheHibernatedWorkflowsMatchingAQueryWithoutHistory() {
            // given
            var configuration = start(true, false, module -> module.withoutHistory());
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when
            manager(configuration).findMany(WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED))
                                  .requestWorkflowCancellation(null)
                                  .orTimeout(10, TimeUnit.SECONDS)
                                  .join();

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.CANCELLED);
        }

        @Test
        void cancelsAWorkflowThatWakesWhileTheQueryListsTheExecutionsInMemory() {
            // given: a hibernated workflow, which wakes for another reason right after a query listed the executions
            // in memory
            var repository = new ListingInterceptingRepository();
            var configuration = start(true, false, module -> module.executionRepository(c -> repository));
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);
            repository.afterNextQuery(() -> wakeWithoutAnEvent(configuration, workflowId));

            // when
            manager(configuration).findMany(WorkflowStateQuery.byWorkflowId(workflowId))
                                  .requestWorkflowCancellation(null)
                                  .orTimeout(10, TimeUnit.SECONDS)
                                  .join();

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.CANCELLED);
        }

        @Test
        void cancelsAWorkflowThatHibernatesAndWakesWhileTheQueryListsTheExecutionsInMemory() {
            // given: a workflow in memory, which only hibernates when asked to, and which a query, after it looked up
            // the hibernated workflows a first time, sees hibernate before it lists the executions in memory and wake
            // for another reason before it looks them up again
            idleDelay = Duration.ofMinutes(1);
            var repository = new ListingInterceptingRepository();
            var configuration = start(true, false, module -> module.executionRepository(c -> repository));
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            repository.aroundNextQuery(() -> hibernateNow(configuration, workflowId),
                                       () -> wakeWithoutAnEvent(configuration, workflowId));

            // when
            manager(configuration).findMany(WorkflowStateQuery.byWorkflowId(workflowId))
                                  .requestWorkflowCancellation(null)
                                  .orTimeout(10, TimeUnit.SECONDS)
                                  .join();

            // then
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.CANCELLED);
        }

        @Test
        void countsAWorkflowThatHibernatesAndWakesWhileAQueryWithoutHistoryListsTheExecutionsInMemory() {
            // given: no history, so a query naming no workflow id considers every hibernated workflow, and a workflow
            // in memory, which only hibernates when asked to, and which the query sees hibernate before it lists the
            // executions in memory and wake for another reason before it looks up the hibernated workflows again
            idleDelay = Duration.ofMinutes(1);
            var repository = new ListingInterceptingRepository();
            var configuration = start(true, false, module -> module.withoutHistory()
                                                                   .executionRepository(c -> repository));
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            repository.aroundNextQuery(() -> hibernateNow(configuration, workflowId),
                                       () -> wakeWithoutAnEvent(configuration, workflowId));

            // when
            var running = manager(configuration).findMany(WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED))
                                                .size()
                                                .orTimeout(10, TimeUnit.SECONDS)
                                                .join();

            // then
            assertThat(running).isEqualTo(1);
        }
    }

    @Nested
    class ReleasedOrResizedSegment {

        @Test
        void keepsAnExecutionInMemoryOnceTheClaimItRunsUnderEnded() {
            // given: a workflow that has not hibernated yet
            idleDelay = Duration.ofMillis(500);
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);

            // when: a new claim of its segment starts, which ends the claim its execution runs under, and has not
            // stopped that execution yet
            WorkflowEngineHibernation.startNewClaimOf(engine(configuration), segmentOf(workflowId));

            // then: the execution does not hibernate under the new claim, which would restore the workflow as well
            awaitStaysInMemory(configuration, workflowId);
        }

        @Test
        void forgetsTheHibernatedWorkflowsOfAReleasedSegment() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when
            releaseAllSegments(configuration);

            // then: the claim restored it like every running workflow, and the event completes it
            await().atMost(5, TimeUnit.SECONDS).until(() -> inMemory(configuration, workflowId));
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
        }

        @Test
        void completesEveryWorkflowByItsEventAndListsNoneAsHibernatedAfterItsSegmentSplits() {
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
        void completesEveryWorkflowByItsEventAndListsNoneAsHibernatedAfterItsSegmentsMerge() {
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
            assertThat(workflowIds).noneMatch(
                    workflowId -> WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)
            );
        }
    }

    @Nested
    class WithAWorkflowIndex {

        @Test
        void keepsAHibernatedWorkflowListedAsRunningUntilItEnds() {
            // given
            workflowIndex = new InMemoryWorkflowIndex();
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when: other workflows start and end, so the segments checkpoint
            var other = startWorkflow(configuration, "wait", "order-2", WAIT_STEP);
            publish(configuration, "paid", "order-2", "");
            awaitWorkflowStatus(configuration, other, WorkflowStatus.COMPLETED);

            // then: the hibernated workflow is still a running one in the index
            await().atMost(5, TimeUnit.SECONDS).until(() -> indexedWorkflowIds().contains(workflowId)
                    && !indexedWorkflowIds().contains(other));
            assertThat(WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)).isTrue();

            // and it leaves the index once it ends
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);
            publish(configuration, "unrelated", "order-3", "");
            await().atMost(5, TimeUnit.SECONDS).until(() -> !indexedWorkflowIds().contains(workflowId));
        }

        private Set<String> indexedWorkflowIds() {
            var index = Objects.requireNonNull(workflowIndex);
            var ids = new HashSet<String>();
            for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
                index.load(MODULE, new Segment(segmentId, SEGMENT_COUNT - 1))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join()
                     .ifPresent(ids::addAll);
            }
            return ids;
        }
    }

    /**
     * Runs the steps the start event asks for: a wait for the order's {@code paid} event or its next {@code start}
     * event, a wait on a predicate, a sleep, an execute step that runs until the test lets it finish or that retries
     * once, or a combinator of them.
     */
    private void body(HibernationContext ctx) {
        bodyDrivers.add(Thread.currentThread());
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
            case "waitOnPredicate" -> ctx.awaitEvent(waitFor(WAIT_STEP,
                                                             paidOnPredicate(orderId),
                                                             Duration.ofMinutes(5)));
            case "anyMatchWaitAndSleep" -> ctx.anyMatch(result -> true,
                                                        ctx.waitForEvent(paidOrder(orderId)),
                                                        ctx.waitForEvent(sleep(Duration.ofMinutes(5))))
                                              .await();
            case "waitThenWait" -> {
                var paid = ctx.waitForEvent(paidOrder(orderId));
                var shipped = ctx.waitForEvent(shippedOrder(orderId));
                ctx.allMatch(result -> true, paid, shipped).await();
            }
            case "recordedWaitThenWait" -> {
                // Records the driver thread of every execution that runs the body.
                drivers.add(Thread.currentThread());
                var paid = ctx.waitForEvent(paidOrder(orderId));
                var shipped = ctx.waitForEvent(shippedOrder(orderId));
                ctx.allMatch(result -> true, paid, shipped).await();
            }
            case "catchingWait" -> {
                try {
                    ctx.awaitEvent(paidOrder(orderId));
                } catch (StepFailedException e) {
                    // Handles a failed wait the way a body handles a timed out one, and ends.
                }
            }
            case "gatedWait" -> {
                if (gatedRuns.incrementAndGet() > 1) {
                    // Every restored body waits for the gate before it waits for the payment again.
                    gateReached.countDown();
                    try {
                        gate.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                ctx.awaitEvent(paidOrder(orderId));
            }
            case "catchingWaitLoop" -> {
                try {
                    while (true) {
                        try {
                            ctx.awaitEvent(paidOrder(orderId));
                            return;
                        } catch (StepFailedException e) {
                            // Waits again, the way a body retries a wait that timed out.
                            caughtFailedWaits.incrementAndGet();
                        }
                    }
                } finally {
                    catchingLoopExits.incrementAndGet();
                }
            }
            case "exceptionCatchingWaitLoop" -> {
                try {
                    // Bounded only so that a body that does not unwind ends instead of spinning on.
                    while (caughtWaitExceptions.get() < 1_000) {
                        try {
                            ctx.awaitEvent(paidOrder(orderId));
                            return;
                        } catch (Exception e) {
                            // Waits again, the way a body polls until its wait succeeds, whatever went wrong.
                            caughtWaitExceptions.incrementAndGet();
                        }
                    }
                } finally {
                    catchingLoopExits.incrementAndGet();
                }
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

    private static WaitForStepDefinition shippedOrder(String orderId) {
        return waitFor(SHIP_STEP,
                       EventConditions.fromQualifiedName(
                               new QualifiedName("shipped"),
                               Associations.associate(payloadProperty("orderId"), "=", orderId)
                       ),
                       Duration.ofMinutes(5));
    }

    /**
     * A wait for the payment of the given order, decided by a predicate, so it records no associations.
     */
    private static EventCondition paidOnPredicate(String orderId) {
        return new EventCondition() {
            @Override
            public BiPredicate<EventMessage, ProcessingContext> predicate() {
                return (event, context) -> orderId.equals(String.valueOf(payloadProperty("orderId").apply(event,
                                                                                                           context)));
            }

            @Override
            public QualifiedName qualifiedName() {
                return new QualifiedName("paid");
            }
        };
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

    private AxonConfiguration start(boolean hibernation) {
        return start(hibernation, false);
    }

    private AxonConfiguration start(boolean hibernation, boolean idFromOrder) {
        return start(hibernation, idFromOrder, Function.identity());
    }

    /**
     * Starts a configuration. The workflow id is the identifier of the start event, or its order id when
     * {@code idFromOrder} is set. Hibernation is set for the application, and the module keeps it.
     */
    private AxonConfiguration start(boolean hibernation,
                                    boolean idFromOrder,
                                    Function<WorkflowModule.OptionalPhase<HibernationContext>,
                                            WorkflowModule.OptionalPhase<HibernationContext>> moduleCustomization) {
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
        moduleCustomization.apply(module);
        var configurer = WorkflowConfigurer.create();
        if (hibernation) {
            // left out otherwise, so the disabled case runs on the default
            var delay = idleDelay;
            var timeout = wakeTimeout;
            configurer.hibernation(h -> h.enabled(true).idleDelay(delay).wakeTimeout(timeout));
        }
        if (workflowIndex != null) {
            var index = workflowIndex;
            configurer.registerWorkflowIndex(c -> index);
        }
        if (scheduler != null) {
            var timers = scheduler;
            configurer.componentRegistry(cr -> cr.registerComponent(WorkflowScheduler.class, cfg -> timers));
        }
        if (clock != null) {
            var time = clock;
            var timers = new DefaultWorkflowScheduler(time, countingTimer());
            countingScheduler = timers;
            configurer.componentRegistry(cr -> cr.registerComponent(Clock.class, cfg -> time)
                                                 .registerComponent(WorkflowScheduler.class, cfg -> timers));
        }
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
     * Starts a workflow of the given kind and returns its id once the given step is durably started.
     */
    private String startWorkflow(AxonConfiguration configuration, String kind, String orderId, String stepName) {
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
     * Waits until the workflow left memory, which the engine's own passes do once it made no progress for the idle
     * delay.
     */
    private static void awaitHibernated(AxonConfiguration configuration, String workflowId) {
        await().atMost(10, TimeUnit.SECONDS)
               .pollInterval(Duration.ofMillis(20))
               .until(() -> WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)
                       && !inMemory(configuration, workflowId));
    }

    /**
     * Waits until the driver thread of the first execution that ran the body exited, after which that execution does
     * nothing anymore, like appending an event.
     */
    private void awaitFirstDriverExited() {
        await().atMost(5, TimeUnit.SECONDS).until(() -> !bodyDrivers.getFirst().isAlive());
    }

    /**
     * Waits until every segment handled every event stored so far: the processor advances the status of a segment past
     * a batch once that batch committed.
     */
    private void awaitEverySegmentHandledTheStoredEvents(AxonConfiguration configuration) {
        var head = storageEngine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            var statuses = engineProcessor(configuration).processingStatus().values();
            return statuses.size() == SEGMENT_COUNT && statuses.stream().allMatch(
                    status -> status.getTrackingToken() != null && status.getTrackingToken().covers(head)
            );
        });
    }

    /**
     * Returns a timer that counts the tasks it ran in {@link #firedTimers}.
     */
    private ScheduledThreadPoolExecutor countingTimer() {
        return new ScheduledThreadPoolExecutor(1) {
            @Override
            protected void afterExecute(Runnable task, @Nullable Throwable failure) {
                // After the timer ran its task, and with it what the completion of that task runs, like a pass.
                firedTimers.incrementAndGet();
            }
        };
    }

    /**
     * Waits until the timer of the configuration ran the given number of tasks more, so at least all but the first of
     * them ran entirely after this method was called, and until the workflows then completed the checks whether they
     * may hibernate that those tasks asked for: a hibernation pass, one such task, only asks for a check, which the
     * execution runs later.
     */
    private void awaitTimersFired(int count) {
        int firedBefore = firedTimers.get();
        await().atMost(10, TimeUnit.SECONDS).until(() -> firedTimers.get() >= firedBefore + count);
        await().atMost(10, TimeUnit.SECONDS).until(() -> configurations.stream().allMatch(configuration -> {
            var engine = engine(configuration);
            return engine.workflowExecutions().stream().noneMatch(
                    execution -> WorkflowEngineHibernation.hasPendingHibernationCheck(engine, execution.workflowId())
            );
        }));
    }

    /**
     * Waits until the workflow checked twice more whether it may hibernate, which it does at every hibernation pass
     * once it made no progress for the idle delay, and asserts that it stayed in memory: the second check started after
     * this method was called. Stops waiting as soon as the workflow left memory.
     */
    private static void awaitStaysInMemory(AxonConfiguration configuration, String workflowId) {
        var engine = engine(configuration);
        int checksBefore = WorkflowEngineHibernation.completedHibernationChecks(engine, workflowId).orElseThrow();
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            var checks = WorkflowEngineHibernation.completedHibernationChecks(engine, workflowId);
            return checks.isEmpty() || checks.getAsInt() >= checksBefore + 2;
        });
        assertThat(inMemory(configuration, workflowId)).isTrue();
        assertThat(WorkflowEngineHibernation.isHibernated(engine, workflowId)).isFalse();
    }

    /**
     * Asks the given workflow to hibernate, also when it made progress within the idle delay, until it left memory.
     */
    private static void hibernateNow(AxonConfiguration configuration, String workflowId) {
        await().atMost(10, TimeUnit.SECONDS)
               .pollInterval(Duration.ofMillis(20))
               .until(() -> {
                   WorkflowEngineHibernation.requestHibernation(engine(configuration), workflowId);
                   return WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId)
                           && !inMemory(configuration, workflowId);
               });
    }

    /**
     * Wakes the given hibernated workflow without changing what it waits for, by a cancellation request for a step it
     * does not have, and waits until it is back in memory.
     */
    private static void wakeWithoutAnEvent(AxonConfiguration configuration, String workflowId) {
        cancellationService(configuration).requestStepCancellation(workflowId, "unknownStep", null);
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> inMemory(configuration, workflowId)
                       && !WorkflowEngineHibernation.isHibernated(engine(configuration), workflowId));
    }

    /**
     * Releases every segment of the engine's processor, which claims them again after half a second.
     */
    private static void releaseAllSegments(AxonConfiguration configuration) {
        var processor = engineProcessor(configuration);
        IntStream.range(0, SEGMENT_COUNT)
                 .mapToObj(segment -> processor.releaseSegment(segment, 500, TimeUnit.MILLISECONDS))
                 .forEach(release -> release.orTimeout(5, TimeUnit.SECONDS).join());
    }

    private static Segment segmentOf(String workflowId) {
        return IntStream.range(0, SEGMENT_COUNT)
                        .mapToObj(segmentId -> new Segment(segmentId, SEGMENT_COUNT - 1))
                        .filter(candidate -> candidate.matches(workflowId))
                        .findFirst()
                        .orElseThrow();
    }

    private void awaitWorkflowStatus(AxonConfiguration configuration, String workflowId, WorkflowStatus status) {
        await().atMost(10, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, workflowId).workflowStatus() == status);
    }

    private static WorkflowExecution execution(Configuration configuration, String workflowId) {
        return engine(configuration).workflowExecutions()
                                    .stream()
                                    .filter(candidate -> candidate.workflowId().equals(workflowId))
                                    .findFirst()
                                    .orElseThrow();
    }

    private static boolean inMemory(Configuration configuration, String workflowId) {
        return engine(configuration).workflowExecutions()
                                    .stream()
                                    .map(WorkflowExecution::workflowId)
                                    .anyMatch(workflowId::equals);
    }

    private static EventMessage paidEvent(String orderId) {
        return new GenericEventMessage(new MessageType("paid"), Map.of("orderId", orderId, "kind", ""));
    }

    private static String publish(AxonConfiguration configuration, String eventName, String orderId, String kind) {
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

    /**
     * Appends the given workflow event straight to the storage engine, the way another writer would.
     */
    private void appendDirectly(EventMessage event) {
        var tags = new WorkflowEventTagResolver().resolve(event);
        storageEngine.appendEvents(AppendCondition.none(),
                                   null,
                                   List.of(new GenericTaggedEventMessage<>(event, Set.copyOf(tags))))
                     .thenCompose(transaction -> transaction.commit())
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    private static WorkflowEngine engine(Configuration configuration) {
        return find(configuration, WorkflowEngine.class).orElseThrow();
    }

    private static WorkflowCancellationService cancellationService(Configuration configuration) {
        return find(configuration, WorkflowCancellationService.class).orElseThrow();
    }

    private static WorkflowManager manager(Configuration configuration) {
        return find(configuration, WorkflowManager.class).orElseThrow();
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
    private static EventSourcedWorkflowState durableState(AxonConfiguration configuration, String workflowId) {
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
     * Delegates to an in-memory storage engine. It fails every sourcing, or a given number of the next ones, or never
     * completes one, while told to, the way an unreachable or stalled event store would, while streaming keeps working,
     * so the processor keeps handling events. It counts the sourcings it is asked for, and those whose context carries
     * a tracking token. It also holds the first append of a given step's events until released, so the workflow waits
     * for that step's STARTED event.
     */
    private static final class ControlledStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final AtomicInteger sourcings = new AtomicInteger();
        private final AtomicInteger sourcingsToFail = new AtomicInteger();
        private final AtomicInteger failedSourcings = new AtomicInteger();
        private final CountDownLatch startHeld = new CountDownLatch(1);
        private final CompletableFuture<Void> heldStartReleased = new CompletableFuture<>();
        private volatile boolean failSourcing;
        private volatile boolean hangSourcing;
        private volatile @Nullable String holdStartOf;

        private ControlledStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private void failSourcing(boolean fail) {
            this.failSourcing = fail;
        }

        private void hangSourcing(boolean hang) {
            this.hangSourcing = hang;
        }

        private void failNextSourcings(int count) {
            sourcingsToFail.set(count);
        }

        private int failedSourcings() {
            return failedSourcings.get();
        }

        private int sourcings() {
            return sourcings.get();
        }

        private void resetSourcings() {
            sourcings.set(0);
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
                                                                    @Nullable ProcessingContext processingContext,
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
        public MessageStream<EventMessage> source(SourcingCondition condition,
                                                  @Nullable ProcessingContext processingContext) {
            sourcings.incrementAndGet();
            if (failSourcing || sourcingsToFail.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                failedSourcings.incrementAndGet();
                return MessageStream.failed(new IllegalStateException("The event store is unreachable."));
            }
            if (hangSourcing) {
                return MessageStream.fromFuture(new CompletableFuture<EventMessage>());
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
     * Schedules every task on a timer of its own, like the default scheduler. Once told to, it fires the pending task
     * with the same deadline as a task it is asked to schedule, before scheduling that one: the timer of a wait then
     * fires while its workflow hibernates, as a hibernating workflow schedules its wake for the deadline of that wait.
     */
    private static final class TwinFiringScheduler implements WorkflowScheduler {

        private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        private final List<TimedTask> tasks = new CopyOnWriteArrayList<>();
        private volatile boolean fireTwins;

        private void fireTwins() {
            this.fireTwins = true;
        }

        private void shutdown() {
            timer.shutdownNow();
        }

        @Override
        public ScheduledTask schedule(Instant deadline) {
            if (fireTwins) {
                tasks.stream()
                     .filter(task -> task.deadline().equals(deadline))
                     .forEach(task -> task.completion().complete(null));
            }
            var task = new TimedTask(deadline, new CompletableFuture<>());
            tasks.add(task);
            timer.schedule(() -> task.completion().complete(null),
                           Math.max(0, Duration.between(Instant.now(), deadline).toMillis()),
                           TimeUnit.MILLISECONDS);
            return task;
        }

        private record TimedTask(Instant deadline, CompletableFuture<Void> completion) implements ScheduledTask {

            @Override
            public void cancel() {
                completion.cancel(false);
            }
        }
    }

    /**
     * Keeps the executions in memory like the default repository, and runs given actions once around the next listing
     * of the executions of a segment, as a broadcast event does, which is the only one while no segment is claimed or
     * released, or around the next listing for a query, as the workflow manager does: the moments between the lookups
     * of the executions in memory and of the hibernated workflows, which a workflow that wakes or hibernates crosses
     * unseen unless the lookups account for it.
     */
    private static final class ListingInterceptingRepository extends InMemoryWorkflowExecutionRepository {

        private final AtomicReference<@Nullable Runnable> beforeNextSegmentListing = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextSegmentListing = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> beforeNextQuery = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextQuery = new AtomicReference<>();

        private void aroundNextSegmentListing(Runnable before, Runnable after) {
            afterNextSegmentListing.set(after);
            beforeNextSegmentListing.set(before);
        }

        private void afterNextSegmentListing(Runnable action) {
            aroundNextSegmentListing(() -> {
            }, action);
        }

        private void aroundNextQuery(Runnable before, Runnable after) {
            afterNextQuery.set(after);
            beforeNextQuery.set(before);
        }

        private void afterNextQuery(Runnable action) {
            aroundNextQuery(() -> {
            }, action);
        }

        private static void runOnce(AtomicReference<@Nullable Runnable> hook) {
            var action = hook.getAndSet(null);
            if (action != null) {
                action.run();
            }
        }

        @Override
        public Set<WorkflowExecution> findAll() {
            // Lists every execution, as the hibernation passes and the test do, which is no lookup to intercept.
            return super.findAll(execution -> true);
        }

        @Override
        public Set<WorkflowExecution> findAll(Predicate<WorkflowExecution> predicate) {
            runOnce(beforeNextSegmentListing);
            var listing = super.findAll(predicate);
            runOnce(afterNextSegmentListing);
            return listing;
        }

        @Override
        public CompletableFuture<Set<WorkflowExecution>> findAll(WorkflowStateQuery query) {
            runOnce(beforeNextQuery);
            var listing = super.findAll(query);
            runOnce(afterNextQuery);
            return listing;
        }
    }

    /**
     * A clock that stands still until the test moves it on.
     */
    private static final class AdjustableClock extends Clock {

        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());

        private void advance(Duration duration) {
            now.updateAndGet(instant -> instant.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return zone.equals(getZone()) ? this : Clock.fixed(instant(), zone);
        }

        @Override
        public Instant instant() {
            return now.get();
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
