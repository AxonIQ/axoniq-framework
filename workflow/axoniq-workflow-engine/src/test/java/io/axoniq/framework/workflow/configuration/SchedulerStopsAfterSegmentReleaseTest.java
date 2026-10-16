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

package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.configuration.AbstractEventSourcedEntityRepositoryTestBase.TestContext;
import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.ExecuteStepDefinition;
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.api.PayloadProcessor;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.DefaultWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
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
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.modelling.repository.Repository;
import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests that the workflow timer keeps running until the processor has released its segments at shutdown, so a step
 * that starts during the release completes after a restart instead of failing.
 */
class SchedulerStopsAfterSegmentReleaseTest {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerStopsAfterSegmentReleaseTest.class);
    private static final String MODULE = "scheduler-stopped";
    private static final Duration ACK_DELAY = Duration.ofSeconds(3);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final InMemoryTokenStore tokenStore = new InMemoryTokenStore();
    private final SlowAckStorageEngine storageEngine = new SlowAckStorageEngine(new InMemoryEventStorageEngine());
    private final List<AxonConfiguration> configurations = new ArrayList<>();
    private final CountDownLatch holderBusy = new CountDownLatch(1);
    private final CountDownLatch holderGate = new CountDownLatch(1);
    private final CountDownLatch victimBusy = new CountDownLatch(1);
    private final CountDownLatch victimGate = new CountDownLatch(1);
    private final AtomicInteger secondRuns = new AtomicInteger();
    private final ScheduledThreadPoolExecutor survivingTimer = new ScheduledThreadPoolExecutor(1, runnable -> {
        var thread = new Thread(runnable, "surviving-workflow-timer");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void tearDown() {
        holderGate.countDown();
        victimGate.countDown();
        configurations.forEach(AxonConfiguration::shutdown);
        survivingTimer.shutdownNow();
    }

    @Nested
    class DefaultSchedulerStopsWithTheProcessors {

        @Test
        void stepStartedWhileTheProcessorReleasesCompletesTheWorkflowAfterARestart() {
            scenario(false);
        }
    }

    @Nested
    class SchedulerOutlivesTheProcessors {

        @Test
        void stepStartedWhileTheProcessorReleasesCompletesTheWorkflowAfterARestart() {
            scenario(true);
        }
    }

    private void scenario(boolean schedulerOutlivesProcessors) {
        // given
        AxonConfiguration first = start(schedulerOutlivesProcessors);
        WorkflowScheduler scheduler = first.getComponent(WorkflowScheduler.class);
        String victimId = publish(first, "start", "victim");
        awaitLatch(victimBusy);
        publish(first, "start", "holder");
        awaitLatch(holderBusy);
        storageEngine.delayAckOfStartedStep("second");
        victimGate.countDown();
        await().atMost(TIMEOUT).untilTrue(storageEngine.ackDelayed);
        // the victim already saw its started step and now waits for the slow ack; the holder parks in the poke delivery
        await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);
        publish(first, "poke", "holder");
        await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);

        // when
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(first::shutdown);
        await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);
        boolean schedulerRejects = rejects(scheduler);
        await().atMost(TIMEOUT).until(() -> secondRuns.get() == 1);
        await().pollDelay(Duration.ofMillis(500)).atMost(TIMEOUT).until(() -> true);
        boolean shutdownDoneBeforeHolderReleased = shutdown.isDone();
        StepStatus secondBeforeRestart = stepStatus(first, victimId, "second");
        holderGate.countDown();
        shutdown.orTimeout(30, TimeUnit.SECONDS).join();
        AxonConfiguration restarted = start(schedulerOutlivesProcessors);

        // then
        try {
            await().atMost(TIMEOUT).until(() -> durableStatus(restarted, victimId).isTerminal());
        } catch (ConditionTimeoutException e) {
            logger.warn("victim not terminal after restart");
        }
        WorkflowStatus status = durableStatus(restarted, victimId);
        StepStatus secondAfterRestart = stepStatus(restarted, victimId, "second");
        logger.warn("schedulerOutlivesProcessors={} schedulerRejectsDuringRelease={} "
                            + "shutdownDoneBeforeHolderReleased={} secondRuns={} secondBeforeRestart={} "
                            + "secondAfterRestart={} victimStatus={}",
                    schedulerOutlivesProcessors, schedulerRejects, shutdownDoneBeforeHolderReleased,
                    secondRuns.get(), secondBeforeRestart, secondAfterRestart, status);
        assertThat(status)
                .as("victim workflow after restart (scheduler rejected during release: %s, step 'second' ran %d "
                            + "time(s), was %s before the restart and %s after)",
                    schedulerRejects, secondRuns.get(), secondBeforeRestart, secondAfterRestart)
                .isEqualTo(WorkflowStatus.COMPLETED);
    }

    private void body(TestContext ctx) {
        if ("holder".equals(ctx.workflowPayload().get("role"))) {
            var poke = ctx.waitForEvent(waitFor("awaitPoke", afterHolderGate(new QualifiedName("poke"))));
            holderBusy.countDown();
            poke.await();
            return;
        }
        ctx.awaitExecute(step("first", (pc, payload) -> {
            victimBusy.countDown();
            awaitLatch(victimGate);
            return payload;
        }));
        ctx.awaitExecute(step("second", (pc, payload) -> {
            secondRuns.incrementAndGet();
            return payload;
        }));
    }

    private static ExecuteStepDefinition step(String name, PayloadProcessor action) {
        return new ExecuteStepDefinition(
                new PrimitiveMetadata(name, DefaultEventNameCustomizer.Builder.defaults()),
                Map.of(),
                action,
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(Duration.ofSeconds(30)),
                RetryPolicy.NONE
        );
    }

    private static WaitForStepDefinition waitFor(String name, EventCondition condition) {
        return new WaitForStepDefinition(
                new PrimitiveMetadata(name, DefaultEventNameCustomizer.Builder.defaults()),
                condition,
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(Duration.ofMinutes(5))
        );
    }

    // Evaluated inside the poke's delivery, so the holder drains its queue until the poke arrives. Blocking in the body
    // instead can strand a checkpoint latch behind the holder's started step and stall the processor before the victim
    // completes its first step.
    private EventCondition afterHolderGate(QualifiedName eventName) {
        return new EventCondition() {
            @Override
            public BiPredicate<EventMessage, ProcessingContext> predicate() {
                return (event, context) -> {
                    awaitLatch(holderGate);
                    return true;
                };
            }

            @Override
            public QualifiedName qualifiedName() {
                return eventName;
            }
        };
    }

    private static boolean rejects(WorkflowScheduler scheduler) {
        try {
            scheduler.schedule(Instant.now().plusSeconds(60)).cancel();
            return false;
        } catch (RejectedExecutionException e) {
            return true;
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private AxonConfiguration start(boolean schedulerOutlivesProcessors) {
        var module = WorkflowModule.defaults(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> this::body)
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(1))
                                   .contextFactory(c -> TestContext::new);
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> {
            cr.registerComponent(EventStorageEngine.class, cfg -> storageEngine)
              .registerComponent(TokenStore.class, cfg -> tokenStore)
              .registerModule(module);
            if (schedulerOutlivesProcessors) {
                cr.registerComponent(WorkflowScheduler.class,
                                     cfg -> new DefaultWorkflowScheduler(Clock.systemUTC(), survivingTimer));
            }
        });
        var configuration = configurer.build();
        configurations.add(configuration);
        configuration.start();
        return configuration;
    }

    private static String publish(AxonConfiguration configuration, String eventName, String role) {
        var event = new GenericEventMessage(new MessageType(eventName), Map.of("role", role));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + eventName)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class).publish(ctx, event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }

    private static WorkflowStatus durableStatus(AxonConfiguration configuration, String workflowId) {
        return durableState(configuration, workflowId).workflowStatus();
    }

    private static @Nullable StepStatus stepStatus(AxonConfiguration configuration, String workflowId, String step) {
        var state = durableState(configuration, workflowId);
        return state.containsStep(step) ? state.getStep(step).status() : null;
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

    private static final class SlowAckStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final AtomicBoolean ackDelayed = new AtomicBoolean();
        private volatile @Nullable String stepName;

        private SlowAckStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private void delayAckOfStartedStep(String stepName) {
            this.stepName = stepName;
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            var appended = delegate.appendEvents(condition, processingContext, events);
            if (events.stream().noneMatch(this::isStartedStep) || ackDelayed.getAndSet(true)) {
                return appended;
            }
            return appended.thenApply(SlowAckStorageEngine::slowAck);
        }

        private boolean isStartedStep(TaggedEventMessage<?> tagged) {
            var metadata = tagged.event().metadata();
            return stepName != null
                    && stepName.equals(metadata.get(MetadataUtils.METADATA_KEY_STEP_NAME))
                    && "STARTED".equals(metadata.get(MetadataUtils.METADATA_KEY_TYPE));
        }

        private static <R> AppendTransaction<?> slowAck(AppendTransaction<R> transaction) {
            return new AppendTransaction<R>() {
                @Override
                public CompletableFuture<R> commit() {
                    return transaction.commit().thenCompose(result -> CompletableFuture.supplyAsync(
                            () -> result,
                            CompletableFuture.delayedExecutor(ACK_DELAY.toMillis(), TimeUnit.MILLISECONDS)));
                }

                @Override
                public void rollback() {
                    transaction.rollback();
                }

                @Override
                public CompletableFuture<ConsistencyMarker> afterCommit(R commitResult) {
                    return transaction.afterCommit(commitResult);
                }
            };
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition, ProcessingContext processingContext) {
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
}
