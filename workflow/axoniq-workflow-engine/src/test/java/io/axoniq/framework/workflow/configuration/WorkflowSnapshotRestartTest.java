/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.configuration.AbstractEventSourcedEntityRepositoryTestBase.TestContext;
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
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.DelegatingMessageStream;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.modelling.repository.ManagedEntity;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Restarts the default {@link WorkflowConfigurer} wiring with snapshots enabled, against an event store, a token store
 * and a snapshot store that all outlive the stopped configuration, the way their durable counterparts do.
 * <p>
 * The oracles are how often each step action ran, the durable status of every workflow, and the state a configuration
 * without snapshots replays from the same event store.
 *
 * @author Stefan Dragisic
 */
class WorkflowSnapshotRestartTest {

    private static final String MODULE = "snapshot-restart";
    private static final String WAIT_STEP = "awaitPaid";
    private static final WaitForStepDefinition AWAIT_PAID = new WaitForStepDefinition(
            new PrimitiveMetadata(WAIT_STEP, DefaultEventNameCustomizer.Builder.defaults()),
            EventConditions.fromQualifiedName(new QualifiedName("paid")),
            new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
            new Timing(Duration.ofMinutes(5))
    );

    private final EventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final TokenStore tokenStore = new InMemoryTokenStore();
    private final Converter converter = new JacksonConverter();
    private final SnapshotStore snapshotStore = new InMemorySnapshotStore(converter);
    private final Map<String, AtomicInteger> stepExecutions = new ConcurrentHashMap<>();
    private final Map<AxonConfiguration, DisconnectableStorageEngine> configurations = new LinkedHashMap<>();

    @AfterEach
    void tearDown() {
        List.copyOf(configurations.keySet()).forEach(this::stop);
    }

    @Nested
    class ManyWorkflows {

        private static final int WORKFLOWS = 200;
        private static final int STEPS = 3;

        @Test
        void parkedWorkflowsContinueFromTheirSnapshotsAfterRestart() {
            // given - workflows that each ran their steps and now wait for an event
            var first = start(workflowWithSteps(STEPS), true);
            var workflowIds = startWorkflows(first, WORKFLOWS);
            awaitParked(first, workflowIds);
            assertThat(workflowIds).allSatisfy(
                    workflowId -> assertThat(snapshotOf(first, EventSourcedWorkflowState.class, workflowId))
                            .isNotNull()
            );
            stop(first);
            // The running-workflow list is sourced on a restart only, so the first restart is the one snapshotting it.
            var second = start(workflowWithSteps(STEPS), true);
            await().atMost(30, TimeUnit.SECONDS)
                   .until(() -> engine(second).workflowExecutions().size() == WORKFLOWS);
            assertThat(snapshotOf(second, EventSourcedRunningWorkflows.class, EventSourcedRunningWorkflows.ENTITY_ID))
                    .isNotNull();
            stop(second);

            // when - the list and every workflow state continue from their snapshot
            var restarted = start(workflowWithSteps(STEPS), true);
            await().atMost(30, TimeUnit.SECONDS)
                   .until(() -> engine(restarted).workflowExecutions().size() == WORKFLOWS);
            publish(restarted, "paid");

            // then - every workflow completes, and no step ran a second time
            await().atMost(60, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> runningWorkflowIds(restarted).isEmpty());
            assertThat(workflowIds).allSatisfy(
                    workflowId -> assertThat(durableState(restarted, workflowId).workflowStatus())
                            .isEqualTo(WorkflowStatus.COMPLETED)
            );
            assertThat(stepExecutions).hasSize(WORKFLOWS * (STEPS + 1));
            assertThat(stepExecutions.values()).allSatisfy(executions -> assertThat(executions).hasValue(1));
        }

        @Test
        void stateContinuedFromSnapshotEqualsStateReplayedFromEveryEvent() {
            // given - workflows parked by a stopped configuration, restored by a restarted one
            var stopped = start(workflowWithSteps(STEPS), true);
            var workflowIds = startWorkflows(stopped, WORKFLOWS);
            awaitParked(stopped, workflowIds);
            stop(stopped);
            var restarted = start(workflowWithSteps(STEPS), true);
            await().atMost(30, TimeUnit.SECONDS)
                   .until(() -> engine(restarted).workflowExecutions().size() == WORKFLOWS);
            // A configuration without snapshots that is never started only reads: it replays every event.
            var replaying = build(workflowWithSteps(STEPS), false);

            // when / then - while parked
            assertThat(runningWorkflowIds(restarted)).isEqualTo(runningWorkflowIds(replaying));
            assertThat(workflowIds).allSatisfy(
                    workflowId -> assertThat(describe(durableState(restarted, workflowId)))
                            .isEqualTo(describe(durableState(replaying, workflowId)))
            );

            // when / then - after every workflow completed
            publish(restarted, "paid");
            await().atMost(60, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> runningWorkflowIds(restarted).isEmpty());
            assertThat(runningWorkflowIds(replaying)).isEmpty();
            assertThat(workflowIds).allSatisfy(
                    workflowId -> assertThat(describe(durableState(restarted, workflowId)))
                            .isEqualTo(describe(durableState(replaying, workflowId)))
            );
        }
    }

    @Nested
    class LongWorkflow {

        private static final int STEPS = 300;

        @Test
        void runsNoStepAgainAfterRestartingTwiceFromItsSnapshot() {
            // given - one workflow that ran all its steps and now waits for an event
            var first = start(workflowWithSteps(STEPS), true);
            var workflowId = startWorkflows(first, 1).getFirst();
            awaitParked(first, List.of(workflowId));
            stop(first);

            // when
            var second = start(workflowWithSteps(STEPS), true);
            await().atMost(30, TimeUnit.SECONDS).until(() -> engine(second).workflowExecutions().size() == 1);
            stop(second);
            var third = start(workflowWithSteps(STEPS), true);
            await().atMost(30, TimeUnit.SECONDS).until(() -> engine(third).workflowExecutions().size() == 1);
            publish(third, "paid");

            // then
            await().atMost(30, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableState(third, workflowId).workflowStatus() == WorkflowStatus.COMPLETED);
            assertThat(snapshotOf(third, EventSourcedWorkflowState.class, workflowId)).isNotNull();
            assertThat(stepExecutions).hasSize(STEPS + 1);
            assertThat(stepExecutions.values()).allSatisfy(executions -> assertThat(executions).hasValue(1));
            assertThat(durableState(third, workflowId).payload()).containsEntry("step-" + (STEPS - 1), "done")
                                                                 .containsEntry("ship", "done");
        }
    }

    @Nested
    class RetriedStep {

        @Test
        void keepsItsUsedRetryAfterRestartFromItsSnapshot() {
            // given - a step with one retry, whose first attempt failed and that now waits for that retry
            Consumer<TestContext> body = ctx -> ctx.awaitExecute(failingTwiceStep(ctx.workflowId()));
            var stopped = start(body, true);
            var workflowId = startWorkflows(stopped, 1).getFirst();
            await().atMost(10, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableState(stopped, workflowId).getStep("charge").status() == StepStatus.RETRYING);
            assertThat(snapshotOf(stopped, EventSourcedWorkflowState.class, workflowId)).isNotNull();
            stop(stopped);

            // when
            var restarted = start(body, true);

            // then - the failing second attempt is the last one. A step that forgot its first attempt would try a
            // third time, and succeed.
            await().atMost(30, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableState(restarted, workflowId).workflowStatus() == WorkflowStatus.FAILED);
            assertThat(stepExecutions.get(workflowId + "/charge")).hasValue(2);
        }

        private ExecuteStepDefinition failingTwiceStep(String workflowId) {
            return new ExecuteStepDefinition(
                    new PrimitiveMetadata("charge", DefaultEventNameCustomizer.Builder.defaults()),
                    Map.of(),
                    (processingContext, payload) -> {
                        if (executionsOf(workflowId, "charge").incrementAndGet() <= 2) {
                            throw new IllegalStateException("card declined");
                        }
                        return Map.of("charge", "done");
                    },
                    new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, CombineGlobalAndLocalPayloadReducer.INSTANCE),
                    new Timing(Duration.ofMinutes(5)),
                    RetryPolicy.maxRetries(1).withBackoff(BackoffStrategy.fixed(Duration.ofSeconds(5)))
            );
        }
    }

    @Nested
    class StepResultOfNestedValues {

        private static final Map<String, Object> QUOTE = Map.of(
                "items", 3,
                "weight", 2.5,
                "express", true,
                "address", Map.of("city", "Belgrade", "zip", 11000),
                "tags", List.of("a", "b")
        );

        private final List<Map<String, Object>> quotesRead = new CopyOnWriteArrayList<>();

        @Test
        void readsTheSameAfterRestartFromItsSnapshot() {
            // given - a workflow whose first step returned numbers and nested values, parked at its wait
            var stopped = start(quoteWorkflow(), true);
            var workflowId = startWorkflows(stopped, 1).getFirst();
            awaitParked(stopped, List.of(workflowId));
            assertThat(snapshotOf(stopped, EventSourcedWorkflowState.class, workflowId)).isNotNull();
            stop(stopped);

            // when
            var restarted = start(quoteWorkflow(), true);
            await().atMost(30, TimeUnit.SECONDS).until(() -> engine(restarted).workflowExecutions().size() == 1);
            var replaying = build(quoteWorkflow(), false);

            // then - the state continued from the snapshot equals the state replayed from every event
            assertThat(describe(durableState(restarted, workflowId)))
                    .isEqualTo(describe(durableState(replaying, workflowId)));

            // and - the restored workflow read the step result as the first one did, without running the step again
            publish(restarted, "paid");
            await().atMost(30, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableState(restarted, workflowId).workflowStatus() == WorkflowStatus.COMPLETED);
            assertThat(quotesRead).hasSizeGreaterThanOrEqualTo(2).containsOnly(QUOTE);
            assertThat(durableState(restarted, workflowId).payload()).containsAllEntriesOf(QUOTE);
            assertThat(stepExecutions.get(workflowId + "/quote")).hasValue(1);
        }

        private Consumer<TestContext> quoteWorkflow() {
            return ctx -> {
                quotesRead.add(ctx.awaitExecute(quoteStep(ctx.workflowId())));
                ctx.awaitEvent(AWAIT_PAID);
                ctx.awaitExecute(countedStep(ctx.workflowId(), "ship"));
            };
        }

        private ExecuteStepDefinition quoteStep(String workflowId) {
            return new ExecuteStepDefinition(
                    new PrimitiveMetadata("quote", DefaultEventNameCustomizer.Builder.defaults()),
                    Map.of(),
                    (processingContext, payload) -> {
                        executionsOf(workflowId, "quote").incrementAndGet();
                        return QUOTE;
                    },
                    new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, CombineGlobalAndLocalPayloadReducer.INSTANCE),
                    new Timing(Duration.ofMinutes(5)),
                    RetryPolicy.NONE
            );
        }
    }

    private Consumer<TestContext> workflowWithSteps(int steps) {
        return ctx -> {
            for (int step = 0; step < steps; step++) {
                ctx.awaitExecute(countedStep(ctx.workflowId(), "step-" + step));
            }
            ctx.awaitEvent(AWAIT_PAID);
            ctx.awaitExecute(countedStep(ctx.workflowId(), "ship"));
        };
    }

    private ExecuteStepDefinition countedStep(String workflowId, String stepName) {
        return new ExecuteStepDefinition(
                new PrimitiveMetadata(stepName, DefaultEventNameCustomizer.Builder.defaults()),
                Map.of(),
                (processingContext, payload) -> {
                    executionsOf(workflowId, stepName).incrementAndGet();
                    return Map.of(stepName, "done");
                },
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, CombineGlobalAndLocalPayloadReducer.INSTANCE),
                new Timing(Duration.ofMinutes(5)),
                RetryPolicy.NONE
        );
    }

    private AtomicInteger executionsOf(String workflowId, String stepName) {
        return stepExecutions.computeIfAbsent(workflowId + "/" + stepName, key -> new AtomicInteger());
    }

    private AxonConfiguration start(Consumer<TestContext> body, boolean snapshots) {
        var configuration = build(body, snapshots);
        configuration.start();
        return configuration;
    }

    private AxonConfiguration build(Consumer<TestContext> body, boolean snapshots) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> body::accept)
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(4))
                                   .contextFactory(c -> TestContext::new);
        var nodeStorageEngine = new DisconnectableStorageEngine(storageEngine);
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> nodeStorageEngine)
                .registerComponent(TokenStore.class, cfg -> tokenStore)
                .registerModule(module));
        if (snapshots) {
            configurer.componentRegistry(cr -> cr.registerComponent(SnapshotStore.class, cfg -> snapshotStore))
                      .snapshots(s -> s.enabled(true)
                                       .runningWorkflows(SnapshotPolicy.afterEvents(10))
                                       .workflowState(SnapshotPolicy.afterEvents(2)));
        }
        var configuration = configurer.build();
        configurations.put(configuration, nodeStorageEngine);
        return configuration;
    }

    private void stop(AxonConfiguration configuration) {
        configuration.shutdown();
        configurations.remove(configuration).disconnect();
    }

    /**
     * Starts the given number of workflows and returns their ids. The default workflow id is the identifier of the
     * start event.
     */
    private static List<String> startWorkflows(AxonConfiguration configuration, int count) {
        return IntStream.range(0, count).mapToObj(index -> publish(configuration, "start")).toList();
    }

    private void awaitParked(AxonConfiguration configuration, List<String> workflowIds) {
        await().atMost(60, TimeUnit.SECONDS)
               .until(() -> engine(configuration).workflowExecutions().size() == workflowIds.size());
        await().atMost(60, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> workflowIds.stream()
                                       .allMatch(workflowId -> durableState(configuration, workflowId)
                                               .containsStep(WAIT_STEP)));
    }

    private static String publish(AxonConfiguration configuration, String eventName) {
        var event = new GenericEventMessage(new MessageType(eventName), Map.of("orderId", "order-1"));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + eventName)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }

    private static WorkflowEngine engine(AxonConfiguration configuration) {
        return configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[" + MODULE + "]");
    }

    private static Set<String> runningWorkflowIds(AxonConfiguration configuration) {
        return load(configuration, EventSourcedRunningWorkflows.class, EventSourcedRunningWorkflows.ENTITY_ID)
                .workflowIds();
    }

    private static EventSourcedWorkflowState durableState(AxonConfiguration configuration, String workflowId) {
        return load(configuration, EventSourcedWorkflowState.class, workflowId);
    }

    @SuppressWarnings("unchecked")
    private static <E> E load(AxonConfiguration configuration, Class<E> entityType, String identifier) {
        var repository = (Repository<String, E>) configuration
                .getComponents(Repository.class)
                .values()
                .stream()
                .filter(candidate -> candidate.entityType().equals(entityType))
                .findFirst()
                .orElseThrow();
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("read-" + identifier)
                            .executeWithResult(ctx -> repository.loadOrCreate(identifier, ctx)
                                                                .thenApply(ManagedEntity::entity))
                            .orTimeout(5, TimeUnit.SECONDS)
                            .join();
    }

    @Nullable
    private Snapshot snapshotOf(AxonConfiguration configuration, Class<?> entityType, String identifier) {
        var snapshotName = configuration.getComponent(MessageTypeResolver.class)
                                        .resolveOrThrow(entityType)
                                        .qualifiedName();
        return FutureUtils.joinAndUnwrap(snapshotStore.load(snapshotName, identifier, null));
    }

    /**
     * Describes a state by its plain values, with every step result converted to a map. A state continued from a
     * snapshot holds the map the converter read, where a replayed state may hold the event payload object itself.
     */
    private Map<String, Object> describe(EventSourcedWorkflowState state) {
        var steps = new LinkedHashMap<String, Object>();
        for (String stepName : state.workflowStepNames().stream().sorted().toList()) {
            var step = state.getStep(stepName);
            Object result = converter.convert(step.result(), EventSourcedWorkflowState.PAYLOAD_TYPE.getType());
            steps.put(stepName, List.of(
                    step.status(), step.timestamp(), String.valueOf(result), String.valueOf(step.error())
            ));
        }
        return Map.of(
                "workflowId", state.workflowId(),
                "status", state.workflowStatus(),
                "definition", state.workflowDefinitionId(),
                "payload", state.payload(),
                "versions", state.versionMigrations(),
                "steps", steps
        );
    }

    /**
     * The event store as one configuration sees it: the shared store, which stops notifying that configuration once it
     * stopped, the way a connection to a durable event store ends with its process. The in-memory store itself keeps
     * notifying every stream it ever opened, so a later append would call into the stopped event processor and fail.
     */
    private static final class DisconnectableStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final AtomicBoolean connected = new AtomicBoolean(true);

        private DisconnectableStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private void disconnect() {
            connected.set(false);
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            return delegate.appendEvents(condition, processingContext, events);
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition, ProcessingContext processingContext) {
            return delegate.source(condition, processingContext);
        }

        @Override
        public MessageStream<EventMessage> stream(StreamingCondition condition) {
            return new DelegatingMessageStream<EventMessage, EventMessage>(delegate.stream(condition)) {
                @Override
                public Optional<Entry<EventMessage>> next() {
                    return delegate().next();
                }

                @Override
                public Optional<Entry<EventMessage>> peek() {
                    return delegate().peek();
                }

                @Override
                public void setCallback(Runnable callback) {
                    super.setCallback(() -> {
                        if (connected.get()) {
                            callback.run();
                        }
                    });
                }
            };
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
