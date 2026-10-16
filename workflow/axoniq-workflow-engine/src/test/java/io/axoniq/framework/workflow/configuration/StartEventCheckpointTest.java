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
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
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
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests that a checkpoint does not pass the start event of a new workflow before its STARTED event is saved, so a
 * crash between the two does not lose the workflow.
 *
 * @author Stefan Dragisic
 */
class StartEventCheckpointTest {

    private static final Logger logger = LoggerFactory.getLogger(StartEventCheckpointTest.class);
    private static final String MODULE = "start-checkpoint";
    static final String ENGINE = "WorkflowEngine[" + MODULE + "]";
    private static final String PROCESSOR = MODULE;

    private final StallingStorageEngine storageEngine = new StallingStorageEngine(new InMemoryEventStorageEngine());
    private final List<AxonConfiguration> configurations = new ArrayList<>();

    @AfterEach
    void tearDown() {
        storageEngine.failStalledAppend();
        configurations.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class CrashBetweenTheStartBatchCommitAndTheStartedAppend {

        @Test
        void startBatchCommitDoesNotStoreATokenPastTheStartEvent() {
            // given
            var crashedTokenStore = new InMemoryTokenStore();
            var crashed = start(crashedTokenStore);
            var warmUpPosition = publishAndAwaitCheckpoint(crashed, crashedTokenStore, "warm-up");
            storageEngine.stallFirstWorkflowStartedAppend();

            // when
            var workflowId = publish(crashed, "start");
            var startPosition = latestToken();
            // The body only runs once the unit of work of the batch that delivered the start event has completed, so
            // a stalled STARTED append proves that batch, its checkpoint included, is done.
            await().atMost(5, TimeUnit.SECONDS).until(storageEngine::startedAppendStalled);

            // then
            assertThat(durableWorkflowStatuses(workflowId)).as("nothing of the new instance is durable yet").isEmpty();
            assertThat(execution(crashed, workflowId)).as("the body of the new instance was started")
                                                      .hasValueSatisfying(e -> assertThat(e.isRunning()).isTrue());
            var storedAtCrash = storedToken(crashedTokenStore);
            logger.info("Window open for [{}]: STARTED append stalled, durable statuses {}, stored token {}, "
                                + "warm-up at {}, start event at {}",
                        workflowId, durableWorkflowStatuses(workflowId), storedAtCrash, warmUpPosition, startPosition);
            assertThat(covers(storedAtCrash, warmUpPosition)).as("the segment checkpoints on this node").isTrue();
            assertThat(covers(storedAtCrash, startPosition))
                    .as("stored token %s passes the start event at %s before STARTED is durable",
                        storedAtCrash, startPosition)
                    .isFalse();
        }

        @Test
        void restartFromTheTokenStoredBeforeTheStartedEventWasDurableStartsTheWorkflow() {
            // given
            var crashedTokenStore = new InMemoryTokenStore();
            var crashed = start(crashedTokenStore);
            publishAndAwaitCheckpoint(crashed, crashedTokenStore, "warm-up");
            storageEngine.stallFirstWorkflowStartedAppend();
            var workflowId = publish(crashed, "start");
            await().atMost(5, TimeUnit.SECONDS).until(storageEngine::startedAppendStalled);
            var tokenAtCrash = storedToken(crashedTokenStore);

            // when
            // The crashed node never appends its STARTED event: its append stays stalled until the test ends.
            start(tokenStoreAt(tokenAtCrash));

            // then
            await().atMost(10, TimeUnit.SECONDS)
                   .until(() -> durableWorkflowStatuses(workflowId).contains("COMPLETED"));
            logger.info("Restart from {} for [{}]: durable statuses {}",
                        tokenAtCrash, workflowId, durableWorkflowStatuses(workflowId));
            assertThat(durableWorkflowStatuses(workflowId)).containsExactly("STARTED", "COMPLETED");
        }

        /**
         * Control arm proving the restart oracle can fail: a token stored at the start event's own position, which is
         * what the suspected defect would store, does lose the start.
         */
        @Test
        void restartFromATokenAtTheStartEventNeverStartsTheWorkflow() {
            // given
            var crashedTokenStore = new InMemoryTokenStore();
            var crashed = start(crashedTokenStore);
            publishAndAwaitCheckpoint(crashed, crashedTokenStore, "warm-up");
            storageEngine.stallFirstWorkflowStartedAppend();
            var workflowId = publish(crashed, "start");
            var startPosition = latestToken();
            await().atMost(5, TimeUnit.SECONDS).until(storageEngine::startedAppendStalled);

            // when
            var restartedTokenStore = tokenStoreAt(startPosition);
            var restarted = start(restartedTokenStore);
            // Once the restarted node checkpoints a later event, it has handled everything before it.
            publishAndAwaitCheckpoint(restarted, restartedTokenStore, "later");

            // then
            logger.info("Restart from {} for [{}]: durable statuses {}",
                        startPosition, workflowId, durableWorkflowStatuses(workflowId));
            assertThat(durableWorkflowStatuses(workflowId)).isEmpty();
        }
    }

    private AxonConfiguration start(TokenStore tokenStore) {
        var configuration = startNode(storageEngine, tokenStore);
        configurations.add(configuration);
        return configuration;
    }

    private TrackingToken publishAndAwaitCheckpoint(AxonConfiguration configuration,
                                                    TokenStore tokenStore,
                                                    String eventName) {
        return publishAndAwaitCheckpoint(storageEngine, configuration, tokenStore, eventName);
    }

    private TrackingToken latestToken() {
        return storageEngine.latestPosition();
    }

    private List<String> durableWorkflowStatuses(String workflowId) {
        return storageEngine.workflowStatuses(workflowId);
    }

    /**
     * Builds and starts one node of the default {@link WorkflowConfigurer} wiring over the given shared stores, with a
     * single segment and one workflow with an empty body that starts on events named {@code start}.
     */
    static AxonConfiguration startNode(StallingStorageEngine storageEngine, TokenStore tokenStore) {
        return startNode(storageEngine, tokenStore, registry -> {
        });
    }

    /**
     * Same as {@link #startNode(StallingStorageEngine, TokenStore)}, with extra component registrations that take
     * precedence over the workflow defaults.
     */
    static AxonConfiguration startNode(StallingStorageEngine storageEngine,
                                       TokenStore tokenStore,
                                       Consumer<ComponentRegistry> extraRegistrations) {
        var module = WorkflowModule.defaults(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(1))
                                   .contextFactory(c -> TestContext::new);
        ((SimpleWorkflowModule<?>) module).componentRegistry(extraRegistrations);
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr.registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                                             .registerComponent(TokenStore.class, cfg -> tokenStore)
                                             .registerModule(module));
        var configuration = configurer.build();
        configuration.start();
        return configuration;
    }

    static TrackingToken publishAndAwaitCheckpoint(StallingStorageEngine storageEngine,
                                                   AxonConfiguration configuration,
                                                   TokenStore tokenStore,
                                                   String eventName) {
        publish(configuration, eventName);
        var position = storageEngine.latestPosition();
        await().atMost(5, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> covers(storedToken(tokenStore), position));
        return position;
    }

    static String publish(AxonConfiguration configuration, String eventName) {
        var event = new GenericEventMessage(new MessageType(eventName), Map.of("orderId", "order-1"));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + eventName)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        // The default workflow id is the identifier of the start event.
        return event.identifier();
    }

    static Optional<WorkflowExecution> execution(AxonConfiguration configuration, String workflowId) {
        return configuration.getComponents(WorkflowEngine.class)
                            .get(ENGINE)
                            .workflowExecutions()
                            .stream()
                            .filter(execution -> execution.workflowId().equals(workflowId))
                            .findFirst();
    }

    static TokenStore tokenStoreAt(@Nullable TrackingToken token) {
        var tokenStore = new InMemoryTokenStore();
        tokenStore.initializeTokenSegments(PROCESSOR, 1, token, null).orTimeout(5, TimeUnit.SECONDS).join();
        return tokenStore;
    }

    @Nullable
    static TrackingToken storedToken(TokenStore tokenStore) {
        return tokenStore.fetchToken(PROCESSOR, 0, null).orTimeout(5, TimeUnit.SECONDS).join();
    }

    static boolean covers(@Nullable TrackingToken stored, TrackingToken position) {
        return stored != null && stored.covers(position);
    }

    /**
     * Holds back the first append carrying a workflow STARTED event until the test ends, the way a slow store or a
     * crashing node leaves a new instance without anything durable.
     */
    static final class StallingStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final CompletableFuture<Void> gate = new CompletableFuture<>();
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicBoolean stalled = new AtomicBoolean();

        StallingStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        void stallFirstWorkflowStartedAppend() {
            armed.set(true);
        }

        boolean startedAppendStalled() {
            return stalled.get();
        }

        void failStalledAppend() {
            gate.completeExceptionally(new IllegalStateException("node crashed before the append completed"));
        }

        TrackingToken latestPosition() {
            return delegate.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
        }

        /**
         * Reads the workflow statuses recorded durably for the given instance, in append order.
         */
        List<String> workflowStatuses(String workflowId) {
            var condition = SourcingCondition.conditionFor(EventSourcedWorkflowState.criteriaBuilder(workflowId));
            return delegate.source(condition, null)
                           .reduce(new ArrayList<String>(), (statuses, entry) -> {
                               var status = entry.message() == null
                                       ? null
                                       : entry.message().metadata().get(MetadataUtils.METADATA_KEY_WORKFLOW_STATUS);
                               if (status != null) {
                                   statuses.add(status);
                               }
                               return statuses;
                           })
                           .orTimeout(5, TimeUnit.SECONDS)
                           .join();
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            if (events.stream().anyMatch(StallingStorageEngine::isWorkflowStarted) && armed.compareAndSet(true, false)) {
                stalled.set(true);
                return gate.thenCompose(ignored -> delegate.appendEvents(condition, processingContext, events));
            }
            return delegate.appendEvents(condition, processingContext, events);
        }

        private static boolean isWorkflowStarted(TaggedEventMessage<?> tagged) {
            return "STARTED".equals(tagged.event().metadata().get(MetadataUtils.METADATA_KEY_WORKFLOW_STATUS));
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition,
                                                  @Nullable ProcessingContext processingContext) {
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
