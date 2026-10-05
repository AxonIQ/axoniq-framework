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
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.AxonConfiguration;
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
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;

/**
 * Drives the default {@link WorkflowConfigurer} wiring, with a real event processor, token store and event store, to
 * check that a wait step's awaited event is only passed by the stored token once the wake is durable.
 * <p>
 * The oracle is the stored token of the single segment compared with the position of the awaited event, and after a
 * restart the durable status of the workflow: a restored wait that never sees its event again stays non-terminal.
 *
 * @author Stefan Dragisic
 */
class WorkflowConfigurerWakeCheckpointTest {

    private static final String MODULE = "wake-checkpoint";
    private static final String PROCESSOR = MODULE;
    private static final String WAIT_STEP = "awaitPaid";
    private static final WaitForStepDefinition AWAIT_PAID = new WaitForStepDefinition(
            new PrimitiveMetadata(WAIT_STEP, DefaultEventNameCustomizer.Builder.defaults()),
            EventConditions.fromQualifiedName(new QualifiedName("paid")),
            new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
            new Timing(Duration.ofMinutes(5))
    );
    private static final Consumer<TestContext> AWAIT_PAID_BODY = ctx -> ctx.awaitEvent(AWAIT_PAID);

    private final InMemoryTokenStore tokenStore = new InMemoryTokenStore();
    private final GatedStorageEngine storageEngine = new GatedStorageEngine(new InMemoryEventStorageEngine());
    private final List<AxonConfiguration> configurations = new ArrayList<>();

    @AfterEach
    void tearDown() {
        storageEngine.failStalledAppend();
        configurations.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class CompletedStepAppendInFlight {

        @Test
        void storedTokenStaysBeforeTheAwaitedEventUntilTheCompletedStepIsDurable() {
            // given
            var configuration = start(tokenStore, AWAIT_PAID_BODY);
            var workflowId = startWorkflow(configuration);
            storageEngine.stallNextCompletedAppendOf(WAIT_STEP);

            // when
            publish(configuration, "paid");
            var paidPosition = latestToken();
            await().atMost(5, TimeUnit.SECONDS).until(storageEngine::completedAppendStalled);

            // then
            await().during(Duration.ofMillis(1000)).atMost(2, TimeUnit.SECONDS)
                   .until(() -> !covers(storedToken(tokenStore), paidPosition));

            // when
            storageEngine.releaseStalledAppend();

            // then
            await().atMost(5, TimeUnit.SECONDS).until(() -> covers(storedToken(tokenStore), paidPosition));
            await().atMost(5, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableStatus(configuration, workflowId) == WorkflowStatus.COMPLETED);
        }

        @Test
        void restoredWaitCompletesAfterACrashBetweenTheWakeAndItsCompletedStep() {
            // given
            var crashed = start(tokenStore, AWAIT_PAID_BODY);
            var workflowId = startWorkflow(crashed);
            storageEngine.stallNextCompletedAppendOf(WAIT_STEP);
            publish(crashed, "paid");
            await().atMost(5, TimeUnit.SECONDS).until(storageEngine::completedAppendStalled);
            // Give the crashed node the time to store whatever token it is going to store.
            await().pollDelay(Duration.ofMillis(500)).until(() -> true);
            var tokenAtCrash = storedToken(tokenStore);
            // A second event after the awaited one makes the restarted node catch up over more than one event.
            publish(crashed, "unrelated");

            // when
            var restartedTokenStore = new InMemoryTokenStore();
            restartedTokenStore.initializeTokenSegments(PROCESSOR, 1, tokenAtCrash, null)
                               .orTimeout(5, TimeUnit.SECONDS)
                               .join();
            var restarted = start(restartedTokenStore, AWAIT_PAID_BODY);

            // then
            await().atMost(10, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableStatus(restarted, workflowId) == WorkflowStatus.COMPLETED);
        }
    }

    @Nested
    class WorkflowStart {

        @Test
        void newWorkflowRunsToCompletionAndItsStartEventIsCheckpointed() {
            // given
            var configuration = start(tokenStore, ctx -> {
            });

            // when
            // The default workflow id is the identifier of the start event.
            var workflowId = publish(configuration, "start");
            var startPosition = latestToken();

            // then
            await().atMost(5, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> durableStatus(configuration, workflowId) == WorkflowStatus.COMPLETED);
            await().atMost(5, TimeUnit.SECONDS).until(() -> covers(storedToken(tokenStore), startPosition));
        }
    }

    private AxonConfiguration start(TokenStore tokenStore,
                                    Consumer<TestContext> body) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> body::accept)
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(1))
                                   .contextFactory(c -> TestContext::new);
        var configurer = WorkflowConfigurer.create();
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
     * Starts one workflow and returns its id once its wait step is durably started.
     */
    private String startWorkflow(AxonConfiguration configuration) {
        publish(configuration, "start");
        var engine = configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[" + MODULE + "]");
        await().atMost(5, TimeUnit.SECONDS).until(() -> engine.workflowExecutions().size() == 1);
        var workflowId = engine.workflowExecutions().iterator().next().workflowId();
        await().atMost(5, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, workflowId).containsStep(WAIT_STEP));
        return workflowId;
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

    private TrackingToken latestToken() {
        return storageEngine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
    }

    @Nullable
    private static TrackingToken storedToken(TokenStore tokenStore) {
        return tokenStore.fetchToken(PROCESSOR, 0, null).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static boolean covers(@Nullable TrackingToken stored, TrackingToken position) {
        return stored != null && stored.covers(position);
    }

    private static WorkflowStatus durableStatus(AxonConfiguration configuration, String workflowId) {
        return durableState(configuration, workflowId).workflowStatus();
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
     * Holds back the first append of a given step's completed event until the test releases it, the way a slow or
     * crashing event store leaves a wake without its durable result.
     */
    private static final class GatedStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final CompletableFuture<Void> gate = new CompletableFuture<>();
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicBoolean stalled = new AtomicBoolean();
        private volatile String stepName;

        private GatedStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private void stallNextCompletedAppendOf(String stepName) {
            this.stepName = stepName;
            armed.set(true);
        }

        private boolean completedAppendStalled() {
            return stalled.get();
        }

        private void releaseStalledAppend() {
            gate.complete(null);
        }

        private void failStalledAppend() {
            gate.completeExceptionally(new IllegalStateException("node crashed before the append completed"));
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            if (events.stream().anyMatch(this::isArmedCompletedStep) && armed.compareAndSet(true, false)) {
                stalled.set(true);
                return gate.thenCompose(ignored -> delegate.appendEvents(condition, processingContext, events));
            }
            return delegate.appendEvents(condition, processingContext, events);
        }

        private boolean isArmedCompletedStep(TaggedEventMessage<?> tagged) {
            var metadata = tagged.event().metadata();
            return stepName != null
                    && stepName.equals(metadata.get(MetadataUtils.METADATA_KEY_STEP_NAME))
                    && "COMPLETED".equals(metadata.get(MetadataUtils.METADATA_KEY_TYPE));
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
