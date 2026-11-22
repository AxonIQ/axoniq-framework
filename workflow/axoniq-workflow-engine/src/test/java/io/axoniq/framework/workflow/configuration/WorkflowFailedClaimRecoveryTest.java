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
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
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
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Restarts the default {@link WorkflowConfigurer} wiring on a node whose first claims of the segment cannot load the
 * running workflows, while an event that wakes a running workflow arrives.
 * <p>
 * A claim whose restore failed must not let its segment move past that event: the event processor has to release the
 * segment without storing its position, and claim it again later, backing off in between, so that a claim that
 * restores the workflow delivers the event to it.
 *
 * @author Allard Buijze
 */
class WorkflowFailedClaimRecoveryTest {

    private static final String MODULE = "failed-claim-recovery";
    private static final int FAILING_CLAIMS = 2;
    private static final WaitForStepDefinition AWAIT_PAID = new WaitForStepDefinition(
            new PrimitiveMetadata("awaitPaid", DefaultEventNameCustomizer.Builder.defaults()),
            EventConditions.fromQualifiedName(new QualifiedName("paid")),
            new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
            new Timing(Duration.ofMinutes(5))
    );

    private final EventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final TokenStore tokenStore = new InMemoryTokenStore();
    private final Set<String> runningBodies = ConcurrentHashMap.newKeySet();
    private final Set<String> completedBodies = ConcurrentHashMap.newKeySet();
    private final List<AxonConfiguration> configurations = new ArrayList<>();

    @AfterEach
    void tearDown() {
        configurations.forEach(AxonConfiguration::shutdown);
    }

    @Test
    void wakesARunningWorkflowWhoseEventArrivedWhileClaimsOfItsSegmentFailedToRestoreIt() {
        // given - a workflow waiting for its payment, on a node that stopped
        var firstStorageEngine = new DisconnectableStorageEngine(storageEngine);
        var first = start(firstStorageEngine);
        var workflowId = UUID.randomUUID().toString();
        publish(first, "start", workflowId);
        await().atMost(10, TimeUnit.SECONDS).until(() -> runningBodies.contains(workflowId));
        first.shutdown();
        configurations.remove(first);
        firstStorageEngine.disconnect();
        // and - a node whose first claims of the segment cannot load the running workflows
        var failingStorageEngine = new RunningWorkflowsLoadFailingStorageEngine(storageEngine);
        var second = start(failingStorageEngine);
        await().atMost(10, TimeUnit.SECONDS).until(() -> failingStorageEngine.runningWorkflowLoads() >= 1);

        // when - the payment arrives while the segment's claims fail to restore the workflow
        publish(second, "paid", workflowId);

        // then - a later claim restores the workflow and hands it the payment
        await().atMost(30, TimeUnit.SECONDS).until(() -> completedBodies.contains(workflowId));
        // and - the failed claims were retried with a back-off, not in a tight loop
        assertThat(failingStorageEngine.runningWorkflowLoads()).isBetween(FAILING_CLAIMS + 1, FAILING_CLAIMS + 3);
    }

    private AxonConfiguration start(EventStorageEngine nodeStorageEngine) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                               runningBodies.add(ctx.workflowId());
                                               ctx.awaitEvent(AWAIT_PAID);
                                               completedBodies.add(ctx.workflowId());
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(1)
                                                                   .tokenClaimInterval(200))
                                   .contextFactory(c -> TestContext::new);
        var configuration = WorkflowConfigurer.create().componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> nodeStorageEngine)
                .registerComponent(TokenStore.class, cfg -> tokenStore)
                .registerModule(module)
        ).build();
        configurations.add(configuration);
        configuration.start();
        return configuration;
    }

    /**
     * Publishes an event of the given type, whose identifier is the given {@code workflowId}. The default workflow id
     * of a started workflow is the identifier of its start event.
     */
    private static void publish(AxonConfiguration configuration, String type, String workflowId) {
        String identifier = type.equals("start") ? workflowId : UUID.randomUUID().toString();
        var event = new GenericEventMessage(identifier,
                                            new MessageType(type),
                                            Map.of("orderId", workflowId),
                                            Map.of(),
                                            Instant.now());
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + type)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    /**
     * Fails loading the running workflows for the first {@link #FAILING_CLAIMS} times, the way an event store that is
     * briefly unavailable fails the restore of a claim.
     */
    private static final class RunningWorkflowsLoadFailingStorageEngine implements EventStorageEngine {

        private static final Tag LIFECYCLE = Tag.of(WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE,
                                                    WorkflowEventTags.TAG_VALUE_EVENT_TYPE_LIFECYCLE);

        private final EventStorageEngine delegate;
        private final AtomicInteger runningWorkflowLoads = new AtomicInteger();

        private RunningWorkflowsLoadFailingStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        int runningWorkflowLoads() {
            return runningWorkflowLoads.get();
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition, ProcessingContext processingContext) {
            boolean loadsRunningWorkflows = condition.criteria()
                                                     .flatten()
                                                     .stream()
                                                     .anyMatch(criterion -> criterion.tags().contains(LIFECYCLE));
            if (loadsRunningWorkflows && runningWorkflowLoads.incrementAndGet() <= FAILING_CLAIMS) {
                return MessageStream.failed(new IllegalStateException("event store briefly unavailable"));
            }
            return delegate.source(condition, processingContext);
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            return delegate.appendEvents(condition, processingContext, events);
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
