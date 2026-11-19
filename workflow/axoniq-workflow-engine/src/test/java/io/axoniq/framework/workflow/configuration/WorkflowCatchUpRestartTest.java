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
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * Restarts the default {@link WorkflowConfigurer} wiring after a crash that left the stored token of a segment behind
 * the head of the stream, while every event after that token belongs to another segment.
 * <p>
 * The restored workflows of such a segment wait for the segment to catch up with the head before they run. The segment
 * is never handed an event at or after the head, as every event in its backlog is skipped, so catching up has to be
 * observed from the progress of the segment itself.
 *
 * @author Allard Buijze
 */
class WorkflowCatchUpRestartTest {

    private static final String MODULE = "catch-up-restart";
    private static final int SEGMENTS = 2;
    private static final WaitForStepDefinition AWAIT_PAID = new WaitForStepDefinition(
            new PrimitiveMetadata("awaitPaid", DefaultEventNameCustomizer.Builder.defaults()),
            EventConditions.fromQualifiedName(new QualifiedName("paid")),
            new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
            new Timing(Duration.ofMinutes(5))
    );

    private final EventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final FreezableTokenStore tokenStore = new FreezableTokenStore(new InMemoryTokenStore());
    private final Set<String> runningBodies = ConcurrentHashMap.newKeySet();
    private final Map<AxonConfiguration, DisconnectableStorageEngine> configurations = new LinkedHashMap<>();

    @AfterEach
    void tearDown() {
        List.copyOf(configurations.keySet()).forEach(this::stop);
    }

    @Test
    void runsTheRestoredWorkflowsOfASegmentWhoseBacklogHoldsOnlyEventsOfOtherSegments() {
        // given - a workflow of the first segment, then a crash that loses the progress of both segments while a
        // workflow of the second segment starts
        var first = start();
        var resident = workflowIdOwnedBy(new Segment(0, SEGMENTS - 1));
        publishStart(first, resident);
        awaitRunning(Set.of(resident));
        tokenStore.freeze();
        var other = workflowIdOwnedBy(new Segment(1, SEGMENTS - 1));
        publishStart(first, other);
        awaitRunning(Set.of(resident, other));
        stop(first);
        tokenStore.unfreeze();
        runningBodies.clear();

        // when
        start();

        // then - both restored workflows run, also the one whose segment only skips events up to the head
        awaitRunning(Set.of(resident, other));
    }

    private AxonConfiguration start() {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                               runningBodies.add(ctx.workflowId());
                                               ctx.awaitEvent(AWAIT_PAID);
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(SEGMENTS))
                                   .contextFactory(c -> TestContext::new);
        var nodeStorageEngine = new DisconnectableStorageEngine(storageEngine);
        var configuration = WorkflowConfigurer.create().componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> nodeStorageEngine)
                .registerComponent(TokenStore.class, cfg -> tokenStore)
                .registerModule(module)
        ).build();
        configurations.put(configuration, nodeStorageEngine);
        configuration.start();
        return configuration;
    }

    private void stop(AxonConfiguration configuration) {
        configuration.shutdown();
        configurations.remove(configuration).disconnect();
    }

    private void awaitRunning(Set<String> workflowIds) {
        await().atMost(10, TimeUnit.SECONDS).until(() -> runningBodies.containsAll(workflowIds));
    }

    /**
     * Returns a new workflow id owned by the given segment. The default workflow id is the identifier of the start
     * event.
     */
    private static String workflowIdOwnedBy(Segment segment) {
        return Stream.generate(() -> UUID.randomUUID().toString())
                     .filter(segment::matches)
                     .findFirst()
                     .orElseThrow();
    }

    private static void publishStart(AxonConfiguration configuration, String workflowId) {
        var event = new GenericEventMessage(workflowId,
                                            new MessageType("start"),
                                            Map.of("orderId", workflowId),
                                            Map.of(),
                                            Instant.now());
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-start")
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    /**
     * A token store that stops storing tokens while frozen, the way a node that crashed stores nothing more. Claims are
     * still released, so a restarted node does not wait for them to time out.
     */
    private static final class FreezableTokenStore implements TokenStore {

        private final TokenStore delegate;
        private final AtomicBoolean frozen = new AtomicBoolean();

        private FreezableTokenStore(TokenStore delegate) {
            this.delegate = delegate;
        }

        void freeze() {
            frozen.set(true);
        }

        void unfreeze() {
            frozen.set(false);
        }

        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName,
                                                                        int segmentCount,
                                                                        @Nullable TrackingToken initialToken,
                                                                        @Nullable ProcessingContext context) {
            return delegate.initializeTokenSegments(processorName, segmentCount, initialToken, context);
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token,
                                                  String processorName,
                                                  int segment,
                                                  @Nullable ProcessingContext context) {
            return frozen.get()
                    ? CompletableFuture.completedFuture(null)
                    : delegate.storeToken(token, processorName, segment, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           int segment,
                                                           @Nullable ProcessingContext context) {
            return delegate.fetchToken(processorName, segment, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           Segment segment,
                                                           @Nullable ProcessingContext context) {
            return delegate.fetchToken(processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> extendClaim(String processorName,
                                                   int segment,
                                                   @Nullable ProcessingContext context) {
            return delegate.extendClaim(processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName,
                                                    int segment,
                                                    @Nullable ProcessingContext context) {
            return delegate.releaseClaim(processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token,
                                                         String processorName,
                                                         Segment segment,
                                                         @Nullable ProcessingContext context) {
            return delegate.initializeSegment(token, processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName,
                                                   int segment,
                                                   @Nullable ProcessingContext context) {
            return delegate.deleteToken(processorName, segment, context);
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName,
                                                       int segmentId,
                                                       @Nullable ProcessingContext context) {
            return delegate.fetchSegment(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchSegments(String processorName,
                                                              @Nullable ProcessingContext context) {
            return delegate.fetchSegments(processorName, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                       @Nullable ProcessingContext context) {
            return delegate.fetchAvailableSegments(processorName, context);
        }

        @Override
        public CompletableFuture<String> retrieveStorageIdentifier(@Nullable ProcessingContext context) {
            return delegate.retrieveStorageIdentifier(context);
        }
    }
}
