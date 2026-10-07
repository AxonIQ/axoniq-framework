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
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.assertj.core.api.AbstractLongAssert;
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
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests shutdown while a segment release waits for a slow save: shutdown stops waiting after the lifecycle phase
 * timeout and leaves the claim in place, and with a longer timeout it returns only after the release.
 */
class ShutdownReleaseOutlivesPhaseTimeoutTest {

    private static final Logger logger = LoggerFactory.getLogger(ShutdownReleaseOutlivesPhaseTimeoutTest.class);
    private static final String MODULE = "shutdown-release";
    private static final String PROCESSOR = MODULE;
    private static final String WAIT_STEP = "awaitPaid";
    private static final WaitForStepDefinition AWAIT_PAID = new WaitForStepDefinition(
            new PrimitiveMetadata(WAIT_STEP, DefaultEventNameCustomizer.Builder.defaults()),
            EventConditions.fromQualifiedName(new QualifiedName("paid")),
            new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
            new Timing(Duration.ofMinutes(5))
    );
    private static final Duration SLOW_APPEND = Duration.ofSeconds(9);

    private final ReleaseRecordingTokenStore tokenStore = new ReleaseRecordingTokenStore();
    private final SlowStorageEngine storageEngine = new SlowStorageEngine(new InMemoryEventStorageEngine());
    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Nested
    class DefaultLifecyclePhaseTimeout {

        @Test
        void shutdownReturnsBeforeTheSegmentClaimIsReleased() {
            // given
            start(null);
            startWorkflowAndDelayTheWakeAppend();

            // when
            Shutdown shutdown = shutdown();

            // then
            // Flips to isNotZero once shutdown waits for the slow release beyond the lifecycle phase timeout.
            assertThatReleasedBeforeShutdownReturned(shutdown).isZero();
        }
    }

    @Nested
    class LifecyclePhaseTimeoutAboveTheSlowAppend {

        @Test
        void shutdownReturnsOnlyAfterTheSegmentClaimIsReleased() {
            // given
            start(SLOW_APPEND.plusSeconds(10));
            startWorkflowAndDelayTheWakeAppend();

            // when
            Shutdown shutdown = shutdown();

            // then
            assertThatReleasedBeforeShutdownReturned(shutdown).isNotZero();
        }
    }

    private record Shutdown(long start, long returned, long releasedBeforeReturn) {

    }

    private void startWorkflowAndDelayTheWakeAppend() {
        startWorkflow();
        storageEngine.delayNextCompletedAppendOf(WAIT_STEP);
        publish("paid");
        await().atMost(5, TimeUnit.SECONDS).untilTrue(storageEngine.delayed);
    }

    private Shutdown shutdown() {
        long start = System.nanoTime();
        configuration.shutdown();
        long returned = System.nanoTime();
        return new Shutdown(start, returned, tokenStore.releasedAt.get());
    }

    private AbstractLongAssert<?> assertThatReleasedBeforeShutdownReturned(Shutdown shutdown) {
        await().pollDelay(SLOW_APPEND.plusSeconds(3)).atMost(SLOW_APPEND.plusSeconds(10)).until(() -> true);
        long shutdownMs = TimeUnit.NANOSECONDS.toMillis(shutdown.returned() - shutdown.start());
        long released = tokenStore.releasedAt.get();
        String releasedMs = released == 0 ? "never"
                : String.valueOf(TimeUnit.NANOSECONDS.toMillis(released - shutdown.start()));
        List<Long> storesAfterReturnMs = tokenStore.storedAt.stream()
                                                           .filter(at -> at > shutdown.returned())
                                                           .map(at -> TimeUnit.NANOSECONDS.toMillis(
                                                                   at - shutdown.start()))
                                                           .toList();
        logger.warn("shutdownReturnedAfterMs={} claimReleasedAfterMs={} tokenStoredAfterReturnAtMs={}",
                    shutdownMs, releasedMs, storesAfterReturnMs);
        return assertThat(shutdown.releasedBeforeReturn())
                .as("claim of segment 0 released before shutdown() returned (returned after %d ms, released after "
                            + "%s ms, token stored after the return at %s ms)",
                    shutdownMs, releasedMs, storesAfterReturnMs);
    }

    private void start(@Nullable Duration lifecyclePhaseTimeout) {
        var module = WorkflowModule.defaults(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> ctx.awaitEvent(AWAIT_PAID))
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(1))
                                   .contextFactory(c -> TestContext::new);
        var configurer = WorkflowConfigurer.create();
        if (lifecyclePhaseTimeout != null) {
            configurer.lifecycleRegistry(lifecycle -> lifecycle.registerLifecyclePhaseTimeout(
                    lifecyclePhaseTimeout.toMillis(), TimeUnit.MILLISECONDS));
        }
        configurer.componentRegistry(cr -> cr.registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                                             .registerComponent(TokenStore.class, cfg -> tokenStore)
                                             .registerModule(module));
        configuration = configurer.build();
        configuration.start();
    }

    private void startWorkflow() {
        publish("start");
        var engine = configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[" + MODULE + "]");
        await().atMost(5, TimeUnit.SECONDS).until(() -> engine.workflowExecutions().size() == 1);
        await().atMost(5, TimeUnit.SECONDS).until(storageEngine.waitStepStarted::get);
    }

    private void publish(String eventName) {
        var event = new GenericEventMessage(new MessageType(eventName), Map.of("orderId", "order-1"));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + eventName)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class).publish(ctx, event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    private static final class ReleaseRecordingTokenStore implements TokenStore {

        private final InMemoryTokenStore delegate = new InMemoryTokenStore();
        private final AtomicLong releasedAt = new AtomicLong();
        private final List<Long> storedAt = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName, int segmentId,
                                                    @Nullable ProcessingContext context) {
            return delegate.releaseClaim(processorName, segmentId, context).thenRun(() -> {
                if (PROCESSOR.equals(processorName) && segmentId == 0) {
                    releasedAt.compareAndSet(0, System.nanoTime());
                }
            });
        }

        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName, int segmentCount,
                                                                        @Nullable TrackingToken initialToken,
                                                                        @Nullable ProcessingContext context) {
            return delegate.initializeTokenSegments(processorName, segmentCount, initialToken, context);
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token, String processorName, int segmentId,
                                                  @Nullable ProcessingContext context) {
            return delegate.storeToken(token, processorName, segmentId, context)
                           .thenRun(() -> storedAt.add(System.nanoTime()));
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName, int segmentId,
                                                           @Nullable ProcessingContext context) {
            return delegate.fetchToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName, Segment segment,
                                                           @Nullable ProcessingContext context) {
            return delegate.fetchToken(processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> extendClaim(String processorName, int segmentId,
                                                   @Nullable ProcessingContext context) {
            return delegate.extendClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token, String processorName,
                                                         Segment segment, @Nullable ProcessingContext context) {
            return delegate.initializeSegment(token, processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName, int segmentId,
                                                   @Nullable ProcessingContext context) {
            return delegate.deleteToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName, int segmentId,
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

    private static final class SlowStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicBoolean delayed = new AtomicBoolean();
        private final AtomicBoolean waitStepStarted = new AtomicBoolean();
        private volatile @Nullable String stepName;

        private SlowStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private void delayNextCompletedAppendOf(String stepName) {
            this.stepName = stepName;
            armed.set(true);
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            if (events.stream().anyMatch(tagged -> isStep(tagged, WAIT_STEP, "STARTED"))) {
                waitStepStarted.set(true);
            }
            if (events.stream().anyMatch(tagged -> isStep(tagged, stepName, "COMPLETED"))
                    && armed.compareAndSet(true, false)) {
                delayed.set(true);
                return CompletableFuture.runAsync(() -> {
                }, CompletableFuture.delayedExecutor(SLOW_APPEND.toMillis(), TimeUnit.MILLISECONDS))
                                        .thenCompose(ignored -> delegate.appendEvents(condition,
                                                                                      processingContext,
                                                                                      events));
            }
            return delegate.appendEvents(condition, processingContext, events);
        }

        private static boolean isStep(TaggedEventMessage<?> tagged, @Nullable String step, String type) {
            var metadata = tagged.event().metadata();
            return step != null
                    && step.equals(metadata.get(MetadataUtils.METADATA_KEY_STEP_NAME))
                    && type.equals(metadata.get(MetadataUtils.METADATA_KEY_TYPE));
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
