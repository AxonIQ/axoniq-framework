/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.workflow.runtime.execution.LifecycleEventFixtures.*;
import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RunningWorkflowsClaimLoader}.
 *
 * @author Allard Buijze
 */
class RunningWorkflowsClaimLoaderTest {

    private static final String INDEX = "orders";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String LONG_ID = "w".repeat(2_000);

    private final ControllableWorkflowIndex workflowIndex = new ControllableWorkflowIndex();
    private final RecordingWorkflowStore workflowStore = new RecordingWorkflowStore();
    private final RunningWorkflowsClaimLoader loader = new RunningWorkflowsClaimLoader(
            workflowIndex, INDEX, workflowStore
    );

    private Set<String> stored(Segment segment) {
        return await(workflowIndex.load(INDEX, segment)).orElseThrow();
    }

    private static <T> T await(CompletableFuture<T> future) {
        return future.orTimeout(5, TimeUnit.SECONDS).join();
    }

    @Nested
    class Load {

        private final InMemoryEventStorageEngine storageEngine = new InMemoryEventStorageEngine();
        private final EventStore eventStore = eventStore(storageEngine);
        private final StubProcessingContext context = StubProcessingContext.withComponent(EventStore.class,
                                                                                           eventStore);

        @Nested
        class WithoutAnInitializedIndex {

            @Test
            void initializesTheIndexWithEveryRunningWorkflowFromTheRunningWorkflowList() {
                // given
                var segment = FOUR_SEGMENTS.getFirst();
                var owned = anyIdOn(segment);
                var foreign = anyIdOn(anotherSegmentThan(segment));
                workflowStore.running(owned, foreign);

                // when
                var claimed = await(loader.load(segment, token(3), context, TIMEOUT));

                // then - the index holds the running workflows of every segment, so nothing is pending
                assertThat(claimed.runningWorkflowIds()).containsExactly(owned);
                assertThat(claimed.changes()).isEmpty();
                assertThat(workflowIndex.initializations()).containsExactly(Set.of(owned, foreign));
                assertThat(stored(anotherSegmentThan(segment))).containsExactly(foreign);
                assertThat(workflowStore.runningWorkflowLoads()).isEqualTo(1);
            }

            @Test
            void addsEveryRunningWorkflowWhenTheIndexCannotBeInitialized() {
                // given
                workflowIndex.failInitializations(new IllegalStateException("database down"));
                workflowStore.running("order-1", "order-2");

                // when
                var claimed = await(loader.load(Segment.ROOT_SEGMENT, token(3), context, TIMEOUT));

                // then
                assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
                assertThat(claimed.changes()).containsOnly(Map.entry("order-1", true), Map.entry("order-2", true));
                assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).isEmpty();
            }

            @Test
            void initializesTheIndexWithAWorkflowWithALongId() {
                // given
                workflowStore.running("order-1", LONG_ID);

                // when
                var claimed = await(loader.load(Segment.ROOT_SEGMENT, token(3), context, TIMEOUT));

                // then
                assertThat(claimed.changes()).isEmpty();
                assertThat(workflowIndex.initializations()).containsExactly(Set.of("order-1", LONG_ID));
            }
        }

        @Test
        void combinesTheStoredIndexWithTheLifecycleEventsFromTheClaimedToken() {
            // given - an index of order-1 and order-2, and a segment that resumes after their start events
            append(storageEngine,
                   lifecycleEvent("order-1", WorkflowStatus.STARTED),
                   lifecycleEvent("order-2", WorkflowStatus.STARTED));
            TrackingToken claimedFrom = await(eventStore.latestToken(context));
            workflowIndex.initialized(INDEX, "order-1", "order-2");
            // and - order-3 started and order-2 completed after that token
            append(storageEngine,
                   lifecycleEvent("order-3", WorkflowStatus.STARTED),
                   stepEvent("order-1"),
                   publishedEvent("order-4", WorkflowStatus.STARTED),
                   lifecycleEvent("order-2", WorkflowStatus.COMPLETED));

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, claimedFrom, context, TIMEOUT));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-3");
            assertThat(claimed.changes()).containsOnly(Map.entry("order-3", true), Map.entry("order-2", false));
            assertThat(workflowStore.runningWorkflowLoads()).isZero();
        }

        @Test
        void keepsOnlyTheInstancesOfTheSegmentFromTheEventsAfterTheClaimedToken() {
            // given
            var segment = FOUR_SEGMENTS.getFirst();
            var owned = anyIdOn(segment);
            var foreign = anyIdOn(anotherSegmentThan(segment));
            workflowIndex.initialized(INDEX);
            append(storageEngine,
                   lifecycleEvent(owned, WorkflowStatus.STARTED),
                   lifecycleEvent(foreign, WorkflowStatus.STARTED));

            // when
            var claimed = await(loader.load(segment, token(0), context, TIMEOUT));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactly(owned);
        }

        @Test
        void returnsTheStoredIndexWhenNothingWasAppendedAfterTheClaimedToken() {
            // given
            append(storageEngine, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            TrackingToken claimedFrom = await(eventStore.latestToken(context));
            workflowIndex.initialized(INDEX, "order-1");

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, claimedFrom, context, TIMEOUT));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactly("order-1");
            assertThat(claimed.changes()).isEmpty();
        }

        @Test
        void streamsTheTailFromTheLowerBoundOfAWrappedClaimedToken() {
            // given - a segment that resumes within a replay, from before the start of order-2
            append(storageEngine, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            TrackingToken lowerBound = await(eventStore.latestToken(context));
            append(storageEngine, lifecycleEvent("order-2", WorkflowStatus.STARTED));
            TrackingToken head = await(eventStore.latestToken(context));
            workflowIndex.initialized(INDEX, "order-1");
            var claimedFrom = ReplayToken.createReplayToken(head, lowerBound);

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, claimedFrom, context, TIMEOUT));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
            assertThat(workflowStore.runningWorkflowLoads()).isZero();
        }

        @Test
        void readsTheTailOfAStreamThatDeliversAsynchronously() {
            // given - a stream that, like a remote one, has nothing available right after opening
            var remoteStore = eventStore(new RemoteLikeStorageEngine(storageEngine, StreamBehaviour.DELAYED));
            var remoteContext = StubProcessingContext.withComponent(EventStore.class, remoteStore);
            var claimedFrom = indexAfter(lifecycleEvent("order-1", WorkflowStatus.STARTED), eventStore);
            append(storageEngine, lifecycleEvent("order-3", WorkflowStatus.STARTED));

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, claimedFrom, remoteContext, TIMEOUT));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-3");
            assertThat(workflowStore.runningWorkflowLoads()).isZero();
        }

        @Test
        void stopsReadingTheTailAtTheHeadPositionWhenStreamedTokensHaveGaps() {
            // given - an event store whose head token has no gaps, while every streamed token has one below the head
            var gapAwareStore = eventStore(new RemoteLikeStorageEngine(storageEngine, StreamBehaviour.GAP_AWARE));
            var gapAwareContext = StubProcessingContext.withComponent(EventStore.class, gapAwareStore);
            var claimedFrom = indexAfter(lifecycleEvent("order-1", WorkflowStatus.STARTED), gapAwareStore);
            append(storageEngine, lifecycleEvent("order-3", WorkflowStatus.STARTED));

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, claimedFrom, gapAwareContext, TIMEOUT));

            // then - the tail is read, not abandoned after the timeout
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-3");
            assertThat(workflowStore.runningWorkflowLoads()).isZero();
        }

        @Test
        void readsAnEventFillingAGapOfTheClaimedTokenWhenNothingFollowsThatToken() {
            // given - a segment at 3 that missed the start of order-2 at 2, committed late
            var gapFillingStore = eventStore(new GapFillingStorageEngine(storageEngine, Set.of()));
            var gapFillingContext = StubProcessingContext.withComponent(EventStore.class, gapFillingStore);
            append(storageEngine,
                   lifecycleEvent("order-1", WorkflowStatus.STARTED),
                   lifecycleEvent("order-2", WorkflowStatus.STARTED),
                   stepEvent("order-1"));
            workflowIndex.initialized(INDEX, "order-1");

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT,
                                            GapAwareTrackingToken.newInstance(3, List.of(2L)),
                                            gapFillingContext,
                                            TIMEOUT));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
            assertThat(workflowStore.runningWorkflowLoads()).isZero();
        }

        @Test
        void readsEveryEventFillingAGapOfTheClaimedTokenWithoutWaitingForAGapThatStaysOpen() {
            // given - a segment at 5 that missed 2, 3 and 4, where 2 and 4 got filled and 3 never will
            var gapFillingStore = eventStore(new GapFillingStorageEngine(storageEngine, Set.of(3L)));
            var gapFillingContext = StubProcessingContext.withComponent(EventStore.class, gapFillingStore);
            append(storageEngine,
                   lifecycleEvent("order-1", WorkflowStatus.STARTED),
                   lifecycleEvent("order-2", WorkflowStatus.STARTED),
                   lifecycleEvent("rolled-back", WorkflowStatus.STARTED),
                   lifecycleEvent("order-3", WorkflowStatus.STARTED),
                   stepEvent("order-1"));
            workflowIndex.initialized(INDEX, "order-1");

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT,
                                            GapAwareTrackingToken.newInstance(5, List.of(2L, 3L, 4L)),
                                            gapFillingContext,
                                            TIMEOUT));

            // then - the tail is read up to the head, not abandoned after the timeout
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2", "order-3");
            assertThat(workflowStore.runningWorkflowLoads()).isZero();
        }

        @Test
        void fallsBackToTheRunningWorkflowListWhenTheTailFails() {
            // given
            var failingStore = eventStore(new RemoteLikeStorageEngine(storageEngine, StreamBehaviour.FAILING));
            var failingContext = StubProcessingContext.withComponent(EventStore.class, failingStore);
            var claimedFrom = indexAfter(lifecycleEvent("order-1", WorkflowStatus.STARTED), eventStore);
            append(storageEngine, lifecycleEvent("order-3", WorkflowStatus.STARTED));
            workflowStore.running("order-1", "order-3");

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, claimedFrom, failingContext, TIMEOUT));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-3");
            assertThat(claimed.changes()).containsOnly(Map.entry("order-3", true));
            assertThat(workflowStore.runningWorkflowLoads()).isEqualTo(1);
        }

        @Test
        void fallsBackToTheRunningWorkflowListWhenTheTailDoesNotReachTheHeadInTime() {
            // given
            var silentStore = eventStore(new RemoteLikeStorageEngine(storageEngine, StreamBehaviour.SILENT));
            var silentContext = StubProcessingContext.withComponent(EventStore.class, silentStore);
            var claimedFrom = indexAfter(lifecycleEvent("order-1", WorkflowStatus.STARTED), eventStore);
            append(storageEngine, lifecycleEvent("order-3", WorkflowStatus.STARTED));
            workflowStore.running("order-1", "order-3");

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, claimedFrom, silentContext,
                                            Duration.ofMillis(100)));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-3");
            assertThat(workflowStore.runningWorkflowLoads()).isEqualTo(1);
        }

        @Test
        void fallsBackToTheRunningWorkflowListForASegmentThatConsumedNothingWhileTheStreamHoldsEvents() {
            // given
            append(storageEngine, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            workflowIndex.initialized(INDEX, "order-1", "ended");
            workflowStore.running("order-1");

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, null, context, TIMEOUT));

            // then - streaming from the start would read every event, the full list never costs more
            assertThat(claimed.runningWorkflowIds()).containsExactly("order-1");
            assertThat(claimed.changes()).containsOnly(Map.entry("ended", false));
            assertThat(workflowStore.runningWorkflowLoads()).isEqualTo(1);
        }

        @Test
        void addsEveryRunningWorkflowWithoutRemovingAnyWhenTheIndexCannotBeLoaded() {
            // given
            workflowIndex.failLoads(new IllegalStateException("database down"));
            workflowStore.running("order-1", "order-2");

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, token(3), context, TIMEOUT));

            // then - the claim cannot tell which changes the index lacks, so it writes every running instance, which
            // may leave an entry of an instance that ended, but never drops a running one
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
            assertThat(claimed.changes()).containsOnly(Map.entry("order-1", true), Map.entry("order-2", true));
            assertThat(workflowStore.runningWorkflowLoads()).isEqualTo(1);
        }

        @Test
        void addsEveryRunningWorkflowWhenTheIndexDoesNotLoadInTime() {
            // given
            workflowIndex.stallLoads();
            workflowStore.running("order-1");

            // when
            var claimed = await(loader.load(Segment.ROOT_SEGMENT, token(3), context, Duration.ofMillis(100)));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactly("order-1");
            assertThat(claimed.changes()).containsOnly(Map.entry("order-1", true));
        }

        /**
         * Appends the given event, initializes the index holding its instance, and returns the latest token of the
         * given event store, as the token a segment resumes from after handling the event.
         */
        private TrackingToken indexAfter(EventMessage started, EventStore tokenSource) {
            append(storageEngine, started);
            TrackingToken claimedFrom = await(tokenSource.latestToken(context));
            workflowIndex.initialized(INDEX, MetadataUtils.getWorkflowId(started.metadata()));
            return claimedFrom;
        }
    }

    @Nested
    class WithoutAnIndex {

        private final RunningWorkflowsClaimLoader withoutIndex = new RunningWorkflowsClaimLoader(
                WorkflowIndex.none(), INDEX, workflowStore
        );

        @Test
        void loadsTheRunningWorkflowListWithoutPendingChanges() {
            // given
            workflowStore.running("order-1", LONG_ID);

            // when
            var claimed = await(withoutIndex.load(Segment.ROOT_SEGMENT, token(3), new StubProcessingContext(),
                                                  TIMEOUT));

            // then
            assertThat(claimed.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", LONG_ID);
            assertThat(claimed.changes()).isEmpty();
        }
    }

    @Nested
    class StaleInstances {

        @Test
        void becomePendingRemovals() {
            // given
            var claimed = new ClaimedIndex(Set.of("order-1", "order-2", "order-3"), Map.of("order-3", true));

            // when
            var healed = claimed.withoutStale(Set.of("order-2", "order-3"));

            // then
            assertThat(healed.runningWorkflowIds()).containsExactly("order-1");
            assertThat(healed.changes()).containsOnly(Map.entry("order-2", false), Map.entry("order-3", false));
        }
    }

    /**
     * How the streams of a {@link RemoteLikeStorageEngine} behave.
     */
    private enum StreamBehaviour {
        /**
         * Nothing is available right after opening; the events arrive shortly after, announced by the callback.
         */
        DELAYED,
        /**
         * The stream fails.
         */
        FAILING,
        /**
         * The stream never delivers anything.
         */
        SILENT,
        /**
         * Tokens are gap-aware: the latest token has no gaps, every streamed token has a gap at the first position.
         */
        GAP_AWARE
    }

    /**
     * A storage engine streaming the way a remote event store does: the events of a stream arrive asynchronously, after
     * opening it. Everything else is delegated to an in-memory storage engine.
     */
    private record RemoteLikeStorageEngine(InMemoryEventStorageEngine delegate, StreamBehaviour behaviour)
            implements EventStorageEngine {

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    @Nullable ProcessingContext context,
                                                                    List<TaggedEventMessage<?>> events) {
            return delegate.appendEvents(condition, context, events);
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
            return delegate.source(condition, context);
        }

        @Override
        public MessageStream<EventMessage> stream(StreamingCondition condition) {
            return switch (behaviour) {
                case DELAYED -> new DelayedStream(delegate.stream(condition), true);
                case FAILING -> MessageStream.failed(new IllegalStateException("connection lost"));
                case SILENT -> new DelayedStream(delegate.stream(condition), false);
                case GAP_AWARE -> delegate.stream(condition).map(entry -> entry.withResource(
                        TrackingToken.RESOURCE_KEY,
                        GapAwareTrackingToken.newInstance(positionOf(entry), List.of(0L))
                ));
            };
        }

        @Override
        public CompletableFuture<TrackingToken> firstToken() {
            return delegate.firstToken();
        }

        @Override
        public CompletableFuture<TrackingToken> latestToken() {
            return behaviour == StreamBehaviour.GAP_AWARE
                    ? delegate.latestToken().thenApply(
                            latest -> GapAwareTrackingToken.newInstance(latest.position().orElseThrow(), List.of())
                    )
                    : delegate.latestToken();
        }

        private static long positionOf(MessageStream.Entry<EventMessage> entry) {
            return TrackingToken.fromContext(entry).orElseThrow().position().orElseThrow();
        }

        @Override
        public CompletableFuture<TrackingToken> tokenAt(Instant at) {
            return delegate.tokenAt(at);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            delegate.describeTo(descriptor);
        }
    }

    /**
     * A storage engine with gap-aware tokens, streaming the way a relational event store does: a stream from a token
     * delivers the events filling its gaps, then those after its index, and never completes. Its latest token has no
     * gaps. Events at the {@code missing} positions are never delivered, as if their transaction rolled back.
     * Everything else is delegated to an in-memory storage engine, whose first event is at position 1.
     */
    private record GapFillingStorageEngine(InMemoryEventStorageEngine delegate, Set<Long> missing)
            implements EventStorageEngine {

        @Override
        public MessageStream<EventMessage> stream(StreamingCondition condition) {
            var from = (GapAwareTrackingToken) condition.position();
            long delegateStart = from.getGaps().isEmpty() ? from.getIndex() : from.getGaps().first() - 1;
            var streamed = new AtomicReference<>(from);
            return delegate.stream(StreamingCondition.startingFrom(new GlobalSequenceTrackingToken(delegateStart)))
                           .filter(entry -> {
                               long position = positionOf(entry);
                               return !missing.contains(position)
                                       && (position > from.getIndex() || from.getGaps().contains(position));
                           })
                           .map(entry -> entry.withResource(
                                   TrackingToken.RESOURCE_KEY,
                                   streamed.updateAndGet(token -> token.advanceTo(positionOf(entry), 10_000))
                           ));
        }

        @Override
        public CompletableFuture<TrackingToken> latestToken() {
            return delegate.latestToken().thenApply(
                    latest -> GapAwareTrackingToken.newInstance(latest.position().orElseThrow(), List.of())
            );
        }

        private static long positionOf(MessageStream.Entry<EventMessage> entry) {
            return TrackingToken.fromContext(entry).orElseThrow().position().orElseThrow();
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    @Nullable ProcessingContext context,
                                                                    List<TaggedEventMessage<?>> events) {
            return delegate.appendEvents(condition, context, events);
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
            return delegate.source(condition, context);
        }

        @Override
        public CompletableFuture<TrackingToken> firstToken() {
            return delegate.firstToken();
        }

        @Override
        public CompletableFuture<TrackingToken> tokenAt(Instant at) {
            return delegate.tokenAt(at);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            delegate.describeTo(descriptor);
        }
    }

    /**
     * A stream that has nothing available until its events arrive 100 milliseconds after opening, if they arrive at
     * all, and that never completes.
     */
    private static final class DelayedStream implements MessageStream<EventMessage> {

        private final MessageStream<EventMessage> delegate;
        private volatile boolean arrived;
        private volatile Runnable callback = () -> {
        };

        private DelayedStream(MessageStream<EventMessage> delegate, boolean arrives) {
            this.delegate = delegate;
            if (arrives) {
                CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS).execute(() -> {
                    arrived = true;
                    callback.run();
                });
            }
        }

        @Override
        public Optional<Entry<EventMessage>> next() {
            return arrived ? delegate.next() : Optional.empty();
        }

        @Override
        public Optional<Entry<EventMessage>> peek() {
            return arrived ? delegate.peek() : Optional.empty();
        }

        @Override
        public void setCallback(Runnable callback) {
            this.callback = callback;
            delegate.setCallback(() -> {
                if (arrived) {
                    callback.run();
                }
            });
        }

        @Override
        public Optional<Throwable> error() {
            return delegate.error();
        }

        @Override
        public boolean isCompleted() {
            return false;
        }

        @Override
        public boolean hasNextAvailable() {
            return arrived && delegate.hasNextAvailable();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
