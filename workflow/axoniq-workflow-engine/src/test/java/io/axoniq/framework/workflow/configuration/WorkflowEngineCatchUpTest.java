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

import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.AbstractWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.common.configuration.AxonConfiguration;
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
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToClaimTokenException;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests restored workflows while their segment catches up after a crash or a take-over: a wait times out on a quiet
 * segment, a wait is woken after a backlog larger than the task queue, and the STARTED handler does not run again.
 */
class WorkflowEngineCatchUpTest {

    private static final String MODULE = "catch-up";
    private static final String PROCESSOR = MODULE;
    private static final String WAIT_STEP = "awaitPaid";
    private static final String MODE = "mode";
    private static final String WAITING = "waiting";
    private static final String DONE = "done";
    private static final Duration LONG_WAIT = Duration.ofMinutes(5);
    private static final List<Segment> TWO_SEGMENTS = Segment.splitBalanced(Segment.ROOT_SEGMENT, 1);
    private static final Segment QUIET_SEGMENT = TWO_SEGMENTS.get(0);
    private static final Segment BUSY_SEGMENT = TWO_SEGMENTS.get(1);

    private final RecordingStorageEngine storageEngine = new RecordingStorageEngine(new InMemoryEventStorageEngine());
    private final List<AxonConfiguration> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class QuietSegmentAfterACrash {

        private static final Duration SHORT_WAIT = Duration.ofSeconds(3);

        @Test
        void restoredWaitTimesOutAlthoughItsSegmentReceivesNoFurtherEvents() {
            // given
            var restart = restartBehindTheBusySegmentsTail();

            // then
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(waitStepStatus(restart.node(), restart.workflowId()))
                           .as(() -> describe(restart))
                           .isEqualTo(StepStatus.TIMED_OUT));
        }

        @Test
        void restoredWaitTimesOutOnceAnUnrelatedEventReachesItsSegment() {
            // given
            var restart = restartBehindTheBusySegmentsTail();

            // when
            publish(restart.node(), event("unrelated", Map.of()));

            // then
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(waitStepStatus(restart.node(), restart.workflowId()))
                           .as(() -> describe(restart))
                           .isEqualTo(StepStatus.TIMED_OUT));
        }

        private Restart restartBehindTheBusySegmentsTail() {
            var crashedTokens = new InMemoryTokenStore();
            var crashed = startNode(crashedTokens, 2, SHORT_WAIT, counting(new AtomicInteger()));
            var quietId = idOwnedBy(QUIET_SEGMENT, "quiet-");
            var busyId = idOwnedBy(BUSY_SEGMENT, "busy-");
            publish(crashed, startEvent(quietId, WAITING));
            await().atMost(5, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> waitStepStatus(crashed, quietId) == StepStatus.STARTED);
            var quietToken = awaitStoredTokenCovering(crashedTokens, QUIET_SEGMENT, latestPosition());
            publish(crashed, startEvent(busyId, DONE));
            await().atMost(5, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> workflowStatus(crashed, busyId) == WorkflowStatus.COMPLETED);
            var busyToken = awaitStoredTokenCovering(crashedTokens, BUSY_SEGMENT, latestPosition());
            crash(crashed);
            var head = latestPosition();
            assertThat(covers(quietToken, head)).as("quiet segment token %s covers head %s", quietToken, head)
                                                .isFalse();
            var restarted =
                    startNode(tokenStoreAt(quietToken, busyToken), 2, SHORT_WAIT, counting(new AtomicInteger()));
            assertThat(waitStepStatus(restarted, quietId)).isEqualTo(StepStatus.STARTED);
            await().atMost(5, TimeUnit.SECONDS).until(() -> execution(restarted, quietId).isPresent());
            return new Restart(restarted, quietId, quietToken, head);
        }
    }

    @Nested
    class CatchUpBacklogLargerThanTheTaskQueue {

        @Test
        void restoredWaitIsWokenAfterCatchingUpOnMoreEventsThanTheTaskQueueHolds() {
            // given
            var restart = restartBehind(1100);

            // when
            publish(restart.node(), event("paid", Map.of()));

            // then
            await().atMost(30, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(workflowStatus(restart.node(), restart.workflowId()))
                           .as(() -> describe(restart))
                           .isEqualTo(WorkflowStatus.COMPLETED));
        }

        @Test
        void restoredWaitIsWokenAfterCatchingUpOnFewerEventsThanTheTaskQueueHolds() {
            // given
            var restart = restartBehind(900);

            // when
            publish(restart.node(), event("paid", Map.of()));

            // then
            await().atMost(30, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(workflowStatus(restart.node(), restart.workflowId()))
                           .as(() -> describe(restart))
                           .isEqualTo(WorkflowStatus.COMPLETED));
        }

        private Restart restartBehind(int backlog) {
            var crashedTokens = new InMemoryTokenStore();
            var crashed = startNode(crashedTokens, 1, LONG_WAIT, counting(new AtomicInteger()));
            var workflowId = publish(crashed, startEvent("behind-" + backlog, WAITING));
            await().atMost(5, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> waitStepStatus(crashed, workflowId) == StepStatus.STARTED);
            var tokenAtCrash = awaitStoredTokenCovering(crashedTokens, Segment.ROOT_SEGMENT, latestPosition());
            crash(crashed);
            storageEngine.appendDirectly(IntStream.range(0, backlog)
                                                  .mapToObj(i -> event("noise", Map.of("n", i)))
                                                  .toList());
            var head = latestPosition();
            var restarted = startNode(tokenStoreAt(tokenAtCrash), 1, LONG_WAIT, counting(new AtomicInteger()));
            return new Restart(restarted, workflowId, tokenAtCrash, head);
        }
    }

    @Nested
    class TakeOverOfALaggingSegment {

        @Test
        void nodeThatStartedBeforeTheTakeOverDoesNotRerunTheStartedHandlerOfARestoredWorkflow() {
            // given
            var previousOwnerTokens = new InMemoryTokenStore();
            var previousOwnerStarted = new AtomicInteger();
            var previousOwner = startNode(previousOwnerTokens, 1, LONG_WAIT, counting(previousOwnerStarted));
            publish(previousOwner, event("warm-up", Map.of()));
            var lastStoredToken =
                    awaitStoredTokenCovering(previousOwnerTokens, Segment.ROOT_SEGMENT, latestPosition());
            var takeOverTokens = new HandOverTokenStore(tokenStoreAt(lastStoredToken));
            var takeOverStarted = new AtomicInteger();
            var takeOver = startNode(takeOverTokens, 1, LONG_WAIT, counting(takeOverStarted));
            var takeOverStartupHead = latestPosition();
            var workflowId = publish(previousOwner, startEvent("moved", WAITING));
            await().atMost(5, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> waitStepStatus(previousOwner, workflowId) == StepStatus.STARTED);
            crash(previousOwner);
            var headAtTakeOver = latestPosition();
            assertThat(previousOwnerStarted).hasValue(1);
            assertThat(covers(lastStoredToken, takeOverStartupHead)).isTrue();
            assertThat(covers(lastStoredToken, headAtTakeOver)).isFalse();

            // when
            takeOverTokens.handOver();
            publish(takeOver, event("paid", Map.of()));

            // then
            await().atMost(10, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> workflowStatus(takeOver, workflowId) == WorkflowStatus.COMPLETED);
            assertThat(takeOverStarted).as("STARTED handler invocations on the node that took the segment over")
                                       .hasValue(0);
        }

        @Test
        void nodeThatStartedAfterTheCrashDoesNotRerunTheStartedHandlerOfARestoredWorkflow() {
            // given
            var previousOwnerTokens = new InMemoryTokenStore();
            var previousOwnerStarted = new AtomicInteger();
            var previousOwner = startNode(previousOwnerTokens, 1, LONG_WAIT, counting(previousOwnerStarted));
            publish(previousOwner, event("warm-up", Map.of()));
            var lastStoredToken =
                    awaitStoredTokenCovering(previousOwnerTokens, Segment.ROOT_SEGMENT, latestPosition());
            var workflowId = publish(previousOwner, startEvent("moved", WAITING));
            await().atMost(5, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> waitStepStatus(previousOwner, workflowId) == StepStatus.STARTED);
            crash(previousOwner);
            assertThat(previousOwnerStarted).hasValue(1);

            // when
            var takeOverStarted = new AtomicInteger();
            var takeOver = startNode(tokenStoreAt(lastStoredToken), 1, LONG_WAIT, counting(takeOverStarted));
            publish(takeOver, event("paid", Map.of()));

            // then
            await().atMost(10, TimeUnit.SECONDS)
                   .ignoreExceptions()
                   .until(() -> workflowStatus(takeOver, workflowId) == WorkflowStatus.COMPLETED);
            assertThat(takeOverStarted).as("STARTED handler invocations on the node that took the segment over")
                                       .hasValue(0);
        }
    }

    private AxonConfiguration startNode(TokenStore tokenStore,
                                        int segmentCount,
                                        Duration waitTimeout,
                                        WorkflowStatusChangeListener startedListener) {
        var awaitPaid = new WaitForStepDefinition(
                new PrimitiveMetadata(WAIT_STEP, DefaultEventNameCustomizer.Builder.defaults()),
                EventConditions.fromQualifiedName(new QualifiedName("paid")),
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(waitTimeout)
        );
        Consumer<CatchUpContext> body = ctx -> {
            if (WAITING.equals(ctx.workflowPayload().get(MODE))) {
                ctx.awaitEvent(awaitPaid);
            }
        };
        var module = WorkflowModule.defaults(MODULE, CatchUpContext.class)
                                   .definition(d -> d
                                           .declarative(c -> body::accept)
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .customized((c, customization) -> customization
                                                   .registerWorkflowStatusChangeListener(WorkflowStatus.STARTED,
                                                                                         startedListener))
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(segmentCount))
                                   .contextFactory(c -> CatchUpContext::new);
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr.registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                                             .registerComponent(TokenStore.class, cfg -> tokenStore)
                                             .registerModule(module));
        var node = configurer.build();
        nodes.add(node);
        node.start();
        return node;
    }

    private void crash(AxonConfiguration node) {
        nodes.remove(node);
        node.shutdown();
    }

    private static WorkflowStatusChangeListener counting(AtomicInteger counter) {
        return (status, context, processingContext) -> counter.incrementAndGet();
    }

    private static String idOwnedBy(Segment segment, String prefix) {
        return IntStream.range(0, 512)
                        .mapToObj(i -> prefix + i)
                        .filter(segment::matches)
                        .findFirst()
                        .orElseThrow();
    }

    private static EventMessage startEvent(String workflowId, String mode) {
        return new GenericEventMessage(workflowId, new MessageType("start"), Map.of(MODE, mode), Map.of(),
                                       Instant.now());
    }

    private static EventMessage event(String name, Map<String, Object> payload) {
        return new GenericEventMessage(new MessageType(name), payload);
    }

    private static String publish(AxonConfiguration node, EventMessage event) {
        node.getComponent(UnitOfWorkFactory.class)
            .create("publish-" + event.type().name())
            .executeWithResult(ctx -> node.getComponent(EventStore.class)
                                          .publish(ctx, event)
                                          .thenApply(ignored -> event))
            .orTimeout(5, TimeUnit.SECONDS)
            .join();
        return event.identifier();
    }

    private TrackingToken latestPosition() {
        return storageEngine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static TokenStore tokenStoreAt(@Nullable TrackingToken... tokens) {
        var tokenStore = new InMemoryTokenStore();
        var segments = Segment.splitBalanced(Segment.ROOT_SEGMENT, tokens.length - 1);
        for (var segment : segments) {
            tokenStore.initializeSegment(tokens[segment.getSegmentId()], PROCESSOR, segment, null)
                      .orTimeout(5, TimeUnit.SECONDS)
                      .join();
        }
        return tokenStore;
    }

    @Nullable
    private static TrackingToken storedToken(TokenStore tokenStore, Segment segment) {
        return tokenStore.fetchToken(PROCESSOR, segment.getSegmentId(), null).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static TrackingToken awaitStoredTokenCovering(TokenStore tokenStore,
                                                          Segment segment,
                                                          TrackingToken position) {
        await().atMost(5, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> covers(storedToken(tokenStore, segment), position));
        return storedToken(tokenStore, segment);
    }

    private static boolean covers(@Nullable TrackingToken stored, TrackingToken position) {
        return stored != null && stored.covers(position);
    }

    private static Optional<WorkflowExecution> execution(AxonConfiguration node, String workflowId) {
        return node.getComponents(WorkflowEngine.class).get("WorkflowEngine[" + MODULE + "]")
                   .workflowExecutions()
                   .stream()
                   .filter(execution -> execution.workflowId().equals(workflowId))
                   .findFirst();
    }

    @Nullable
    private static StepStatus waitStepStatus(AxonConfiguration node, String workflowId) {
        var state = durableState(node, workflowId);
        return state.containsStep(WAIT_STEP) ? state.getStep(WAIT_STEP).status() : null;
    }

    private static WorkflowStatus workflowStatus(AxonConfiguration node, String workflowId) {
        return durableState(node, workflowId).workflowStatus();
    }

    @SuppressWarnings("unchecked")
    private static EventSourcedWorkflowState durableState(AxonConfiguration node, String workflowId) {
        var repository = (Repository<String, EventSourcedWorkflowState>) node
                .getComponents(Repository.class)
                .values()
                .stream()
                .filter(candidate -> candidate.entityType().equals(EventSourcedWorkflowState.class))
                .findFirst()
                .orElseThrow();
        return node.getComponent(UnitOfWorkFactory.class)
                   .create("read-" + workflowId)
                   .executeWithResult(ctx -> repository.loadOrCreate(workflowId, ctx)
                                                       .thenApply(managed -> managed.entity()))
                   .orTimeout(5, TimeUnit.SECONDS)
                   .join();
    }

    private String describe(Restart restart) {
        var execution = execution(restart.node(), restart.workflowId());
        return "claimed from %s, head at restart %s, restored execution present=%s running=%s, streams opened from %s"
                .formatted(restart.claimedFrom(),
                           restart.headAtRestart(),
                           execution.isPresent(),
                           execution.map(WorkflowExecution::isRunning).orElse(false),
                           storageEngine.streamPositions());
    }

    private record Restart(AxonConfiguration node,
                           String workflowId,
                           TrackingToken claimedFrom,
                           TrackingToken headAtRestart) {

    }

    static class CatchUpContext extends AbstractWorkflowContext {

        CatchUpContext(Map<String, @Nullable Object> payload,
                    String workflowId,
                    ProcessingContext processingContext,
                    WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    private static final class HandOverTokenStore implements TokenStore {

        private final TokenStore delegate;
        private volatile boolean handedOver;

        private HandOverTokenStore(TokenStore delegate) {
            this.delegate = delegate;
        }

        private void handOver() {
            handedOver = true;
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                       ProcessingContext context) {
            return handedOver
                    ? delegate.fetchAvailableSegments(processorName, context)
                    : CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           int segmentId,
                                                           @Nullable ProcessingContext context) {
            return handedOver
                    ? delegate.fetchToken(processorName, segmentId, context)
                    : CompletableFuture.failedFuture(new UnableToClaimTokenException("claimed by another node"));
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           Segment segment,
                                                           @Nullable ProcessingContext context) {
            return handedOver
                    ? delegate.fetchToken(processorName, segment, context)
                    : CompletableFuture.failedFuture(new UnableToClaimTokenException("claimed by another node"));
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
                                                  int segmentId,
                                                  @Nullable ProcessingContext context) {
            return delegate.storeToken(token, processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName,
                                                    int segmentId,
                                                    @Nullable ProcessingContext context) {
            return delegate.releaseClaim(processorName, segmentId, context);
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
                                                   int segmentId,
                                                   @Nullable ProcessingContext context) {
            return delegate.deleteToken(processorName, segmentId, context);
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
        public CompletableFuture<String> retrieveStorageIdentifier(@Nullable ProcessingContext context) {
            return delegate.retrieveStorageIdentifier(context);
        }
    }

    private static final class DetachingStream implements MessageStream<EventMessage> {

        private final MessageStream<EventMessage> delegate;
        private volatile boolean closed;

        private DetachingStream(MessageStream<EventMessage> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Entry<EventMessage>> next() {
            return delegate.next();
        }

        @Override
        public Optional<Entry<EventMessage>> peek() {
            return delegate.peek();
        }

        @Override
        public void setCallback(Runnable callback) {
            delegate.setCallback(() -> {
                if (!closed) {
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
            return delegate.isCompleted();
        }

        @Override
        public boolean hasNextAvailable() {
            return delegate.hasNextAvailable();
        }

        @Override
        public void close() {
            closed = true;
            delegate.close();
        }
    }

    private static final class RecordingStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final List<@Nullable TrackingToken> streamPositions = new CopyOnWriteArrayList<>();

        private RecordingStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private List<@Nullable TrackingToken> streamPositions() {
            return new ArrayList<>(streamPositions);
        }

        private void appendDirectly(List<EventMessage> events) {
            List<TaggedEventMessage<?>> tagged = events.stream()
                                                       .<TaggedEventMessage<?>>map(
                                                               e -> new GenericTaggedEventMessage<>(e, Set.of()))
                                                       .toList();
            delegate.appendEvents(AppendCondition.none(), null, tagged)
                    .thenCompose(EventStorageEngine.AppendTransaction::commit)
                    .orTimeout(5, TimeUnit.SECONDS)
                    .join();
        }

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    @Nullable ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            return delegate.appendEvents(condition, processingContext, events);
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition,
                                                  @Nullable ProcessingContext processingContext) {
            return delegate.source(condition, processingContext);
        }

        @Override
        public MessageStream<EventMessage> stream(StreamingCondition condition) {
            streamPositions.add(condition.position());
            return new DetachingStream(delegate.stream(condition));
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
