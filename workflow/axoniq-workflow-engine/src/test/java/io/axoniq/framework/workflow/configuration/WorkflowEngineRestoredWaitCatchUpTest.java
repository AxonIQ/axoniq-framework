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

import io.axoniq.framework.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineCheckpointingSupport;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
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
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Restores an instance whose {@code awaitEvent} step is started in its history while the awaited event already sits in
 * the store behind the processor's position, so the event is never delivered to the instance again.
 * <p>
 * No processor runs here: the claim and any delivery are played by hand, so the only way the restored wait can see the
 * awaited event is the read of the store it does when the body reaches it. The oracle is the durable log of the
 * instance.
 */
class WorkflowEngineRestoredWaitCatchUpTest extends AbstractEventSourcedEntityRepositoryTestBase {

    private static final String MODULE = "restored-wait-catch-up";
    private static final String WORKFLOW_ID = "wf-1";
    private static final VersionedType DEFINITION_ID =
            VersionedType.of(new QualifiedName(MODULE), Version.DEFAULT_VERSION);
    private static final QualifiedName PAYMENT = new QualifiedName("io.axoniq.test", "PaymentReceived");
    private static final String AWAIT_PAYMENT = "awaitPayment";
    private static final Duration WAIT_TIMEOUT = Duration.ofHours(1);
    private static final Duration QUIET_PERIOD = Duration.ofMillis(500);

    private final DefaultEventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();
    private final TypeReadRecordingStorageEngine storageEngine =
            new TypeReadRecordingStorageEngine(new InMemoryEventStorageEngine(), PAYMENT);
    private WorkflowContext instance;

    @BeforeEach
    void setUp() {
        configuration = configurationWith(storageEngine);
        // Wired on start, which would also start the processor this test plays by hand.
        var workflowEngine = configuration.getComponent(WorkflowEngine.class);
        workflowEngine.setCheckpointingSupport(configuration.getComponent(WorkflowEngineCheckpointingSupport.class));
        instance = workflowContext(WORKFLOW_ID, Version.DEFAULT_VERSION);
        publish(EventMessageUtils.startedWorkflow(instance, MODULE, DEFINITION_ID, customizer));
    }

    @AfterEach
    void stopEngine() {
        configuration.getComponent(WorkflowEngine.class).shutdown();
    }

    @Nested
    class RestoredStartedWait {

        @Test
        void completesWithAnAwaitedEventStoredAfterItsStart() {
            // given
            publish(waitStarted(AWAIT_PAYMENT));
            publish(payment(Instant.now()));

            // when
            restoreSegment();

            // then
            await().atMost(Duration.ofSeconds(5))
                   .untilAsserted(() -> assertThat(completions(AWAIT_PAYMENT)).isEqualTo(1));
        }

        @Test
        void ignoresAnAwaitedEventStoredBeforeItsStart() {
            // given
            publish(payment(Instant.now()));
            publish(waitStarted(AWAIT_PAYMENT));

            // when
            restoreSegment();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> storageEngine.typeReads() == 1);
            await().during(QUIET_PERIOD)
                   .atMost(QUIET_PERIOD.multipliedBy(4))
                   .until(() -> completions(AWAIT_PAYMENT) == 0);
        }

        @Test
        void ignoresAnAwaitedEventLaterThanItsTimeoutDeadline() {
            // given
            publish(waitStarted(AWAIT_PAYMENT));
            publish(payment(Instant.now().plus(WAIT_TIMEOUT.multipliedBy(2))));

            // when
            restoreSegment();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> storageEngine.typeReads() == 1);
            await().during(QUIET_PERIOD)
                   .atMost(QUIET_PERIOD.multipliedBy(4))
                   .until(() -> completions(AWAIT_PAYMENT) == 0);
        }

        @Test
        void completesOnceWhenTheSameEventIsDeliveredAfterTheCatchUp() {
            // given
            publish(waitStarted(AWAIT_PAYMENT));
            var paymentEvent = payment(Instant.now());
            publish(paymentEvent);
            restoreSegment();
            await().atMost(Duration.ofSeconds(5))
                   .untilAsserted(() -> assertThat(completions(AWAIT_PAYMENT)).isEqualTo(1));

            // when
            configuration.getComponent(WorkflowEngine.class).handle(paymentEvent, deliveryContext());

            // then
            await().during(QUIET_PERIOD)
                   .atMost(QUIET_PERIOD.multipliedBy(4))
                   .until(() -> completions(AWAIT_PAYMENT) == 1);
        }
    }

    @Nested
    class LiveStartedWait {

        @Test
        void doesNotReadTheStoreForAwaitedEvents() {
            // given the history holds no wait step, so the restored body starts the wait itself
            restoreSegment();
            await().atMost(Duration.ofSeconds(5)).until(() -> steps(AWAIT_PAYMENT, StepStatus.STARTED).size() == 1);

            // when the processor delivers the start back, the body parks on the registered wait
            var workflowEngine = configuration.getComponent(WorkflowEngine.class);
            workflowEngine.handle(steps(AWAIT_PAYMENT, StepStatus.STARTED).getFirst(), deliveryContext());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> workflowEngine
                    .workflowExecutions().stream()
                    .anyMatch(execution -> execution.state().containsStep(AWAIT_PAYMENT)));
            await().during(QUIET_PERIOD)
                   .atMost(QUIET_PERIOD.multipliedBy(4))
                   .until(() -> storageEngine.typeReads() == 0);
        }
    }

    private EventMessage waitStarted(String stepName) {
        return EventMessageUtils.startedWaitForEventStep(instance, stepName, Map.of(), customizer);
    }

    private static EventMessage payment(Instant timestamp) {
        return new GenericEventMessage(UUID.randomUUID().toString(),
                                       new MessageType(PAYMENT),
                                       Map.of("orderId", WORKFLOW_ID),
                                       Map.of(),
                                       timestamp);
    }

    private int completions(String stepName) {
        return steps(stepName, StepStatus.COMPLETED).size();
    }

    private List<EventMessage> steps(String stepName, StepStatus status) {
        var eventStore = configuration.getComponent(EventStore.class);
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("read-" + WORKFLOW_ID)
                            .executeWithResult(ctx -> eventStore
                                    .transaction(ctx)
                                    .source(SourcingCondition.conditionFor(
                                            EventSourcedWorkflowState.criteriaBuilder(WORKFLOW_ID)))
                                    .filter(entry -> isStep(entry.message(), stepName, status))
                                    .reduce(List.<EventMessage>of(), (found, entry) -> Stream
                                            .concat(found.stream(), Stream.of(entry.message())).toList()))
                            .orTimeout(5, TimeUnit.SECONDS)
                            .join();
    }

    private static boolean isStep(EventMessage event, String stepName, StepStatus status) {
        var metadata = event.metadata();
        return MetadataUtils.getStepStatus(metadata).filter(status::equals).isPresent()
                && stepName.equals(MetadataUtils.getStepName(metadata));
    }

    /**
     * Claims the whole key space and restores every instance it owns, the way the processor's claim callback does.
     */
    private void restoreSegment() {
        var workflowEngine = configuration.getComponent(WorkflowEngine.class);
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("segment-claim")
                     .executeWithResult(claim -> workflowEngine
                             .restoreWorkflowsFor(Segment.ROOT_SEGMENT, null, claim, claim)
                             .thenApply(ignored -> null))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    private static ProcessingContext deliveryContext() {
        var context = new StubProcessingContext();
        context.putResource(Segment.RESOURCE_KEY, Segment.ROOT_SEGMENT);
        return context;
    }

    private AxonConfiguration configurationWith(EventStorageEngine engine) {
        var module = WorkflowModule.defaults(MODULE, TestContext.class)
                                   .workflowContextFactory(c -> TestContext::new)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                               ctx.waitForEvent(waitFor(AWAIT_PAYMENT)).await();
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   );
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> engine)
                .registerModule(module));
        return configurer.build();
    }

    private PrimitiveCommands.WorkflowStepResultWaitForCommand waitFor(String stepName) {
        return new PrimitiveCommands.WorkflowStepResultWaitForCommand(stepName,
                                                                      EventConditions.fromQualifiedName(PAYMENT),
                                                                      GlobalOnlyPayloadReducer.INSTANCE,
                                                                      WAIT_TIMEOUT,
                                                                      customizer);
    }

    /**
     * Counts the sourcing reads that select events by the given type alone, which is what a restored wait catching up
     * on the store does and a live wait must never do.
     */
    private static final class TypeReadRecordingStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final QualifiedName type;
        private final AtomicInteger typeReads = new AtomicInteger();

        private TypeReadRecordingStorageEngine(EventStorageEngine delegate, QualifiedName type) {
            this.delegate = delegate;
            this.type = type;
        }

        private int typeReads() {
            return typeReads.get();
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition,
                                                  ProcessingContext processingContext) {
            if (condition.criteria().flatten().stream()
                         .anyMatch(criterion -> criterion.tags().isEmpty() && criterion.types().contains(type))) {
                typeReads.incrementAndGet();
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
