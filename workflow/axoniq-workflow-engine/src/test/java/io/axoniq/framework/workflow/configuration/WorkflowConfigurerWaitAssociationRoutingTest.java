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
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Tests, on a real configuration with an in-memory event store, that an event reaches the workflows waiting for it
 * through the wait associations, also after a restart, and that segments receiving no events still store their
 * progress.
 *
 * @author Stefan Dragisic
 */
class WorkflowConfigurerWaitAssociationRoutingTest {

    private static final String MODULE = "wait-association-routing";
    private static final String PROCESSOR = MODULE;
    private static final int SEGMENT_COUNT = 4;
    private static final String WAIT_STEP = "awaitPaid";

    private final InMemoryTokenStore tokenStore = new InMemoryTokenStore();
    private final InMemoryEventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final List<AxonConfiguration> configurations = new ArrayList<>();

    @AfterEach
    void tearDown() {
        configurations.forEach(AxonConfiguration::shutdown);
    }

    /**
     * Waits for a {@code paid} event of the order the workflow was started for.
     */
    private static void awaitPaymentOfOwnOrder(TestContext ctx) {
        var orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
        ctx.awaitEvent(new WaitForStepDefinition(
                new PrimitiveMetadata(WAIT_STEP, DefaultEventNameCustomizer.Builder.defaults()),
                EventConditions.fromQualifiedName(new QualifiedName("paid"),
                                                  Associations.associate(payloadProperty("orderId"), "=", orderId)),
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(Duration.ofMinutes(5))
        ));
    }

    @Test
    void aPaidEventWakesOnlyTheWorkflowOfItsOrder() {
        // given
        var configuration = start(Duration.ofSeconds(5));
        var firstOrder = startWaitingWorkflow(configuration, "order-1");
        var secondOrder = startWaitingWorkflow(configuration, "order-2");

        // when
        publish(configuration, "paid", "order-1");

        // then
        await().atMost(5, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, firstOrder).workflowStatus() == WorkflowStatus.COMPLETED);
        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS)
               .until(() -> durableState(configuration, secondOrder).workflowStatus() == WorkflowStatus.STARTED);
    }

    @Test
    void aPaidEventWakesEveryWorkflowWaitingForIt() {
        // given: two workflows of the same order, most likely on different segments
        var configuration = start(Duration.ofSeconds(5));
        var first = startWaitingWorkflow(configuration, "order-1");
        var second = startWaitingWorkflow(configuration, "order-1");

        // when
        publish(configuration, "paid", "order-1");

        // then
        await().atMost(5, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, first).workflowStatus() == WorkflowStatus.COMPLETED
                       && durableState(configuration, second).workflowStatus() == WorkflowStatus.COMPLETED);
    }

    @Test
    void aWorkflowRestoredByANewClaimIsWokenThroughItsDurableWait() {
        // given
        var configuration = start(Duration.ofSeconds(5));
        var workflowId = startWaitingWorkflow(configuration, "order-1");
        var processor = engineProcessor(configuration);

        // when: every segment is released and claimed again, which restores the workflow and loads its open wait
        // from the event store, while the awaited event arrives
        IntStream.range(0, SEGMENT_COUNT)
                 .mapToObj(segment -> processor.releaseSegment(segment, 500, TimeUnit.MILLISECONDS))
                 .forEach(release -> release.orTimeout(5, TimeUnit.SECONDS).join());
        publish(configuration, "paid", "order-1");

        // then
        await().atMost(10, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, workflowId).workflowStatus() == WorkflowStatus.COMPLETED);
    }

    @Test
    void theStoreLoadsTheOpenWaits() {
        // given
        var configuration = start(Duration.ofSeconds(5));
        var workflowId = startWaitingWorkflow(configuration, "order-1");

        // when
        var waits = configuration.getComponent(UnitOfWorkFactory.class)
                                 .create("load-waits")
                                 .executeWithResult(ctx -> configuration.getComponent(WorkflowStore.class)
                                                                        .loadWaitAssociations(ctx)
                                                                        .thenApply(loaded -> loaded.waitAssociations()))
                                 .orTimeout(5, TimeUnit.SECONDS)
                                 .join();

        // then
        assertThat(waits).extracting(WaitAssociation::workflowId, WaitAssociation::stepName)
                         .containsExactly(tuple(workflowId, WAIT_STEP));
        assertThat(waits.iterator().next().associations()).containsExactly("payload:orderId=order-1");
    }

    @Test
    void segmentsThatHandleNoEventStillStoreTheirProgress() {
        // given: workflows that finish right away, all owned by segment 0. A start event with a single candidate and
        // a workflow's own events reach only the owning segment, so segments 1 to 3 never handle an event and never
        // request a checkpoint. A short claim extension threshold paces their idle progress.
        var configuration = startCompletingWorkflows(Duration.ofMillis(200));
        var ownedBySegmentZero = IntStream.range(0, 10_000)
                                          .mapToObj(i -> "order-" + i)
                                          .filter(id -> new Segment(0, SEGMENT_COUNT - 1).matches(id))
                                          .iterator();
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(
                () -> IntStream.range(0, SEGMENT_COUNT)
                               .forEach(segment -> assertThat(storedToken(segment)).isNotNull())
        );
        // the segments finish their claims, so routing is targeted, not broadcast
        publish(configuration, "start", ownedBySegmentZero.next());
        await().pollDelay(Duration.ofMillis(500)).until(() -> true);

        // when
        IntStream.range(0, 10).forEach(i -> publish(configuration, "start", ownedBySegmentZero.next()));
        var latest = storageEngine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();

        // then: the other segments store a position covering those events anyway, through the idle check. The stream
        // keeps moving meanwhile, as it does in production: a segment stores idle progress on the first skipped event
        // after its claim extension threshold.
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            publish(configuration, "start", ownedBySegmentZero.next());
            for (int segment = 1; segment < SEGMENT_COUNT; segment++) {
                var stored = storedToken(segment);
                assertThat(stored).as("stored token of segment %d", segment).isNotNull();
                assertThat(stored.covers(latest)).as("stored token of segment %d covers the events", segment)
                                                 .isTrue();
            }
        });
    }

    /**
     * Starts a module whose workflows finish right away, with the workflow id taken from the event's {@code orderId}.
     */
    private AxonConfiguration startCompletingWorkflows(Duration claimExtensionThreshold) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .customized((c, customization) -> customization.workflowIdProvider(
                                                   event -> String.valueOf(event.payloadAs(Map.class).get("orderId"))
                                           ))
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(SEGMENT_COUNT)
                                                                   .claimExtensionThreshold(
                                                                           claimExtensionThreshold.toMillis()
                                                                   ))
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

    private AxonConfiguration start(Duration claimExtensionThreshold) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> WorkflowConfigurerWaitAssociationRoutingTest
                                                   ::awaitPaymentOfOwnOrder)
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(SEGMENT_COUNT)
                                                                   .claimExtensionThreshold(
                                                                           claimExtensionThreshold.toMillis()
                                                                   ))
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
     * Starts a workflow for the given order and returns its id once its wait is durably started.
     */
    private String startWaitingWorkflow(AxonConfiguration configuration, String orderId) {
        // The default workflow id is the identifier of the start event.
        var workflowId = publish(configuration, "start", orderId);
        await().atMost(5, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> durableState(configuration, workflowId).containsStep(WAIT_STEP)
                       && durableState(configuration, workflowId).getStep(WAIT_STEP).status() == StepStatus.STARTED);
        return workflowId;
    }

    private static String publish(AxonConfiguration configuration, String eventName, String orderId) {
        var event = new GenericEventMessage(new MessageType(eventName), Map.of("orderId", orderId));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + eventName)
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }

    /**
     * Finds the workflow engine's processor, registered in a nested module of the workflow module.
     */
    private static PooledStreamingEventProcessor engineProcessor(Configuration configuration) {
        return findProcessor(configuration).orElseThrow();
    }

    private static Optional<PooledStreamingEventProcessor> findProcessor(Configuration configuration) {
        return configuration.getComponents(EventProcessor.class)
                            .values()
                            .stream()
                            .filter(processor -> processor instanceof PooledStreamingEventProcessor)
                            .map(PooledStreamingEventProcessor.class::cast)
                            .filter(processor -> PROCESSOR.equals(processor.name()))
                            .findFirst()
                            .or(() -> configuration.getModuleConfigurations()
                                                   .stream()
                                                   .map(WorkflowConfigurerWaitAssociationRoutingTest::findProcessor)
                                                   .flatMap(Optional::stream)
                                                   .findFirst());
    }

    @Nullable
    private TrackingToken storedToken(int segment) {
        return tokenStore.fetchToken(PROCESSOR, segment, null).orTimeout(5, TimeUnit.SECONDS).join();
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
}
