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
package io.axoniq.workflow.itest;

import io.axoniq.workflow.configuration.WorkflowConfigurationDefaults;
import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.SafePointStore;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowEventTagResolver;
import io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.axoniq.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.workflow.runtime.association.Associations.associate;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Replay simulation test.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class WorkflowReplayPreparedStateTest {

    private static final JacksonConverter JACKSON_CONVERTER = new JacksonConverter();

    @Test
    void restoresOnlyEarliestStillRunningWorkflow() {
        var prepared = new PreparedState();
        prepared.appendWarmupEvents();
        prepared.appendStartReplayWorkflowEvent("first", "wait");
        prepared.appendWorkflowStarted("first", payload("first", "wait"));
        prepared.appendStepStarted("first", "waitForResume");
        prepared.appendStartReplayWorkflowEvent("second", "complete");
        prepared.appendWorkflowStarted("second", payload("second", "complete"));
        prepared.appendWorkflowCompleted("second");
        prepared.seedSafePoint(prepared.tokenAt(1));

        var executedWorkflowIds = new CopyOnWriteArrayList<String>();
        try (var app = prepared.startApp(executedWorkflowIds)) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).containsExactly("first");
                assertThat(executedWorkflowIds).containsExactly("first");
            });

            app.publish(new ResumeReplayWorkflowEvent("first"));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).isEmpty();
                assertThat(executedWorkflowIds).containsExactly("first");
            });
        }
    }

    @Test
    void restoresNextRunningWorkflowAfterEarlierWorkflowAlreadyCompleted() {
        var prepared = new PreparedState();
        prepared.appendWarmupEvents();
        prepared.appendStartReplayWorkflowEvent("first", "complete");
        prepared.appendWorkflowStarted("first", payload("first", "complete"));
        prepared.appendWorkflowCompleted("first");
        prepared.appendStartReplayWorkflowEvent("second", "wait");
        prepared.appendWorkflowStarted("second", payload("second", "wait"));
        prepared.appendStepStarted("second", "waitForResume");
        prepared.seedSafePoint(prepared.tokenAt(4));

        var executedWorkflowIds = new CopyOnWriteArrayList<String>();
        try (var app = prepared.startApp(executedWorkflowIds)) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).containsExactly("second");
                assertThat(executedWorkflowIds).containsExactly("second");
            });

            app.publish(new ResumeReplayWorkflowEvent("second"));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).isEmpty();
                assertThat(executedWorkflowIds).containsExactly("second");
            });
        }
    }

    @Test
    void doesNotRestoreAnyWorkflowWhenSafePointIsLatestTrackingToken() {
        var prepared = new PreparedState();
        prepared.appendWarmupEvents();
        prepared.appendStartReplayWorkflowEvent("only", "complete");
        prepared.appendWorkflowStarted("only", payload("only", "complete"));
        prepared.appendWorkflowCompleted("only");
        prepared.seedSafePoint(prepared.latestToken());

        var executedWorkflowIds = new CopyOnWriteArrayList<String>();
        try (var app = prepared.startApp(executedWorkflowIds)) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).isEmpty();
                assertThat(executedWorkflowIds).isEmpty();
            });
        }
    }

    @Test
    void replayToleratesStepEventsOfCompletedWorkflowBehindSafePoint() {
        var prepared = new PreparedState();
        prepared.appendWarmupEvents();
        prepared.appendStartReplayWorkflowEvent("first", "wait");
        prepared.appendWorkflowStarted("first", payload("first", "wait"));   // idx 3: before safe point
        prepared.appendStartReplayWorkflowEvent("second", "wait");           // idx 4: safe point
        prepared.appendWorkflowStarted("second", payload("second", "wait"));
        prepared.appendStepStarted("second", "waitForResume");
        prepared.appendStepStarted("first", "waitForResume");                // idx 7: step event, no definitionId
        prepared.appendWorkflowCompleted("first");                           // idx 8: first is terminal
        prepared.seedSafePoint(prepared.tokenAt(4)); // first's Started is NOT replayed, its step event IS

        var executedWorkflowIds = new CopyOnWriteArrayList<String>();
        try (var app = prepared.startApp(executedWorkflowIds)) {
            // Getting past token 7 proves the projector tolerated the metadata-less step event.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).containsExactly("second");
                assertThat(executedWorkflowIds).containsExactly("second");
            });
        }
    }


    private static Map<String, Object> payload(String id, String mode) {
        var payload = new LinkedHashMap<String, Object>();
        payload.put("id", id);
        payload.put("mode", mode);
        return payload;
    }

    private static final class PreparedState {

        private final EventStorageEngine eventStorageEngine = new InMemoryEventStorageEngine();
        private final TokenStore processingTokenStore = new InMemoryTokenStore();
        private final InMemoryWorkflowHistoryRepository historyRepository = new InMemoryWorkflowHistoryRepository();
        private final RecordingSafePointStore safePointStore = new RecordingSafePointStore();
        private final List<TrackingToken> appendedTokens = new ArrayList<>();

        private void appendWarmupEvents() {
            appendTypedPayloadEvent(IgnoredReplayWarmupEvent.class, Map.of("id", "warmup-1"));
            appendTypedPayloadEvent(IgnoredReplayWarmupEvent.class, Map.of("id", "warmup-2"));
        }

        private void appendStartReplayWorkflowEvent(String id, String mode) {
            appendTypedPayloadEvent(StartReplayWorkflowEvent.class, payload(id, mode));
        }

        private void appendTypedPayloadEvent(Class<?> payloadType, Map<String, Object> payload) {
            appendEvent(new GenericEventMessage(new MessageType(payloadType), payload)
                                .withConverter(JACKSON_CONVERTER));
        }

        private void appendWorkflowStarted(String workflowId, Map<String, Object> payload) {
            appendEvent(new GenericEventMessage(
                    new MessageType("ReplayAwareWorkflowStarted"),
                    payload,
                    MetadataUtils.create(workflowId,
                                         WorkflowStatus.STARTED,
                                         new MessageType(new QualifiedName("ReplayAwareWorkflow"),
                                                         MessageType.DEFAULT_VERSION))
                                 .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD,
                                      CombineGlobalAndLocalPayloadReducer.NAME)
            ).withConverter(JACKSON_CONVERTER));
        }

        private void appendWorkflowCompleted(String workflowId) {
            appendEvent(new GenericEventMessage(
                    new MessageType("ReplayAwareWorkflowCompleted"),
                    Map.of(),
                    MetadataUtils.create(workflowId,
                                         WorkflowStatus.COMPLETED,
                                         new MessageType(new QualifiedName("ReplayAwareWorkflow"),
                                                                  MessageType.DEFAULT_VERSION))
            ).withConverter(JACKSON_CONVERTER));
        }

        private void appendStepStarted(String workflowId, String stepName) {
            appendEvent(new GenericEventMessage(
                    new MessageType(stepName + "Started"),
                    Map.of("stepName", stepName),
                    MetadataUtils.create(workflowId, stepName, StepStatus.STARTED)
            ).withConverter(JACKSON_CONVERTER));
        }

        private void appendEvent(EventMessage eventMessage) {
            eventStorageEngine.appendEvents(
                                      AppendCondition.none(),
                                      null,
                                      List.of(new GenericTaggedEventMessage<>(eventMessage, tagsFor(eventMessage)))
                              )
                              .join()
                              .commit()
                              .join();
            appendedTokens.add(eventStorageEngine.latestToken().join());
        }

        private static Set<Tag> tagsFor(EventMessage eventMessage) {
            var tags = new LinkedHashSet<>(new WorkflowEventTagResolver().resolve(eventMessage));
            tags.add(Tag.of("type", eventMessage.type().qualifiedName().toString()));
            return Set.copyOf(tags);
        }

        private void seedSafePoint(TrackingToken token) {
            safePointStore.storeSafePointToken(token);
        }

        private TrackingToken tokenAt(int index) {
            return appendedTokens.get(index);
        }

        private TrackingToken latestToken() {
            return appendedTokens.get(appendedTokens.size() - 1);
        }

        private WorkflowTestApp startApp(List<String> executedWorkflowIds) {
            var configurer = WorkflowConfigurer.create();
            var workflow = new ReplayAwareWorkflow(executedWorkflowIds);

            configurer.componentRegistry(cr -> cr
                    .registerComponent(EventStorageEngine.class, cfg -> eventStorageEngine)
                    .registerComponent(MutableWorkflowHistoryRepository.class, cfg -> historyRepository)
                    .registerComponent(TokenStore.class,
                                       WorkflowEventProcessingRegistrationEnhancer.tokenStoreComponentName(
                                               WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME
                                       ),
                                       cfg -> processingTokenStore)

                    .registerComponent(SafePointStore.class,
                                       WorkflowConfigurationDefaults.COMPONENT_SAFE_POINT_STORE,
                                       cfg -> safePointStore)
                    .registerModule(
                            WorkflowModule.defaults("replay-prepared-state", SimpleWorkflowContext.class)
                                          .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                                          .definition(d -> d
                                                  .autodetected(c -> workflow)
                                          )
                    ));

            return new WorkflowTestApp(configurer.start());
        }
    }

    private static final class WorkflowTestApp implements AutoCloseable {

        private final AxonConfiguration configuration;
        private final WorkflowEngine workflowEngine;
        private final EventSink eventSink;
        private final MessageTypeResolver messageTypeResolver;

        private WorkflowTestApp(AxonConfiguration configuration) {
            this.configuration = configuration;
            this.workflowEngine = configuration.getComponent(WorkflowEngine.class);
            this.eventSink = configuration.getComponent(EventSink.class);
            this.messageTypeResolver = configuration.getComponent(MessageTypeResolver.class);
        }

        private void publish(Object event) {
            var eventMessage = new GenericEventMessage(
                    messageTypeResolver.resolveOrThrow(event),
                    event
            ).withConverter(JACKSON_CONVERTER);
            eventSink.publish(null, eventMessage);
        }

        private List<String> workflowIds() {
            return workflowEngine.workflowExecutions().stream().map(WorkflowExecution::workflowId).sorted().toList();
        }

        @Override
        public void close() {
            workflowEngine.shutdown();
            configuration.shutdown();
        }
    }

    private static final class RecordingSafePointStore implements SafePointStore {

        @Nullable
        private volatile TrackingToken currentToken;

        @Override
        public CompletableFuture<TrackingToken> fetchSafePointToken() {
            return CompletableFuture.completedFuture(currentToken);
        }

        @Override
        public CompletableFuture<Void> storeSafePointToken(@Nonnull TrackingToken token) {
            currentToken = token;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void describeTo(@Nonnull ComponentDescriptor descriptor) {
            descriptor.describeProperty("safePointTokenPresent", currentToken != null);
        }
    }

    public static final class ReplayAwareWorkflow {

        private final List<String> executedWorkflowIds;

        public ReplayAwareWorkflow(List<String> executedWorkflowIds) {
            this.executedWorkflowIds = executedWorkflowIds;
        }

        @Workflow(
                workflowName = "ReplayAwareWorkflow",
                idProperty = "id",
                startOnEventClass = StartReplayWorkflowEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            executedWorkflowIds.add(ctx.workflowId());

            if ("wait".equals(ctx.workflowPayload().get("mode"))) {
                ctx.awaitEvent(
                        "waitForResume",
                        ResumeReplayWorkflowEvent.class,
                        associate(payloadProperty("id"), equalsTo(ctx.workflowPayload().get("id"))),
                        step -> step.timeout(Duration.ofSeconds(60))
                );
                ctx.awaitModifyPayload("markResumed", payload -> {
                    Map<String, Object> updated = new LinkedHashMap<>(payload);
                    updated.put("resumed", true);
                    return updated;
                });
                return;
            }

            ctx.awaitModifyPayload("completeImmediately", payload -> {
                Map<String, Object> updated = new LinkedHashMap<>(payload);
                updated.put("completed", true);
                return updated;
            });
        }
    }

    record IgnoredReplayWarmupEvent(String id) {

    }

    record StartReplayWorkflowEvent(String id, String mode) {

    }

    record ResumeReplayWorkflowEvent(String id) {

    }
}
