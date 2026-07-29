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
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import io.axoniq.workflow.runtime.util.WorkflowEventTagResolver;
import org.axonframework.common.configuration.AxonConfiguration;
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
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
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
import java.util.Optional;
import java.util.Set;
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
public class WorkflowReplayPreparedStateTest {

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
        prepared.seedProcessorToken(prepared.tokenAt(1));

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
        prepared.seedProcessorToken(prepared.tokenAt(4));

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
    void restoredWaitingWorkflowConsumesReplayBacklogEventDuringCatchUp() {
        var prepared = new PreparedState();
        prepared.appendWarmupEvents();
        prepared.appendStartReplayWorkflowEvent("first", "wait");
        prepared.appendWorkflowStarted("first", payload("first", "wait"));
        prepared.appendStepStarted("first", "waitForResume");
        prepared.appendResumeReplayWorkflowEvent("first");
        prepared.seedProcessorToken(prepared.tokenAt(1));

        var executedWorkflowIds = new CopyOnWriteArrayList<String>();
        try (var app = prepared.startApp(executedWorkflowIds)) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).isEmpty();
                assertThat(executedWorkflowIds).containsExactly("first");
                assertThat(app.history("first"))
                        .hasValueSatisfying(history -> {
                            assertThat(history.state().workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
                            assertThat(history.state().payload()).containsEntry("resumed", true);
                        });
            });
        }
    }

    @Test
    void restoredWaitingWorkflowTriggersTimeoutAfterStartup() {
        var prepared = new PreparedState();
        prepared.appendWarmupEvents();
        prepared.appendStartReplayWorkflowEvent("first", "wait-timeout");
        prepared.appendWorkflowStarted("first", payload("first", "wait-timeout"));
        prepared.appendStepStarted("first", "waitForResume");
        prepared.seedProcessorToken(prepared.tokenAt(1));

        var executedWorkflowIds = new CopyOnWriteArrayList<String>();
        try (var app = prepared.startApp(executedWorkflowIds)) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).isEmpty();
                assertThat(executedWorkflowIds).containsExactly("first");
                assertThat(app.history("first"))
                        .hasValueSatisfying(history -> {
                            assertThat(history.state().workflowStatus()).isEqualTo(WorkflowStatus.FAILED);
                            assertThat(history.state().getStep("waitForResume").status()).isEqualTo(
                                    StepStatus.TIMED_OUT
                            );
                        });
            });
        }
    }

    @Test
    void doesNotRestoreAnyWorkflowWhenProcessorTokenIsLatestTrackingToken() {
        var prepared = new PreparedState();
        prepared.appendWarmupEvents();
        prepared.appendStartReplayWorkflowEvent("only", "complete");
        prepared.appendWorkflowStarted("only", payload("only", "complete"));
        prepared.appendWorkflowCompleted("only");
        prepared.seedProcessorToken(prepared.latestToken());

        var executedWorkflowIds = new CopyOnWriteArrayList<String>();
        try (var app = prepared.startApp(executedWorkflowIds)) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.workflowIds()).isEmpty();
                assertThat(executedWorkflowIds).isEmpty();
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
        private final List<TrackingToken> appendedTokens = new ArrayList<>();

        private void appendWarmupEvents() {
            appendTypedPayloadEvent(IgnoredReplayWarmupEvent.class, Map.of("id", "warmup-1"));
            appendTypedPayloadEvent(IgnoredReplayWarmupEvent.class, Map.of("id", "warmup-2"));
        }

        private void appendStartReplayWorkflowEvent(String id, String mode) {
            appendTypedPayloadEvent(StartReplayWorkflowEvent.class, payload(id, mode));
        }

        private void appendResumeReplayWorkflowEvent(String id) {
            appendTypedPayloadEvent(ResumeReplayWorkflowEvent.class, Map.of("id", id));
        }

        private void appendTypedPayloadEvent(Class<?> payloadType, Map<String, Object> payload) {
            appendEvent(new GenericEventMessage(new MessageType(payloadType), payload));
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
            ));
        }

        private void appendWorkflowCompleted(String workflowId) {
            appendEvent(new GenericEventMessage(
                    new MessageType("ReplayAwareWorkflowCompleted"),
                    Map.of(),
                    MetadataUtils.create(workflowId,
                                         WorkflowStatus.COMPLETED,
                                         new MessageType(new QualifiedName("ReplayAwareWorkflow"),
                                                                  MessageType.DEFAULT_VERSION))
            ));
        }

        private void appendStepStarted(String workflowId, String stepName) {
            appendEvent(new GenericEventMessage(
                    new MessageType(stepName + "Started"),
                    Map.of("stepName", stepName),
                    MetadataUtils.create(workflowId, stepName, StepStatus.STARTED)
            ));
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

        private void seedProcessorToken(TrackingToken token) {
            var segments = processingTokenStore.fetchSegments(
                    WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME,
                    null
            ).join();
            if (segments.isEmpty()) {
                processingTokenStore.initializeTokenSegments(
                        WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME,
                        1,
                        token,
                        null
                ).join();
            } else {
                processingTokenStore.storeToken(
                        token,
                        WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME,
                        0,
                        null
                ).join();
            }
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
                                       WorkflowEventProcessingRegistrationEnhancer.tokenStoreName(
                                               WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME
                                       ),
                                       cfg -> processingTokenStore)
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
        private final EventConverter eventConverter;

        private WorkflowTestApp(AxonConfiguration configuration) {
            this.configuration = configuration;
            this.workflowEngine = configuration.getComponent(WorkflowEngine.class);
            this.eventSink = configuration.getComponent(EventSink.class);
            this.messageTypeResolver = configuration.getComponent(MessageTypeResolver.class);
            this.eventConverter = configuration.getComponent(EventConverter.class);
        }

        private void publish(Object event) {
            var eventMessage = new GenericEventMessage(messageTypeResolver.resolveOrThrow(event), event)
                    .withConverter(eventConverter);
            eventSink.publish(null, eventMessage);
        }

        private List<String> workflowIds() {
            return workflowEngine.workflowExecutions().stream().map(WorkflowExecution::workflowId).sorted().toList();
        }

        private Optional<io.axoniq.workflow.history.api.WorkflowHistory> history(String workflowId) {
            return configuration.getComponent(MutableWorkflowHistoryRepository.class).findById(workflowId);
        }

        @Override
        public void close() {
            workflowEngine.shutdown();
            configuration.shutdown();
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

            var mode = String.valueOf(ctx.workflowPayload().get("mode"));
            if ("wait".equals(mode) || "wait-timeout".equals(mode)) {
                var timeout = "wait-timeout".equals(mode) ? Duration.ofMillis(200) : Duration.ofSeconds(60);
                try {
                    ctx.awaitEvent(
                            "waitForResume",
                            ResumeReplayWorkflowEvent.class,
                            associate(payloadProperty("id"), equalsTo(ctx.workflowPayload().get("id"))),
                            step -> step.timeout(timeout)
                    );
                } catch (Throwable t) {
                    if ("wait-timeout".equals(mode)) {
                        ctx.fail(t);
                    }
                    throw t;
                }
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
