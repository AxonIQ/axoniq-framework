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
package io.axoniq.example.workflow.workflow;

import io.axoniq.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.workflow.runtime.association.Associations.associate;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves instance-partitioned segment ownership under MVP multi-node sharding with the workflow event processor
 * initialized with four segments:
 * <ul>
 *     <li>instances whose ids hash to different segments both start (unique start candidates route to their owning
 *     segment) and complete;</li>
 *     <li>their correlated resume events have no start candidate, so they are broadcast to every segment via
 *     {@code SequencingPolicy.BROADCAST} - each instance is woken by the delivery at its owning segment, regardless
 *     of where the event's natural sequencing hash lands (cross-segment wake);</li>
 *     <li>a start event matching several definitions (multiple start candidates, hence broadcast to all four
 *     segments) starts each candidate exactly once - the ownership guard skips the start on every non-owning
 *     segment.</li>
 * </ul>
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
class SegmentShardingDeclarativeTest {

    private static final int SEGMENT_COUNT = 4;
    private static final int SEGMENT_MASK = SEGMENT_COUNT - 1;

    @Test
    void workflowsOwnedByDifferentSegmentsBothProgressAndComplete() {
        var workflowIds = idsOnDifferentSegments();
        assertThat(workflowIds.get(0).hashCode() & SEGMENT_MASK)
                .as("test ids must be owned by different segments")
                .isNotEqualTo(workflowIds.get(1).hashCode() & SEGMENT_MASK);

        try (var app = startApp()) {
            workflowIds.forEach(id -> app.publish(new StartShardedWorkflowEvent(id)));

            await().atMost(Duration.ofSeconds(10))
                   .untilAsserted(
                           () -> assertThat(app.runningWorkflowIds()).containsExactlyInAnyOrderElementsOf(workflowIds)
                   );

            workflowIds.forEach(id -> app.publish(new ResumeShardedWorkflowEvent(id)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.runningWorkflowIds()).isEmpty();
                assertThat(app.historyRepository.findAll())
                        .hasSize(workflowIds.size())
                        .allMatch(history -> history.state().workflowStatus() == WorkflowStatus.COMPLETED);
            });
        }
    }

    @Test
    void broadcastStartEventStartsEachCandidateExactlyOnce() {
        var bodyRuns = new ConcurrentHashMap<String, AtomicInteger>();
        try (var app = startBroadcastStartApp(bodyRuns)) {
            app.publish(new StartShardedWorkflowEvent("multi-1"));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(app.historyRepository.findAll())
                        .hasSize(2)
                        .allMatch(history -> history.state().workflowStatus() == WorkflowStatus.COMPLETED);
                assertThat(app.runningWorkflowIds()).isEmpty();
            });

            // The start event matches two definitions -> two start candidates -> broadcast to all four segments.
            // Only the owning segment starts each candidate: every workflow body ran exactly once.
            assertThat(bodyRuns.keySet()).containsExactlyInAnyOrder("primary-multi-1", "mirror-multi-1");
            assertThat(bodyRuns.values()).allMatch(runs -> runs.get() == 1);
        }
    }

    /**
     * Returns two deterministic workflow ids whose hashes fall into different segments under {@link #SEGMENT_COUNT}
     * balanced segments.
     */
    private static List<String> idsOnDifferentSegments() {
        var first = "sharded-0";
        for (var i = 1; i < 64; i++) {
            var candidate = "sharded-" + i;
            if ((candidate.hashCode() & SEGMENT_MASK) != (first.hashCode() & SEGMENT_MASK)) {
                return List.of(first, candidate);
            }
        }
        throw new IllegalStateException("No workflow id hashing to another segment found");
    }

    private static ShardedWorkflowApp startApp() {
        var configurer = EventSourcingConfigurer.create();
        configurer.componentRegistry(cr -> cr
                .registerEnhancer(new WorkflowEventProcessingRegistrationEnhancer(
                        WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME,
                        null,
                        null,
                        true,
                        SEGMENT_COUNT
                ))
                .registerModule(
                        WorkflowModule.defaults("segment-sharding", SimpleWorkflowContext.class)
                                      .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                                      .definition(d -> d
                                              .declarative(c -> SegmentShardingDeclarativeTest::runShardedWorkflow)
                                              .workflowName("ShardedWorkflow")
                                              .on(EventConditions.fromType(StartShardedWorkflowEvent.class))
                                              .customized((c, w) -> w.workflowIdProvider(
                                                      fromPayloadAttribute(c, "id", Object::toString)
                                              ))
                                      )
                ));
        return new ShardedWorkflowApp(configurer.start());
    }

    private static ShardedWorkflowApp startBroadcastStartApp(ConcurrentHashMap<String, AtomicInteger> bodyRuns) {
        var configurer = EventSourcingConfigurer.create();
        configurer.componentRegistry(cr -> cr
                .registerEnhancer(new WorkflowEventProcessingRegistrationEnhancer(
                        WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME,
                        null,
                        null,
                        true,
                        SEGMENT_COUNT
                ))
                .registerModule(
                        WorkflowModule.defaults("segment-broadcast-start", SimpleWorkflowContext.class)
                                      .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                                      .definition(d -> d
                                              .declarative(c -> ctx -> runCountingWorkflow(ctx, bodyRuns))
                                              .workflowName("PrimaryShardedWorkflow")
                                              .on(EventConditions.fromType(StartShardedWorkflowEvent.class))
                                              .customized((c, w) -> w.workflowIdProvider(
                                                      fromPayloadAttribute(c, "id", id -> "primary-" + id)
                                              ))
                                      )
                                      .definition(d -> d
                                              .declarative(c -> ctx -> runCountingWorkflow(ctx, bodyRuns))
                                              .workflowName("MirrorShardedWorkflow")
                                              .on(EventConditions.fromType(StartShardedWorkflowEvent.class))
                                              .customized((c, w) -> w.workflowIdProvider(
                                                      fromPayloadAttribute(c, "id", id -> "mirror-" + id)
                                              ))
                                      )
                ));
        return new ShardedWorkflowApp(configurer.start());
    }

    private static void runCountingWorkflow(SimpleWorkflowContext ctx,
                                            ConcurrentHashMap<String, AtomicInteger> bodyRuns) {
        bodyRuns.computeIfAbsent(ctx.workflowId(), id -> new AtomicInteger()).incrementAndGet();
        ctx.awaitModifyPayload("markProcessed", payload -> {
            Map<String, Object> updated = new LinkedHashMap<>(payload);
            updated.put("processed", true);
            return updated;
        });
    }

    private static void runShardedWorkflow(SimpleWorkflowContext ctx) {
        ctx.awaitEvent(
                "waitForResume",
                ResumeShardedWorkflowEvent.class,
                associate(payloadProperty("id"), equalsTo(ctx.workflowPayload().get("id"))),
                step -> step.timeout(Duration.ofSeconds(30))
        );
        ctx.awaitModifyPayload("markResumed", payload -> {
            Map<String, Object> updated = new LinkedHashMap<>(payload);
            updated.put("resumed", true);
            return updated;
        });
    }

    private static final class ShardedWorkflowApp implements AutoCloseable {

        private final AxonConfiguration configuration;
        private final WorkflowEngine workflowEngine;
        private final MutableWorkflowHistoryRepository historyRepository;
        private final EventSink eventSink;
        private final MessageTypeResolver messageTypeResolver;
        private final EventConverter eventConverter;

        private ShardedWorkflowApp(AxonConfiguration configuration) {
            this.configuration = configuration;
            this.workflowEngine = configuration.getComponent(WorkflowEngine.class);
            this.historyRepository = configuration.getComponent(MutableWorkflowHistoryRepository.class);
            this.eventSink = configuration.getComponent(EventSink.class);
            this.messageTypeResolver = configuration.getComponent(MessageTypeResolver.class);
            this.eventConverter = configuration.getComponent(EventConverter.class);
        }

        private void publish(Object event) {
            var eventMessage = new GenericEventMessage(
                    messageTypeResolver.resolveOrThrow(event),
                    event
            ).withConverter(eventConverter);
            eventSink.publish(null, eventMessage);
        }

        private List<String> runningWorkflowIds() {
            return workflowEngine.workflowExecutions().stream().map(WorkflowExecution::workflowId).sorted().toList();
        }

        @Override
        public void close() {
            workflowEngine.shutdown();
            configuration.shutdown();
        }
    }

    record StartShardedWorkflowEvent(String id) {

    }

    record ResumeShardedWorkflowEvent(String id) {

    }
}
