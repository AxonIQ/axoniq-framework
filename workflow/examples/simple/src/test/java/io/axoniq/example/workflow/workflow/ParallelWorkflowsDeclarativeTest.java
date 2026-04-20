/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.OrderPlacedEvent;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Mirrors the "Parallel workflows" example from the reference docs:
 * a single {@link OrderPlacedEvent} starts two independent workflows, one keyed
 * by {@code orderId} and the other by {@code customerId}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class ParallelWorkflowsDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public ParallelWorkflowsDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        return d -> d
                .declarative(c -> ctx -> ctx.awaitExecute(
                        "reserveStock",
                        Map.of(),
                        p -> Map.of("reserved", true)
                ))
                .workflowName("OrderFulfillmentWorkflow")
                .on(EventConditions.fromType(OrderPlacedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.parallel.fulfillment").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "orderId", id -> "order-" + id))
                )
                .declarative(c -> ctx -> ctx.awaitExecute(
                        "sendConfirmationEmail",
                        Map.of(),
                        p -> Map.of("sent", true)
                ))
                .workflowName("CustomerNotificationWorkflow")
                .on(EventConditions.fromType(OrderPlacedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.parallel.notification").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "customerId", id -> "customer-" + id))
                );
    }

    @Test
    void oneEventStartsTwoParallelWorkflowsWithDistinctIds() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(200, new OrderPlacedEvent("123", "456"))
        ));

        delayedPublisher.start();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).hasSize(2);
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        });

        var histories = workflowHistoryRepository.findAll();
        assertThat(histories)
                .extracting(WorkflowHistory::workflowId)
                .containsExactlyInAnyOrder("order-123", "customer-456");
    }
}
