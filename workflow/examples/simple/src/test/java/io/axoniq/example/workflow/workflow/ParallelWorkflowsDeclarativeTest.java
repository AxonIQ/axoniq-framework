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

import io.axoniq.example.workflow.fixture.OrderPlacedEvent;
import io.axoniq.workflow.configuration.WorkflowConfigurer;
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
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Mirrors the "Parallel workflows" example from the reference docs: a single {@link OrderPlacedEvent} starts two
 * independent workflows, one keyed by {@code orderId} and the other by {@code customerId}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class ParallelWorkflowsDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public ParallelWorkflowsDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
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
                );
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        return c -> c.componentRegistry(r -> r.registerModule(
                                                WorkflowModule
                                                        .defaults("another", SimpleWorkflowContext.class)
                                                        .workflowContextFactory(conf -> new SimpleWorkflowContextFactory())
                                                        .definition(
                                                                d -> d.declarative(c0 -> ctx -> ctx.awaitExecute(
                                                                              "sendConfirmationEmail",
                                                                              Map.of(),
                                                                              p -> Map.of("sent", true)
                                                                      ))
                                                                      .workflowName("CustomerNotificationWorkflow")
                                                                      .on(EventConditions.fromType(OrderPlacedEvent.class))
                                                                      .customized((c1, w) -> w
                                                                              .eventNameCustomizer(namespace(
                                                                                      "io.axoniq.dsl.parallel.notification").workflowBaseName(
                                                                                      "Workflow"))
                                                                              .workflowIdProvider(fromPayloadAttribute(c1,
                                                                                                                       "customerId",
                                                                                                                       id -> "customer-"
                                                                                                                               + id))
                                                                      )

                                                        )
                                        )
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
