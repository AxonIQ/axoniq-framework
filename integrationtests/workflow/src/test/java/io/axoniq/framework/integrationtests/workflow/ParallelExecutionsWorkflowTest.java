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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mirrors the "Parallel workflows" example from the reference docs: a single {@link OrderPlacedEvent} starts two
 * independent workflows, one keyed by {@code orderId} and the other by {@code customerId}.
 *
 * @author Stefan Dragisic
 */
class ParallelExecutionsWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public ParallelExecutionsWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new OrderFulfillmentWorkflow());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        return c -> super.configure().apply(c).componentRegistry(r -> r.registerModule(
                                                WorkflowModule
                                                        .defaults("another", BaseWorkflowContext.class)
                                                        .workflowContextFactory(conf -> new BaseWorkflowContextFactory())
                                                        .definition(
                                                                d -> d.autodetected(c0 -> new CustomerNotificationWorkflow())
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

        testDriver.historyMatches(h -> h.workflowId().equals("order-123")
                && h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        testDriver.historyMatches(h -> h.workflowId().equals("customer-456")
                && h.state().workflowStatus() == WorkflowStatus.COMPLETED);

        var histories = workflowHistoryRepository.findAll().join();
        assertThat(histories)
                .extracting(WorkflowHistory::workflowId)
                .containsExactlyInAnyOrder("order-123", "customer-456");
    }

    public static class OrderFulfillmentWorkflow {

        @Workflow(
                workflowName = "OrderFulfillmentWorkflow",
                workflowNamespace = "io.axoniq.dsl.parallel.fulfillment",
                idPropertyProvider = OrderWorkflowIdProvider.class,
                startOnEventClass = OrderPlacedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            ctx.awaitExecute("reserveStock", Map.of(), (pc, p) -> Map.of("reserved", true));
        }
    }

    public static class CustomerNotificationWorkflow {

        @Workflow(
                workflowName = "CustomerNotificationWorkflow",
                workflowNamespace = "io.axoniq.dsl.parallel.notification",
                idPropertyProvider = CustomerWorkflowIdProvider.class,
                startOnEventClass = OrderPlacedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            ctx.awaitExecute("sendConfirmationEmail", Map.of(), (pc, p) -> Map.of("sent", true));
        }
    }

    public static class OrderWorkflowIdProvider extends PayloadPropertyWorkflowIdProvider {

        public OrderWorkflowIdProvider() {
            super("orderId", id -> "order-" + id);
        }
    }

    public static class CustomerWorkflowIdProvider extends PayloadPropertyWorkflowIdProvider {

        public CustomerWorkflowIdProvider() {
            super("customerId", id -> "customer-" + id);
        }
    }

    @Event(namespace = "my.custom", name = "OrderPlaced")
    public record OrderPlacedEvent(String orderId, String customerId) {

    }
}
