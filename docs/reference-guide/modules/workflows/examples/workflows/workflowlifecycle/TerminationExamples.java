/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.workflowlifecycle;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;

import java.time.Duration;
import java.util.Map;

public class TerminationExamples {

    public void failInternal(SimpleWorkflowContext context) {
        // tag::fail-internal[]
        // Start two steps in parallel (non-blocking)
        var shipping = context.execute(
                "shipOrder",
                context.workflowPayload(),
                ShippingService::shipOrder,
                step -> step.timeout(Duration.ofMinutes(5))
        );

        var notification = context.execute(
                "notifyCustomer",
                context.workflowPayload(),
                NotificationService::notifyCustomer,
                step -> step.timeout(Duration.ofMinutes(1))
        );

        // Meanwhile, check stock - if unavailable, fail the workflow
        var reservation = context.awaitExecute(
                "reserveStock",
                Map.of(),
                (pc, input) -> Map.of("reserved", InventoryService.reserveStock())
        );
        if (!Boolean.TRUE.equals(reservation.get("reserved"))) {
            context.fail(new RuntimeException("Stock unavailable")); // <1>
        }
        // end::fail-internal[]
    }

    public void cancelParallelSteps(SimpleWorkflowContext context) {
        // tag::cancel-parallel-steps[]
        // Inside the workflow - two long-running steps in parallel
        var shipping = context.execute(
                "shipOrder",
                context.workflowPayload(),
                ShippingService::shipOrder,
                step -> step.timeout(Duration.ofMinutes(5))
        );

        var notification = context.execute(
                "notifyCustomer",
                context.workflowPayload(),
                NotificationService::notifyCustomer,
                step -> step.timeout(Duration.ofMinutes(1))
        );
        // end::cancel-parallel-steps[]
    }

    public void cancelCondition(SimpleWorkflowContext context) {
        // tag::cancel-condition[]
        // A business condition reached by the workflow cancels it and interrupts
        // shipOrder and notifyCustomer.
        if (orderCancelled()) {
            context.cancel("Order cancelled by customer");
        }
        // end::cancel-condition[]
    }

    public void externalCancellation(WorkflowManager workflowManager, String orderId) {
        // tag::external-cancellation[]
        workflowManager.findOne(WorkflowStateQuery.byWorkflowId(orderId))
                       .requestWorkflowCancellation(null);
        // end::external-cancellation[]
    }

    // tag::fire-and-forget[]
    public void execute(SimpleWorkflowContext context) {
        // fire-and-forget - notice there is no .await() on the returned result
        context.execute(
                "sendReceipt",
                context.workflowPayload(),
                NotificationService::sendReceipt,
                step -> step.timeout(Duration.ofMinutes(1))
        );
        // workflow body returns while sendReceipt is still running
    }
    // end::fire-and-forget[]

    private boolean orderCancelled() {
        return false;
    }
}
