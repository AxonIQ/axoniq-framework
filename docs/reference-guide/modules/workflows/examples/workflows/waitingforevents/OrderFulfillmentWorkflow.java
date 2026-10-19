/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.waitingforevents;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

public class OrderFulfillmentWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(OrderFulfillmentWorkflow.class);

    // tag::updated-workflow[]
    @Workflow(
            idProperty = "orderId",
            startOnEventClass = OrderPlacedEvent.class
    )
    public void execute(SimpleWorkflowContext context) {
        var reservation = context.awaitExecute(
                "reserveStock",
                Map.of(),
                (pc, input) -> Map.of("reserved", InventoryService.reserveStock())
        );
        if (!Boolean.TRUE.equals(reservation.get("reserved"))) {
            context.fail(new RuntimeException("Stock unavailable"));
            return;
        }

        // First: subscribe to the payment confirmation event (non-blocking)
        var paymentConfirmation = context.waitForEvent(
                "awaitPayment", // <1>
                PaymentConfirmed.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofMinutes(15))
        );

        // Then: initiate payment (non-blocking) - this triggers the external service
        var paymentInitiation = context.execute(
                "initiatePayment",  // <2>
                context.workflowPayload(),
                PaymentService::initiatePayment,
                step -> step.timeout(Duration.ofSeconds(30))
        );

        // Wait for both to complete
        context.allMatch(
                WorkflowStepResult::success,    // <3>
                paymentConfirmation, paymentInitiation
        ).await();

        logger.info("Payment initiated and confirmed for order {}",
                    context.workflowPayload().get("orderId"));
    }
    // end::updated-workflow[]
}
