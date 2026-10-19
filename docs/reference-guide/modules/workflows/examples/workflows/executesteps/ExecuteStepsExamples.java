/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.executesteps;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.Payload.payload;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.baseName;

public class ExecuteStepsExamples {

    public void awaitExecuteTyped(SimpleWorkflowContext context) {
        // tag::awaitexecute-typed[]
        var reserved = context.awaitExecute(
                "reserveStock",
                Boolean.class,
                InventoryService::reserveStock
        );
        // end::awaitexecute-typed[]
    }

    public void awaitExecuteCanonical(SimpleWorkflowContext context) {
        // tag::awaitexecute-canonical[]
        var reservation = context.awaitExecute(
                "reserveStock",
                Map.of(),
                (pc, input) -> Map.of("reserved", InventoryService.reserveStock())
        );
        // end::awaitexecute-canonical[]
    }

    public void awaitExecuteWithPayload(SimpleWorkflowContext context) {
        // tag::awaitexecute-with-payload[]
        var customerId = context.workflowPayload().get("customerId");
        var amount = context.workflowPayload().get("amount");

        var reservation = context.awaitExecute(
                "reserveStock",
                payload("customerId", customerId, "amount", amount).getValues(),
                (pc, input) -> Map.of("reserved",
                                      InventoryService.reserveStock(input)) // <1>
        );
        if (!Boolean.TRUE.equals(reservation.get("reserved"))) {
            context.fail(new RuntimeException("Stock unavailable")); // <2>
            return;
        }
        // end::awaitexecute-with-payload[]
    }

    public void awaitExecuteTimeoutAndCustomizer(SimpleWorkflowContext context) {
        var customerId = context.workflowPayload().get("customerId");
        var amount = context.workflowPayload().get("amount");

        // tag::awaitexecute-timeout-customizer[]
        context.awaitExecute(
                "initiatePayment",
                payload("customerId", customerId, "amount", amount).getValues(),
                PaymentService::initiatePayment,
                step -> step.timeout(Duration.ofSeconds(30))
                            .eventNameCustomizer(baseName("InitiatingPaymentForCustomer")) // <1>
        );
        // end::awaitexecute-timeout-customizer[]
    }

    public void executeNonBlocking(SimpleWorkflowContext context) {
        // tag::execute-non-blocking[]
        var shipping = context.execute(
                "shipOrder",
                context.workflowPayload(),
                ShippingService::shipOrder,
                step -> step.timeout(Duration.ofMinutes(5))
        );
        // end::execute-non-blocking[]
    }
}
