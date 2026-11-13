/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package gettingstarted.order.withpayload;

import gettingstarted.order.InventoryService;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.Payload.payload;

// tag::workflow[]
public class OrderFulfillmentWorkflow {

    Logger logger = LoggerFactory.getLogger(OrderFulfillmentWorkflow.class);

    @Workflow(idProperty = "orderId", startOnEventName = "OrderPlaced")
    public void execute(SimpleWorkflowContext workflowContext) {

        var customerId = workflowContext.workflowPayload().get("customerId");
        var amount = workflowContext.workflowPayload().get("amount");

        var reservation = workflowContext.awaitExecute("reserveStock",
                payload("customerId", customerId, "amount", amount).getValues(),
                (pc, input) -> Map.of("reserved", InventoryService.reserveStock(input))); // <1>
        if (!Boolean.TRUE.equals(reservation.get("reserved"))) {
            workflowContext.fail(new RuntimeException("Stock unavailable")); // <2>
            return;
        }

        logger.info("Stock reserved for order {}", workflowContext.workflowPayload().get("orderId"));
    }
}
// end::workflow[]
