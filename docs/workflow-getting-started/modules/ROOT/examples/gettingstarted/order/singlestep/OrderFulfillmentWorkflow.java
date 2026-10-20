/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package gettingstarted.order.singlestep;

import gettingstarted.order.InventoryService;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

// tag::workflow[]
public class OrderFulfillmentWorkflow {

    @Workflow(idProperty = "orderId", startOnEventName = "OrderPlaced")
    public void execute(SimpleWorkflowContext workflowContext) {
        workflowContext.awaitExecute("reserveStock", Boolean.class,
                InventoryService::reserveStock); // <1>
    }
}
// end::workflow[]
